use crate::types::{
    QueryCall, QueryContext, QueryEntry, QuerySession, QuerySqlAttributes,
    QuerySqlStatementObjects, QueryStats,
};
use crate::Error;
use alloc::borrow::Cow;
use core::borrow::BorrowMut;
use core::fmt::{Display, Formatter};
use core::str::FromStr;
use mysql_slowlog_parser::SqlStatementContext;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow, SqliteSynchronous};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use std::ops::AddAssign;
use std::path::Path;

#[derive(Error, Debug)]
pub struct InvalidPrimaryKey {
    value: i64,
}

impl Display for InvalidPrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "value: {}", self.value)
    }
}

pub async fn open_db(p: Option<&Path>) -> Result<SqlitePool, Error> {
    let pool_timeout = 30;
    let concurrency = 50;

    let pool_max_connections = if concurrency == 1 {
        2
    } else {
        concurrency as u32
    };

    let connection_options = SqliteConnectOptions::from_str(&db_url(p))?
        //    .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .serialized(true)
        .foreign_keys(false)
        .busy_timeout(std::time::Duration::from_secs(pool_timeout));

    let db = SqlitePoolOptions::new()
        .max_connections(pool_max_connections)
        .connect_with(connection_options)
        .await?;

    sqlx::migrate!("./migrations").run(&db).await?;

    Ok(db)
}

pub fn db_url(p: Option<&Path>) -> String {
    match p {
        Some(p) => format!("sqlite://{}", p.to_string_lossy()),
        None => format!("sqlite://:memory:"),
    }
}

pub async fn record_entry(c: SqlitePool, e: QueryEntry) -> Result<u32, Error> {
    let mut tx = c.begin().await?;

    let query_call_id = insert_entry(&mut tx, e).await?;

    tx.commit().await?;

    Ok(query_call_id)
}

pub async fn insert_entry(tx: &mut Transaction<'_, Sqlite>, e: QueryEntry) -> Result<u32, Error> {
    let query_id = insert_query(tx, e.sql_attributes()).await?;

    let query_call_id = insert_query_call(
        tx,
        InsertQueryCallParams {
            query_id,
            query_call: e.call(),
        },
    )
    .await?;

    println!("inserted query call");

    let _ = insert_query_session(
        tx,
        InsertQuerySessionParams {
            query_call_id,
            session: e.session(),
        },
    )
    .await?;

    println!("inserted a query call id");

    let _ = insert_query_stats(
        tx,
        InsertQueryStatsParams {
            query_call_id,
            stats: e.stats(),
        },
    )
    .await?;

    if let Some(c) = e.sql_attributes().statement.sql_context() {
        let _ = insert_query_context(
            tx,
            InsertQueryContextParams {
                query_call_id,
                context: QueryContext(SqlStatementContext {
                    request_id: c.request_id.clone(),
                    caller: c.caller.clone(),
                    function: c.function.clone(),
                    line: c.line.clone(),
                }),
            },
        )
        .await?;
    }

    println!("got query context");

    Ok(query_call_id)
}

async fn find_query(
    tx: &mut Transaction<'_, Sqlite>,
    params: &QuerySqlAttributes,
) -> Result<u32, Error> {
    let r = sqlx::query("SELECT id FROM db_ojects WHERE schema = ?")
        .bind(params.sql())
        .fetch_one(tx)
        .await?;

    let id = r.try_get(0)?;

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

async fn find_db_object(
    tx: &mut Transaction<'_, Sqlite>,
    schema: Option<Cow<'_, str>>,
    object_name: Cow<'_, str>,
) -> Result<u32, Error> {
    let r = sqlx::query(
        format!(
            "SELECT id from db_objects WHERE table_name = ? AND schema name {}",
            schema.clone().map_or("IS ?", |_| "= ?")
        )
        .as_str(),
    )
    .bind(object_name)
    .bind(schema)
    .fetch_one(tx)
    .await?;

    let id = r.try_get(0)?;

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

async fn insert_query(
    tx: &mut Transaction<'_, Sqlite>,
    params: QuerySqlAttributes,
) -> Result<u32, Error> {
    if let Ok(id) = find_query(tx, &params).await {
        return Ok(id);
    }

    let result = sqlx::query("INSERT INTO queries (sql, sql_type) VALUES (?, ?)")
        .bind(&params.sql())
        .bind(&params.sql_type().and_then(|t| Some(t.to_string())))
        .execute(tx.borrow_mut())
        .await
        .unwrap();

    let id = result.last_insert_rowid();

    let id = u32::try_from(id).or::<Error>(Err(InvalidPrimaryKey { value: id }.into()))?;

    insert_query_objects(tx, id, params).await?;

    Ok(id)
}

async fn insert_query_objects(
    tx: &mut Transaction<'_, Sqlite>,
    query_id: u32,
    params: QuerySqlAttributes,
) -> Result<(), Error> {
    if let Some(l) = params.objects() {
        for o in l {
            let db_object_id = insert_db_objects(tx, QuerySqlStatementObjects(o)).await?;

            let _ = sqlx::query(
                "INSERT INTO query_objects (query_id, db_object_id) VALUES \
            (?, ?)",
            )
            .bind(query_id)
            .bind(db_object_id)
            .execute(tx.borrow_mut())
            .await
            .unwrap();
        }
    }

    Ok(())
}

async fn insert_db_objects(
    tx: &mut Transaction<'_, Sqlite>,
    object: QuerySqlStatementObjects,
) -> Result<u32, Error> {
    if let Ok(id) = find_db_object(tx, object.schema_name(), object.object_name()).await {
        return Ok(id);
    }

    let result = sqlx::query("INSERT INTO db_objects (schema_name, object_name) VALUES (?, ?)")
        .bind(object.schema_name())
        .bind(object.object_name())
        .execute(tx)
        .await
        .unwrap();

    let id = result.last_insert_rowid();

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

struct InsertQueryCallParams {
    query_id: u32,
    query_call: QueryCall,
}

async fn insert_query_call(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryCallParams,
) -> Result<u32, Error> {
    let result = sqlx::query(
        "INSERT INTO query_calls (query_id, start_time, log_time)
                 VALUES (?, ?, ?)
            ",
    )
    .bind(params.query_id)
    .bind(&params.query_call.start_time())
    .bind(&params.query_call.log_time())
    .execute(tx)
    .await?;

    let id = result.last_insert_rowid();

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

struct InsertQuerySessionParams {
    query_call_id: u32,
    session: QuerySession,
}

async fn insert_query_session(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQuerySessionParams,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO query_call_session (query_call_id, user_name, sys_user_name, host_name,
        ip_address, thread_id)
        VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(params.query_call_id)
    .bind(params.session.user_name())
    .bind(params.session.sys_user_name())
    .bind(params.session.host_name())
    .bind(params.session.ip_address())
    .bind(params.session.thread_id())
    .execute(tx)
    .await?;

    Ok(())
}

struct InsertQueryStatsParams {
    query_call_id: u32,
    stats: QueryStats,
}

async fn insert_query_stats(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryStatsParams,
) -> Result<(), Error> {
    let _ = sqlx::query(
        "INSERT INTO query_call_stats (query_call_id, query_time, lock_time, rows_sent, rows_examined)
        VALUES (?, ?, ?, ?, ?)")
        .bind(&params.query_call_id)
        .bind(&params.stats.query_time())
        .bind(&params.stats.lock_time())
        .bind(&params.stats.rows_sent())
        .bind(&params.stats.rows_examined())
        .execute(tx)
        .await?;

    Ok(())
}

struct InsertQueryContextParams {
    query_call_id: u32,
    context: QueryContext,
}

async fn insert_query_context(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryContextParams,
) -> Result<(), Error> {
    let _ = sqlx::query(
        "INSERT INTO query_call_context (query_call_id, request_id, caller, function, line)
        VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&params.query_call_id)
    .bind(&params.context.caller())
    .bind(&params.context.function())
    .bind(&params.context.line())
    .execute(tx)
    .await?;

    Ok(())
}

pub trait ColumnSet: Sized {
    fn sql(s: &Option<SortingPlan>) -> String {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(Self::set_sql(s));

        if let Some(s) = s {
            s.sql_clauses(&mut qb);
        }

        qb.into_sql()
    }

    fn set_sql(s: &Option<SortingPlan>) -> String;

    fn from_row(r: SqliteRow) -> Result<Self, Error>;

    fn columns() -> Vec<String>;

    fn key() -> String;

    fn column_list(prefix: Option<&str>) -> String {
        let (_, out) = Self::columns()
            .iter()
            .fold((0, String::new()), |(mut i, mut acc), c| {
                if i > 0 {
                    acc.push_str(", ");
                }

                if let Some(p) = prefix {
                    acc.push_str(format!("{}.", p).as_str())
                }

                acc.push_str(format!("{}", c).as_str());
                i.add_assign(1);

                (i, acc)
            });

        out
    }

    fn vertical_display_column_values(&self) -> String {
        self.display_values()
            .iter()
            .fold(String::new(), |mut acc, (k, v)| {
                acc.push_str(format!("{}: {}\n", k, v).as_str());
                acc
            })
    }

    fn display_values(&self) -> Vec<(&str, String)>;
}

#[derive(PartialEq)]
pub struct AggregateStats<C> {
    query_time: f64,
    lock_time: f64,
    rows_sent: f64,
    rows_examined: f64,
    calls: u32,
    filter: C,
}

impl<C: ColumnSet> ColumnSet for AggregateStats<C> {
    fn set_sql(_: &Option<SortingPlan>) -> String {
        format!(
            "
    WITH filter AS (
    {f_sql}
    )
    SELECT
    {f_cols},
    COUNT(stats.query_call_id) AS calls,
    CAST(AVG(stats.query_time) AS REAL) AS query_time,
    CAST(AVG(stats.lock_time) AS REAL) AS lock_time,
    AVG(stats.rows_sent) AS rows_sent,
    AVG(stats.rows_examined) AS rows_examined
    FROM filter
    JOIN query_call_stats stats ON stats.{f_key} = filter.{f_key}
    GROUP BY {f_cols}",
            f_cols = C::column_list(Some("filter")),
            f_sql = C::set_sql(&None),
            f_key = C::key(),
        )
    }

    fn from_row(r: SqliteRow) -> Result<Self, Error> {
        Ok(Self {
            query_time: r.try_get("query_time")?,
            lock_time: r.try_get("lock_time")?,
            rows_sent: r.try_get("rows_sent")?,
            rows_examined: r.try_get("rows_examined")?,
            calls: r.try_get("calls")?,
            filter: C::from_row(r)?,
        })
    }
    fn columns() -> Vec<String> {
        vec![
            "calls".into(),
            "query_time".into(),
            "lock_time".into(),
            "rows_sent".into(),
            "rows_examined".into(),
        ]
    }

    fn key() -> String {
        "query_call_id".to_string()
    }

    fn display_values(&self) -> Vec<(&str, String)> {
        let mut acc = vec![];

        acc.append(&mut self.filter.display_values());
        acc.push(("calls", self.calls.to_string()));
        acc.push(("query_time", self.query_time.to_string()));
        acc.push(("lock_time", self.lock_time.to_string()));
        acc.push(("rows_sent", self.rows_sent.to_string()));
        acc.push(("rows_examined", self.rows_examined.to_string()));

        acc
    }
}

pub struct Calls<F> {
    calls: u32,
    filter: F,
}

impl<C: ColumnSet> ColumnSet for Calls<C> {
    fn set_sql(_: &Option<SortingPlan>) -> String {
        format!(
            "
    WITH filter AS (
    {f_sql}
    )
    SELECT
    {f_cols},
    COUNT(stats.query_call_id) AS calls
    FROM filter
    JOIN query_call_stats stats ON stats.{f_key} = filter.{f_key}
    GROUP BY {f_cols}",
            f_cols = C::column_list(Some("filter")),
            f_sql = C::set_sql(&None),
            f_key = C::key(),
        )
    }

    fn from_row(r: SqliteRow) -> Result<Self, Error> {
        Ok(Self {
            calls: r.try_get("calls")?,
            filter: C::from_row(r)?,
        })
    }

    fn columns() -> Vec<String> {
        vec!["calls".into()]
    }

    fn key() -> String {
        "query_call_id".to_string()
    }

    fn display_values(&self) -> Vec<(&str, String)> {
        let mut acc = vec![];

        acc.append(&mut self.filter.display_values());
        acc.push(("calls", self.calls.to_string()));

        acc
    }
}

/// ## query_column_set
///
/// Runs sql for a columnset and fetches rows returning the rows as a RelationalObject
///
/// ```
/// use mysql_slowlog_analyzer::{ColumnSet, OrderBy, Ordering,
/// SortingPlan };
/// use mysql_slowlog_analyzer::Error;
///         use std::collections::BTreeMap;
///
/// use sqlx::sqlite::SqliteRow;
/// use sqlx::Row;
/// use sqlx::FromRow;
/// use mysql_slowlog_analyzer::types::QueryStats;
///
///#[derive(Debug)]
///struct StatsBySql {
///    sql: String,
///}
///
/// impl ColumnSet for StatsBySql {
///     fn set_sql(_: &Option<SortingPlan>) -> String {
///        use mysql_slowlog_analyzer::db::SortingPlan;
/// format!("
///        SELECT q.sql, qc.id AS query_call_id
///        FROM query_calls qc
///        JOIN queries q ON q.id = qc.query_id
///         ")
///     }
///
///     fn key() -> String {
///         format!("query_call_id")
///     }
///
///     fn from_row(r: SqliteRow) -> Result<Self, Error> {
///         Ok(Self {
///            sql: r.try_get("sql")?,
///         })
///     }
///
///     fn columns() -> Vec<String> {
///         vec![
///            "sql".into()
///         ]
///     }
///
///     fn display_values(&self) -> Vec<(&str, String)> {
///         let mut acc = vec![];
///
///          acc.push(("sql", self.sql.to_string()));
///
///          acc
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     use std::fs::{File, metadata, remove_dir_all};
///     use std::io::BufReader;
///     use std::path::PathBuf;
///     use std::str::FromStr;
///     use mysql_slowlog_analyzer::db::{AggregateStats, Limit, open_db, OrderedColumn, query_column_set };
///     use mysql_slowlog_analyzer::{LogData, LogDataConfig};
///
///     let data_dir = PathBuf::from("/tmp/mysql_slowlog_stats_by_sql_doc");
///
///     if metadata(&data_dir).is_ok() {
///         remove_dir_all(&data_dir).unwrap();
///     }
///
///     let p = PathBuf::from("data/slow-test-queries.log");
///
///     let context = LogDataConfig {
///         data_path: Some(data_dir),
///     };
///
///     let mut s = LogData::open(&p, context).await.unwrap();
///     s.record_db().await.unwrap();
///
///     let c = s.db_pool();
///
///     let sorting = SortingPlan {
///         order_by: Some(OrderBy { columns: vec![OrderedColumn::from_str("calls DESC").unwrap()] }),
///         limit: Some(Limit {
///             limit: 5,
///             offset: None
///          })
///     };
///
///     let stats = query_column_set::<AggregateStats<StatsBySql>>(&c, Some(sorting)).await.unwrap();
///
///     panic!("stats:\n{}", stats.display_vertical());
/// }
/// ```

#[derive(Debug)]
pub struct RelationalObject<A> {
    rows: Vec<A>,
}

impl<C: ColumnSet> RelationalObject<C> {
    pub fn display_vertical(&self) -> String {
        let (_, out) = self
            .rows
            .iter()
            .fold((0, String::new()), |(mut i, mut acc), v| {
                i.add_assign(1);
                acc.push_str(
                    format!(
                        "*************************** {}. row ***************************\n",
                        i
                    )
                    .as_str(),
                );

                acc.push_str(v.vertical_display_column_values().as_str());

                (i, acc)
            });

        out
    }
}

impl Display for dyn NullableDisplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.nullable_display())
    }
}

trait NullableDisplay {
    fn nullable_display(&self) -> String;
}

impl<T: NullableDisplay> NullableDisplay for Option<T> {
    fn nullable_display(&self) -> String {
        match self {
            Some(v) => v.nullable_display(),
            None => "NULL".to_string(),
        }
    }
}

impl NullableDisplay for String {
    fn nullable_display(&self) -> String {
        self.to_string()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Ordering {
    Asc,
    Desc,
}

impl Default for Ordering {
    fn default() -> Self {
        Self::Asc
    }
}

impl Display for Ordering {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Asc => "ASC",
                Self::Desc => "DESC",
            }
        )
    }
}

impl FromStr for Ordering {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ASC" => Ok(Self::Asc),
            "DESC" => Ok(Self::Desc),
            _ => Err(Error::InvalidOrderBy(s.to_string())),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderedColumn {
    column: String,
    ordering: Ordering,
}

impl Display for OrderedColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} {}", self.column, self.ordering)
    }
}

impl FromStr for OrderedColumn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Vec<&str> = s.split_whitespace().collect();

        let column = v
            .get(0)
            .ok_or(Error::InvalidOrderBy(s.to_string()))?
            .to_string();
        let ordering = v.get(1);

        Ok(Self {
            column,
            ordering: match ordering {
                Some(o) => Ordering::from_str(o)?,
                None => Ordering::Asc,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderBy {
    pub columns: Vec<OrderedColumn>,
}

impl OrderBy {
    fn sql_clause(&self, b: &mut QueryBuilder<Sqlite>) {
        b.push("\nORDER BY ");

        let mut seperated = b.separated(", ");

        for c in &self.columns {
            seperated.push(c.to_string());
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Limit {
    pub limit: u32,
    pub offset: Option<u32>,
}

impl Limit {
    fn sql_clause(&self, b: &mut QueryBuilder<Sqlite>) {
        b.push("\nLIMIT");

        let mut seperated = b.separated(",");

        if let Some(o) = &self.offset {
            seperated.push(format!(" {}", o));
        }

        seperated.push(&format!(" {}", self.limit));
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SortingPlan {
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

impl SortingPlan {
    fn sql_clauses(&self, b: &mut QueryBuilder<Sqlite>) {
        if let Some(o) = &self.order_by {
            o.sql_clause(b);
        }

        if let Some(l) = &self.limit {
            l.sql_clause(b);
        }
    }
}

pub async fn query_column_set<C: ColumnSet>(
    c: &SqlitePool,
    s: Option<SortingPlan>,
) -> Result<RelationalObject<C>, Error> {
    let sql = C::sql(&s);

    println!("sql: {}", sql);
    let rows = sqlx::query(&sql).fetch_all(c).await?;

    let mut acc = vec![];

    for r in rows.into_iter() {
        let f = C::from_row(r)?;
        acc.push(f);
    }

    Ok(RelationalObject { rows: acc })
}

#[cfg(test)]
mod test {
    use crate::db::OrderedColumn;
    use crate::Ordering;
    use core::str::FromStr;

    #[test]
    fn ordering_parses() {
        assert_eq!(Ordering::from_str("ASC").unwrap(), Ordering::Asc);
        assert_eq!(Ordering::from_str("DESC").unwrap(), Ordering::Desc);
    }

    #[test]
    #[should_panic]
    fn ordering_parses_invalid() {
        Ordering::from_str("ASCC").unwrap();
    }

    #[test]
    fn ordered_column_parsing() {
        assert_eq!(
            OrderedColumn::from_str("column_name").unwrap(),
            OrderedColumn {
                column: "column_name".to_string(),
                ordering: Default::default()
            }
        );

        assert_eq!(
            OrderedColumn::from_str("column_name ASC").unwrap(),
            OrderedColumn {
                column: "column_name".to_string(),
                ordering: Ordering::Asc,
            }
        );
    }
}

use crate::Error;
use core::borrow::BorrowMut;
use core::fmt::{Display, Formatter};
use core::str::FromStr;
use mysql_slowlog_parser::EntryStatement::{AdminCommand, SqlStatement};
use mysql_slowlog_parser::{Entry, EntrySqlStatementObject, EntryStatement};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow, SqliteSynchronous};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use std::ops::AddAssign;
use time::format_description::well_known::iso8601::Iso8601;
use time::OffsetDateTime;

#[derive(Error, Debug)]
pub struct InvalidPrimaryKey {
    value: i64,
}

impl Display for InvalidPrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "value: {}", self.value)
    }
}

pub async fn open_db(db: Option<String>) -> Result<SqlitePool, Error> {
    let url = url(db);

    let pool_timeout = 30;
    let concurrency = 50;

    let pool_max_connections = if concurrency == 1 {
        2
    } else {
        concurrency as u32
    };

    let connection_options = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        //    .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .serialized(true)
        .foreign_keys(false)
        .busy_timeout(std::time::Duration::from_secs(pool_timeout));

    let db = SqlitePoolOptions::new()
        .max_connections(pool_max_connections)
        .connect_with(connection_options)
        .await?;

    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let migrations = std::path::Path::new(&crate_dir).join("./migrations");
    let _ = sqlx::migrate::Migrator::new(migrations)
        .await
        .unwrap()
        .run(&db)
        .await?;

    Ok(db)
}

pub fn url(p: Option<String>) -> String {
    match p {
        Some(p) => format!("sqlite://{}", p),
        None => format!("sqlite://:memory:"),
    }
}

pub async fn record_entry(c: SqlitePool, e: Entry) -> Result<u32, Error> {
    let mut tx = c.begin().await?;

    let query_call_id = insert_entry(&mut tx, e).await?;

    tx.commit().await?;

    Ok(query_call_id)
}

pub async fn insert_entry(tx: &mut Transaction<'_, Sqlite>, e: Entry) -> Result<u32, Error> {
    let (sql, sql_type, objects, details) = match e.statement() {
        SqlStatement(s) => (
            s.statement.to_string(),
            Some(s.entry_sql_type().to_string()),
            Some(s.objects()),
            Some(json!(s.details.clone())),
        ),
        EntryStatement::InvalidStatement(s) => (s.into(), None, None, None),
        AdminCommand(ac) => (ac.command.to_string(), None, None, None),
    };

    let query_id = insert_query(
        tx,
        InsertQueryParams {
            sql,
            sql_type,
            objects,
        },
    )
    .await?;

    let query_call_id = insert_query_call(
        tx,
        InsertQueryCallParams::new(query_id, e.start_timestamp(), e.time().to_string()),
    )
    .await?;

    let _ = insert_query_session(
        tx,
        InsertQuerySessionParams {
            query_call_id,
            user_name: e.user().to_string(),
            sys_user_name: e.sys_user().to_string(),
            host_name: e.host().to_string(),
            ip_address: e.ip_address().to_string(),
            thread_id: e.thread_id(),
        },
    )
    .await?;

    let _ = insert_query_stats(
        tx,
        InsertQueryStatsParams {
            query_call_id,
            query_time: e.query_time(),
            lock_time: e.lock_time(),
            rows_sent: e.rows_sent(),
            rows_examined: e.rows_examined(),
        },
    )
    .await?;

    if let Some(d) = details {
        let _ = insert_query_details(
            tx,
            InsertQueryDetailsParams {
                query_call_id,
                details: d,
            },
        )
        .await?;
    }

    Ok(query_call_id)
}

struct InsertQueryParams {
    sql: String,
    sql_type: Option<String>,
    objects: Option<Vec<EntrySqlStatementObject>>,
}

async fn find_query(
    tx: &mut Transaction<'_, Sqlite>,
    params: &InsertQueryParams,
) -> Result<u32, Error> {
    let r = sqlx::query("SELECT id FROM db_ojects WHERE schema = ?")
        .bind(params.sql.to_string())
        .fetch_one(tx)
        .await?;

    let id = r.try_get(0)?;

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

async fn find_db_object(
    tx: &mut Transaction<'_, Sqlite>,
    schema: Option<String>,
    object_name: String,
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
    params: InsertQueryParams,
) -> Result<u32, Error> {
    if let Ok(id) = find_query(tx, &params).await {
        return Ok(id);
    }

    let result = sqlx::query("INSERT INTO queries (sql, sql_type) VALUES (?, ?)")
        .bind(&params.sql)
        .bind(&params.sql_type)
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
    params: InsertQueryParams,
) -> Result<(), Error> {
    if let Some(l) = params.objects {
        for o in l {
            let db_object_id = insert_db_objects(tx, o).await?;

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
    object: EntrySqlStatementObject,
) -> Result<u32, Error> {
    if let Ok(id) = find_db_object(tx, object.schema_name.clone(), object.object_name.clone()).await
    {
        return Ok(id);
    }

    let result = sqlx::query("INSERT INTO db_objects (schema_name, object_name) VALUES (?, ?)")
        .bind(object.schema_name.clone())
        .bind(object.object_name.clone())
        .execute(tx)
        .await
        .unwrap();

    let id = result.last_insert_rowid();

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

struct InsertQueryCallParams {
    query_id: u32,
    start_time: OffsetDateTime,
    log_time: OffsetDateTime,
}

impl InsertQueryCallParams {
    fn new(query_id: u32, start_time: u32, log_time: String) -> Self {
        Self {
            query_id,
            start_time: OffsetDateTime::from_unix_timestamp(start_time as i64).unwrap(),
            log_time: OffsetDateTime::parse(&log_time, &Iso8601::DEFAULT).unwrap(),
        }
    }
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
    .bind(&params.start_time)
    .bind(&params.log_time)
    .execute(tx)
    .await?;

    let id = result.last_insert_rowid();

    u32::try_from(id).or(Err(InvalidPrimaryKey { value: id }.into()))
}

struct InsertQuerySessionParams {
    query_call_id: u32,
    user_name: String,
    sys_user_name: String,
    host_name: String,
    ip_address: String,
    thread_id: u32,
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
    .bind(params.user_name)
    .bind(params.sys_user_name)
    .bind(params.host_name)
    .bind(params.ip_address)
    .bind(params.thread_id)
    .execute(tx)
    .await?;

    Ok(())
}

struct InsertQueryStatsParams {
    query_call_id: u32,
    query_time: f64,
    lock_time: f64,
    rows_sent: u32,
    rows_examined: u32,
}

async fn insert_query_stats(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryStatsParams,
) -> Result<(), Error> {
    let _ = sqlx::query(
        "INSERT INTO query_call_stats (query_call_id, query_time, lock_time, rows_sent, rows_examined)
        VALUES (?, ?, ?, ?, ?)")
        .bind(params.query_call_id)
        .bind(&params.query_time)
        .bind(&params.lock_time)
        .bind(&params.rows_sent)
        .bind(&params.rows_examined)
        .execute(tx)
        .await?;

    Ok(())
}

struct InsertQueryDetailsParams {
    query_call_id: u32,
    details: Value,
}

async fn insert_query_details(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryDetailsParams,
) -> Result<(), Error> {
    let _ = sqlx::query(
        "INSERT INTO query_call_details (query_call_id, details)
        VALUES (?, ?)",
    )
    .bind(&params.query_call_id)
    .bind(&params.details)
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

pub struct Stats<C> {
    calls: u32,
    query_time: f32,
    lock_time: f32,
    rows_sent: f32,
    rows_examined: f32,
    filter: C,
}

impl<C: ColumnSet> ColumnSet for Stats<C> {
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
            calls: r.try_get("calls")?,
            query_time: r.try_get("query_time")?,
            lock_time: r.try_get("lock_time")?,
            rows_sent: r.try_get("rows_sent")?,
            rows_examined: r.try_get("rows_examined")?,
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
/// SortingPlan, Stats};
/// use mysql_slowlog_analyzer::Error;
///         use std::collections::BTreeMap;
///
/// use sqlx::sqlite::SqliteRow;
/// use sqlx::Row;
/// use sqlx::FromRow;
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
///     use std::fs::File;
///     use std::io::BufReader;
///     use mysql_slowlog_analyzer::db::{Limit, open_db, query_column_set, Stats};
///     use mysql_slowlog_analyzer::record_log;
///     let c = open_db(None)
///     .await
///     .unwrap();
///
///     let mut f = BufReader::new(File::open("data/slow-test-queries.log").unwrap());
///
///     record_log(&c, &mut f).await.unwrap();
///
///     let sorting = SortingPlan {
///         order_by: Some(OrderBy { columns: vec![("calls".to_string(), Ordering::Desc)]}),
///         limit: Some(Limit {
///             limit: 5,
///             offset: None
///          })
///     };
///
///     let stats = query_column_set::<Stats<StatsBySql>>(&c, Some(sorting)).await.unwrap();
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

impl ToString for Ordering {
    fn to_string(&self) -> String {
        format!(
            "{}",
            match self {
                Self::Asc => "ASC",
                Self::Desc => "DESC",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderBy {
    pub columns: Vec<(String, Ordering)>,
}

impl OrderBy {
    fn sql_clause(&self, b: &mut QueryBuilder<Sqlite>) {
        b.push("\nORDER BY ");

        let mut seperated = b.separated(", ");

        for c in &self.columns {
            seperated.push(format!("{} {}", c.0, c.1.to_string()));
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

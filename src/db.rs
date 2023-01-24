use crate::Error;
use core::fmt::{Display, Formatter};
use core::str::FromStr;
use mysql_slowlog_parser::EntryStatement::{AdminCommand, SqlStatement};
use mysql_slowlog_parser::{Entry, EntryStatement};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
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

pub async fn open(db: Option<String>) -> Result<SqlitePool, Error> {
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
    let (sql, details) = match e.statement() {
        SqlStatement(s) => (s.statement.to_string(), Some(json!(s.details.clone()))),
        EntryStatement::InvalidStatement(s) => (s.into(), None),
        AdminCommand(ac) => (ac.command.to_string(), None),
    };

    let query_id = insert_query(tx, InsertQueryParams { sql }).await?;

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
}

async fn find_query(
    tx: &mut Transaction<'_, Sqlite>,
    params: &InsertQueryParams,
) -> Result<u32, Error> {
    let r = sqlx::query("SELECT id FROM queries WHERE sql = ?")
        .bind(params.sql.to_string())
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

    let result = sqlx::query("INSERT INTO queries (sql) VALUES (?)")
        .bind(&params.sql)
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
        VALUES (?, ?, ?, ?, ?, ?)")
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

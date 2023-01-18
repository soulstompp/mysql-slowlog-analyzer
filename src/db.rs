use std::collections::HashMap;
use crate::Error;
use mysql_slowlog_parser::EntryStatement::{AdminCommand, SqlStatement};
use mysql_slowlog_parser::{Entry, EntryStatement};
use serde_json::{json, Value};
use std::path::Path;
use sqlx::migrate::MigrateDatabase;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use sqlx::sqlite::SqlitePoolOptions;
use time::format_description::well_known::iso8601::Iso8601;
use time::OffsetDateTime;
use tokio::task;

pub async fn open(db: Option<String>) -> Result<SqlitePool, Error> {
    let url = url(db);

    println!("connecting: {}", url);

    if !Sqlite::database_exists(&url).await.unwrap_or(false) {
        println!("Creating database {}", &url);
        match Sqlite::create_database(&url).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    }

    let db = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await.unwrap();

    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let migrations = std::path::Path::new(&crate_dir).join("./migrations");
    let migration_results = sqlx::migrate::Migrator::new(migrations)
        .await
        .unwrap()
        .run(&db)
        .await;

    Ok(db)
}

pub fn url(p: Option<String>) -> String {
    match p {
        Some(p) => format!("sqlite://{}", p),
        None => format!("sqlite://:memory:")
    }
}

pub async fn record_entry(c: SqlitePool, e: Entry) -> Result<i64, Error> {
    task::spawn(async move {
        let mut tx = c.begin().await?;

        let query_call_id = insert_entry(&mut tx, e).await?;

        tx.commit().await?;

        Ok::<i64, Error>(query_call_id)
    });

    // Yield, allowing the newly-spawned task to execute first.
    task::yield_now().await;
    println!("main task done!");

    Ok(0)
}

pub async fn insert_entry(tx: &mut Transaction<'_, Sqlite>, e: Entry) -> Result<i64, Error> {
    let (sql, details) = match e.statement() {
        SqlStatement(s) => (s.statement.to_string(), Some(json!(s.details.clone()))),
        EntryStatement::InvalidStatement(s) => (s.into(), None),
        AdminCommand(ac) => (ac.command.to_string(), None),
    };

    let query_id = insert_query(tx, InsertQueryParams { sql }).await?;

    println!("query: {}", query_id);

    let query_call_id = insert_query_call(
        tx,
        InsertQueryCallParams::new(query_id, e.start_timestamp(), e.time().to_string()),
    ).await?;

    println!("query call id: {}", query_call_id);

    let _ = insert_query_stats(
        tx,
        InsertQueryStatsParams {
            query_call_id,
            query_time: e.query_time(),
            lock_time: e.lock_time(),
            rows_sent: e.rows_sent(),
            rows_examined: e.rows_examined(),
        },
    ).await?;

    println!("query stats ran");

    if let Some(d) = details {
        let _ = insert_query_details(
            tx,
            InsertQueryDetailsParams {
                query_call_id,
                details: d,
            },
        ).await?;
    }

    println!("query details ran");

    Ok(query_call_id)
}

struct InsertQueryParams {
    sql: String,
}

async fn find_query(tx: &mut Transaction<'_, Sqlite>, id: i64) -> Result<i64, Error> {
    let r = sqlx::query("SELECT id FROM queries WHERE sql = ?")
        .bind(id)
        .fetch_one(tx)
        .await?;

    let id = r.try_get(0)?;

    Ok(id)
}

async fn insert_query(tx: &mut Transaction<'_, Sqlite>, params: InsertQueryParams) -> Result<i64,
    Error> {

    let result = sqlx::query("INSERT INTO queries (sql) VALUES (?)")
        .bind(&params.sql)
        .execute(tx)
        .await
        .unwrap();

        let id = result.last_insert_rowid();

        Ok(id)
}

struct InsertQueryCallParams {
    //TODO: don't allow negatives
    query_id: i64,
    start_time: OffsetDateTime,
    log_time: OffsetDateTime,
}

impl InsertQueryCallParams {
    fn new(query_id: i64, start_time: u32, log_time: String) -> Self {
        Self {
            query_id,
            start_time: OffsetDateTime::from_unix_timestamp(start_time as i64).unwrap(),
            log_time: OffsetDateTime::parse(&log_time, &Iso8601::DEFAULT).unwrap(),
        }
    }
}

async fn insert_query_call(tx: &mut Transaction<'_, Sqlite>, params: InsertQueryCallParams) ->
                                                                                         Result<i64,
    Error> {
    let result = sqlx::query("INSERT INTO query_calls (query_id, start_time, log_time)
                 VALUES (?, ?, ?)
            ")
    .bind(&params.query_id)
        .bind(&params.start_time)
        .bind(&params.log_time)
        .execute(tx)
        .await?;

    let id = result.last_insert_rowid();

    Ok(id)
}

struct InsertQueryStatsParams {
    //TODO: don't allow negatives
    query_call_id: i64,
    query_time: f64,
    lock_time: f64,
    rows_sent: u32,
    rows_examined: u32,
}

async fn insert_query_stats(tx: &mut Transaction<'_, Sqlite>, params: InsertQueryStatsParams) ->
                                                                                     Result<i64,
Error> {
    let result = sqlx::query(
    "INSERT INTO query_call_stats (query_call_id, query_time, lock_time, rows_sent, rows_examined)
        VALUES (?, ?, ?, ?, ?)")
        .bind(&params.query_call_id)
        .bind(&params.query_time)
        .bind(&params.lock_time)
        .bind(&params.rows_sent)
        .bind(&params.rows_examined)
        .execute(tx)
        .await?;

    let id = result.last_insert_rowid();

    Ok(id)
}

struct InsertQueryDetailsParams {
    query_call_id: i64,
    details: Value,
}

async fn insert_query_details(
    tx: &mut Transaction<'_, Sqlite>,
    params: InsertQueryDetailsParams,
) -> Result<i64, Error> {
    let result = sqlx::query(
        "INSERT INTO query_call_details (query_call_id, details)
        VALUES (?, ?)")
        .bind(&params.query_call_id)
        .bind(&params.details)
        .execute(tx)
        .await?;

    let id = result.last_insert_rowid();

    Ok(id)
}


pub struct Query {
    id: u32,
    sql: String,
}

pub struct QueryCall {
    id: u32,
    query_id: u32,
    start_time: OffsetDateTime,
    log_time: OffsetDateTime,
    query: Query,
    stats: QueryStats,
    details: QueryDetails,
}

pub struct QueryDetails {
    id: u32,
    query_call_id: u32,
    details: HashMap<String, String>,
}

struct QueryStats {
    id: u32,
    query_call_id: u32,
    query_time: f64,
    lock_time: f64,
    rows_sent: u32,
    rows_examined: u32,
}

fn select_query_call_by_id(id: u32) -> Result<usize, Error> {
    unimplemented!();
    /*
    let sql = r#"
SELECT
    q.id as query_id,
    q.sql
    qc.start_time,
    qc.log_time,
    qs.query_time,
    qs.lock_time,
    qs.rows_sent,
    qs.rows_examined,
    qd.details
FROM queries q
JOIN query_call qc ON qc.query_id = q.id
JOIN query_stats qs ON qs.query_call_id = qc.id
LEFT JOIN query_details qd ON qd.query_call_id = q
WHERE
qc.id = :query_call_id
    "#;
    let mut stmt = tx.prepare(sql)?;

    Ok(stmt.query_row(
        named_params! {
            ":query_call_id": id,
        },
        |row| {
            row.get(0)
        },
    )?)

     */
}

#![feature(associated_type_defaults)]
#[macro_use]
extern crate alloc;
extern crate core;

mod arrow2;
pub mod db;
mod dirs;
pub mod types;

use core::borrow::Borrow;
use std::collections::HashMap;
use std::io;
use std::ops::AddAssign;

use async_compat::{Compat, CompatExt};
use tokio::fs::File;

use ::polars::error::{ArrowError, PolarsError};

use ::polars::export::arrow::chunk::Chunk;
use futures::{SinkExt, StreamExt};

#[doc(inline)]
pub use crate::db::{
    open_db, query_column_set, record_entry, ColumnSet, OrderBy, OrderedColumn, Ordering,
    RelationalObject, SortingPlan,
};
use mysql_slowlog_parser::{CodecError, Entry, EntryCodec, EntrySqlType, ReadError};
use sha2::{Digest, Sha256};
use sqlx::migrate::{MigrateDatabase, MigrateError};
use sqlx::{Sqlite, SqlitePool};
use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::fs::create_dir_all;

use crate::arrow2::write_options;
use crate::db::db_url;
use crate::dirs::{DirError, SourceDataDir};
use db::InvalidPrimaryKey;

use crate::types::QueryEntry;
use polars::export::arrow::io::parquet::write::FileSink;
use time::Instant;
use tokio_util::codec::FramedRead;

#[derive(Error, Debug)]
pub enum Error {
    #[error("this log has already recorded, delete the corresponding directoru to record again")]
    LogAlreadyRecorded,
    #[error("{0}")]
    DirError(#[from] DirError),
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("invalid primary key: {0}")]
    InvalidPrimaryKey(#[from] InvalidPrimaryKey),
    #[error("invalid order by clause: {0}")]
    InvalidOrderBy(String),
    #[error("migration error: {0}")]
    Migrate(#[from] MigrateError),
    #[error("reader error: {0}")]
    Reader(#[from] ReadError),
    #[error("db error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("arrow error: {0}")]
    ArrowError(#[from] ArrowError),
}

pub struct LogData {
    path: PathBuf,
    dirs: SourceDataDir,
    pool: SqlitePool,
    config: LogDataConfig,
    context: LogDataContext,
}

impl LogData {
    pub async fn open(p: &Path, config: LogDataConfig) -> Result<Self, Error> {
        let mut context = LogDataContext::default();

        let dirs = Self::data_dir(p, &config).await?;

        let sqlitep = dirs.sqlite_dir()?;

        create_dir_all(&sqlitep).await?;

        let mut dbp = sqlitep.clone();
        dbp.push("entries");

        let db_url = db_url(Some(&dbp));

        if Sqlite::database_exists(&db_url).await? {
            context.db_recorded = true;
        } else {
            Sqlite::create_database(&db_url).await?;
        }

        let pool = open_db(Some(&dbp)).await?;

        Ok(Self {
            path: p.to_path_buf(),
            dirs,
            pool,
            config,
            context,
        })
    }

    async fn data_dir(p: &Path, config: &LogDataConfig) -> Result<SourceDataDir, Error> {
        let f = File::open(&p).await?;

        let mut hash = Sha256::default();

        io::copy(&mut f.into_std().await, &mut hash)?;

        let hash = format!("{:x}", hash.finalize());

        let dirs = SourceDataDir {
            hash: hash.to_string(),
            data_dir: config
                .data_path
                .as_ref()
                .and_then(|p| Some(p.to_path_buf())),
        };

        Ok(dirs)
    }

    pub fn parquet_dir(&self) -> Result<PathBuf, Error> {
        self.dirs.parquet_dir()
    }

    pub fn sqllite_dir(&self) -> Result<PathBuf, Error> {
        self.dirs.sqlite_dir()
    }

    pub async fn record_db(&mut self) -> Result<(), Error> {
        if self.context.db_recorded {
            return Err(Error::LogAlreadyRecorded);
        }

        let mut r = self.reader().await?;

        let c = self.db_pool();

        let mut i = 0;

        while let Some(res) = r.next().await {
            let e = res.unwrap();
            let _ = record_entry(c.clone(), QueryEntry(e)).await?;
            i.add_assign(1);
        }

        self.context.db_recorded = true;

        Ok(())
    }

    pub async fn record_parquet(&mut self) -> Result<(), Error> {
        //TODO if the directory already exists just return OK(()), waiting until testing is done
        // to add.

        println!("recording");
        create_dir_all(self.parquet_dir().unwrap()).await?;
        let r = self
            .reader()
            .await?
            .map(|res: Result<Entry, CodecError>| match res {
                Ok(e) => {
                    let qe = QueryEntry(e);
                    Ok((
                        qe.0.sql_attributes.sql_type().clone(),
                        Chunk::new(qe.arrow2_arrays()),
                    ))
                }
                Err(e) => Err(ArrowError::External(e.to_string(), Box::new(e))),
            });

        let mut files: HashMap<&str, File> = HashMap::new();

        for part in [
            "contextual",
            "create",
            "delete",
            "read",
            "transactional",
            "update",
        ] {
            let mut p = self.dirs.parquet_dir().unwrap();
            p.push(part);

            let f = File::create(&p).await.unwrap();

            files.insert(part, f);
        }

        let capacity = 50;

        r.ready_chunks(capacity)
            .for_each_concurrent(4, |ready| async {
                let ready_count = ready.len();

                let write_began = Instant::now();
                let mut sinks: HashMap<&str, FileSink<Compat<File>>> = HashMap::new();

                for e in ready.into_iter() {
                    let (sql_type, log_entry) = e.unwrap();
                    /*
                                   "SELECT" => "create",
                                   "INSERT" => "read",
                                   "UPDATE" => "update",
                                   "DELETE" => "delete",
                                   "COMMIT TRANSACTION" => "transactional",
                                   "ROLLBACK TRANSACTION" => "transactional",
                                   "SAVEPOINT" => "transactional",

                    */
                    let part = match sql_type {
                        Some(t) => match t {
                            EntrySqlType::Query => "read",
                            EntrySqlType::Insert => "create",
                            EntrySqlType::Update => "update",
                            EntrySqlType::Delete => "delete",
                            EntrySqlType::StartTransaction => "transactional",
                            EntrySqlType::SetTransaction => "transactional",
                            EntrySqlType::Commit => "transactional",
                            EntrySqlType::Rollback => "transactional",
                            EntrySqlType::Savepoint => "transactional",
                            _ => "contextual",
                        },
                        _ => "contextual",
                    };

                    let file = files.get(part).unwrap();

                    let buffer = file.try_clone().await.unwrap().compat();

                    let sink = sinks.entry(part).or_insert(
                        FileSink::try_new(
                            buffer,
                            QueryEntry::arrow2_schema(),
                            QueryEntry::encodings(),
                            write_options(),
                        )
                        .unwrap(),
                    );

                    sink.feed(log_entry).await.unwrap();
                }

                println!(
                    "feeding of the {} in {} seconds",
                    ready_count,
                    write_began.elapsed()
                );

                for sink in sinks.values_mut() {
                    sink.close().await.unwrap();
                }

                println!("sinks closed in {} seconds", write_began.elapsed());

                drop(sinks);
            })
            .await;

        Ok(())
    }

    pub fn db_pool(&self) -> &SqlitePool {
        self.pool.borrow()
    }

    pub async fn reader(&self) -> Result<FramedRead<File, EntryCodec>, Error> {
        let f = File::open(&self.path).await?;
        Ok(FramedRead::with_capacity(f, EntryCodec::default(), 14229))
    }
}

#[derive(Debug, Default)]
pub struct LogDataConfig {
    pub data_path: Option<PathBuf>,
}

#[derive(Debug, Default)]
pub struct LogDataContext {
    config: LogDataConfig,
    db_recorded: bool,
    aliases: Vec<String>,
}

#[cfg(test)]
mod tests {
    use crate::db::{AggregateStats, Calls, Limit, OrderBy, OrderedColumn, SortingPlan};
    use crate::Error;
    use crate::{query_column_set, ColumnSet, LogData, LogDataConfig};
    use core::str::FromStr;
    use sqlx::sqlite::SqliteRow;
    use sqlx::Row;
    use std::fs::{metadata, remove_dir_all};
    use std::path::PathBuf;

    #[derive(Debug)]
    struct FilterUser {
        user_name: String,
        host_name: String,
    }

    impl ColumnSet for FilterUser {
        fn set_sql(_: &Option<SortingPlan>) -> String {
            format!(
                r#"
            SELECT qs.user_name, qs.host_name, qc.id AS query_call_id
            FROM query_calls qc
            JOIN query_call_session qs ON qs.query_call_id = qc.id
        "#
            )
        }

        fn from_row(r: SqliteRow) -> Result<Self, Error> {
            Ok(Self {
                user_name: r.try_get("user_name")?,
                host_name: r.try_get("host_name")?,
            })
        }

        fn columns() -> Vec<String> {
            vec!["user_name".to_string(), "host_name".to_string()]
        }

        fn key() -> String {
            format!("query_call_id")
        }

        fn display_values(&self) -> Vec<(&str, String)> {
            let mut acc = vec![];

            acc.push(("user_name", self.user_name.to_string()));
            acc.push(("host_name", self.host_name.to_string()));

            acc
        }
    }

    #[derive(Debug)]
    struct FilterObject {
        schema_name: Option<String>,
        object_name: String,
    }

    impl ColumnSet for FilterObject {
        fn set_sql(_: &Option<SortingPlan>) -> String {
            format!(
                r#"
            SELECT do.schema_name, do.object_name, qc.id AS query_call_id
            FROM db_objects do
            JOIN query_objects qo ON qo.db_object_id = do.id
            JOIN query_calls qc ON qc.query_id = qo.query_id
            "#
            )
        }

        fn from_row(r: SqliteRow) -> Result<Self, Error> {
            Ok(Self {
                schema_name: r.try_get("schema_name")?,
                object_name: r.try_get("object_name")?,
            })
        }

        fn columns() -> Vec<String> {
            vec!["schema_name".into(), "object_name".into()]
        }

        fn key() -> String {
            format!("query_call_id")
        }

        fn display_values(&self) -> Vec<(&str, String)> {
            let mut acc = vec![];

            acc.push((
                "schema_name",
                self.schema_name
                    .clone()
                    .unwrap_or("NULL".into())
                    .to_string(),
            ));
            acc.push(("object_name", self.object_name.to_string()));

            acc
        }
    }

    #[tokio::test]
    async fn can_record_parquet() {
        let p = PathBuf::from("data/slow-test-queries.log");

        let data_dir = PathBuf::from("/tmp/can_record_parquet");

        if metadata(&data_dir).is_ok() {
            remove_dir_all(&data_dir).unwrap();
        }

        let context = LogDataConfig {
            data_path: Some(data_dir),
        };

        let mut s = LogData::open(&p, context).await.unwrap();

        s.record_parquet().await.unwrap();
    }

    #[tokio::test]
    async fn can_record_db() {
        let p = PathBuf::from("data/slow-test-queries.log");

        let data_dir = PathBuf::from("/tmp/can_record_db");

        if metadata(&data_dir).is_ok() {
            remove_dir_all(&data_dir).unwrap();
        }

        let context = LogDataConfig {
            data_path: Some(data_dir),
        };

        let mut s = LogData::open(&p, context).await.unwrap();

        s.record_db().await.unwrap();

        let c = s.db_pool();

        let stats = query_column_set::<AggregateStats<FilterUser>>(&c, None)
            .await
            .unwrap();

        println!("user stats:\n{}", stats.display_vertical());

        let stats = query_column_set::<Calls<FilterUser>>(&c, None)
            .await
            .unwrap();

        println!("user calls:\n{}", stats.display_vertical());

        let sorting = SortingPlan {
            order_by: Some(OrderBy {
                columns: vec![OrderedColumn::from_str("calls DESC").unwrap()],
            }),
            limit: Some(Limit {
                limit: 5,
                offset: Some(5),
            }),
        };

        let stats = query_column_set::<AggregateStats<FilterObject>>(&c, Some(sorting.clone()))
            .await
            .unwrap();

        println!("object stats:\n{}", stats.display_vertical());

        let stats = query_column_set::<Calls<FilterObject>>(&c, Some(sorting))
            .await
            .unwrap();

        println!("object stats:\n{}", stats.display_vertical());
    }
}

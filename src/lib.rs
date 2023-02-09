#[macro_use]
extern crate alloc;
extern crate core;

pub mod db;
mod dirs;

use core::borrow::Borrow;
use std::fs::File;
use std::io;

#[doc(inline)]
pub use crate::db::{
    open_db, query_column_set, record_entry, ColumnSet, OrderBy, OrderedColumn, Ordering,
    RelationalObject, SortingPlan, Stats,
};
use async_stream::try_stream;
use futures::TryStreamExt;
use mysql_slowlog_parser::{EntryMasking, ReadError, Reader};
use sha2::{Digest, Sha256};
use sqlx::migrate::{MigrateDatabase, MigrateError};
use sqlx::{Sqlite, SqlitePool};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::create_dir_all;

use crate::db::db_url;
use crate::dirs::{DirError, SourceDataDir};
use db::InvalidPrimaryKey;

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
    #[error("parser error: {0}")]
    Parser(#[from] ReadError),
    #[error("db error: {0}")]
    SqlxError(#[from] sqlx::Error),
}

pub struct LogData {
    path: PathBuf,
    hash: String,
    pool: SqlitePool,
    config: LogDataConfig,
    context: LogDataContext,
}

impl LogData {
    pub async fn open(p: &Path, config: LogDataConfig) -> Result<Self, Error> {
        let mut context = LogDataContext::default();

        let mut f = File::open(&p)?;
        let mut hash = Sha256::default();

        io::copy(&mut f, &mut hash)?;

        let hash = format!("{:x}", hash.finalize());

        let dirs = SourceDataDir {
            hash: hash.to_string(),
            data_dir: config
                .data_path
                .as_ref()
                .and_then(|p| Some(p.to_path_buf())),
        };

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
            hash: hash.to_string(),
            pool,
            config,
            context,
        })
    }

    pub async fn record(&mut self) -> Result<(), Error> {
        if self.context.db_recorded {
            return Err(Error::LogAlreadyRecorded);
        }

        let mut br = self.reader()?;

        let mut r = Reader::new(br.by_ref(), EntryMasking::PlaceHolder)?;

        let c = self.db_pool();

        let s = try_stream! {
            while let Some(e) = r.read_entry()? {
                yield e;
            }
        };

        let _: Result<(), Error> = s
            .try_for_each(|e| async {
                let _ = record_entry(c.clone(), e).await?;

                Ok(())
            })
            .await;

        self.context.db_recorded = true;

        Ok(())
    }

    pub fn db_pool(&self) -> &SqlitePool {
        self.pool.borrow()
    }

    fn dirs(&self) -> SourceDataDir {
        SourceDataDir {
            hash: self.hash.to_string(),
            data_dir: self
                .config
                .data_path
                .as_ref()
                .and_then(|p| Some(p.to_path_buf())),
        }
    }

    fn reader(&self) -> Result<BufReader<File>, Error> {
        Ok(BufReader::new(File::open(&self.path)?))
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
    use crate::db::{Calls, Limit, OrderBy, OrderedColumn, SortingPlan};
    use crate::Error;
    use crate::{query_column_set, ColumnSet, LogData, LogDataConfig, Stats};
    use core::str::FromStr;
    use sqlx::sqlite::SqliteRow;
    use sqlx::Row;
    use std::fs::{metadata, remove_dir_all};
    use std::path::PathBuf;

    #[derive(Debug)]
    struct StatsByUser {
        user_name: String,
        host_name: String,
    }

    impl ColumnSet for StatsByUser {
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
            vec!["user_name".into(), "host_name".into()]
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
    struct StatsByObject {
        schema_name: Option<String>,
        object_name: String,
    }

    impl ColumnSet for StatsByObject {
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
    async fn can_record_log() {
        let p = PathBuf::from("data/slow-test-queries.log");

        let data_dir = PathBuf::from("/tmp/can_record_log");

        if metadata(&data_dir).is_ok() {
            remove_dir_all(&data_dir).unwrap();
        }

        let context = LogDataConfig {
            data_path: Some(data_dir),
        };

        let mut s = LogData::open(&p, context).await.unwrap();

        s.record().await.unwrap();

        let c = s.db_pool();

        let stats = query_column_set::<Stats<StatsByUser>>(&c, None)
            .await
            .unwrap();

        println!("user stats:\n{}", stats.display_vertical());

        let stats = query_column_set::<Calls<StatsByUser>>(&c, None)
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

        let stats = query_column_set::<Stats<StatsByObject>>(&c, Some(sorting.clone()))
            .await
            .unwrap();

        println!("object stats:\n{}", stats.display_vertical());

        let stats = query_column_set::<Calls<StatsByObject>>(&c, Some(sorting))
            .await
            .unwrap();

        println!("object stats:\n{}", stats.display_vertical());
    }
}

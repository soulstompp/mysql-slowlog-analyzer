#![feature(associated_type_defaults)]
#[macro_use]
extern crate alloc;
extern crate core;

pub mod db;
mod dirs;
pub mod types;

use std::fs::remove_dir_all;
use std::ops::AddAssign;
use std::{fs, io};

use tokio::fs::File;

#[doc(inline)]
pub use crate::db::{
    open_db, query_column_set, record_entry, ColumnSet, OrderBy, OrderedColumn, Ordering,
    RelationalObject, SortingPlan,
};
use duckdb::Connection;
use futures::StreamExt;
use mysql_slowlog_parser::{CodecError, EntryCodec, EntryCodecConfig, ReadError};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::create_dir_all;

use crate::dirs::{DirError, SourceDataDir};
use db::InvalidPrimaryKey;

use crate::types::QueryEntry;
use tokio_util::codec::FramedRead;

use duckdb::Error as DuckDbError;
use log::debug;

#[derive(Error, Debug)]
pub enum Error {
    #[error("this log has already recorded, delete the corresponding directoru to record again")]
    LogAlreadyRecorded(String),
    #[error("chrono date parsing error: {0}")]
    ChronoParseError(chrono::ParseError),
    #[error("{0}")]
    DirError(#[from] DirError),
    #[error("duck db error: {0}")]
    DuckDbError(#[from] DuckDbError),
    #[error("io error: {0}")]
    IO(#[from] io::Error),
    #[error("invalid primary key: {0}")]
    InvalidPrimaryKey(#[from] InvalidPrimaryKey),
    #[error("invalid order by clause: {0}")]
    InvalidOrderBy(String),
    #[error("reader error: {0}")]
    Reader(#[from] ReadError),
    #[error("codec error: {0}")]
    CodecError(#[from] CodecError),
}

pub struct LogData {
    path: PathBuf,
    dirs: SourceDataDir,
    connection: Connection,
    config: LogDataConfig,
    context: LogDataContext,
}

impl LogData {
    pub async fn open(p: &Path, config: LogDataConfig) -> Result<Self, Error> {
        let mut context = LogDataContext { db_recorded: false };

        let dirs = Self::data_dir(p, config.clone()).await?;

        let duckdb_path = dirs.duckdb_dir()?;

        create_dir_all(&duckdb_path).await?;

        let mut dbp = duckdb_path.clone();
        dbp.push("entries");

        if dbp.exists() {
            debug!("db path exists: {}", dbp.display());
            context.db_recorded = true;
        }

        debug!("db path: {}", dbp.display());
        let pool = open_db(Some(&dbp)).await?;

        Ok(Self {
            path: p.to_path_buf(),
            dirs,
            connection: pool,
            config: config.clone(),
            context,
        })
    }

    async fn data_dir(p: &Path, config: LogDataConfig) -> Result<SourceDataDir, Error> {
        let f = File::open(&p).await?;

        let mut hash = Sha256::default();

        io::copy(&mut f.into_std().await, &mut hash)?;

        let hash = format!("{:x}", hash.finalize());

        let dirs = SourceDataDir {
            hash: hash.to_string(),
            data_dir: config.data_path.as_ref().map(|p| p.to_path_buf()),
        };

        Ok(dirs)
    }

    pub fn parquet_dir(&self) -> Result<PathBuf, Error> {
        self.dirs.parquet_dir()
    }

    pub fn duckdb_dir(&self) -> Result<PathBuf, Error> {
        self.dirs.duckdb_dir()
    }

    pub fn setup_db_tables(&mut self) -> Result<(), Error> {
        let init_sql = fs::read_to_string("migrations/init.sql")?;

        let c = self.connection();

        c.execute_batch(init_sql.as_str())?;

        Ok(())
    }

    pub async fn record_db(&mut self) -> Result<(), Error> {
        if self.context.db_recorded {
            //return Err(Error::LogAlreadyRecorded(self.context.config.data_path.clone().unwrap_or(PathBuf::from(":memory:")).to_string_lossy().to_string()));
            debug!("database has already been recorded");
            return Ok(());
        }

        debug!("recording db");
        self.setup_db_tables()?;

        let mut r = self.reader().await?;

        let c = self.connection();

        let mut i = 0;

        while let Some(res) = r.next().await {
            let e = res?;
            let _ = record_entry(c, QueryEntry(e)).await?;
            i.add_assign(1);
        }

        self.context.db_recorded = true;

        Ok(())
    }

    pub async fn drop_db(p: &Path, config: LogDataConfig) -> Result<(), Error> {
        let dirs = LogData::data_dir(p, config).await?;
        let duckdb_path = dirs.duckdb_dir()?;
        let db_file = duckdb_path.join("entries");

        if db_file.exists() {
            remove_dir_all(&duckdb_path)?;
            debug!("Database dropped: {}", db_file.display());
        } else {
            debug!("Database file does not exist: {}", db_file.display());
        }

        Ok(())
    }

    pub fn connection(&mut self) -> &mut Connection {
        &mut self.connection
    }

    pub async fn reader(&self) -> Result<FramedRead<File, EntryCodec>, Error> {
        let f = File::open(&self.path).await?;
        Ok(FramedRead::with_capacity(
            f,
            EntryCodec::new(self.config.codec_config),
            30000000,
        ))
    }
}

#[derive(Clone, Debug, Default)]
pub struct LogDataConfig {
    pub data_path: Option<PathBuf>,
    pub codec_config: EntryCodecConfig,
}

#[derive(Debug, Default)]
pub struct LogDataContext {
    db_recorded: bool,
}

#[cfg(test)]
mod tests {
    use crate::db::{AggregateStats, Calls, Limit, OrderBy, OrderedColumn, SortingPlan};
    use crate::Error;
    use crate::{query_column_set, ColumnSet, LogData, LogDataConfig};
    use core::str::FromStr;
    use duckdb::Row;
    use std::path::PathBuf;

    #[derive(Debug, PartialEq)]
    struct FilterUser {
        user_name: String,
        host_name: String,
    }

    impl ColumnSet for FilterUser {
        fn set_sql(_: &Option<SortingPlan>) -> String {
            r#"
            SELECT qs.user_name, qs.host_name, qc.id AS query_call_id
            FROM query_calls qc
            JOIN query_call_session qs ON qs.query_call_id = qc.id
        "#
            .to_string()
        }

        fn from_row(r: &Row) -> Result<Self, Error> {
            Ok(Self {
                user_name: r.get("user_name")?,
                host_name: r.get("host_name")?,
            })
        }

        fn columns() -> Vec<String> {
            vec!["user_name".to_string(), "host_name".to_string()]
        }

        fn key() -> String {
            "query_call_id".to_string()
        }

        fn display_values(&self) -> Vec<(&str, String)> {
            vec![
                ("user_name", self.user_name.to_string()),
                ("host_name", self.host_name.to_string()),
            ]
        }
    }

    #[derive(Debug, PartialEq)]
    struct FilterObject {
        schema_name: Option<String>,
        object_name: String,
    }

    impl ColumnSet for FilterObject {
        fn set_sql(_: &Option<SortingPlan>) -> String {
            r#"
            SELECT dbo.schema_name, dbo.object_name, qc.id AS query_call_id
            FROM db_objects dbo
            JOIN query_objects qo ON qo.db_object_id = dbo.id
            JOIN query_calls qc ON qc.query_id = qo.query_id
            "#
            .to_string()
        }

        fn from_row(r: &Row) -> Result<Self, Error> {
            Ok(Self {
                schema_name: r.get("schema_name")?,
                object_name: r.get("object_name")?,
            })
        }

        fn columns() -> Vec<String> {
            vec!["schema_name".into(), "object_name".into()]
        }

        fn key() -> String {
            "query_call_id".to_string()
        }

        fn display_values(&self) -> Vec<(&str, String)> {
            vec![
                (
                    "schema_name",
                    self.schema_name
                        .clone()
                        .unwrap_or("NULL".into())
                        .to_string(),
                ),
                ("object_name", self.object_name.to_string()),
            ]
        }
    }

    #[tokio::test]
    async fn can_record_db() {
        let p = PathBuf::from("data/slow-test-queries.log");

        let data_dir = PathBuf::from("/tmp/can_record_db");

        let context = LogDataConfig {
            data_path: Some(data_dir),
            codec_config: Default::default(),
        };

        LogData::drop_db(&p, context.clone()).await.unwrap();

        let mut s = LogData::open(&p, context).await.unwrap();

        s.record_db().await.unwrap();

        let c = s.connection();

        let user_sorting = SortingPlan {
            order_by: Some(OrderBy {
                columns: vec![OrderedColumn::from_str("user_name ASC").unwrap()],
            }),
            limit: Some(Limit {
                limit: 1,
                offset: None,
            }),
        };

        let res = query_column_set::<AggregateStats<FilterUser>>(c, Some(user_sorting.clone()))
            .await
            .unwrap();

        let expected = [AggregateStats {
            query_time: 0.01570645160973072,
            lock_time: 0.00014193548122420907,
            rows_sent: 0,
            rows_examined: 0,
            calls: 310,
            filter: FilterUser {
                user_name: "msandbox".to_string(),
                host_name: "localhost".to_string(),
            },
        }];

        assert_eq!(res.rows(), &expected);

        let res = query_column_set::<Calls<FilterUser>>(c, Some(user_sorting.clone()))
            .await
            .unwrap();

        let expected = vec![Calls {
            calls: 310,
            filter: FilterUser {
                user_name: "msandbox".to_string(),
                host_name: "localhost".to_string(),
            },
        }];

        assert_eq!(res.rows(), &expected);

        let object_sorting = SortingPlan {
            order_by: Some(OrderBy {
                columns: vec![OrderedColumn::from_str("object_name DESC").unwrap()],
            }),
            limit: Some(Limit {
                limit: 1,
                offset: None,
            }),
        };

        let res = query_column_set::<AggregateStats<FilterObject>>(c, Some(object_sorting.clone()))
            .await
            .unwrap();

        let expected = [AggregateStats {
            query_time: 0.0,
            lock_time: 0.0,
            rows_sent: 0,
            rows_examined: 0,
            calls: 1,
            filter: FilterObject {
                schema_name: None,
                object_name: "user".into(),
            },
        }];

        assert_eq!(res.rows(), &expected);

        let res = query_column_set::<Calls<FilterObject>>(c, Some(object_sorting))
            .await
            .unwrap();

        let expected = vec![Calls {
            calls: 1,
            filter: FilterObject {
                schema_name: None,
                object_name: "user".into(),
            },
        }];

        assert_eq!(res.rows(), &expected);
    }
}

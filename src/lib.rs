#[macro_use]
extern crate alloc;

pub mod db;

#[doc(inline)]
pub use crate::db::{
    open_db, query_stat_report, record_entry, ColumnSet, Filter, RelationalObject, Stats,
};
use async_stream::try_stream;
use futures::TryStreamExt;
use mysql_slowlog_parser::{EntryMasking, ReadError, Reader};
use sqlx::migrate::MigrateError;
use sqlx::SqlitePool;
use std::io::BufRead;
use thiserror::Error;

use db::InvalidPrimaryKey;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("invalid primary key: {0}")]
    InvalidPrimaryKey(#[from] InvalidPrimaryKey),
    #[error("migration error: {0}")]
    Migrate(#[from] MigrateError),
    #[error("parser error: {0}")]
    Parser(#[from] ReadError),
    #[error("db error: {0}")]
    SqlxError(#[from] sqlx::Error),
}

pub async fn record_log<'a>(c: &SqlitePool, br: &'a mut dyn BufRead) -> Result<(), Error> {
    let mut r = Reader::new(br, EntryMasking::PlaceHolder)?;

    let s = try_stream! {
        while let Some(e) = r.read_entry()? {
            yield e;
        }
    };

    s.try_for_each(|e| async {
        let _ = record_entry(c.clone(), e).await?;

        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::{open_db, query_stat_report, ColumnSet, Filter, Stats};
    use crate::{record_log, Error};
    use fs::File;
    use sqlx::sqlite::SqliteRow;
    use sqlx::{FromRow, Row};
    use std::fs;
    use std::io::BufReader;

    #[derive(Debug)]
    struct StatsByUser {
        user_name: String,
        host_name: String,
        stats: Stats,
    }

    impl ColumnSet for StatsByUser {
        fn columns() -> Vec<String> {
            vec!["user_name".into(), "host_name".into()]
        }

        fn display_values(&self) -> Vec<(&str, String)> {
            let mut acc = vec![];

            acc.push(("user_name", self.user_name.to_string()));
            acc.push(("host_name", self.host_name.to_string()));

            acc.append(&mut self.stats.display_values());

            acc
        }
    }

    impl Filter for StatsByUser {
        fn sql() -> String {
            format!(
                r#"
            SELECT qs.user_name, qs.host_name, qc.id AS query_call_id
            FROM query_calls qc
            JOIN query_call_session qs ON qs.query_call_id = qc.id
        "#
            )
        }

        fn key() -> String {
            format!("query_call_id")
        }

        fn from_row(r: SqliteRow) -> Result<Self, Error> {
            Ok(Self {
                user_name: r.try_get("user_name")?,
                host_name: r.try_get("host_name")?,
                stats: Stats::from_row(&r)?,
            })
        }
    }

    #[derive(Debug)]
    struct StatsByObject {
        schema_name: Option<String>,
        object_name: String,
        stats: Stats,
    }

    impl ColumnSet for StatsByObject {
        fn columns() -> Vec<String> {
            vec!["schema_name".into(), "object_name".into()]
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

            acc.append(&mut self.stats.display_values());

            acc
        }
    }

    impl Filter for StatsByObject {
        fn sql() -> String {
            format!(
                r#"
            SELECT do.schema_name, do.object_name, qc.id AS query_call_id
            FROM db_objects do
            JOIN query_objects qo ON qo.db_object_id = do.id
            JOIN query_calls qc ON qc.query_id = qo.query_id
            "#
            )
        }

        fn key() -> String {
            format!("query_call_id")
        }

        fn from_row(r: SqliteRow) -> Result<Self, Error> {
            Ok(Self {
                schema_name: r.try_get("schema_name")?,
                object_name: r.try_get("object_name")?,
                stats: Stats::from_row(&r)?,
            })
        }
    }

    #[tokio::test]
    async fn can_record_log() {
        let c = open_db(Some("/tmp/mysql-slowlog-analyzer-test".into()))
            .await
            .unwrap();

        let mut f = BufReader::new(File::open("data/slow-test-queries.log").unwrap());

        record_log(&c, &mut f).await.unwrap();

        let stats = query_stat_report::<StatsByUser, Stats>(&c).await.unwrap();

        println!("user stats:\n{}", stats.display_vertical());

        let stats = query_stat_report::<StatsByObject, Stats>(&c).await.unwrap();

        println!("object stats:\n{}", stats.display_vertical());
    }
}

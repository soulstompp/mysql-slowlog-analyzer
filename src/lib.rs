#[macro_use]
extern crate alloc;
extern crate core;

pub mod db;

#[doc(inline)]
pub use crate::db::{
    open_db, query_column_set, record_entry, ColumnSet, OrderBy, OrderedColumn, Ordering,
    RelationalObject, SortingPlan, Stats,
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
    #[error("invalid order by clause: {0}")]
    InvalidOrderBy(String),
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
    use crate::db::{Calls, Limit, OrderBy, OrderedColumn, SortingPlan};
    use crate::{open_db, query_column_set, ColumnSet, Stats};
    use crate::{record_log, Error};
    use core::str::FromStr;
    use fs::File;
    use sqlx::sqlite::SqliteRow;
    use sqlx::Row;
    use std::fs;
    use std::io::BufReader;

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
        let c = open_db(Some("/tmp/mysql-slowlog-analyzer-test".into()))
            .await
            .unwrap();

        let mut f = BufReader::new(File::open("data/slow-test-queries.log").unwrap());

        record_log(&c, &mut f).await.unwrap();

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

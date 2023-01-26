#[macro_use]
extern crate alloc;
extern crate core;

pub mod db;

use crate::db::record_entry;
use async_stream::try_stream;
use futures::stream::TryStreamExt;
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
    use crate::db::{open, query_stats, StatList, StatsByObject, StatsBySql, StatsByUser};
    use crate::record_log;
    use fs::File;
    use std::fs;
    use std::io::BufReader;

    #[tokio::test]
    async fn can_record_log() {
        let c = open(Some("/tmp/mysql-slowlog-analyzer-test".into()))
            .await
            .unwrap();

        let mut f = BufReader::new(File::open("data/slow-test-queries.log").unwrap());

        record_log(&c, &mut f).await.unwrap();

        let stats: Vec<StatsBySql> = query_stats(&c).await.unwrap();

        println!("sql stats:\n{}", stats.display_vertical());

        let stats: Vec<StatsByUser> = query_stats(&c).await.unwrap();

        println!("user stats:\n{}", stats.display_vertical());

        let stats: Vec<StatsByObject> = query_stats(&c).await.unwrap();

        println!("object stats:\n{}", stats.display_vertical());
    }
}

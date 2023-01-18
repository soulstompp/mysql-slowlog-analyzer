#[macro_use]
extern crate core;
extern crate alloc;

pub mod db;

use std::borrow::{Borrow, BorrowMut};
use crate::db::record_entry;
use mysql_slowlog_parser::{EntryMasking, ReadError, Reader};
use sqlx::{Row, Sqlite, SqlitePool};
use std::io::BufRead;
use thiserror::Error;
use tokio::task;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("parser error: {0}")]
    Parser(#[from] ReadError),
    #[error("db error: {0}")]
    SqlxError(#[from] sqlx::Error),
}

pub async fn record_log<'a>(c: SqlitePool, br: &'a mut dyn BufRead) -> Result<(), Error> {
    let mut r = Reader::new(br, EntryMasking::PlaceHolder)?;

    while let Some(e) = r.read_entry()? {
            record_entry(c.clone(), e).await.unwrap();
    }

    return Ok(());
}

#[cfg(test)]
mod tests {
    use crate::db::{open};
    use crate::record_log;
    use fs::File;
    use std::fs;
    use std::io::BufReader;

    #[tokio::test]
    async fn can_record_log() {
        let mut c = open(None).await.unwrap();

        let mut f = BufReader::new(File::open("data/slow-test-queries.log").unwrap());

        record_log(c, &mut f).await.unwrap();
    }
}

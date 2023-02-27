use mysql_slowlog_analyzer::{LogData, LogDataConfig};
use std::path::PathBuf;
use tokio::time::Instant;

use polars::prelude::*;

#[tokio::main]
async fn main() {
    let p = PathBuf::from("data/slow-test-queries.log");

    let mut s = LogData::open(&p, LogDataConfig::default()).await.unwrap();

    let instant = Instant::now();

    s.record_parquet().await.unwrap();

    println!("wrote in {}", instant.elapsed().as_secs_f64());

    let dir = s.parquet_dir().unwrap();

    println!("dir: {}", dir.to_string_lossy());

    let lf = LazyFrame::scan_parquet(&dir, Default::default()).unwrap();

    let df = lf.collect().unwrap();

    for r in df.iter() {
        println!("r: {}", r);
    }
}

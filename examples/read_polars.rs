use mysql_slowlog_analyzer::{LogData, LogDataConfig};
use std::path::PathBuf;
use polars::datatypes::ArrowDataType::Struct;
use polars::export::arrow::array::{Array, ListArray};
use polars::export::chrono::NaiveDateTime;
use tokio::time::Instant;

use polars::prelude::*;
use polars::prelude::aggregations::AggList;
use polars::prelude::DataType::Datetime;
use polars::prelude::TimeUnit::{Microseconds, Milliseconds};
use sha2::digest::generic_array::arr;

use polars::time::date_range;

#[tokio::main]
async fn main() {
    //let p = PathBuf::from("data/slow-test-queries.log");
    let p = PathBuf::from("/home/soulstompp/dev/mysql8-stresser/data/mysql-slow-lobsters-normal\
    .log");

    let mut s = LogData::open(&p, LogDataConfig::default()).await.unwrap();

    let instant = Instant::now();

    s.record_parquet().await.unwrap();

    println!("wrote in {}", instant.elapsed().as_secs_f64());

    let instant = Instant::now();

    let dir = s.parquet_dir().unwrap();

    println!("dir: {}", dir.to_string_lossy());

    let lf = LazyFrame::scan_parquet(&dir, Default::default()).unwrap();
    let df = lf.select([
        col("*"),
        as_struct(&[col("start_time"), (col("start_time") + col("query_time")).alias("end_time")])
            .map_list(
                |s| {
                    let ca = s.struct_()?;

                    let start_times = &ca.fields()[0].datetime().unwrap();
                    let end_times = &ca.fields()[1].datetime().unwrap();

                    let out: ListChunked =
                        start_times
                        .into_iter()
                        .zip(end_times.into_iter())
                        .map(|(start, end)| {
                           Some(date_range("run_range",
                                       NaiveDateTime::from_timestamp_millis(start.unwrap())
                                           .unwrap(),
                                       NaiveDateTime::from_timestamp_millis(end.unwrap())
                                               .unwrap(),
                                       Duration::parse("1s"),
                                       ClosedWindow::Both,
                                       TimeUnit::Milliseconds,
                                       None
                            ).unwrap().into_series())
                        }).collect();


                       Ok(Some(out.into_series()))
                },
                GetOutput::from_type(DataType::Datetime(TimeUnit::Microseconds, None)),
            )
            .alias("effected_times"),
            ])
        .explode([col("effected_times")])
        .collect().unwrap();

    println!("df: {:#?}", df);

    println!("read in {}", instant.elapsed().as_secs_f64());
    //for r in df.iter() {
        //println!("r: {}", r);
    //}
}

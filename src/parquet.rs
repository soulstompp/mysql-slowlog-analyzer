use crate::types::QueryEntry;
use futures::StreamExt;
use mysql_slowlog_parser::EntryCodec;
use polars::prelude::*;
use std::path::Path;
use tokio::fs::File;
use tokio_util::codec::FramedRead;

/// Collects parsed log entries into column vectors for building a DataFrame.
#[derive(Default)]
struct EntryCollector {
    sql: Vec<String>,
    sql_type: Vec<Option<String>>,
    objects: Vec<Option<String>>,
    start_time: Vec<i64>,
    log_time: Vec<i64>,
    user_name: Vec<String>,
    sys_user_name: Vec<String>,
    host_name: Vec<Option<String>>,
    ip_address: Vec<Option<String>>,
    thread_id: Vec<u32>,
    query_time: Vec<f64>,
    lock_time: Vec<f64>,
    rows_sent: Vec<u32>,
    rows_examined: Vec<u32>,
    request_id: Vec<Option<String>>,
    caller: Vec<Option<String>>,
    function: Vec<Option<String>>,
    line: Vec<Option<u32>>,
}

impl EntryCollector {
    fn push(&mut self, entry: QueryEntry) {
        let e = &entry.0;

        self.sql
            .push(String::from_utf8_lossy(&e.sql_attributes.sql).into_owned());
        self.sql_type
            .push(e.sql_attributes.sql_type().map(|t| t.to_string()));

        let objects_str = e.sql_attributes.objects().map(|objs| {
            objs.iter()
                .map(|o| {
                    String::from_utf8_lossy(&o.full_object_name_bytes()).into_owned()
                })
                .collect::<Vec<_>>()
                .join(",")
        });
        self.objects.push(objects_str);

        let start_ts = e
            .call
            .log_time()
            .into_offset()
            .map(|t| t.unix_timestamp() * 1_000_000)
            .unwrap_or(0);
        let log_ts = e
            .call
            .log_time()
            .into_offset()
            .map(|t| t.unix_timestamp() * 1_000_000)
            .unwrap_or(0);
        self.start_time.push(start_ts);
        self.log_time.push(log_ts);

        self.user_name
            .push(e.session.user_name().into_owned());
        self.sys_user_name
            .push(e.session.sys_user_name().into_owned());
        self.host_name
            .push(e.session.host_name().map(|s| s.into_owned()));
        self.ip_address
            .push(e.session.ip_address().map(|s| s.into_owned()));
        self.thread_id.push(e.session.thread_id());

        self.query_time.push(e.stats.query_time());
        self.lock_time.push(e.stats.lock_time());
        self.rows_sent.push(e.stats.rows_sent());
        self.rows_examined.push(e.stats.rows_examined());

        if let Some(ctx) = e.sql_attributes.statement.sql_context() {
            self.request_id
                .push(ctx.request_id().map(|s| s.into_owned()));
            self.caller.push(ctx.caller().map(|s| s.into_owned()));
            self.function.push(ctx.function().map(|s| s.into_owned()));
            self.line.push(ctx.line());
        } else {
            self.request_id.push(None);
            self.caller.push(None);
            self.function.push(None);
            self.line.push(None);
        }
    }

    fn into_dataframe(self) -> PolarsResult<DataFrame> {
        let start_time = Series::new(
            PlSmallStr::from("start_time"),
            self.start_time,
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

        let log_time = Series::new(
            PlSmallStr::from("log_time"),
            self.log_time,
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

        DataFrame::new(vec![
            Column::new(PlSmallStr::from("sql"), self.sql),
            Column::new(PlSmallStr::from("sql_type"), self.sql_type),
            Column::new(PlSmallStr::from("objects"), self.objects),
            start_time.into(),
            log_time.into(),
            Column::new(PlSmallStr::from("user_name"), self.user_name),
            Column::new(PlSmallStr::from("sys_user_name"), self.sys_user_name),
            Column::new(PlSmallStr::from("host_name"), self.host_name),
            Column::new(PlSmallStr::from("ip_address"), self.ip_address),
            Column::new(
                PlSmallStr::from("thread_id"),
                self.thread_id,
            ),
            Column::new(PlSmallStr::from("query_time"), self.query_time),
            Column::new(PlSmallStr::from("lock_time"), self.lock_time),
            Column::new(
                PlSmallStr::from("rows_sent"),
                self.rows_sent,
            ),
            Column::new(
                PlSmallStr::from("rows_examined"),
                self.rows_examined,
            ),
            Column::new(PlSmallStr::from("request_id"), self.request_id),
            Column::new(PlSmallStr::from("caller"), self.caller),
            Column::new(PlSmallStr::from("function"), self.function),
            Column::new(PlSmallStr::from("line"), self.line),
        ])
    }
}

/// Writes raw parquet from a FramedRead stream of entries.
pub async fn write_raw_parquet(
    reader: &mut FramedRead<File, EntryCodec>,
    path: &Path,
) -> Result<(), crate::Error> {
    let mut collector = EntryCollector::default();

    while let Some(res) = reader.next().await {
        match res {
            Ok(entry) => collector.push(QueryEntry(entry)),
            Err(e) => {
                eprintln!("skipping unparseable entry: {}", e);
            }
        }
    }

    let mut df = collector.into_dataframe()?;

    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_statistics(StatisticsOptions::full())
        .finish(&mut df)?;

    Ok(())
}

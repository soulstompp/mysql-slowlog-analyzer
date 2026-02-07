use polars::prelude::*;
use std::path::Path;

/// Reads raw parquet, groups by time bucket + sql, and writes aggregated parquet.
pub fn write_bucketed_parquet(
    raw_path: &Path,
    bucketed_path: &Path,
    bucket_duration: &str,
) -> Result<(), crate::Error> {
    let lf = LazyFrame::scan_parquet(raw_path, Default::default())?;

    let aggregated = lf
        .with_column(
            col("start_time")
                .dt()
                .truncate(lit(bucket_duration))
                .alias("time_bucket"),
        )
        .group_by([col("time_bucket"), col("sql"), col("sql_type")])
        .agg([
            len().alias("execution_count"),
            // query_time aggregates
            col("query_time").min().alias("query_time_min"),
            col("query_time").max().alias("query_time_max"),
            col("query_time").mean().alias("query_time_avg"),
            col("query_time")
                .quantile(lit(0.95), QuantileMethod::Linear)
                .alias("query_time_p95"),
            // lock_time aggregates
            col("lock_time").min().alias("lock_time_min"),
            col("lock_time").max().alias("lock_time_max"),
            col("lock_time").mean().alias("lock_time_avg"),
            col("lock_time")
                .quantile(lit(0.95), QuantileMethod::Linear)
                .alias("lock_time_p95"),
            // rows_sent aggregates (cast to f64 for mean/quantile)
            col("rows_sent").min().alias("rows_sent_min"),
            col("rows_sent").max().alias("rows_sent_max"),
            col("rows_sent")
                .cast(DataType::Float64)
                .mean()
                .alias("rows_sent_avg"),
            col("rows_sent")
                .cast(DataType::Float64)
                .quantile(lit(0.95), QuantileMethod::Linear)
                .alias("rows_sent_p95"),
            // rows_examined aggregates
            col("rows_examined").min().alias("rows_examined_min"),
            col("rows_examined").max().alias("rows_examined_max"),
            col("rows_examined")
                .cast(DataType::Float64)
                .mean()
                .alias("rows_examined_avg"),
            col("rows_examined")
                .cast(DataType::Float64)
                .quantile(lit(0.95), QuantileMethod::Linear)
                .alias("rows_examined_p95"),
        ])
        .sort(
            ["time_bucket", "execution_count"],
            SortMultipleOptions::default().with_order_descending_multi([false, true]),
        );

    let mut df = aggregated.collect()?;

    let file = std::fs::File::create(bucketed_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_statistics(StatisticsOptions::full())
        .finish(&mut df)?;

    Ok(())
}

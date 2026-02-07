# src/ — Rust source

## Module map

| File | Visibility | Role |
|------|-----------|------|
| `lib.rs` | pub | Crate root. Exports `LogData`, `LogDataConfig`, `Error`. Orchestrates parsing and writing. |
| `main.rs` | binary | CLI entry point (`slowlog`). Imports the library crate. Uses clap for arg parsing, outputs JSON. |
| `parquet.rs` | pub(crate) | Streaming parser → raw Parquet writer. `EntryCollector` accumulates column vectors. |
| `aggregate.rs` | pub(crate) | Reads raw.parquet → groups by (time_bucket, sql, sql_type) → writes bucketed.parquet. |
| `dirs.rs` | pub(crate) | `SourceDataDir` manages output path structure: `{base}/{sha256}/parquet/`. |
| `types.rs` | pub | `QueryEntry` newtype over `mysql_slowlog_parser::Entry`. |
| `metrics.rs` | pub(crate) | Empty placeholder behind `otlp` feature flag. |

## Data flow

1. `LogData::open()` — opens the log file, computes SHA256 hash, resolves output directory
2. `LogData::record_raw()` — creates `FramedRead` stream → `parquet::write_raw_parquet()` → `raw.parquet`
3. `LogData::record_bucketed()` — calls `record_raw()` first, then `aggregate::write_bucketed_parquet()` → `bucketed.parquet`

## Key patterns

### Async boundary
The library is async (tokio). Parsing in `parquet.rs` uses `FramedRead` + `StreamExt::next().await`. Aggregation in `aggregate.rs` is synchronous (Polars LazyFrame on an already-written file). The CLI uses `#[tokio::main]`.

### Error handling
- `Error` enum in `lib.rs` uses `thiserror` derive.
- Parsing errors in `parquet.rs` are **non-fatal**: individual malformed entries print to stderr and are skipped. This is deliberate — real slow logs contain garbage.
- All other errors (I/O, Polars, directory) are fatal and propagate up.

### Parquet writing
Both writers use:
- `ParquetWriter::new(file).with_compression(ParquetCompression::Zstd(None)).with_statistics(StatisticsOptions::full())`
- Zstd compression for size, full statistics for predicate pushdown.

### Column type conversions in parquet.rs
- Timestamps are stored as `i64` microseconds, then cast to `Datetime(Microseconds, None)` via `with_column`.
- `rows_sent` and `rows_examined` are `u32` in raw, cast to `f64` in aggregate.rs for mean/quantile calculations.

### Idempotency
`record_raw()` checks if `raw.parquet` exists and returns early if so. To re-record, the caller must delete the hash directory. `record_bucketed()` always overwrites `bucketed.parquet` (it may be called with different bucket durations).

## Testing

Two integration tests in `lib.rs`:
- `can_record_raw` — writes to `/tmp/can_record_raw`, verifies 18 columns exist and row count > 0
- `can_record_bucketed` — writes to `/tmp/can_record_bucketed`, verifies 20 columns

Both use `data/slow-test-queries.log` as input. Tests clean up their output directory before running.

Run with: `cargo test`

## Adding new functionality

- **New aggregate columns**: modify `aggregate.rs` `agg([...])` list, update the test in `lib.rs` to check for the new column.
- **New raw columns**: modify `EntryCollector` in `parquet.rs` (add field, update `push()`, update `into_df()`), update test expected columns.
- **New CLI flags**: add to the `Cli` struct in `main.rs` (clap derive), wire through to `LogDataConfig` or pass to library methods.
- **New error variants**: add to the `Error` enum in `lib.rs` with a `#[error("...")]` message.
- The `main.rs` binary should only use public API from the library. Do not make internal modules pub for the CLI's sake.

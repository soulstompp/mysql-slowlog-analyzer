# mysql-slowlog-analyzer

Parses MySQL slow query logs into Parquet files (raw entries + time-bucketed aggregates) for analysis with Polars or any Parquet-compatible tool. Ships a CLI binary (`slowlog`) and a Nushell module (`nu/slowlog.nu`) for interactive exploration.

## Architecture

```
MySQL slow log file
        │
  ┌─────▼──────┐
  │  main.rs    │  CLI: parse args, call library, emit JSON
  └─────┬──────┘
        │
  ┌─────▼──────┐
  │  lib.rs     │  Orchestrator: open log, hash file, delegate to parquet/aggregate
  │             │  Public API: LogData, LogDataConfig, Error
  └──┬──────┬──┘
     │      │
┌────▼──┐ ┌─▼──────────┐
│parquet │ │ aggregate   │  parquet.rs: stream-parse → raw.parquet
│  .rs   │ │    .rs      │  aggregate.rs: raw.parquet → bucketed.parquet (LazyFrame)
└────────┘ └─────────────┘
     │           │
  ┌──▼───────────▼──┐
  │    dirs.rs       │  Path management: {data_dir}/{sha256}/parquet/{raw,bucketed}.parquet
  └─────────────────┘
```

### Source files

| File | Role |
|------|------|
| `src/lib.rs` | Public API: `LogData` (open, record_raw, record_bucketed), `LogDataConfig`, `Error` enum. Hashes the log file with SHA256 to create a unique output directory. |
| `src/parquet.rs` | Streaming parser. Uses `tokio_util::codec::FramedRead` with `mysql-slowlog-parser::EntryCodec`. Collects entries into column vectors, builds a DataFrame, writes Zstd-compressed Parquet. Skips unparseable entries (logs to stderr). |
| `src/aggregate.rs` | Reads raw.parquet as a Polars LazyFrame. Groups by `(time_bucket, sql, sql_type)`. Produces min/max/avg/p95 for query_time, lock_time, rows_sent, rows_examined. |
| `src/dirs.rs` | `SourceDataDir`: resolves output paths. Uses `directories` crate for platform default, or a user-supplied `--data-dir`. Structure: `{base}/{sha256_hash}/parquet/`. |
| `src/types.rs` | `QueryEntry` — newtype wrapper around `mysql_slowlog_parser::Entry` with `Deref`. |
| `src/metrics.rs` | Placeholder for optional OpenTelemetry metrics (behind `otlp` feature flag, currently empty). |
| `src/main.rs` | CLI binary (`slowlog`). Parses args with clap, calls `LogData::open()` + `record_bucketed()`, prints JSON with absolute paths to generated parquet files. |
| `nu/slowlog.nu` | Nushell module with 6 commands for recording and querying parquet output. See `nu/AGENTS.md`. |

### Key data types

**`LogData`** — Main entry point. Created via `LogData::open(path, config).await`. Methods:
- `record_raw() -> PathBuf` — parse log → raw.parquet (idempotent: skips if file exists)
- `record_bucketed(bucket_duration) -> PathBuf` — calls record_raw internally, then aggregates
- `raw_parquet_path()`, `bucketed_parquet_path()`, `parquet_dir()` — path accessors

**`LogDataConfig`** — `{ data_path: Option<PathBuf>, codec_config: EntryCodecConfig }`

**`Error`** — variants: `LogAlreadyRecorded`, `DirError`, `IO`, `Reader`, `Polars`

### Parquet schemas

**raw.parquet** (18 columns — one row per slow query entry):

| Column | Type | Notes |
|--------|------|-------|
| sql | String | Normalized SQL (literal values replaced with `?`) |
| sql_type | Option\<String\> | SELECT, INSERT, UPDATE, DELETE, etc. |
| objects | Option\<String\> | Comma-separated table names |
| start_time | Datetime(µs) | When the query was logged |
| log_time | Datetime(µs) | Same as start_time |
| user_name | String | MySQL user |
| sys_user_name | String | OS-level user |
| host_name | Option\<String\> | Client hostname |
| ip_address | Option\<String\> | Client IP |
| thread_id | u32 | MySQL connection thread ID |
| query_time | f64 | Execution time in seconds |
| lock_time | f64 | Lock wait time in seconds |
| rows_sent | u32 | Rows returned to client |
| rows_examined | u32 | Rows scanned by engine |
| request_id | Option\<String\> | From SQL comment context |
| caller | Option\<String\> | From SQL comment context |
| function | Option\<String\> | From SQL comment context |
| line | Option\<u32\> | From SQL comment context |

**bucketed.parquet** (20 columns — one row per (time_bucket, sql, sql_type)):

Groups raw entries by truncated `start_time` (default 1s buckets). For each of `query_time`, `lock_time`, `rows_sent`, `rows_examined`: produces `*_min`, `*_max`, `*_avg`, `*_p95`. Also includes `execution_count`. Sorted by time_bucket asc, execution_count desc.

## CLI usage

```
slowlog <FILE> [--data-dir DIR] [--bucket-duration DURATION]
```

Outputs JSON to stdout:
```json
{
  "log_file": "/absolute/path/to/slow.log",
  "raw_parquet": "/path/to/raw.parquet",
  "bucketed_parquet": "/path/to/bucketed.parquet",
  "parquet_dir": "/path/to/parquet/"
}
```

Errors go to stderr, exit code 1. Input path is canonicalized so output always contains absolute paths.

## Dependencies

- **polars 0.46** (parquet, lazy, dtype-struct features) — core data engine
- **mysql-slowlog-parser 0.5** — MySQL slow log format parsing
- **tokio** (rt-multi-thread) — async runtime for streaming I/O
- **tokio-util** (codec) — `FramedRead` for streaming parsed entries
- **clap 4** (derive) — CLI argument parsing
- **serde / serde_json** — JSON serialization for CLI output
- **sha2** — SHA256 file hashing for deduplication
- **thiserror** — error derive macros
- **directories** — platform-specific default data directory
- **winnow_datetime** — date/time parsing (used by mysql-slowlog-parser)

## Building and testing

```sh
cargo build          # compiles library + slowlog binary
cargo test           # 2 integration tests (can_record_raw, can_record_bucketed)
cargo clippy         # lint check
```

Tests use `data/slow-test-queries.log` and write to `/tmp/can_record_raw` and `/tmp/can_record_bucketed`. They verify column presence and row count.

Manual CLI test:
```sh
cargo run -- data/slow-test-queries.log --data-dir /tmp/test-cli
```

## Design decisions and conventions

- **Async everywhere**: library API is async (tokio). The CLI uses `#[tokio::main]`.
- **Fault-tolerant parsing**: unparseable log entries are skipped with a stderr warning, not fatal errors. This is intentional — real slow logs often have malformed entries.
- **SHA256 deduplication**: the log file is hashed and the hash becomes the output directory name. This prevents accidentally reprocessing the same log. To re-process, delete the hash directory.
- **Idempotent raw recording**: `record_raw()` returns early if `raw.parquet` already exists.
- **Polars LazyFrame for aggregation**: aggregation in `aggregate.rs` uses lazy evaluation for query optimization. The raw parquet writer uses eager collection because it streams from an async reader.
- **Zstd compression + full statistics**: both parquet files use Zstd compression and write full column statistics for efficient predicate pushdown.
- **No lib.rs changes for CLI**: the binary imports the library as a crate dependency. `main.rs` only uses public API.

## Leftover artifacts

- `migrations/init.sql` — DuckDB schema from a previous architecture iteration. Not used by current code.
- `examples/read_duckdb.rs` — deleted in current branch (was a DuckDB usage example).
- `README.md` — describes the old SQLite/relational schema. Outdated.

## Feature flags

- `otlp` — enables OpenTelemetry metrics via `opentelemetry` + `opentelemetry-otlp`. Currently a placeholder (`src/metrics.rs` is empty).

## Git workflow

- **main**: stable branch
- **polars-focused**: current development branch (moved from DuckDB to pure Polars/Parquet)
- `Cargo.lock` is committed (binary target)

# nu/slowlog.nu — Nushell module

This directory contains a Nushell module for interactive MySQL slow log analysis. It wraps the `slowlog` CLI binary for recording and uses Nushell's `polars` plugin for querying the generated Parquet files.

## Prerequisites

- **Nushell** (v0.100+)
- **polars plugin** (`nu_plugin_polars`) — required for all query commands (`top`, `by-type`, `by-user`, `timeline`, `outliers`)
- **slowlog binary** — either in `$PATH` or pointed to by `$env.SLOWLOG_BIN`

## Loading the module

```nushell
use nu/slowlog.nu
```

All commands are under the `slowlog` namespace for tab completion.

## Commands

### Recording

**`slowlog record <file> [--data-dir] [--bucket-duration "1s"]`**

Calls the `slowlog` CLI binary. Returns a record:
```nushell
{ log_file: "/abs/path", raw_parquet: "...", bucketed_parquet: "...", parquet_dir: "..." }
```

Binary resolution: checks `$env.SLOWLOG_BIN` first, falls back to `which slowlog`.

### Raw parquet queries (operate on `raw_parquet` path)

**`slowlog top <file> [--limit 10] [--by query_time]`**
Top N queries sorted by a metric descending. Selects: sql, sql_type, user_name, the sort column, rows_sent, rows_examined, start_time.

**`slowlog by-type <file>`**
Group by sql_type. Aggregates: count, total/avg query_time, total/avg lock_time. Sorted by count desc.

**`slowlog by-user <file>`**
Group by user_name. Same aggregates as by-type. Sorted by total_query_time desc.

### Bucketed parquet queries (operate on `bucketed_parquet` path)

**`slowlog timeline <file>`**
Re-aggregates bucketed data by time_bucket only: total_queries, avg_query_time, max_p95, peak_query_time. Sorted by time_bucket asc.

**`slowlog outliers <file> [--threshold 3.0]`**
Finds queries where `p95 / avg >= threshold`. Shows: sql, sql_type, execution_count, query_time_avg, query_time_p95, variance_ratio, time_bucket. Sorted by variance_ratio desc.

## Typical workflow

```nushell
use nu/slowlog.nu

# Record a log file (generates parquet)
let result = slowlog record data/slow-test-queries.log --data-dir /tmp/analysis

# Query raw data
slowlog top $result.raw_parquet --limit 20 --by lock_time
slowlog by-type $result.raw_parquet
slowlog by-user $result.raw_parquet

# Query bucketed data
slowlog timeline $result.bucketed_parquet
slowlog outliers $result.bucketed_parquet --threshold 2.0
```

## Design notes

- **No Nushell plugin**: intentionally avoids the Nushell plugin system (unstable, version-locked, pre-1.0). Instead, the CLI binary outputs JSON and the `.nu` module wraps it.
- **polars lazy pipeline**: all query commands use `polars open` (lazy) → transforms → `polars collect` → `polars into-nu`. This keeps memory usage low for large parquet files.
- **Shell-agnostic CLI**: the binary outputs JSON, so it works with any shell or scripting language. The `.nu` module is a convenience layer.

## Editing guidelines

- Commands follow the pattern: `polars open` → lazy transforms → `polars collect` → `polars into-nu`. Keep this consistent.
- The `--by` flag on `slowlog top` must reference a column that exists in `raw.parquet`. Valid sort columns: query_time, lock_time, rows_sent, rows_examined, start_time.
- The `--threshold` on `slowlog outliers` is a float ratio (p95/avg). Default 3.0 is a reasonable starting point; lower values catch more queries.
- When adding new commands, follow the `slowlog <verb>` naming convention for tab completion.
- Raw parquet columns are documented in the root `CLAUDE.md` under "Parquet schemas".

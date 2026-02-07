# MySQL slow log analyzer — Nushell module
#
# Usage:
#   use nu/slowlog.nu
#   slowlog record data/slow-test-queries.log | slowlog top $in.raw_parquet

# Resolve the slowlog binary path from $env.SLOWLOG_BIN or PATH.
def slowlog-bin []: nothing -> string {
    $env.SLOWLOG_BIN? | default (which slowlog | get 0.path)
}

# Parse a MySQL slow log into raw + bucketed parquet files.
# Returns a record with log_file, raw_parquet, bucketed_parquet, parquet_dir.
export def "slowlog record" [
    file: path          # Path to the MySQL slow log file
    --data-dir: path    # Directory to store parquet output
    --bucket-duration: string = "1s" # Polars duration string for time buckets
]: nothing -> record {
    mut args = [$file "--bucket-duration" $bucket_duration]

    if $data_dir != null {
        $args = ($args | append ["--data-dir" $data_dir])
    }

    let bin = slowlog-bin
    run-external $bin ...$args | from json
}

# Top N queries from raw parquet by a given metric (descending).
export def "slowlog top" [
    file: path        # Path to raw parquet file
    --limit: int = 10 # Number of results to return
    --by: string = "query_time" # Column to sort by
]: nothing -> table {
    polars open $file --type parquet
    | polars select sql sql_type user_name $by rows_sent rows_examined start_time
    | polars sort-by $by --reverse
    | polars first $limit
    | polars collect
    | polars into-nu
}

# Aggregate raw queries grouped by SQL type.
export def "slowlog by-type" [
    file: path  # Path to raw parquet file
]: nothing -> table {
    polars open $file --type parquet
    | polars group-by sql_type
    | polars agg [
        (polars col sql | polars count | polars as count)
        (polars col query_time | polars sum | polars as total_query_time)
        (polars col query_time | polars mean | polars as avg_query_time)
        (polars col lock_time | polars sum | polars as total_lock_time)
        (polars col lock_time | polars mean | polars as avg_lock_time)
    ]
    | polars sort-by count --reverse
    | polars collect
    | polars into-nu
}

# Aggregate raw queries grouped by user.
export def "slowlog by-user" [
    file: path  # Path to raw parquet file
]: nothing -> table {
    polars open $file --type parquet
    | polars group-by user_name
    | polars agg [
        (polars col sql | polars count | polars as count)
        (polars col query_time | polars sum | polars as total_query_time)
        (polars col query_time | polars mean | polars as avg_query_time)
        (polars col lock_time | polars sum | polars as total_lock_time)
        (polars col lock_time | polars mean | polars as avg_lock_time)
    ]
    | polars sort-by total_query_time --reverse
    | polars collect
    | polars into-nu
}

# Timeline view: re-aggregate bucketed parquet by time_bucket only.
export def "slowlog timeline" [
    file: path  # Path to bucketed parquet file
]: nothing -> table {
    polars open $file --type parquet
    | polars group-by time_bucket
    | polars agg [
        (polars col execution_count | polars sum | polars as total_queries)
        (polars col query_time_avg | polars mean | polars as avg_query_time)
        (polars col query_time_p95 | polars max | polars as max_p95)
        (polars col query_time_max | polars max | polars as peak_query_time)
    ]
    | polars sort-by time_bucket
    | polars collect
    | polars into-nu
}

# Find queries where p95 is disproportionately high relative to the average.
# A variance_ratio (p95/avg) above the threshold flags unstable queries.
export def "slowlog outliers" [
    file: path              # Path to bucketed parquet file
    --threshold: float = 3.0 # Minimum p95/avg ratio to flag
]: nothing -> table {
    polars open $file --type parquet
    | polars with-column [(
        (polars col query_time_p95) / (polars col query_time_avg) | polars as variance_ratio
    )]
    | polars filter-with ((polars col variance_ratio) >= $threshold)
    | polars select sql sql_type execution_count query_time_avg query_time_p95 variance_ratio time_bucket
    | polars sort-by variance_ratio --reverse
    | polars collect
    | polars into-nu
}

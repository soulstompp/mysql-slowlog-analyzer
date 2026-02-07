use std::path::PathBuf;
use std::process;

use clap::Parser;
use mysql_slowlog_analyzer::{LogData, LogDataConfig};
use serde::Serialize;

#[derive(Parser)]
#[command(name = "slowlog", about = "Parse MySQL slow logs into parquet files")]
struct Cli {
    /// Path to the MySQL slow log file
    file: PathBuf,

    /// Directory to store parquet output (default: platform data dir)
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Polars duration string for time buckets (e.g. "1s", "5m", "1h")
    #[arg(long, default_value = "1s")]
    bucket_duration: String,
}

#[derive(Serialize)]
struct Output {
    log_file: PathBuf,
    raw_parquet: PathBuf,
    bucketed_parquet: PathBuf,
    parquet_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let path = cli.file.canonicalize()?;

    let config = LogDataConfig {
        data_path: cli.data_dir,
        codec_config: Default::default(),
    };

    let mut log_data = LogData::open(&path, config).await?;
    log_data.record_bucketed(Some(&cli.bucket_duration)).await?;

    let output = Output {
        log_file: path,
        raw_parquet: log_data.raw_parquet_path()?,
        bucketed_parquet: log_data.bucketed_parquet_path()?,
        parquet_dir: log_data.parquet_dir()?,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);

    Ok(())
}

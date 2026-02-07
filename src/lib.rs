mod aggregate;
mod dirs;
mod parquet;
pub mod types;

#[cfg(feature = "otlp")]
mod metrics;

use std::io;
use std::path::{Path, PathBuf};

use mysql_slowlog_parser::EntryCodec;
use polars::prelude::PolarsError;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::{create_dir_all, File};
use tokio_util::codec::FramedRead;

use crate::dirs::{DirError, SourceDataDir};

#[derive(Error, Debug)]
pub enum Error {
    #[error("this log has already been recorded, delete the corresponding directory to record again")]
    LogAlreadyRecorded,
    #[error("{0}")]
    DirError(#[from] DirError),
    #[error("duck db error: {0}")]
    DuckDbError(#[from] DuckDbError),
    #[error("io error: {0}")]
    IO(#[from] io::Error),
    #[error("reader error: {0}")]
    Reader(#[from] mysql_slowlog_parser::ReadError),
    #[error("polars error: {0}")]
    Polars(#[from] PolarsError),
}

pub struct LogData {
    path: PathBuf,
    dirs: SourceDataDir,
}

impl LogData {
    pub async fn open(p: &Path, config: LogDataConfig) -> Result<Self, Error> {
        let dirs = Self::data_dir(p, &config).await?;

        Ok(Self {
            path: p.to_path_buf(),
            dirs,
        })
    }

    async fn data_dir(p: &Path, config: &LogDataConfig) -> Result<SourceDataDir, Error> {
        let f = File::open(p).await?;

        let mut hash = Sha256::default();
        io::copy(&mut f.into_std().await, &mut hash)?;
        let hash = format!("{:x}", hash.finalize());

        Ok(SourceDataDir {
            hash,
            data_dir: config.data_path.clone(),
        })
    }

    pub fn parquet_dir(&self) -> Result<PathBuf, Error> {
        self.dirs.parquet_dir()
    }

    pub fn raw_parquet_path(&self) -> Result<PathBuf, Error> {
        self.dirs.raw_parquet_path()
    }

    pub fn bucketed_parquet_path(&self) -> Result<PathBuf, Error> {
        self.dirs.bucketed_parquet_path()
    }

    /// Parse the slow log and write a raw parquet file. Returns the path to the written file.
    pub async fn record_raw(&mut self) -> Result<PathBuf, Error> {
        let raw_path = self.dirs.raw_parquet_path()?;

        if raw_path.exists() {
            return Ok(raw_path);
        }

        let parquet_dir = self.dirs.parquet_dir()?;
        create_dir_all(&parquet_dir).await?;

        let mut reader = self.reader().await?;
        parquet::write_raw_parquet(&mut reader, &raw_path).await?;

        Ok(raw_path)
    }

    /// Parse and aggregate into time-bucketed parquet. Writes raw first if needed.
    /// `bucket_duration` is a polars duration string, e.g. "1s", "5m", "1h". Defaults to "1s".
    pub async fn record_bucketed(&mut self, bucket_duration: Option<&str>) -> Result<PathBuf, Error> {
        let bucket_duration = bucket_duration.unwrap_or("1s");
        let raw_path = self.record_raw().await?;
        let bucketed_path = self.dirs.bucketed_parquet_path()?;

        aggregate::write_bucketed_parquet(&raw_path, &bucketed_path, bucket_duration)?;

        Ok(bucketed_path)
    }

    pub async fn reader(&self) -> Result<FramedRead<File, EntryCodec>, Error> {
        let f = File::open(&self.path).await?;
        Ok(FramedRead::with_capacity(
            f,
            EntryCodec::new(self.config.codec_config),
            30000000,
        ))
    }
}

#[derive(Clone, Debug, Default)]
pub struct LogDataConfig {
    pub data_path: Option<PathBuf>,
    pub codec_config: EntryCodecConfig,
}

#[cfg(test)]
mod tests {
    use crate::{LogData, LogDataConfig};
    use std::fs::{metadata, remove_dir_all};
    use std::path::PathBuf;

    #[tokio::test]
    async fn can_record_raw() {
        let p = PathBuf::from("data/slow-test-queries.log");
        let data_dir = PathBuf::from("/tmp/can_record_raw");

        if metadata(&data_dir).is_ok() {
            remove_dir_all(&data_dir).unwrap();
        }

        let config = LogDataConfig {
            data_path: Some(data_dir),
        };

        let mut s = LogData::open(&p, config).await.unwrap();
        let raw_path = s.record_raw().await.unwrap();

        assert!(raw_path.exists());

        // Verify the file is readable and has expected columns
        let lf = polars::prelude::LazyFrame::scan_parquet(&raw_path, Default::default()).unwrap();
        let df = lf.collect().unwrap();

        assert!(df.height() > 0, "raw parquet should have rows");

        let expected_cols = [
            "sql", "sql_type", "objects", "start_time", "log_time",
            "user_name", "sys_user_name", "host_name", "ip_address", "thread_id",
            "query_time", "lock_time", "rows_sent", "rows_examined",
            "request_id", "caller", "function", "line",
        ];

        for col in expected_cols {
            assert!(
                df.column(col).is_ok(),
                "raw parquet should have column '{}'",
                col
            );
        }
    }

    #[tokio::test]
    async fn can_record_bucketed() {
        let p = PathBuf::from("data/slow-test-queries.log");
        let data_dir = PathBuf::from("/tmp/can_record_bucketed");

        if metadata(&data_dir).is_ok() {
            remove_dir_all(&data_dir).unwrap();
        }

        let config = LogDataConfig {
            data_path: Some(data_dir),
            codec_config: Default::default(),
        };

        let mut s = LogData::open(&p, config).await.unwrap();
        let bucketed_path = s.record_bucketed(None).await.unwrap();

        assert!(bucketed_path.exists());

        let lf =
            polars::prelude::LazyFrame::scan_parquet(&bucketed_path, Default::default()).unwrap();
        let df = lf.collect().unwrap();

        assert!(df.height() > 0, "bucketed parquet should have rows");

        let expected_cols = [
            "time_bucket",
            "sql",
            "sql_type",
            "execution_count",
            "query_time_min",
            "query_time_max",
            "query_time_avg",
            "query_time_p95",
            "lock_time_min",
            "lock_time_max",
            "lock_time_avg",
            "lock_time_p95",
            "rows_sent_min",
            "rows_sent_max",
            "rows_sent_avg",
            "rows_sent_p95",
            "rows_examined_min",
            "rows_examined_max",
            "rows_examined_avg",
            "rows_examined_p95",
        ];

        for col in expected_cols {
            assert!(
                df.column(col).is_ok(),
                "bucketed parquet should have column '{}'",
                col
            );
        }
    }
}

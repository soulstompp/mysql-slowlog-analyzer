use directories::ProjectDirs;
use std::path::PathBuf;

use crate::Error;

#[derive(Error, Debug)]
pub enum DirError {
    #[error("unable to create project directory")]
    UnableToCreateProjectDir,
}

fn project_dirs() -> Result<ProjectDirs, Error> {
    ProjectDirs::from("org", "MySQL Analyzer", "MySQL Analyzer")
        .ok_or(DirError::UnableToCreateProjectDir.into())
}

#[derive(Clone)]
pub struct SourceDataDir {
    pub hash: String,
    pub data_dir: Option<PathBuf>,
}

impl SourceDataDir {
    pub fn sample_data_dir(&self) -> Result<PathBuf, Error> {
        let mut p = if let Some(p) = &self.data_dir {
            p.to_path_buf()
        } else {
            PathBuf::from(project_dirs()?.data_dir())
        };

        p.push(&self.hash);

        Ok(p)
    }

    pub fn parquet_dir(&self) -> Result<PathBuf, Error> {
        let mut p = self.sample_data_dir()?;
        p.push("parquet");
        Ok(p)
    }

    pub fn raw_parquet_path(&self) -> Result<PathBuf, Error> {
        let mut p = self.parquet_dir()?;
        p.push("raw.parquet");
        Ok(p)
    }

    pub fn bucketed_parquet_path(&self) -> Result<PathBuf, Error> {
        let mut p = self.parquet_dir()?;
        p.push("bucketed.parquet");
        Ok(p)
    }
}

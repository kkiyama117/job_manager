//! `<job_dir>/.status.toml` の atomic read / write.

use std::path::Path;

use crate::error::JobManagerError;
use crate::job::run::JobRun;

#[must_use = "read_job_run returns the parsed JobRun; ignoring it drops the data"]
pub fn read_job_run(path: &Path) -> Result<JobRun, JobManagerError> {
    let text = super::read_toml_string(path)?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_job_run(path: &Path, run: &JobRun) -> Result<(), JobManagerError> {
    let text = toml::to_string(run)?;
    super::atomic_write(path, text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::lifecycle::Lifecycle;
    use tempfile::tempdir;

    #[test]
    fn round_trip_through_disk() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("sample/.status.toml");
        let run = JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: chrono::Utc::now(),
            slurm_jobid: Some(42),
            slurm_status: None,
            note: Some("hello".to_string()),
        };
        write_job_run(&p, &run).unwrap();
        let restored = read_job_run(&p).unwrap();
        assert_eq!(restored.lifecycle, Lifecycle::Queued);
        assert_eq!(restored.slurm_jobid, Some(42));
        assert_eq!(restored.note.as_deref(), Some("hello"));
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("missing.toml");
        let err = read_job_run(&p).unwrap_err();
        assert!(err.to_string().contains("missing.toml"));
    }

    #[test]
    fn write_creates_parent_dirs() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("a/b/c/.status.toml");
        let run = JobRun {
            lifecycle: Lifecycle::Running,
            updated_at: chrono::Utc::now(),
            slurm_jobid: None,
            slurm_status: None,
            note: None,
        };
        write_job_run(&p, &run).unwrap();
        assert!(p.exists());
    }
}

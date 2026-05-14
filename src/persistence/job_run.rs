//! `<job_dir>/.status.toml` の atomic read / write.

use std::path::Path;

use crate::error::JobManagerError;
use crate::job::run::JobRun;

#[must_use = "read_job_run returns the parsed JobRun; ignoring it drops the data"]
pub fn read_job_run(path: &Path) -> Result<JobRun, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_job_run(path: &Path, run: &JobRun) -> Result<(), JobManagerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let text = toml::to_string(run)?;
    // Suffix tmp file name with PID + nanos + thread id so neither cross-
    // process nor intra-process concurrent writers collide on the same
    // intermediate path.
    let tmp = path.with_extension(super::tmp_extension());
    let result = std::fs::write(&tmp, text)
        .map_err(|source| JobManagerError::Io {
            path: tmp.clone(),
            source,
        })
        .and_then(|()| {
            std::fs::rename(&tmp, path).map_err(|source| JobManagerError::Io {
                path: path.to_path_buf(),
                source,
            })
        });
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
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

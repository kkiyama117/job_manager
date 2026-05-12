//! Atomic read/write for `StatusEntry`.

use std::path::Path;

use crate::error::JobManagerError;
use crate::status::StatusEntry;

pub fn read_status(path: &Path) -> Result<StatusEntry, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_status(path: &Path, entry: &StatusEntry) -> Result<(), JobManagerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let body = toml::to_string_pretty(entry)?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, body).map_err(|source| JobManagerError::Io {
        path: tmp.clone(),
        source,
    })?;
    std::fs::rename(&tmp, path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::PerJobStatus;
    use chrono::Utc;
    use tempfile::TempDir;

    fn entry() -> StatusEntry {
        StatusEntry {
            lifecycle: PerJobStatus::Running,
            updated_at: Utc::now(),
            slurm_jobid: Some(12345),
            slurm_status: None,
            note: Some("promoted".to_string()),
        }
    }

    #[test]
    fn write_then_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("g16/.status.toml");
        let e = entry();
        write_status(&path, &e).unwrap();
        let back = read_status(&path).unwrap();
        assert_eq!(back.lifecycle, e.lifecycle);
        assert_eq!(back.slurm_jobid, e.slurm_jobid);
        assert_eq!(back.note, e.note);
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("missing.toml");
        let err = read_status(&path).unwrap_err();
        assert!(err.to_string().contains("missing.toml"));
    }
}

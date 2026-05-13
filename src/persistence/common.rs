//! `<root>/common.toml` read / write.

use std::path::Path;

use gaussian_job_shared::config::common::CommonConfig;

use crate::error::JobManagerError;

#[must_use = "read_common returns the parsed CommonConfig; ignoring it drops the data"]
pub fn read_common(path: &Path) -> Result<CommonConfig, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_common(path: &Path, common: &CommonConfig) -> Result<(), JobManagerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let text = toml::to_string(common)?;
    let tmp = path.with_extension("toml.tmp");
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
    use gaussian_job_shared::config::common::DirectoryConfig;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn sample() -> CommonConfig {
        CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: "long".to_string(),
                time_limit: None,
                log_stdout: None,
                log_stderr: None,
                comment: None,
                job_name: None,
                array_spec: None,
                dependency: None,
                mail_user: None,
                mail_types: None,
                resource_spec: None,
            },
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
            },
        }
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("common.toml");
        let original = sample();
        write_common(&p, &original).unwrap();
        let restored = read_common(&p).unwrap();
        assert_eq!(restored.slurm_default.partition, "long");
        assert_eq!(restored.directories.project_root, PathBuf::from("/work"));
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("nonexistent.toml");
        let result = read_common(&p);
        assert!(matches!(result, Err(JobManagerError::Io { .. })));
    }
}

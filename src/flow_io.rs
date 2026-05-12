//! Read / write `JobFlow` (TOML) with atomic-rename writes.

use std::path::Path;

use gaussian_job_shared::entities::workflow::JobFlow;

use crate::error::JobManagerError;

/// Read a `JobFlow` from a TOML file at `path`.
pub fn read_flow(path: &Path) -> Result<JobFlow, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

/// Write `flow` to `path` atomically (write to `<path>.tmp` then rename).
/// Creates parent directories if missing.
pub fn write_flow(path: &Path, flow: &JobFlow) -> Result<(), JobManagerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let body = toml::to_string_pretty(flow)?;
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
    use chrono::Utc;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn sample_config() -> SlurmJobConfig {
        SlurmJobConfig {
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
        }
    }

    fn sample_flow() -> JobFlow {
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId::from("g16"),
            Job {
                spec: JobSpec {
                    program: Program::from("g16"),
                    config: sample_config(),
                    body: "echo hi\n".to_string(),
                },
                parents: vec![],
            },
        );
        jobs.insert(
            JobId::from("post"),
            Job {
                spec: JobSpec {
                    program: Program::from("formchk"),
                    config: sample_config(),
                    body: "echo done\n".to_string(),
                },
                parents: vec![JobEdge {
                    from: JobId::from("g16"),
                    kind: DependencyType::AfterOk,
                }],
            },
        );
        JobFlow {
            uuid: Uuid::now_v7(),
            created_at: Utc::now(),
            work_dir: PathBuf::from("/tmp/flow"),
            tags: BTreeMap::new(),
            jobs,
        }
    }

    #[test]
    fn roundtrip_write_read_recovers_jobflow() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.toml");
        let original = sample_flow();
        write_flow(&path, &original).unwrap();
        let back = read_flow(&path).unwrap();
        assert_eq!(back.uuid, original.uuid);
        assert_eq!(back.jobs.len(), 2);
        assert!(back.jobs.contains_key(&JobId::from("g16")));
    }

    #[test]
    fn read_missing_file_returns_io_error_with_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does_not_exist.toml");
        let err = read_flow(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does_not_exist.toml"), "msg = {msg}");
    }

    #[test]
    fn write_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a/b/c");
        let path = nested.join("flow.toml");
        let flow = sample_flow();
        write_flow(&path, &flow).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn write_leaves_no_tmp_file_on_success() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.toml");
        write_flow(&path, &sample_flow()).unwrap();
        let tmp = path.with_extension("toml.tmp");
        assert!(!tmp.exists(), "tmp file leaked: {tmp:?}");
    }
}

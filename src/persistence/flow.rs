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
    // Suffix tmp file name with PID + nanos + thread id so neither cross-
    // process nor intra-process concurrent writers collide on the same
    // intermediate path.
    let tmp = path.with_extension(super::tmp_extension());
    let result = std::fs::write(&tmp, body)
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
        // L-3: write/rename どちらの失敗でも tmp が残る可能性があるため best-effort で削除。
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};
    use std::collections::BTreeMap;
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

    fn lingering_tmp_files(parent: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(parent)
            .unwrap()
            .filter_map(|e| {
                let p = e.ok()?.path();
                let is_tmp = p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"));
                if is_tmp { Some(p) } else { None }
            })
            .collect()
    }

    #[test]
    fn write_leaves_no_tmp_file_on_success() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.toml");
        write_flow(&path, &sample_flow()).unwrap();
        let leaks = lingering_tmp_files(dir.path());
        assert!(leaks.is_empty(), "tmp files leaked: {leaks:?}");
    }

    #[test]
    fn write_flow_cleans_up_tmp_on_rename_failure() {
        // L-3: rename 失敗時に .toml.<pid>.tmp が残らないことを検証。
        // target が既存ディレクトリだと rename(file, dir) は失敗するので、それで誘発する。
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.toml");
        std::fs::create_dir_all(&path).unwrap();
        let flow = sample_flow();
        let result = write_flow(&path, &flow);
        assert!(result.is_err());
        let leaks = lingering_tmp_files(dir.path());
        assert!(leaks.is_empty(), "tmp files leaked: {leaks:?}");
    }
}

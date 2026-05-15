//! Read / write `JobFlow` (TOML) with atomic-rename writes.

use std::path::Path;

use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};

use crate::error::JobManagerError;

/// Walk `[jobs.*.config]` tables, ensuring each has a `partition` key.
/// Missing tables are created. Missing partition entries are filled from
/// `common_partition` (passed as `Option<&str>` so the caller can express
/// "common has no partition either"). Returns `PartitionMissing { job }`
/// if both flow and common lack a partition for some job.
fn inject_partition_defaults(
    v: &mut toml::Value,
    common_partition: Option<&str>,
) -> Result<(), JobManagerError> {
    let jobs = match v.get_mut("jobs").and_then(|j| j.as_table_mut()) {
        Some(t) => t,
        None => return Ok(()), // no [jobs] table — let downstream serde report it
    };

    for (job_id_str, job_val) in jobs.iter_mut() {
        let job_t = match job_val.as_table_mut() {
            Some(t) => t,
            None => continue, // malformed; serde will complain
        };

        let cfg = job_t
            .entry("config")
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        let cfg_t = match cfg.as_table_mut() {
            Some(t) => t,
            None => continue,
        };

        if cfg_t.contains_key("partition") {
            continue;
        }

        match common_partition {
            Some(p) => {
                cfg_t.insert("partition".to_string(), toml::Value::String(p.to_string()));
            }
            None => {
                return Err(JobManagerError::PartitionMissing {
                    job: JobId(job_id_str.clone()),
                });
            }
        }
    }
    Ok(())
}

/// Read a `JobFlow` from a TOML file at `path`, materializing it with
/// `common` defaults (notably injecting `partition` from `common.slurm_default`
/// when omitted in the flow.toml). Returns `PartitionMissing { job }` if any
/// job lacks a partition and common has none either.
pub fn read_flow(path: &Path, common: &CommonConfig) -> Result<JobFlow, JobManagerError> {
    let text = super::read_toml_string(path)?;
    let mut v: toml::Value = toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })?;
    let common_partition = if common.slurm_default.partition.is_empty() {
        None
    } else {
        Some(common.slurm_default.partition.as_str())
    };
    inject_partition_defaults(&mut v, common_partition)?;
    v.try_into().map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

/// Write `flow` to `path` atomically (tmp + fsync + rename).
/// Creates parent directories if missing.
pub fn write_flow(path: &Path, flow: &JobFlow) -> Result<(), JobManagerError> {
    let body = toml::to_string_pretty(flow)?;
    super::atomic_write(path, body.as_bytes())
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

    fn sample_common() -> gaussian_job_shared::config::common::CommonConfig {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use std::path::PathBuf;
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
    fn inject_adds_partition_when_missing_in_flow() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_keeps_partition_when_already_set_in_flow() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
partition = "short"
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "short", "explicit flow partition must win over common");
    }

    #[test]
    fn inject_creates_missing_config_table() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_returns_partition_missing_when_both_missing() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        let err = super::inject_partition_defaults(&mut v, None).unwrap_err();
        match err {
            JobManagerError::PartitionMissing { job } => assert_eq!(job.0, "opt"),
            other => panic!("expected PartitionMissing, got {other:?}"),
        }
    }

    #[test]
    fn inject_idempotent_on_already_injected_table() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_handles_multiple_jobs_mixed() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.a]
program = "echo"
body = "true"
[jobs.a.config]
partition = "short"
[jobs.b]
program = "echo"
body = "true"
[jobs.b.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        assert_eq!(v["jobs"]["a"]["config"]["partition"].as_str().unwrap(), "short");
        assert_eq!(v["jobs"]["b"]["config"]["partition"].as_str().unwrap(), "long");
    }

    #[test]
    fn roundtrip_write_read_recovers_jobflow() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.toml");
        let original = sample_flow();
        write_flow(&path, &original).unwrap();
        let back = read_flow(&path, &sample_common()).unwrap();
        assert_eq!(back.uuid, original.uuid);
        assert_eq!(back.jobs.len(), 2);
        assert!(back.jobs.contains_key(&JobId::from("g16")));
    }

    #[test]
    fn read_missing_file_returns_io_error_with_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does_not_exist.toml");
        let err = read_flow(&path, &sample_common()).unwrap_err();
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

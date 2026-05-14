//! `<root>/common.toml` read / write.

use std::path::Path;

use gaussian_job_shared::config::common::CommonConfig;
use slurm_async_runner::entities::slurm::SlurmJobConfig;

use crate::error::JobManagerError;

#[must_use = "read_common returns the parsed CommonConfig; ignoring it drops the data"]
pub fn read_common(path: &Path) -> Result<CommonConfig, JobManagerError> {
    let text = super::read_toml_string(path)?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_common(path: &Path, common: &CommonConfig) -> Result<(), JobManagerError> {
    let text = toml::to_string(common)?;
    super::atomic_write(path, text.as_bytes())
}

/// Merge `override_` on top of `common.slurm_default`.
/// - Option<T>: override.or(common)
/// - partition (String): override if non-empty, else common
pub fn merge_with_defaults(common: &CommonConfig, override_: &SlurmJobConfig) -> SlurmJobConfig {
    let base = &common.slurm_default;
    SlurmJobConfig {
        partition: if override_.partition.is_empty() {
            base.partition.clone()
        } else {
            override_.partition.clone()
        },
        time_limit: override_.time_limit.or(base.time_limit),
        log_stdout: override_
            .log_stdout
            .clone()
            .or_else(|| base.log_stdout.clone()),
        log_stderr: override_
            .log_stderr
            .clone()
            .or_else(|| base.log_stderr.clone()),
        comment: override_.comment.clone().or_else(|| base.comment.clone()),
        job_name: override_.job_name.clone().or_else(|| base.job_name.clone()),
        array_spec: override_
            .array_spec
            .clone()
            .or_else(|| base.array_spec.clone()),
        dependency: override_
            .dependency
            .clone()
            .or_else(|| base.dependency.clone()),
        mail_user: override_
            .mail_user
            .clone()
            .or_else(|| base.mail_user.clone()),
        mail_types: override_
            .mail_types
            .clone()
            .or_else(|| base.mail_types.clone()),
        resource_spec: override_
            .resource_spec
            .clone()
            .or_else(|| base.resource_spec.clone()),
    }
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

    #[test]
    fn merge_uses_common_default_when_override_partition_is_empty() {
        let common = sample();
        let override_cfg = SlurmJobConfig {
            partition: "".to_string(),
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
        };
        let merged = merge_with_defaults(&common, &override_cfg);
        assert_eq!(merged.partition, "long");
    }

    #[test]
    fn merge_keeps_override_partition_when_set() {
        let common = sample();
        let override_cfg = SlurmJobConfig {
            partition: "short".to_string(),
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
        };
        let merged = merge_with_defaults(&common, &override_cfg);
        assert_eq!(merged.partition, "short");
    }

    #[test]
    fn merge_uses_common_for_optional_field_when_override_is_none() {
        let common = sample();
        let override_cfg = SlurmJobConfig {
            partition: "short".to_string(),
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
        };
        let merged = merge_with_defaults(&common, &override_cfg);
        assert!(
            merged.time_limit.is_none(),
            "common も None なので merge も None"
        );
    }
}

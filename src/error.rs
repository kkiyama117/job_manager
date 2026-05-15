//! Crate-level error types.

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobManagerError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("toml parse error at {path}: {source}")]
    TomlParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("toml serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("flow uuid {uuid} not found under {root}")]
    FlowNotFound { uuid: uuid::Uuid, root: PathBuf },

    #[error("job id {job} not found in flow {flow}")]
    JobNotFound {
        flow: uuid::Uuid,
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error("status file missing for flow {flow} job {job}")]
    StatusNotFound {
        flow: uuid::Uuid,
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error("sbatch submission failed: {source}")]
    SubmitFailed {
        #[source]
        source: anyhow::Error,
    },

    #[error("slurm facade error: {0}")]
    Slurm(String),

    #[error("invalid step id '{0}': must match [A-Za-z0-9_-]+")]
    InvalidStepId(String),

    #[error("invalid job id '{0}': must match [A-Za-z0-9_\\-=]+")]
    InvalidJobId(String),

    #[error("reserved id '{0}' (reserved: flow, plan, experiment, derived, status)")]
    ReservedJobId(String),

    #[error("job id parse error in '{id}' at piece '{piece}': {reason}")]
    JobIdParseError {
        id: String,
        piece: String,
        reason: String,
    },

    #[error(
        "dependency cycle detected in flow {flow}; remaining jobs (cycle members or downstream of them): {remaining:?}"
    )]
    DependencyCycle {
        flow: uuid::Uuid,
        remaining: Vec<gaussian_job_shared::entities::workflow::JobId>,
    },

    #[error("missing plan entry for job {job} in flow {flow}")]
    MissingPlanEntry {
        flow: uuid::Uuid,
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error("bash render failed for job {job}: {reason}")]
    RenderError {
        job: gaussian_job_shared::entities::workflow::JobId,
        reason: String,
    },

    #[error(
        "toml file too large: {path} is {size} bytes, limit is {limit} bytes (set JM_MAX_TOML_SIZE to override)"
    )]
    FileTooLarge {
        path: PathBuf,
        size: u64,
        limit: u64,
    },

    #[error(
        "partition is required but missing: job={job} has no partition and common.toml [slurm_default] has no partition either"
    )]
    PartitionMissing {
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error(
        "effective snapshot missing at {path} (uuid={uuid}): run `jm render <uuid>` first to materialize"
    )]
    SnapshotMissing { path: PathBuf, uuid: String },

    #[error(
        "cannot infer root from flow.toml path {path}: expected <root>/<flow_uuid>/flow.toml layout"
    )]
    RootInferenceFailed { path: PathBuf },

    #[error("{op} blocking task failed: {source}")]
    JoinFailed {
        op: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },

    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum SchemaParseError {
    #[error("parse error: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn io_variant_carries_path_and_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let err = JobManagerError::Io {
            path: PathBuf::from("/tmp/missing.toml"),
            source: inner,
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/missing.toml"), "msg = {msg}");
        assert!(msg.contains("no such file"), "msg = {msg}");
    }

    #[test]
    fn toml_parse_variant_includes_path() {
        let parse: Result<toml::Value, _> = toml::from_str("not = valid toml = bad");
        let err = JobManagerError::TomlParse {
            path: PathBuf::from("/tmp/bad.toml"),
            source: parse.err().unwrap(),
        };
        assert!(err.to_string().contains("/tmp/bad.toml"));
    }

    #[test]
    fn invalid_step_id_carries_input() {
        let err = JobManagerError::InvalidStepId("opt=1".to_string());
        assert!(err.to_string().contains("opt=1"));
    }

    #[test]
    fn reserved_job_id_carries_name() {
        let err = JobManagerError::ReservedJobId("flow".to_string());
        assert!(err.to_string().contains("flow"));
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn partition_missing_carries_job_id() {
        let err = JobManagerError::PartitionMissing {
            job: gaussian_job_shared::entities::workflow::JobId("opt".to_string()),
        };
        let msg = err.to_string();
        assert!(msg.contains("opt"), "msg = {msg}");
        assert!(msg.contains("partition"), "msg = {msg}");
    }

    #[test]
    fn snapshot_missing_carries_path_and_uuid() {
        let err = JobManagerError::SnapshotMissing {
            path: PathBuf::from("/work/abc/.jm/flow.effective.toml"),
            uuid: "01999999-0000-7000-8000-000000000000".to_string(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("/work/abc/.jm/flow.effective.toml"),
            "msg = {msg}"
        );
        assert!(
            msg.contains("jm render"),
            "msg should hint at render: {msg}"
        );
    }

    #[test]
    fn root_inference_failed_carries_path() {
        let err = JobManagerError::RootInferenceFailed {
            path: PathBuf::from("/tmp/x.toml"),
        };
        assert!(err.to_string().contains("/tmp/x.toml"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn join_failed_carries_op_and_preserves_source() {
        use std::error::Error as _;

        let handle = tokio::task::spawn_blocking(|| panic!("intentional"));
        let join_err = handle.await.expect_err("spawn_blocking should panic");

        let err = JobManagerError::JoinFailed {
            op: "write_job_run",
            source: join_err,
        };

        let msg = err.to_string();
        assert!(
            msg.contains("write_job_run"),
            "Display should include op label, got: {msg}",
        );
        assert!(
            msg.contains("blocking task failed"),
            "Display should include canonical phrase, got: {msg}",
        );

        let src = err.source().expect("JoinFailed must expose source()");
        assert!(
            src.is::<tokio::task::JoinError>(),
            "source() should be the typed tokio::task::JoinError",
        );
    }
}

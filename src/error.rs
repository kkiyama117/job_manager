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

    #[error("slurm facade error: {0}")]
    Slurm(String),

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
}

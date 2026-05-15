use pyo3::{
    PyErr,
    exceptions::{PyFileNotFoundError, PyKeyError, PyOSError, PyRuntimeError, PyValueError},
};

use crate::error::{JobManagerError, SchemaParseError};

impl From<SchemaParseError> for PyErr {
    fn from(value: SchemaParseError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<JobManagerError> for PyErr {
    fn from(value: JobManagerError) -> Self {
        let msg = value.to_string();
        match &value {
            // Genuinely missing files / paths → FileNotFoundError.
            JobManagerError::FlowNotFound { .. }
            | JobManagerError::JobNotFound { .. }
            | JobManagerError::StatusNotFound { .. }
            | JobManagerError::SnapshotMissing { .. } => PyFileNotFoundError::new_err(msg),

            // I/O — split by std::io::ErrorKind. NotFound becomes
            // FileNotFoundError (subclass of OSError); everything else
            // (permission denied, disk full, …) becomes OSError.
            JobManagerError::Io { source, .. } => {
                if source.kind() == std::io::ErrorKind::NotFound {
                    PyFileNotFoundError::new_err(msg)
                } else {
                    PyOSError::new_err(msg)
                }
            }

            // PyKeyError chosen over PyFileNotFoundError: the plan entry is a
            // JobId lookup in a map, not a missing file on disk.
            JobManagerError::MissingPlanEntry { .. } => PyKeyError::new_err(msg),

            // User-input / structure validation → ValueError.
            JobManagerError::TomlParse { .. }
            | JobManagerError::InvalidStepId(_)
            | JobManagerError::InvalidJobId(_)
            | JobManagerError::ReservedJobId(_)
            | JobManagerError::JobIdParseError { .. }
            | JobManagerError::PartitionMissing { .. }
            | JobManagerError::PartitionWrongType { .. }
            | JobManagerError::RootInferenceFailed { .. }
            | JobManagerError::FileTooLarge { .. }
            | JobManagerError::DependencyCycle { .. }
            | JobManagerError::RenderError { .. } => PyValueError::new_err(msg),

            // System / infrastructure failures (TomlSer, SubmitFailed, Slurm,
            // Other) stay as PyRuntimeError via the catch-all arm, which also
            // serves as the safe default for any future variants.
            _ => PyRuntimeError::new_err(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::JobId;
    use pyo3::Python;
    use pyo3::exceptions::{PyKeyError, PyOSError};
    use std::path::PathBuf;

    fn ensure_py() {
        // Safe to call multiple times; required because the crate forbids
        // `pyo3/auto-initialize` (see CLAUDE.md). pyo3 0.28 replaced
        // `prepare_freethreaded_python()` with `Python::initialize()`.
        pyo3::Python::initialize();
    }

    #[test]
    fn partition_missing_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::PartitionMissing {
                job: JobId("opt".to_string()),
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains("opt"));
        });
    }

    #[test]
    fn partition_wrong_type_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::PartitionWrongType {
                job: JobId("opt".to_string()),
                found: "integer",
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
            let msg = err.to_string();
            assert!(msg.contains("opt"), "msg should name the job: {msg}");
            assert!(
                msg.contains("integer"),
                "msg should name the offending TOML type: {msg}"
            );
        });
    }

    #[test]
    fn io_not_found_maps_to_file_not_found_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::Io {
                path: PathBuf::from("/tmp/nope.toml"),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
            }
            .into();
            assert!(err.is_instance_of::<PyFileNotFoundError>(py));
        });
    }

    #[test]
    fn io_permission_denied_maps_to_os_error_not_file_not_found() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::Io {
                path: PathBuf::from("/etc/shadow"),
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
            }
            .into();
            assert!(err.is_instance_of::<PyOSError>(py));
            // PyFileNotFoundError is a subclass of PyOSError; exclude it to
            // verify the NotFound/other-kinds split is honored.
            assert!(!err.is_instance_of::<PyFileNotFoundError>(py));
        });
    }

    #[test]
    fn snapshot_missing_maps_to_file_not_found_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::SnapshotMissing {
                path: PathBuf::from("/work/abc/.jm/flow.effective.toml"),
                uuid: "01999999-0000-7000-8000-000000000000".to_string(),
            }
            .into();
            assert!(err.is_instance_of::<PyFileNotFoundError>(py));
        });
    }

    #[test]
    fn root_inference_failed_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::RootInferenceFailed {
                path: PathBuf::from("/tmp/x.toml"),
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn file_too_large_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::FileTooLarge {
                path: PathBuf::from("/tmp/big.toml"),
                size: 2_000_000,
                limit: 1_000_000,
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn slurm_variant_stays_runtime_error() {
        // Regression guard for the catch-all `_` arm: SubmitFailed / Slurm / Other
        // remain PyRuntimeError so new variants default to a safe system-error tier.
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::Slurm("sbatch died".to_string()).into();
            assert!(err.is_instance_of::<PyRuntimeError>(py));
        });
    }

    #[test]
    fn missing_plan_entry_maps_to_key_error() {
        // PyKeyError chosen over PyFileNotFoundError: the plan entry is a JobId
        // -> entry lookup in a map, not a missing file on disk.
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::MissingPlanEntry {
                flow: uuid::Uuid::nil(),
                job: JobId("opt".to_string()),
            }
            .into();
            assert!(err.is_instance_of::<PyKeyError>(py));
        });
    }

    #[test]
    fn dependency_cycle_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::DependencyCycle {
                flow: uuid::Uuid::nil(),
                remaining: vec![JobId("a".to_string()), JobId("b".to_string())],
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn render_error_maps_to_value_error() {
        ensure_py();
        Python::attach(|py| {
            let err: PyErr = JobManagerError::RenderError {
                job: JobId("opt".to_string()),
                reason: "missing template variable".to_string(),
            }
            .into();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }
}

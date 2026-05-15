//! Python wrappers for the persistence layer (`common.toml`, `flow.toml`).
//!
//! `common.toml` is owned by D2 (`CommonConfig`) and has no pyclass, so we
//! exchange data as a TOML string. Same approach for `flow.toml` (`JobFlow`).

use pyo3::prelude::*;

use crate::error::JobManagerError;
use crate::persistence::common::{
    read_common as inner_read_common, write_common as inner_write_common,
};
use crate::persistence::flow::{
    read_flow as inner_read_flow, read_flow_effective as inner_read_flow_effective,
    write_flow as inner_write_flow,
};
use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::JobFlow;

/// Infer the `<root>` path from a `<root>/<flow_uuid>/flow.toml` path so we
/// can locate `<root>/common.toml`. Returns RootInferenceFailed if the path
/// is shorter than `<root>/<flow_uuid>/flow.toml`.
pub(crate) fn infer_root_common(path: &std::path::Path) -> Result<std::path::PathBuf, JobManagerError> {
    path.parent()
        .and_then(|flow_dir| flow_dir.parent())
        .map(|root| root.join("common.toml"))
        .ok_or_else(|| JobManagerError::RootInferenceFailed {
            path: path.to_path_buf(),
        })
}

/// Build a CommonConfig from `<root>/common.toml`, or synthesize an empty
/// one (partition="") when the file doesn't exist. Used so PyO3 callers
/// can keep their simple `read_flow(path)` ergonomics.
pub(crate) fn load_or_synth_common(flow_toml_path: &std::path::Path) -> PyResult<CommonConfig> {
    let common_path = infer_root_common(flow_toml_path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    if common_path.exists() {
        inner_read_common(&common_path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    } else {
        use gaussian_job_shared::config::common::DirectoryConfig;
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        Ok(CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: String::new(),
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
                project_root: std::path::PathBuf::from("."),
            },
        })
    }
}

// Not annotated with `#[pyfunction]` directly — the `#[pymodule]` in
// `py_export/mod.rs` re-declares the pyfunction wrappers to avoid duplicate
// stub-gen registrations.

/// Read `common.toml` and return its serialized TOML body as a `str`.
pub fn read_common(path: std::path::PathBuf) -> PyResult<String> {
    let cc = inner_read_common(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&cc).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Write `common.toml` from a TOML string body.
pub fn write_common(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let cc: CommonConfig = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_common(&path, &cc)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Read `flow.toml` and return its serialized TOML body as a `str`.
///
/// Infers `<root>/common.toml` from the given flow.toml path so the caller
/// doesn't have to pass a `CommonConfig`. Synthesizes an empty `CommonConfig`
/// (partition="") when `common.toml` is absent.
pub fn read_flow(path: std::path::PathBuf) -> PyResult<String> {
    let common = load_or_synth_common(&path)?;
    let fl = inner_read_flow(&path, &common)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&fl).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Write `flow.toml` from a TOML string body.
pub fn write_flow(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let fl: JobFlow = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_flow(&path, &fl)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Read a materialized snapshot (`.flow.effective.toml`). No common.toml required.
pub fn read_flow_effective(path: std::path::PathBuf) -> PyResult<String> {
    let fl = inner_read_flow_effective(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&fl).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

//! Python wrappers for the persistence layer (`common.toml`, `flow.toml`).
//!
//! `common.toml` is owned by D2 (`CommonConfig`) and has no pyclass, so we
//! exchange data as a TOML string. Same approach for `flow.toml` (`JobFlow`).

use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;

use crate::persistence::common::{
    read_common as inner_read_common, write_common as inner_write_common,
};
use crate::persistence::flow::{read_flow as inner_read_flow, write_flow as inner_write_flow};
use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::JobFlow;

/// Read `common.toml` and return its serialized TOML body as a `str`.
///
/// `CommonConfig` is owned by D2 and has no pyclass, so we round-trip via TOML.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn read_common(path: std::path::PathBuf) -> PyResult<String> {
    let cc = inner_read_common(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&cc).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Write `common.toml` from a TOML string body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn write_common(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let cc: CommonConfig = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_common(&path, &cc)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Read `flow.toml` and return its serialized TOML body as a `str`.
///
/// `JobFlow` is owned by D2 and has no pyclass, so we round-trip via TOML.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn read_flow(path: std::path::PathBuf) -> PyResult<String> {
    let fl = inner_read_flow(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&fl).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Write `flow.toml` from a TOML string body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn write_flow(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let fl: JobFlow = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_flow(&path, &fl)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

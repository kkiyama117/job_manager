//! Python wrapper for `render::render_batch_bash`.

use std::collections::BTreeMap;

use pyo3::prelude::*;

use crate::jobid::parse_job_id;
use crate::render::render_batch_bash as inner;
use gaussian_job_shared::entities::workflow::JobId;

/// Render a `batch.bash` script string (env-export style, POSIX safe).
///
/// `params` values are passed as plain strings; they round-trip as
/// `toml::Value::String`. Numeric / boolean / array params must be
/// stringified on the Python side first.
///
/// Not annotated with `#[pyfunction]` directly — the `#[pymodule]` in
/// `py_export/mod.rs` re-declares the pyfunction wrapper to avoid duplicate
/// stub-gen registrations.
pub fn render_batch_bash(
    flow_uuid: &str,
    job_id: &str,
    body: &str,
    params: BTreeMap<String, String>,
    abs_flow_dir: &str,
    abs_job_dir: &str,
) -> PyResult<String> {
    let flow_uuid = uuid::Uuid::parse_str(flow_uuid)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let jid = JobId(job_id.to_string());
    let parts =
        parse_job_id(&jid.0).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let params_toml: BTreeMap<String, toml::Value> = params
        .into_iter()
        .map(|(k, v)| (k, toml::Value::String(v)))
        .collect();
    Ok(inner(
        &flow_uuid,
        &jid,
        &parts,
        &params_toml,
        body,
        std::path::Path::new(abs_flow_dir),
        std::path::Path::new(abs_job_dir),
    ))
}

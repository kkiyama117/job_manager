use std::sync::Arc;

use pyo3::prelude::*;

use crate::path::PathResolver;
use crate::py_export::path::PyPathResolver;
use crate::slurm_facade::A1SlurmFacade;
use crate::tick::tick_many as inner_tick_many;

/// Tick a list of `(flow_uuid: str, job_id: str, slurm_jobid: int)` targets.
///
/// `srun_cmd` overrides the launcher binary used by A1's `SlurmManager`
/// (defaults to `"srun"` when `None`). Useful for SSH-tunneled `sacct`
/// or for tests that route through `"bash"` / `"true"`.
///
/// Returns list of dicts:
/// `{flow_uuid, job_id, previous, new, slurm_state, slurm_reason, queried_jobid, note}`.
/// `slurm_state` / `slurm_reason` come from the raw A1 `JobStatus`; both
/// are `None` when SLURM had no entry for the jobid.
///
/// Stub generation is owned by the thin wrapper in `py_export::mod.rs` to
/// avoid duplicate-overload registration in `pyo3-stub-gen`.
pub fn tick_many<'py>(
    py: Python<'py>,
    resolver: Py<PyPathResolver>,
    targets: Vec<(String, String, u64)>,
    srun_cmd: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let resolver_inner: Arc<PathResolver> =
        Python::attach(|gil_py| resolver.borrow(gil_py).inner.clone());

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let parsed: Vec<(
            uuid::Uuid,
            gaussian_job_shared::entities::workflow::JobId,
            u64,
        )> = targets
            .into_iter()
            .map(|(u, j, s)| {
                let uu = uuid::Uuid::parse_str(&u).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("bad uuid: {e}"))
                })?;
                Ok::<_, PyErr>((
                    uu,
                    gaussian_job_shared::entities::workflow::JobId::from(j.as_str()),
                    s,
                ))
            })
            .collect::<Result<_, _>>()?;

        let slurm_cmd = srun_cmd.map(slurm_async_runner::SlurmCmd::new);
        let slurm = A1SlurmFacade::new(Arc::new(slurm_async_runner::SlurmManager::new(slurm_cmd)));
        let results = inner_tick_many(&parsed, &slurm, resolver_inner.as_ref()).await;

        Python::attach(|py| {
            let list = pyo3::types::PyList::empty(py);
            for r in results {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("flow_uuid", r.flow_uuid.to_string())?;
                d.set_item("job_id", r.job_id.0.clone())?;
                d.set_item(
                    "previous",
                    r.previous.map(|s| format!("{s:?}").to_lowercase()),
                )?;
                d.set_item("new", r.new.map(|s| format!("{s:?}").to_lowercase()))?;
                d.set_item(
                    "slurm_state",
                    r.slurm_status
                        .as_ref()
                        .map(|s| s.state.as_token().to_string()),
                )?;
                d.set_item(
                    "slurm_reason",
                    r.slurm_status
                        .as_ref()
                        .map(|s| s.reason.as_str().to_string()),
                )?;
                d.set_item("queried_jobid", r.queried_jobid)?;
                d.set_item("note", r.note)?;
                list.append(d)?;
            }
            Ok::<Py<PyAny>, PyErr>(list.into_any().unbind())
        })
    })
}

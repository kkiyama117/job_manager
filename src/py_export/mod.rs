#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod error;
pub mod flow;
pub mod job;
pub mod jobid;
pub mod path;
pub mod persistence;
pub mod plan;
pub mod render;
pub mod runner;
pub mod search;
pub mod transition;
pub mod view;
pub mod walk;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

#[pymodule]
#[pyo3(name = "_job_manager_core")]
mod job_manager_core {
    use super::*;
    const PYTHON_MODULE_NAME: &str = "job_manager._job_manager_core";

    #[pymodule_export]
    use super::flow::PyFlowRun;
    #[pymodule_export]
    use super::job::PyJobRun;
    #[pymodule_export]
    use super::job::PyLifecycle;
    #[pymodule_export]
    use super::path::PyPathResolver;
    #[pymodule_export]
    use super::plan::PyExperimentPlan;
    #[pymodule_export]
    use super::search::PySearchFilter;
    #[pymodule_export]
    use super::view::PyCalcView;

    /// Walk all `flow.toml` under `root`. Awaits to a list of items
    /// (each either a parsed flow object or a tuple `(None, str)`
    /// describing an unreadable entry).
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[gen_stub(awaitable, override_return_type(
        type_repr = "builtins.list[typing.Any]",
        imports = ("typing", "builtins")
    ))]
    #[pyfunction]
    fn walk_flows<'py>(py: Python<'py>, root: std::path::PathBuf) -> PyResult<Bound<'py, PyAny>> {
        super::walk::walk_flows(py, root)
    }

    // SP-2: jobid helpers
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn validate_step_id(s: &str) -> PyResult<String> {
        super::jobid::validate_step_id(s)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn validate_job_id(s: &str) -> PyResult<String> {
        super::jobid::validate_job_id(s)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn build_job_id(source_step_id: &str, axis_combo: Vec<(String, usize)>) -> PyResult<String> {
        super::jobid::build_job_id(source_step_id, axis_combo)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn parse_job_id<'py>(py: Python<'py>, s: &str) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        super::jobid::parse_job_id(py, s)
    }

    // SP-2: plan I/O
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_plan(path: std::path::PathBuf) -> PyResult<super::plan::PyExperimentPlan> {
        super::plan::read_plan(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn write_plan(path: std::path::PathBuf, plan: super::plan::PyExperimentPlan) -> PyResult<()> {
        super::plan::write_plan(path, plan)
    }

    // SP-3 G.1: render / submit / persistence
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    #[pyo3(signature = (flow_uuid, job_id, body, params))]
    fn render_batch_bash(
        flow_uuid: &str,
        job_id: &str,
        body: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> PyResult<String> {
        super::render::render_batch_bash(flow_uuid, job_id, body, params)
    }

    /// Submit a flow. Awaits to a `dict[JobId, slurm_jobid]` for the
    /// jobs that were submitted. Empty when `dry_run=True`.
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[gen_stub(awaitable, override_return_type(
        type_repr = "builtins.dict[builtins.str, builtins.int]",
        imports = ("builtins")
    ))]
    #[pyfunction]
    #[pyo3(signature = (root, flow_uuid, dry_run = false))]
    fn submit_flow<'py>(
        py: Python<'py>,
        root: std::path::PathBuf,
        flow_uuid: String,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        super::runner::submit_flow(py, root, flow_uuid, dry_run)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_common(path: std::path::PathBuf) -> PyResult<String> {
        super::persistence::read_common(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn write_common(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
        super::persistence::write_common(path, toml_str)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_flow(path: std::path::PathBuf) -> PyResult<String> {
        super::persistence::read_flow(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn write_flow(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
        super::persistence::write_flow(path, toml_str)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_flow_effective(path: std::path::PathBuf) -> PyResult<String> {
        super::persistence::read_flow_effective(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_job_run(path: std::path::PathBuf) -> PyResult<super::job::PyJobRun> {
        super::job::read_job_run(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn write_job_run(path: std::path::PathBuf, run: super::job::PyJobRun) -> PyResult<()> {
        super::job::write_job_run(path, run)
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        py.import("sys")?
            .getattr("modules")?
            .set_item(PYTHON_MODULE_NAME, m)?;
        log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
        Ok(())
    }
}

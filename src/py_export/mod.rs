#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod error;
pub mod filter;
pub mod jobid;
pub mod path;
pub mod plan;
pub mod status;
pub mod tick;
pub mod view;
pub mod walk;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

#[pymodule]
#[pyo3(name = "_job_manager_core")]
mod job_manager_core {
    use super::*;
    const PYTHON_MODULE_NAME: &str = "job_manager._job_manager_core";

    #[pymodule_export]
    use super::filter::PySearchFilter;
    #[pymodule_export]
    use super::path::PyPathResolver;
    #[pymodule_export]
    use super::plan::PyExperimentPlan;
    #[pymodule_export]
    use super::status::PyPerJobStatus;
    #[pymodule_export]
    use super::view::PyCalcView;

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn walk_flows<'py>(py: Python<'py>, root: std::path::PathBuf) -> PyResult<Bound<'py, PyAny>> {
        super::walk::walk_flows(py, root)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    #[pyo3(signature = (resolver, targets, srun_cmd=None))]
    fn tick_many<'py>(
        py: Python<'py>,
        resolver: Py<super::path::PyPathResolver>,
        targets: Vec<(String, String, u64)>,
        srun_cmd: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        super::tick::tick_many(py, resolver, targets, srun_cmd)
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

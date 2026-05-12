#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod error;
pub mod filter;
pub mod path;
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
    fn tick_many<'py>(
        py: Python<'py>,
        resolver: Py<super::path::PyPathResolver>,
        targets: Vec<(String, String, u64)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        super::tick::tick_many(py, resolver, targets)
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

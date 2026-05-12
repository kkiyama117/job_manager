#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod error;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_job_manager_core")]
mod job_manager {
    use super::*;
    const PYTHON_MODULE_NAME: &str = "job_manager._job_manager_core";

    // Add sub-modules here following the slurm_async_runner pattern:
    //
    //   #[pymodule_export]
    //   use super::manager::inner_module as manager_module;
    //
    // Each sub-module's own `#[pymodule_init]` is responsible for
    // registering itself in `sys.modules` under its fully-qualified
    // name so `import job_manager._job_manager_core.manager` works
    // from Python.
    
    // ------------------- legacy template function -------------------
    /// Formats the sum of two numbers as string.
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
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


//! Python 公開: ExperimentPlan + I/O.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::plan::{ExperimentPlan, io as plan_io};

#[gen_stub_pyclass]
#[pyclass(
    name = "ExperimentPlan",
    module = "job_manager._job_manager_core",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyExperimentPlan {
    pub(crate) inner: ExperimentPlan,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyExperimentPlan {
    #[new]
    fn new(jobs: Bound<'_, pyo3::types::PyDict>) -> PyResult<Self> {
        use std::collections::BTreeMap;

        use gaussian_job_shared::entities::workflow::JobId;
        let mut out_jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();
        for (k, v) in jobs.iter() {
            let jid_str: String = k.extract()?;
            // M-1: job_id key を validate_job_id でゲートする。
            // 不正文字 / 予約名を含む key を plan.toml に挿入させない
            // (SP-3 で job_id をパス構築に再利用するため事前に弾く)。
            crate::jobid::validate_job_id(&jid_str).map_err(PyErr::from)?;
            let params_dict: Bound<'_, pyo3::types::PyDict> = v.cast_into()?;
            let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
            for (pk, pv) in params_dict.iter() {
                let key: String = pk.extract()?;
                let val: toml::Value = pythonize::depythonize(&pv)?;
                params.insert(key, val);
            }
            out_jobs.insert(JobId::from(jid_str), params);
        }
        Ok(PyExperimentPlan {
            inner: ExperimentPlan { jobs: out_jobs },
        })
    }

    #[getter]
    fn jobs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new(py);
        for (k, params) in &self.inner.jobs {
            let pdict = pyo3::types::PyDict::new(py);
            for (pk, pv) in params {
                let py_value = pythonize::pythonize(py, pv)?;
                pdict.set_item(pk, py_value)?;
            }
            // D2 JobId(pub String) の内部 String を Python dict key として使う
            dict.set_item(&k.0, pdict)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!("ExperimentPlan(jobs={} entries)", self.inner.jobs.len())
    }
}

pub(crate) fn read_plan(path: PathBuf) -> PyResult<PyExperimentPlan> {
    let plan = plan_io::read_plan(&path).map_err(PyErr::from)?;
    Ok(PyExperimentPlan { inner: plan })
}

pub(crate) fn write_plan(path: PathBuf, plan: PyExperimentPlan) -> PyResult<()> {
    plan_io::write_plan(&path, &plan.inner).map_err(PyErr::from)?;
    Ok(())
}

use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use pyo3::prelude::*;
use pythonize::pythonize;

use crate::walk::walk_flows as inner_walk;

/// Walk `<root>/*` and return a list of `JobFlow` dicts (pythonized).
/// Errors per-entry are appended as `(None, error_string)` tuples.
/// SP-2 will replace dicts with real `JobFlow` pyclass via bridge.
///
/// Reads `<root>/common.toml` to drive partition defaulting in each flow;
/// synthesizes an empty `CommonConfig` (partition="") when the file is absent.
///
/// Stub generation is owned by the thin wrapper in `py_export::mod.rs` to
/// avoid duplicate-overload registration in `pyo3-stub-gen`.
pub fn walk_flows<'py>(py: Python<'py>, root: PathBuf) -> PyResult<Bound<'py, PyAny>> {
    let common_path = root.join("common.toml");
    let common = if common_path.exists() {
        crate::persistence::read_common(&common_path)?
    } else {
        crate::persistence::synth_empty_common()
    };
    let common = Arc::new(common);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let stream = inner_walk(&root, common);
        let mut stream = std::pin::pin!(stream);
        let mut items: Vec<Result<serde_json::Value, String>> = Vec::new();
        while let Some(r) = stream.next().await {
            match r {
                Ok(flow) => match serde_json::to_value(&flow) {
                    Ok(v) => items.push(Ok(v)),
                    Err(e) => items.push(Err(format!("json: {e}"))),
                },
                Err(e) => items.push(Err(e.to_string())),
            }
        }
        Python::attach(|py| {
            let list = pyo3::types::PyList::empty(py);
            for item in items {
                match item {
                    Ok(v) => {
                        let pyv = pythonize(py, &v).map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                        })?;
                        list.append(pyv)?;
                    }
                    Err(e) => {
                        let pair = (py.None(), e);
                        list.append(pair)?;
                    }
                }
            }
            Ok::<Py<PyAny>, PyErr>(list.into_any().unbind())
        })
    })
}

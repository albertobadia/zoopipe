use pyo3::prelude::*;
use pyo3::types::PyDict;

pub struct PyHookAdapter {
    hook: Py<PyAny>,
}

impl PyHookAdapter {
    pub fn new(hook: Bound<'_, PyAny>) -> Self {
        PyHookAdapter {
            hook: hook.unbind(),
        }
    }

    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        PyHookAdapter {
            hook: self.hook.clone_ref(py),
        }
    }

    pub fn setup(&self, py: Python<'_>, store: &Bound<'_, PyDict>) -> PyResult<()> {
        let hook = self.hook.bind(py);
        if hook.hasattr("setup")? {
            hook.call_method1("setup", (store,))?;
        }
        Ok(())
    }

    pub fn execute<'py>(
        &self,
        py: Python<'py>,
        entries: Bound<'py, PyAny>,
        store: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let hook = self.hook.bind(py);
        hook.call_method1("execute", (entries, store))
    }

    pub fn teardown(&self, py: Python<'_>, store: &Bound<'_, PyDict>) -> PyResult<()> {
        let hook = self.hook.bind(py);
        if hook.hasattr("teardown")? {
            hook.call_method1("teardown", (store,))?;
        }
        Ok(())
    }
}

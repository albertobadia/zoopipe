use pyo3::prelude::*;
use pyo3::types::PyString;

/// Caches frequently used Python strings as interned objects.
///
/// This significantly improves performance by avoiding repeated string
/// allocation and lookup when creating record envelopes in the Rust core.
pub struct InternedKeys {
    pub id: Py<PyString>,
    pub status: Py<PyString>,
    pub raw_data: Py<PyString>,
    pub metadata: Py<PyString>,
    pub position: Py<PyString>,
    pub errors: Py<PyString>,
    pub validated_data: Py<PyString>,
}

impl InternedKeys {
    pub fn new(py: Python<'_>) -> Self {
        Self {
            id: pyo3::intern!(py, "id").clone().unbind(),
            status: pyo3::intern!(py, "status").clone().unbind(),
            raw_data: pyo3::intern!(py, "raw_data").clone().unbind(),
            metadata: pyo3::intern!(py, "metadata").clone().unbind(),
            position: pyo3::intern!(py, "position").clone().unbind(),
            errors: pyo3::intern!(py, "errors").clone().unbind(),
            validated_data: pyo3::intern!(py, "validated_data").clone().unbind(),
        }
    }

    pub fn get_id<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.id.bind(py)
    }
    pub fn get_status<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.status.bind(py)
    }
    pub fn get_raw_data<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.raw_data.bind(py)
    }
    pub fn get_metadata<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.metadata.bind(py)
    }
    pub fn get_position<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.position.bind(py)
    }
    pub fn get_errors<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.errors.bind(py)
    }
    pub fn get_validated_data<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        self.validated_data.bind(py)
    }
}

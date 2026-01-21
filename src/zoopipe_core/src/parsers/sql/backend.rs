use pyo3::prelude::*;
use pyo3::types::PyDict;

pub trait SqlBackend: Send + Sync {
    fn write_batch(
        &self,
        py: Python<'_>,
        records: &[Py<PyDict>],
        fields: &[String],
        table_name: &str,
    ) -> PyResult<()>;
}

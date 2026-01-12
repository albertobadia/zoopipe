use pyo3::prelude::*;
use pyo3::types::{PyList, PyDict};

#[pyclass]
pub struct NativeValidator {
    validator: Py<PyAny>,
    status_validated: Py<PyAny>,
    status_failed: Py<PyAny>,
}

#[pymethods]
impl NativeValidator {
    #[new]
    fn new(py: Python<'_>, validator: Py<PyAny>) -> PyResult<Self> {
        let models = py.import("zoopipe.models.core")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_validated = status_enum.getattr("VALIDATED")?.unbind();
        let status_failed = status_enum.getattr("FAILED")?.unbind();

        Ok(NativeValidator { 
            validator,
            status_validated,
            status_failed,
        })
    }

    pub fn validate<'py>(&self, py: Python<'py>, entry: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let raw_data = entry.get_item(pyo3::intern!(py, "raw_data"))?;
        
        let schema_val = self.validator.bind(py);
        match schema_val.call_method1("validate_python", (raw_data,)) {
            Ok(validated) => {
                entry.set_item(pyo3::intern!(py, "validated_data"), validated)?;
                entry.set_item(pyo3::intern!(py, "status"), self.status_validated.bind(py))?;
                Ok(entry)
            }
            Err(e) => {
                entry.set_item(pyo3::intern!(py, "status"), self.status_failed.bind(py))?;
                
                let errors_any = entry.get_item(pyo3::intern!(py, "errors"))?;
                if let Ok(errors_list) = errors_any.cast::<PyList>() {
                    let err_dict = PyDict::new(py);
                    err_dict.set_item(pyo3::intern!(py, "msg"), e.to_string())?;
                    err_dict.set_item(pyo3::intern!(py, "type"), pyo3::intern!(py, "native_validation_error"))?;
                    errors_list.append(err_dict)?;
                }
                Ok(entry)
            }
        }
    }

    pub fn validate_batch<'py>(&self, py: Python<'py>, entries: Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
        let validator = self.validator.bind(py);
        let status_validated = self.status_validated.bind(py);
        let status_failed = self.status_failed.bind(py);
        
        let raw_data_key = pyo3::intern!(py, "raw_data");
        let validated_data_key = pyo3::intern!(py, "validated_data");
        let status_key = pyo3::intern!(py, "status");
        let errors_key = pyo3::intern!(py, "errors");
        let msg_key = pyo3::intern!(py, "msg");
        let type_key = pyo3::intern!(py, "type");
        let error_type_val = pyo3::intern!(py, "native_validation_error");

        for entry in entries.iter() {
            let raw_data = entry.get_item(raw_data_key)?;
            match validator.call_method1("validate_python", (raw_data,)) {
                Ok(validated) => {
                    entry.set_item(validated_data_key, validated)?;
                    entry.set_item(status_key, status_validated)?;
                }
                Err(e) => {
                    entry.set_item(status_key, status_failed)?;
                    let errors_any = entry.get_item(errors_key)?;
                    if let Ok(errors_list) = errors_any.cast::<PyList>() {
                        let err_dict = PyDict::new(py);
                        err_dict.set_item(msg_key, e.to_string())?;
                        err_dict.set_item(type_key, error_type_val)?;
                        errors_list.append(err_dict)?;
                    }
                }
            }
        }
        Ok(entries)
    }
}

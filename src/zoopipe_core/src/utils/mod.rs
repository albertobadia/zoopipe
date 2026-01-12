use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBool, PyInt, PyFloat, PyString};
use pyo3::exceptions::PyRuntimeError;
use serde_json::Value;

pub fn wrap_py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

pub fn serde_to_py<'py>(py: Python<'py>, value: Value) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(PyBool::new(py, b).as_any().clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PyInt::new(py, i).as_any().clone())
            } else if let Some(f) = n.as_f64() {
                Ok(PyFloat::new(py, f).as_any().clone())
            } else {
                Ok(PyString::new(py, &n.to_string()).as_any().clone())
            }
        }
        Value::String(s) => Ok(PyString::new(py, &s).as_any().clone()),
        Value::Array(arr) => {
            let elements: Vec<_> = arr.into_iter()
                .map(|v| serde_to_py(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            let list = PyList::new(py, elements)?;
            Ok(list.as_any().clone())
        }
        Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, serde_to_py(py, v)?)?;
            }
            Ok(dict.as_any().clone())
        }
    }
}

pub fn python_to_serde(py: Python<'_>, obj: Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::Null)
    } else if let Ok(b) = obj.cast::<PyBool>() {
        Ok(Value::Bool(b.is_true()))
    } else if let Ok(i) = obj.cast::<PyInt>() {
        Ok(Value::Number(serde_json::Number::from(i.extract::<i64>()?)))
    } else if let Ok(f) = obj.cast::<PyFloat>() {
        let f_val = f.extract::<f64>()?;
        if let Some(n) = serde_json::Number::from_f64(f_val) {
            Ok(Value::Number(n))
        } else {
            Ok(Value::Null)
        }
    } else if let Ok(s) = obj.cast::<PyString>() {
        Ok(Value::String(s.to_string()))
    } else if let Ok(l) = obj.cast::<PyList>() {
        let mut vec = Vec::with_capacity(l.len());
        for item in l.iter() {
            vec.push(python_to_serde(py, item)?);
        }
        Ok(Value::Array(vec))
    } else if let Ok(d) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::with_capacity(d.len());
        for (k, v) in d.iter() {
            map.insert(k.to_string(), python_to_serde(py, v)?);
        }
        Ok(Value::Object(map))
    } else {
        if obj.hasattr(pyo3::intern!(py, "value"))? {
            python_to_serde(py, obj.getattr(pyo3::intern!(py, "value"))?)
        } else {
            Ok(Value::String(obj.to_string()))
        }
    }
}

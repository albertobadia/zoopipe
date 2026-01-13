use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBool, PyInt, PyFloat, PyString};
use pyo3::exceptions::PyRuntimeError;
use serde_json::Value;
use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeMap};

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


pub struct PySerializable<'a>(pub Bound<'a, PyAny>);

impl<'a> Serialize for PySerializable<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let obj = &self.0;
        if obj.is_none() {
            serializer.serialize_none()
        } else if let Ok(b) = obj.cast::<PyBool>() {
             serializer.serialize_bool(b.is_true())
        } else if let Ok(i) = obj.cast::<PyInt>() {
             if let Ok(val) = i.extract::<i64>() {
                 serializer.serialize_i64(val)
             } else {
                 serializer.serialize_str(&i.to_string())
             }
        } else if let Ok(f) = obj.cast::<PyFloat>() {
             if let Ok(val) = f.extract::<f64>() {
                 serializer.serialize_f64(val)
             } else {
                 serializer.serialize_none()
             }
        } else if let Ok(s) = obj.cast::<PyString>() {
             serializer.serialize_str(&s.to_string())
        } else if let Ok(l) = obj.cast::<PyList>() {
             let mut seq = serializer.serialize_seq(Some(l.len()))?;
             for item in l.iter() {
                 seq.serialize_element(&PySerializable(item))?;
             }
             seq.end()
        } else if let Ok(d) = obj.cast::<PyDict>() {
             let mut map = serializer.serialize_map(Some(d.len()))?;
             for (k, v) in d.iter() {
                 map.serialize_entry(&k.to_string(), &PySerializable(v))?;
             }
             map.end()
        } else {
            let s = obj.to_string();
            serializer.serialize_str(&s)
        }
    }
}

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde_json::Value;
use crate::utils::wrap_py_err;
use crate::utils::{python_to_serde, serde_to_py};

#[pyfunction]
#[pyo3(signature = (data, compression=None))]
pub fn pack_chunk<'py>(py: Python<'py>, data: Bound<'py, PyAny>, compression: Option<String>) -> PyResult<Bound<'py, PyBytes>> {
    let value = python_to_serde(py, data)?;
    let mut serialized = rmp_serde::to_vec(&value).map_err(wrap_py_err)?;
    
    if let Some(alg) = compression {
        if alg == "lz4" {
            use std::io::Write;
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(&serialized).map_err(wrap_py_err)?;
            serialized = encoder.finish().map_err(wrap_py_err)?;
        }
    }
    
    Ok(PyBytes::new(py, &serialized))
}

#[pyfunction]
#[pyo3(signature = (data, compression=None))]
pub fn unpack_chunk<'py>(py: Python<'py>, data: Vec<u8>, compression: Option<String>) -> PyResult<Bound<'py, PyAny>> {
    let mut decompressed = data;
    if let Some(alg) = compression {
        if alg == "lz4" {
            if decompressed.is_empty() {
                 return Err(pyo3::exceptions::PyValueError::new_err("Empty LZ4 data"));
            } else {
                use std::io::Read;
                let mut decoder = lz4_flex::frame::FrameDecoder::new(&decompressed[..]);
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded).map_err(wrap_py_err)?;
                decompressed = decoded;
            }
        }
    }
    let value: Value = rmp_serde::from_slice(&decompressed).map_err(wrap_py_err)?;
    serde_to_py(py, value)
}

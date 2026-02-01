use crate::utils::wrap_py_err;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};

/// Convert a single value from an Arrow array to a Python object.
pub fn arrow_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }

    macro_rules! to_py_obj {
        ($array_type:ty, $row:expr) => {{
            let val = array
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("Array type should match DataType variant in match arm")
                .value($row);
            let py_val = pyo3::IntoPyObject::into_pyobject(val, py).map_err(wrap_py_err)?;
            Ok(py_val.to_owned().into_any().unbind())
        }};
    }

    match array.data_type() {
        DataType::Int8 => to_py_obj!(Int8Array, row),
        DataType::Int16 => to_py_obj!(Int16Array, row),
        DataType::Int32 => to_py_obj!(Int32Array, row),
        DataType::Int64 => to_py_obj!(Int64Array, row),
        DataType::UInt8 => to_py_obj!(UInt8Array, row),
        DataType::UInt16 => to_py_obj!(UInt16Array, row),
        DataType::UInt32 => to_py_obj!(UInt32Array, row),
        DataType::UInt64 => to_py_obj!(UInt64Array, row),
        DataType::Float32 => to_py_obj!(Float32Array, row),
        DataType::Float64 => to_py_obj!(Float64Array, row),
        DataType::Boolean => {
            let val = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Array type should be BooleanArray for DataType::Boolean")
                .value(row);
            let py_val = pyo3::IntoPyObject::into_pyobject(val, py).map_err(wrap_py_err)?;
            Ok(py_val.to_owned().into_any().unbind())
        }
        DataType::Utf8 => to_py_obj!(StringArray, row),
        DataType::LargeUtf8 => to_py_obj!(LargeStringArray, row),
        _ => Ok(py.None()),
    }
}

/// Infer Arrow DataType from a Python object using "Lax Typing".
pub fn infer_type(val: &Bound<'_, PyAny>) -> DataType {
    if val.is_instance_of::<pyo3::types::PyBool>() {
        return DataType::Boolean;
    } else if val.is_instance_of::<pyo3::types::PyInt>() {
        return DataType::Int64;
    } else if val.is_instance_of::<pyo3::types::PyFloat>() {
        return DataType::Float64;
    }
    DataType::Utf8
}

pub fn make_builder(dt: &DataType, cap: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(cap)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(cap)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(cap)),
        _ => Box::new(StringBuilder::with_capacity(cap, cap * 10)),
    }
}

pub fn append_val(
    builder: &mut dyn ArrayBuilder,
    val: Option<Bound<'_, PyAny>>,
    _py: Python<'_>,
) -> PyResult<()> {
    let Some(v) = val else {
        append_null(builder);
        return Ok(());
    };

    if v.is_none() {
        append_null(builder);
        return Ok(());
    }

    let any = builder.as_any_mut();

    // Lax conversion
    if let Some(b) = any.downcast_mut::<BooleanBuilder>() {
        if let Ok(val) = v.extract::<bool>() {
            b.append_value(val);
        } else if let Ok(s) = v.extract::<&str>() {
            let truthy = s.eq_ignore_ascii_case("true") || s == "1";
            b.append_value(truthy);
        } else {
            b.append_null();
        }
    } else if let Some(b) = any.downcast_mut::<Int64Builder>() {
        if let Ok(val) = v.extract::<i64>() {
            b.append_value(val);
        } else if let Ok(s) = v.extract::<&str>() {
            if let Ok(parsed) = s.parse::<i64>() {
                b.append_value(parsed);
            } else {
                b.append_null();
            }
        } else {
            b.append_null();
        }
    } else if let Some(b) = any.downcast_mut::<Float64Builder>() {
        if let Ok(val) = v.extract::<f64>() {
            b.append_value(val);
        } else if let Ok(s) = v.extract::<&str>() {
            if let Ok(parsed) = s.parse::<f64>() {
                b.append_value(parsed);
            } else {
                b.append_null();
            }
        } else {
            b.append_null();
        }
    } else if let Some(b) = any.downcast_mut::<StringBuilder>() {
        if let Ok(s) = v.extract::<&str>() {
            b.append_value(s);
        } else {
            b.append_value(v.to_string());
        }
    }
    Ok(())
}

fn append_null(builder: &mut dyn ArrayBuilder) {
    let any = builder.as_any_mut();
    if let Some(b) = any.downcast_mut::<BooleanBuilder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Int64Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Float64Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<StringBuilder>() {
        b.append_null();
    }
}

/// Build an Arrow RecordBatch from a list of Python dictionaries.
pub fn build_record_batch(
    py: Python<'_>,
    schema: &SchemaRef,
    list: &Bound<'_, PyList>,
    reused_builders: Option<&mut Vec<Box<dyn ArrayBuilder>>>,
) -> PyResult<RecordBatch> {
    let num_rows = list.len();

    let mut _new_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let builders = match reused_builders {
        Some(b) => {
            if b.is_empty() {
                *b = schema
                    .fields()
                    .iter()
                    .map(|f| make_builder(f.data_type(), num_rows))
                    .collect();
            }
            b
        }
        None => {
            _new_builders = schema
                .fields()
                .iter()
                .map(|f| make_builder(f.data_type(), num_rows))
                .collect();
            &mut _new_builders
        }
    };

    let key_objs: Vec<Bound<'_, PyString>> = schema
        .fields()
        .iter()
        .map(|f| PyString::new(py, f.name()))
        .collect();

    // Iterate Rows

    if !list.is_empty() {
        let first = list.get_item(0)?;
        if first.is_instance_of::<PyDict>() {
            for (item_idx, item) in list.iter().enumerate() {
                if let Ok(dict) = item.cast::<PyDict>() {
                    for (i, key) in key_objs.iter().enumerate() {
                        let val = dict.get_item(key)?;
                        append_val(builders[i].as_mut(), val, py)?;
                    }
                } else {
                    return Err(PyRuntimeError::new_err(format!(
                        "Mixed types in batch: expected Dict at index {}",
                        item_idx
                    )));
                }
            }
        } else {
            for item in list.iter() {
                for (i, key) in key_objs.iter().enumerate() {
                    let val = item.getattr(key).ok();
                    append_val(builders[i].as_mut(), val, py)?;
                }
            }
        }
    }

    let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

    RecordBatch::try_new(schema.clone(), columns).map_err(wrap_py_err)
}

use crate::utils::interning::InternedKeys;
use crate::utils::wrap_in_envelope;

pub fn record_batch_to_py_envelopes<'py>(
    py: Python<'py>,
    batch: RecordBatch,
    keys: &InternedKeys,
    status_pending: &Bound<'py, PyAny>,
    generate_ids: bool,
    start_position: usize,
) -> PyResult<Bound<'py, PyList>> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let fields = schema.fields();
    let column_names: Vec<Bound<'py, PyString>> =
        fields.iter().map(|f| PyString::new(py, f.name())).collect();

    let columns = batch.columns();
    let mut py_columns: Vec<Vec<Py<PyAny>>> = Vec::with_capacity(columns.len());

    // Column-wise conversion
    for col in columns {
        let mut py_col = Vec::with_capacity(num_rows);
        match col.data_type() {
            DataType::Int64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("Type mismatch for Int64Array"))?;
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        py_col.push(py.None());
                    } else {
                        py_col.push(
                            arr.value(i)
                                .into_pyobject(py)
                                .map_err(wrap_py_err)?
                                .into_any()
                                .unbind(),
                        );
                    }
                }
            }
            DataType::Float64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("Type mismatch for Float64Array"))?;
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        py_col.push(py.None());
                    } else {
                        py_col.push(
                            arr.value(i)
                                .into_pyobject(py)
                                .map_err(wrap_py_err)?
                                .into_any()
                                .unbind(),
                        );
                    }
                }
            }
            DataType::Utf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| PyRuntimeError::new_err("Type mismatch for StringArray"))?;
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        py_col.push(py.None());
                    } else {
                        py_col.push(
                            arr.value(i)
                                .into_pyobject(py)
                                .map_err(wrap_py_err)?
                                .into_any()
                                .unbind(),
                        );
                    }
                }
            }
            DataType::Boolean => {
                let arr = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| PyRuntimeError::new_err("Type mismatch for BooleanArray"))?;
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        py_col.push(py.None());
                    } else {
                        py_col.push(
                            pyo3::types::PyBool::new(py, arr.value(i))
                                .as_any()
                                .clone()
                                .unbind(),
                        );
                    }
                }
            }
            _ => {
                for i in 0..num_rows {
                    py_col.push(arrow_to_py(py, col, i)?);
                }
            }
        }
        py_columns.push(py_col);
    }

    // Row-wise construction
    let list = PyList::empty(py);
    let mut column_iters: Vec<_> = py_columns.iter().map(|c| c.iter()).collect();

    for i in 0..num_rows {
        let raw_data = PyDict::new(py);
        for (name_bound, col_iter) in column_names.iter().zip(column_iters.iter_mut()) {
            let val = col_iter.next().expect("Column length mismatch");
            raw_data.set_item(name_bound, val)?;
        }

        let env = wrap_in_envelope(
            py,
            keys,
            raw_data.into_any(),
            status_pending.clone(),
            start_position + i,
            generate_ids,
        )?;
        list.append(env)?;
    }

    Ok(list)
}

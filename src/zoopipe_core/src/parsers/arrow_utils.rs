use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyList, PyAnyMethods};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use crate::utils::wrap_py_err;

pub fn arrow_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }

    macro_rules! to_py_obj {
        ($array_type:ty, $row:expr) => {{
            let val = array.as_any().downcast_ref::<$array_type>()
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
            let val = array.as_any().downcast_ref::<BooleanArray>()
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

pub fn infer_type(val: &Bound<'_, PyAny>) -> DataType {
    if val.is_instance_of::<pyo3::types::PyBool>() { DataType::Boolean }
    else if val.is_instance_of::<pyo3::types::PyInt>() { DataType::Int64 }
    else if val.is_instance_of::<pyo3::types::PyFloat>() { DataType::Float64 }
    else { DataType::Utf8 }
}

pub fn make_builder(dt: &DataType, cap: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(cap)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(cap)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(cap)),
        _ => Box::new(StringBuilder::with_capacity(cap, cap * 10)),
    }
}

pub fn append_val(builder: &mut dyn ArrayBuilder, val: Option<Bound<'_, PyAny>>, _py: Python<'_>) -> PyResult<()> {
    let Some(v) = val else {
        append_null(builder);
        return Ok(());
    };
    
    if v.is_none() {
        append_null(builder);
        return Ok(());
    }
    
    let any = builder.as_any_mut();
    if let Some(b) = any.downcast_mut::<BooleanBuilder>() { b.append_value(v.extract::<bool>()?); }
    else if let Some(b) = any.downcast_mut::<Int64Builder>() { b.append_value(v.extract::<i64>()?); }
    else if let Some(b) = any.downcast_mut::<Float64Builder>() { b.append_value(v.extract::<f64>()?); }
    else if let Some(b) = any.downcast_mut::<StringBuilder>() { 
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
    if let Some(b) = any.downcast_mut::<BooleanBuilder>() { b.append_null(); }
    else if let Some(b) = any.downcast_mut::<Int64Builder>() { b.append_null(); }
    else if let Some(b) = any.downcast_mut::<Float64Builder>() { b.append_null(); }
    else if let Some(b) = any.downcast_mut::<StringBuilder>() { b.append_null(); }
}

pub fn build_record_batch(
    py: Python<'_>,
    schema: &SchemaRef,
    list: &Bound<'_, PyList>,
) -> PyResult<RecordBatch> {
    let num_rows = list.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    let mut dicts = Vec::with_capacity(num_rows);
    for item in list.iter() {
        dicts.push(item.cast::<PyDict>()?.clone());
    }

    let key_objs: Vec<Bound<'_, PyString>> = schema.fields()
        .iter()
        .map(|f| PyString::new(py, f.name()))
        .collect();

    for (i, field) in schema.fields().iter().enumerate() {
        let mut builder = make_builder(field.data_type(), num_rows);
        let key = &key_objs[i];
        
        for dict in &dicts {
            let val = dict.get_item(key)?;
            append_val(builder.as_mut(), val, py)?;
        }
        columns.push(builder.finish());
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(wrap_py_err)
}

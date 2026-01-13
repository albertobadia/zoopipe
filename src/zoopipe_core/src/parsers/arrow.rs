use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::sync::Mutex;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use crate::utils::wrap_py_err;
use pyo3::types::{PyString, PyDict, PyList};

#[pyclass]
pub struct ArrowReader {
    reader: Mutex<FileReader<File>>,
    current_batch: Mutex<Option<(RecordBatch, usize)>>,
    headers: Vec<Py<PyString>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
}

#[pymethods]
impl ArrowReader {
    #[new]
    #[pyo3(signature = (path, generate_ids=true))]
    fn new(py: Python<'_>, path: String, generate_ids: bool) -> PyResult<Self> {
        let file = File::open(&path).map_err(wrap_py_err)?;
        let reader = FileReader::try_new(file, None).map_err(wrap_py_err)?;
        let schema = reader.schema();
        let headers: Vec<Py<PyString>> = schema.fields()
            .iter()
            .map(|f| PyString::new(py, f.name()).unbind())
            .collect();

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ArrowReader {
            reader: Mutex::new(reader),
            current_batch: Mutex::new(None),
            headers,
            position: Mutex::new(0),
            status_pending,
            generate_ids,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let mut reader = slf.reader.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut current_opt = slf.current_batch.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut pos = slf.position.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        if current_opt.is_none() {
            if let Some(batch_res) = reader.next() {
                let batch = batch_res.map_err(wrap_py_err)?;
                *current_opt = Some((batch, 0));
            } else {
                return Ok(None);
            }
        }

        if let Some((batch, row_idx)) = current_opt.as_mut() {
            let current_pos = *pos;
            *pos += 1;

            let raw_data = PyDict::new(py);
            for (i, header) in slf.headers.iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_to_py(py, col, *row_idx)?;
                raw_data.set_item(header.bind(py), val)?;
            }

            *row_idx += 1;
            if *row_idx >= batch.num_rows() {
                *current_opt = None;
            }

            let envelope = PyDict::new(py);
            let id = if slf.generate_ids {
                uuid::Uuid::new_v4().to_string()
            } else {
                String::new()
            };

            envelope.set_item(pyo3::intern!(py, "id"), id)?;
            envelope.set_item(pyo3::intern!(py, "status"), slf.status_pending.bind(py))?;
            envelope.set_item(pyo3::intern!(py, "raw_data"), raw_data)?;
            envelope.set_item(pyo3::intern!(py, "metadata"), PyDict::new(py))?;
            envelope.set_item(pyo3::intern!(py, "position"), current_pos)?;
            envelope.set_item(pyo3::intern!(py, "errors"), PyList::empty(py))?;

            Ok(Some(envelope.into_any()))
        } else {
            Ok(None)
        }
    }
}

fn arrow_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }

    macro_rules! to_py_obj {
        ($array_type:ty, $row:expr) => {{
            let val = array.as_any().downcast_ref::<$array_type>().unwrap().value($row);
            let py_val = val.into_pyobject(py).map_err(wrap_py_err)?;
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
            let val = array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            let py_val = val.into_pyobject(py).map_err(wrap_py_err)?;
            Ok(py_val.to_owned().into_any().unbind())
        }
        DataType::Utf8 => to_py_obj!(StringArray, row),
        DataType::LargeUtf8 => to_py_obj!(LargeStringArray, row),
        _ => Ok(py.None()),
    }
}

#[pyclass]
pub struct ArrowWriter {
    path: String,
    writer: Mutex<Option<FileWriter<File>>>,
}

#[pymethods]
impl ArrowWriter {
    #[new]
    pub fn new(path: String) -> Self {
        ArrowWriter {
            path,
            writer: Mutex::new(None),
        }
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let list = entries.cast::<PyList>()?;
        if list.is_empty() { return Ok(()); }

        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        
        if writer_guard.is_none() {
            let first = list.get_item(0)?;
            let dict = first.cast::<PyDict>()?;
            let mut fields = Vec::new();
            let mut keys: Vec<String> = dict.keys().iter().map(|k| k.to_string()).collect();
            keys.sort();
            
            for key in &keys {
                let val = dict.get_item(key)?.unwrap();
                let dt = infer_type(&val);
                fields.push(Field::new(key.clone(), dt, true));
            }
            let schema = SchemaRef::new(Schema::new(fields));
            let file = File::create(&self.path).map_err(wrap_py_err)?;
            *writer_guard = Some(FileWriter::try_new(file, &schema).map_err(wrap_py_err)?);
        }

        let writer = writer_guard.as_mut().unwrap();
        let schema = writer.schema();
        let num_rows = list.len();
        
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let mut builder = make_builder(field.data_type(), num_rows);
            for entry in list.iter() {
                let dict = entry.cast::<PyDict>()?;
                let val = dict.get_item(field.name())?;
                append_val(builder.as_mut(), val, py)?;
            }
            columns.push(builder.finish());
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(wrap_py_err)?;
        writer.write(&batch).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(mut w) = writer_guard.take() {
            w.finish().map_err(wrap_py_err)?;
        }
        Ok(())
    }
}

fn infer_type(val: &Bound<'_, PyAny>) -> DataType {
    if val.is_instance_of::<pyo3::types::PyBool>() { DataType::Boolean }
    else if val.is_instance_of::<pyo3::types::PyInt>() { DataType::Int64 }
    else if val.is_instance_of::<pyo3::types::PyFloat>() { DataType::Float64 }
    else { DataType::Utf8 }
}

fn make_builder(dt: &DataType, cap: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(cap)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(cap)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(cap)),
        _ => Box::new(StringBuilder::with_capacity(cap, cap * 10)),
    }
}

fn append_val(builder: &mut dyn ArrayBuilder, val: Option<Bound<'_, PyAny>>, _py: Python<'_>) -> PyResult<()> {
    if val.is_none() || val.as_ref().unwrap().is_none() {
        let any = builder.as_any_mut();
        if let Some(b) = any.downcast_mut::<BooleanBuilder>() { b.append_null(); }
        else if let Some(b) = any.downcast_mut::<Int64Builder>() { b.append_null(); }
        else if let Some(b) = any.downcast_mut::<Float64Builder>() { b.append_null(); }
        else if let Some(b) = any.downcast_mut::<StringBuilder>() { b.append_null(); }
        return Ok(());
    }
    let v = val.unwrap();
    let any = builder.as_any_mut();
    if let Some(b) = any.downcast_mut::<BooleanBuilder>() { b.append_value(v.extract::<bool>()?); }
    else if let Some(b) = any.downcast_mut::<Int64Builder>() { b.append_value(v.extract::<i64>()?); }
    else if let Some(b) = any.downcast_mut::<Float64Builder>() { b.append_value(v.extract::<f64>()?); }
    else if let Some(b) = any.downcast_mut::<StringBuilder>() { b.append_value(v.to_string()); }
    Ok(())
}

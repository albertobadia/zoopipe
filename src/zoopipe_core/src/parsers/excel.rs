use crate::error::PipeError;
use crate::io::get_runtime;
use crate::io::storage::StorageController;
use crate::utils::wrap_py_err;
use calamine::{Data, Reader, Xlsx};
use object_store::ObjectStoreExt;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use rust_xlsxwriter::{Format, Workbook, Worksheet};
use std::fs::File;
use std::io::{Cursor, Read};
use std::sync::Mutex;

struct ExcelReaderState {
    rows: std::vec::IntoIter<Vec<Data>>,
    position: usize,
}

/// Fast Excel reader that parses .xlsx and other formats using Calamine.
///
/// It efficiently processes large workbooks and supports sheet selection
/// by name or index, providing a smooth bridge to the pipeline.
#[pyclass]
pub struct ExcelReader {
    state: Mutex<ExcelReaderState>,
    headers: Vec<Py<PyString>>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

use crate::utils::interning::InternedKeys;

#[pymethods]
impl ExcelReader {
    #[new]
    #[pyo3(signature = (path, sheet=None, skip_rows=0, fieldnames=None, generate_ids=true))]
    fn new(
        py: Python<'_>,
        path: String,
        sheet: Option<SheetSelector>,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
        generate_ids: bool,
    ) -> PyResult<Self> {
        let data = if path.starts_with("s3://") {
            let controller = StorageController::new(&path).map_err(wrap_py_err)?;
            get_runtime()
                .block_on(async {
                    controller
                        .store()
                        .get(&object_store::path::Path::from(controller.path()))
                        .await
                        .map_err(wrap_py_err)?
                        .bytes()
                        .await
                        .map_err(wrap_py_err)
                })?
                .to_vec()
        } else {
            let mut file = File::open(&path).map_err(wrap_py_err)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).map_err(wrap_py_err)?;
            buf
        };

        let mut workbook: Xlsx<Cursor<Vec<u8>>> =
            calamine::open_workbook_from_rs(Cursor::new(data)).map_err(wrap_py_err)?;

        let sheet_name = match sheet {
            Some(SheetSelector::Name(name)) => name,
            Some(SheetSelector::Index(idx)) => {
                let names = workbook.sheet_names();
                names
                    .get(idx)
                    .ok_or_else(|| PipeError::Other(format!("Sheet index {} out of range", idx)))?
                    .clone()
            }
            None => workbook
                .sheet_names()
                .first()
                .ok_or_else(|| PipeError::Other("Workbook has no sheets".to_string()))?
                .clone(),
        };

        let range = workbook.worksheet_range(&sheet_name).map_err(wrap_py_err)?;

        let has_header = fieldnames.is_none();
        let header_offset = if has_header { 1 } else { 0 };

        let headers_str = if let Some(fields) = fieldnames {
            fields
        } else {
            range
                .rows()
                .nth(skip_rows)
                .map(|row| row.iter().map(data_to_string).collect())
                .unwrap_or_default()
        };

        let all_rows: Vec<Vec<Data>> = range
            .rows()
            .skip(skip_rows + header_offset)
            .map(|r| r.to_vec())
            .collect();

        let headers: Vec<Py<PyString>> = headers_str
            .into_iter()
            .map(|s: String| PyString::new(py, s.as_str()).unbind())
            .collect();

        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ExcelReader {
            state: Mutex::new(ExcelReaderState {
                rows: all_rows.into_iter(),
                position: 0,
            }),
            headers,
            status_pending,
            generate_ids,
            keys: InternedKeys::new(py),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        slf.next_internal(slf.py())
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let batch = PyList::empty(py);
        let mut count = 0;

        while count < batch_size {
            if let Some(item) = self.next_internal(py)? {
                batch.append(item)?;
                count += 1;
            } else {
                break;
            }
        }

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(batch))
    }
    #[staticmethod]
    pub fn list_sheets(path: String) -> PyResult<Vec<String>> {
        let data = if path.starts_with("s3://") {
            let controller = StorageController::new(&path).map_err(wrap_py_err)?;
            get_runtime()
                .block_on(async {
                    controller
                        .store()
                        .get(&object_store::path::Path::from(controller.path()))
                        .await
                        .map_err(wrap_py_err)?
                        .bytes()
                        .await
                        .map_err(wrap_py_err)
                })?
                .to_vec()
        } else {
            let mut file = File::open(&path).map_err(wrap_py_err)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).map_err(wrap_py_err)?;
            buf
        };

        let workbook: Xlsx<Cursor<Vec<u8>>> =
            calamine::open_workbook_from_rs(Cursor::new(data)).map_err(wrap_py_err)?;
        Ok(workbook.sheet_names().to_vec())
    }

    fn next_internal<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        if let Some(row) = state.rows.next() {
            let current_pos = state.position;
            state.position += 1;
            let raw_data = PyDict::new(py);
            for (header_py, cell) in self.headers.iter().zip(row.iter()) {
                let value = data_to_py_value(py, cell)?;
                raw_data.set_item(header_py.bind(py), value)?;
            }
            let envelope = PyDict::new(py);
            let id = if self.generate_ids {
                crate::utils::generate_entry_id(py)?
            } else {
                py.None().into_bound(py)
            };
            envelope.set_item(self.keys.get_id(py), id)?;
            envelope.set_item(self.keys.get_status(py), self.status_pending.bind(py))?;
            envelope.set_item(self.keys.get_raw_data(py), raw_data)?;
            envelope.set_item(self.keys.get_metadata(py), PyDict::new(py))?;
            envelope.set_item(self.keys.get_position(py), current_pos)?;
            envelope.set_item(self.keys.get_errors(py), PyList::empty(py))?;
            Ok(Some(envelope.into_any()))
        } else {
            Ok(None)
        }
    }
}

#[derive(FromPyObject)]
pub enum SheetSelector {
    Name(String),
    Index(usize),
}

fn data_to_string(data: &Data) -> String {
    match data {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        Data::Float(f) => f.to_string(),
        Data::Int(i) => i.to_string(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => dt.to_string(),
        Data::DateTimeIso(s) => s.clone(),
        Data::DurationIso(s) => s.clone(),
        Data::Error(e) => format!("#ERR:{:?}", e),
    }
}

fn data_to_py_value<'py>(py: Python<'py>, data: &Data) -> PyResult<Bound<'py, PyAny>> {
    match data {
        Data::Empty => Ok(py.None().into_bound(py)),
        Data::String(s) => Ok(PyString::new(py, s).into_any()),
        Data::Float(f) => Ok(f.into_pyobject(py)?.into_any()),
        Data::Int(i) => Ok(i.into_pyobject(py)?.into_any()),
        Data::Bool(b) => Ok(pyo3::types::PyBool::new(py, *b).to_owned().into_any()),
        Data::DateTime(dt) => Ok(dt.to_string().into_pyobject(py)?.into_any()),
        Data::DateTimeIso(s) => Ok(PyString::new(py, s).into_any()),
        Data::DurationIso(s) => Ok(PyString::new(py, s).into_any()),
        Data::Error(e) => Ok(PyString::new(py, &format!("#ERR:{:?}", e)).into_any()),
    }
}

struct ExcelWriterState {
    workbook: Workbook,
    worksheet: Worksheet,
    fieldnames: Option<Vec<Py<PyString>>>,
    header_written: bool,
    current_row: u32,
    path: String,
}

/// optimized Excel writer for creating standard .xlsx workbooks.
///
/// It provides a clean API for producing formatted Excel files with
/// automatic header generation and custom worksheet names.
#[pyclass]
pub struct ExcelWriter {
    state: Mutex<ExcelWriterState>,
}

#[pymethods]
impl ExcelWriter {
    #[new]
    #[pyo3(signature = (path, sheet_name=None, fieldnames=None))]
    pub fn new(
        py: Python<'_>,
        path: String,
        sheet_name: Option<String>,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let workbook = Workbook::new();
        let mut worksheet = Worksheet::new();

        if let Some(name) = sheet_name {
            worksheet.set_name(&name).map_err(wrap_py_err)?;
        }

        let py_fieldnames = fieldnames.map(|names| {
            names
                .into_iter()
                .map(|s| PyString::new(py, &s).unbind())
                .collect()
        });

        Ok(ExcelWriter {
            state: Mutex::new(ExcelWriterState {
                workbook,
                worksheet,
                fieldnames: py_fieldnames,
                header_written: false,
                current_row: 0,
                path,
            }),
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        write_record_to_excel(py, data, &mut state)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;

        let iterator = entries.try_iter()?;
        for entry in iterator {
            write_record_to_excel(py, entry?, &mut state)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;

        let path = state.path.clone();
        let worksheet = std::mem::replace(&mut state.worksheet, Worksheet::new());
        state.workbook.push_worksheet(worksheet);

        if path.starts_with("s3://") {
            let buffer = state.workbook.save_to_buffer().map_err(wrap_py_err)?;
            let controller = StorageController::new(&path).map_err(wrap_py_err)?;
            get_runtime().block_on(async {
                controller
                    .store()
                    .put(
                        &object_store::path::Path::from(controller.path()),
                        buffer.into(),
                    )
                    .await
                    .map_err(wrap_py_err)
            })?;
        } else {
            crate::io::ensure_parent_dir(&path).map_err(wrap_py_err)?;
            state.workbook.save(&path).map_err(wrap_py_err)?;
        }

        Ok(())
    }
}

fn write_record_to_excel(
    py: Python<'_>,
    data: Bound<'_, PyAny>,
    state: &mut ExcelWriterState,
) -> PyResult<()> {
    let record = data.cast::<PyDict>()?;

    if !state.header_written {
        if state.fieldnames.is_none() {
            let keys_list = record.keys();
            let mut record_keys = Vec::with_capacity(keys_list.len());
            for k in keys_list.iter() {
                record_keys.push(k.cast::<PyString>()?.clone());
            }

            record_keys.sort_by(|a, b| {
                let s1 = a.to_str().unwrap_or("");
                let s2 = b.to_str().unwrap_or("");
                s1.cmp(s2)
            });
            let interned: Vec<Py<PyString>> = record_keys.into_iter().map(|s| s.unbind()).collect();
            state.fieldnames = Some(interned);
        }

        let names = state
            .fieldnames
            .as_ref()
            .expect("Fieldnames should be initialized before header write");

        let header_format = Format::new().set_bold();

        for (col, name_py) in names.iter().enumerate() {
            let name_str = name_py.bind(py).to_str().unwrap_or("");
            state
                .worksheet
                .write_string_with_format(state.current_row, col as u16, name_str, &header_format)
                .map_err(wrap_py_err)?;
        }
        state.current_row += 1;
        state.header_written = true;
    }

    if let Some(names) = state.fieldnames.as_ref() {
        for (col, name_py) in names.iter().enumerate() {
            if let Some(value) = record.get_item(name_py.bind(py))? {
                write_cell_value(&mut state.worksheet, state.current_row, col as u16, value)?;
            }
        }
        state.current_row += 1;
    }

    Ok(())
}

fn write_cell_value(
    worksheet: &mut Worksheet,
    row: u32,
    col: u16,
    value: Bound<'_, PyAny>,
) -> PyResult<()> {
    if value.is_none() {
        return Ok(());
    }

    if let Ok(s) = value.cast::<PyString>() {
        worksheet
            .write_string(row, col, s.to_str()?)
            .map_err(wrap_py_err)?;
    } else if let Ok(i) = value.extract::<i64>() {
        worksheet
            .write_number(row, col, i as f64)
            .map_err(wrap_py_err)?;
    } else if let Ok(f) = value.extract::<f64>() {
        worksheet.write_number(row, col, f).map_err(wrap_py_err)?;
    } else if let Ok(b) = value.extract::<bool>() {
        worksheet.write_boolean(row, col, b).map_err(wrap_py_err)?;
    } else {
        worksheet
            .write_string(row, col, value.to_string())
            .map_err(wrap_py_err)?;
    }

    Ok(())
}

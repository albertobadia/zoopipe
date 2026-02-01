use crate::io::{BoxedReader, BoxedWriter, SharedWriter, SmartReader};
use crate::utils::wrap_py_err;
use csv::StringRecord;
use pyo3::prelude::*;
use std::io::{Cursor, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use crate::error::PipeError;
use crate::utils::interning::InternedKeys;
use itoa;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use ryu;
use std::collections::HashSet;

struct CSVReaderState {
    reader: SmartReader<StringRecord>,
    position: usize,
    limit: Option<usize>,
}

struct BoundedCsvIter<R: std::io::Read> {
    iter: csv::StringRecordsIntoIter<R>,
    start_byte: u64,
    end_byte: Option<u64>,
}

impl<R: std::io::Read> Iterator for BoundedCsvIter<R> {
    type Item = Result<StringRecord, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(end) = self.end_byte {
            let current_absolute_pos = self.start_byte + self.iter.reader().position().byte();
            if current_absolute_pos >= end {
                return None;
            }
        }
        self.iter
            .next()
            .map(|res: std::result::Result<csv::StringRecord, csv::Error>| {
                res.map_err(|e| format!("{}", e))
            })
    }
}

/// High-performance CSV reader implemented in Rust.
///
/// It supports streaming from local files, S3, or raw bytes. It uses
/// a threaded architecture to avoid GIL contention during I/O.
#[pyclass]
pub struct CSVReader {
    state: Mutex<CSVReaderState>,
    pub(crate) headers: Vec<Py<PyString>>,
    pub(crate) status_pending: Py<PyAny>,
    pub(crate) generate_ids: bool,
    projection: Option<HashSet<String>>,
    keys: InternedKeys,
}

#[pymethods]
impl CSVReader {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None, generate_ids=true, limit=None, start_byte=0, end_byte=None, projection=None))]
    fn new(
        py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
        generate_ids: bool,
        limit: Option<usize>,
        start_byte: u64,
        end_byte: Option<u64>,
        projection: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mut boxed_reader = if let Ok(mut r) = crate::io::get_reader(&path).map_err(wrap_py_err)
        {
            if start_byte > 0 {
                if path.ends_with(".gz") || path.ends_with(".zst") {
                    return Err(PipeError::Other(
                        "Cannot use start_byte with compressed files".into(),
                    )
                    .into());
                }
                r.seek(SeekFrom::Start(start_byte)).map_err(wrap_py_err)?;
            }
            r
        } else {
            return Err(PipeError::Other(format!("Failed to open file: {}", path)).into());
        };

        let expected_count = if let Some(fields) = &fieldnames {
            fields.len()
        } else {
            0
        };

        // 2a. If start_byte > 0, we perform the Heuristic Scan
        if start_byte > 0 {
            if expected_count == 0 {
                return Err(
                    PipeError::Other("Fieldnames required for start_byte > 0".into()).into(),
                );
            }

            loop {
                let start_pos = boxed_reader.stream_position().map_err(wrap_py_err)?;

                // Read line
                let mut line_buf = String::new();
                // We need to use `read_line` from `BufRead`. BoxedReader impls it.
                let n = std::io::BufRead::read_line(&mut boxed_reader, &mut line_buf)
                    .map_err(wrap_py_err)?;
                if n == 0 {
                    break; // EOF
                }

                // Check validity
                let mut t_rdr = csv::ReaderBuilder::new()
                    .delimiter(delimiter)
                    .quote(quote)
                    .has_headers(false)
                    .from_reader(Cursor::new(line_buf.as_bytes()));

                let mut record = csv::StringRecord::new();
                let is_valid = match t_rdr.read_record(&mut record) {
                    Ok(true) => record.len() == expected_count,
                    _ => false,
                };

                if is_valid {
                    // Found it. Seek back to `start_pos` so the REAL reader starts here.
                    boxed_reader
                        .seek(SeekFrom::Start(start_pos))
                        .map_err(wrap_py_err)?;
                    break;
                }
            }
        }

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .has_headers(start_byte == 0)
            .from_reader(boxed_reader);

        for _ in 0..skip_rows {
            let mut record = csv::StringRecord::new();
            if !reader.read_record(&mut record).map_err(wrap_py_err)? {
                break;
            }
        }

        let headers_str = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let headers_vec: Vec<Py<PyString>> = headers_str
            .into_iter()
            .map(|s| PyString::new(py, &s).unbind())
            .collect();

        let reader = SmartReader::new(&path, reader, move |r| BoundedCsvIter {
            iter: r.into_records(),
            start_byte,
            end_byte,
        });

        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            state: Mutex::new(CSVReaderState {
                reader,
                position: 0,
                limit,
            }),
            headers: headers_vec,
            status_pending,
            generate_ids,
            projection: projection.map(|v: Vec<String>| v.into_iter().collect::<HashSet<String>>()),
            keys: InternedKeys::new(py),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (data, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None, generate_ids=true, limit=None, projection=None))]
    fn from_bytes(
        py: Python<'_>,
        data: Vec<u8>,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
        generate_ids: bool,
        limit: Option<usize>,
        projection: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_reader(BoxedReader::Cursor(Cursor::new(data)));

        for _ in 0..skip_rows {
            let mut record = csv::StringRecord::new();
            if !reader.read_record(&mut record).map_err(wrap_py_err)? {
                break;
            }
        }

        let headers_str = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let headers: Vec<Py<PyString>> = headers_str
            .into_iter()
            .map(|s| PyString::new(py, &s).unbind())
            .collect();

        let reader_impl = SmartReader::new(
            "", // Empty path forces Sync
            reader,
            move |r| {
                BoundedCsvIter {
                    iter: r.into_records(),
                    start_byte: 0,
                    end_byte: limit.map(|_| u64::MAX), // Not using byte limit for bytes reader usually
                }
            },
        );

        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            state: Mutex::new(CSVReaderState {
                reader: reader_impl,
                position: 0,
                limit,
            }),
            headers,
            status_pending,
            generate_ids,
            projection: projection.map(|v: Vec<String>| v.into_iter().collect::<HashSet<String>>()),
            keys: InternedKeys::new(py),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', has_header=true))]
    fn count_rows(path: String, delimiter: u8, quote: u8, has_header: bool) -> PyResult<usize> {
        let boxed_reader = crate::io::get_reader(&path).map_err(wrap_py_err)?;

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .has_headers(has_header)
            .from_reader(boxed_reader);

        let mut count = 0;
        let mut record = csv::ByteRecord::new();
        while reader.read_byte_record(&mut record).map_err(wrap_py_err)? {
            count += 1;
        }

        Ok(count)
    }

    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let mut res = Vec::new();
        for h in &self.headers {
            res.push(h.bind(py).to_string());
        }
        Ok(res)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let mut state = slf.state.lock().map_err(|_| PipeError::MutexLock)?;
        slf.next_internal(py, &mut state)
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        let batch = PyList::empty(py);
        let mut count = 0;

        while count < batch_size {
            if let Some(item) = self.next_internal(py, &mut state)? {
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
}

impl CSVReader {
    fn next_internal<'py>(
        &self,
        py: Python<'py>,
        state: &mut CSVReaderState,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if let Some(lim) = state.limit
            && state.position >= lim
        {
            return Ok(None);
        }

        match state.reader.next() {
            Some(Ok(record)) => {
                let current_pos = state.position;
                state.position += 1;

                let raw_data = PyDict::new(py);
                for (header_py, value) in self.headers.iter().zip(record.iter()) {
                    let h_bound = header_py.bind(py);
                    if let Some(ref proj) = self.projection
                        && !proj.contains(h_bound.to_str().unwrap_or(""))
                    {
                        continue;
                    }
                    raw_data.set_item(h_bound, value)?;
                }

                let env = crate::utils::wrap_in_envelope(
                    py,
                    &self.keys,
                    raw_data.into_any(),
                    self.status_pending.bind(py).clone(),
                    current_pos,
                    self.generate_ids,
                )?;
                Ok(Some(env))
            }
            Some(Err(e)) => {
                let line = state.position + 1;
                Err(PipeError::Other(format!("CSV parse error at line {}: {}", line, e)).into())
            }
            None => Ok(None),
        }
    }
}

#[pyclass]
pub struct CSVWriter {
    state: Mutex<CSVWriterState>,
}

struct CSVWriterState {
    writer: csv::Writer<SharedWriter>,
    inner_writer: Arc<Mutex<BoxedWriter>>,
    fieldnames: Option<Vec<Py<PyString>>>,
    header_written: bool,
}

#[pymethods]
impl CSVWriter {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', fieldnames=None))]
    pub fn new(
        py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let boxed_writer = crate::io::get_writer(&path).map_err(wrap_py_err)?;

        let shared_writer = Arc::new(Mutex::new(boxed_writer));
        let writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_writer(SharedWriter(shared_writer.clone()));

        let py_fieldnames = fieldnames.map(|names| {
            names
                .into_iter()
                .map(|s| PyString::new(py, &s).unbind())
                .collect()
        });

        Ok(CSVWriter {
            state: Mutex::new(CSVWriterState {
                writer,
                inner_writer: shared_writer,
                fieldnames: py_fieldnames,
                header_written: false,
            }),
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        write_record_to_csv(py, data, &mut state)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;

        let iterator = entries.try_iter()?;
        for entry in iterator {
            write_record_to_csv(py, entry?, &mut state)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        state.writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        state.writer.flush().map_err(wrap_py_err)?;
        let mut inner = state
            .inner_writer
            .lock()
            .map_err(|_| PipeError::MutexLock)?;
        inner.close().map_err(wrap_py_err)?;
        Ok(())
    }
}

use std::borrow::Cow;

fn write_record_to_csv(
    py: Python<'_>,
    data: Bound<'_, PyAny>,
    state: &mut CSVWriterState,
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
        let bounds: Vec<Bound<'_, PyString>> = names.iter().map(|n| n.bind(py).clone()).collect();
        let mut name_strs = Vec::with_capacity(names.len());
        for b in &bounds {
            name_strs.push(b.to_str().unwrap_or(""));
        }
        state.writer.write_record(&name_strs).map_err(wrap_py_err)?;
        state.header_written = true;
    }

    if let Some(names) = state.fieldnames.as_ref() {
        let mut row_bounds: Vec<Option<Bound<'_, PyAny>>> = Vec::with_capacity(names.len());
        for name_py in names {
            row_bounds.push(record.get_item(name_py.bind(py))?);
        }

        let mut row_out: Vec<Cow<str>> = Vec::with_capacity(names.len());
        let mut itoa_buf = itoa::Buffer::new();
        let mut ryu_buf = ryu::Buffer::new();
        for opt_val in &row_bounds {
            if let Some(v) = opt_val {
                if let Ok(s) = v.cast::<PyString>() {
                    row_out.push(Cow::Borrowed(s.to_str().unwrap_or("")));
                } else if let Ok(i) = v.extract::<i64>() {
                    row_out.push(Cow::Owned(itoa_buf.format(i).to_owned()));
                } else if let Ok(f) = v.extract::<f64>() {
                    row_out.push(Cow::Owned(ryu_buf.format(f).to_owned()));
                } else if let Ok(b) = v.extract::<bool>() {
                    row_out.push(Cow::Borrowed(if b { "true" } else { "false" }));
                } else {
                    row_out.push(Cow::Owned(v.to_string()));
                }
            } else {
                row_out.push(Cow::Borrowed(""));
            }
        }
        state
            .writer
            .write_record(row_out.iter().map(|c| c.as_bytes()))
            .map_err(wrap_py_err)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bounded_csv_iter_basic() {
        let data = "col1,col2\nval1,val2\nval3,val4";
        let reader = Cursor::new(data);
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);

        let mut iter = BoundedCsvIter {
            iter: csv_reader.into_records(),
            start_byte: 0,
            end_byte: None,
        };

        let rec1 = iter.next().unwrap().unwrap();
        assert_eq!(rec1.get(0).unwrap(), "val1");

        let rec2 = iter.next().unwrap().unwrap();
        assert_eq!(rec2.get(0).unwrap(), "val3");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_bounded_csv_iter_limit_byte() {
        let data = "col1,col2\nval1,val2\nval3,val4";
        let reader = Cursor::new(data);
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);

        let mut iter = BoundedCsvIter {
            iter: csv_reader.into_records(),
            start_byte: 0,
            end_byte: Some(15),
        };

        let rec1 = iter.next().unwrap().unwrap();
        assert_eq!(rec1.get(0).unwrap(), "val1");

        assert!(iter.next().is_none());
    }
}

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyBytes};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::sync::Mutex;
use url::Url;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use crate::utils::interning::InternedKeys;

#[derive(Debug)]
enum KafkaData {
    Message(Vec<u8>, Option<Vec<u8>>),
    Error(String),
}

#[pyclass]
pub struct KafkaReader {
    receiver: Mutex<crossbeam_channel::Receiver<KafkaData>>,
    keys: InternedKeys,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    position: Mutex<usize>,
}

#[pymethods]
impl KafkaReader {
    #[new]
    #[pyo3(signature = (uri, group_id=None, generate_ids=true))]
    fn new(py: Python<'_>, uri: String, group_id: Option<String>, generate_ids: bool) -> PyResult<Self> {
        let parsed_url = Url::parse(&uri).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid Kafka URI: {}", e)))?;
        
        if parsed_url.scheme() != "kafka" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("URI scheme must be 'kafka'"));
        }

        let brokers: Vec<String> = parsed_url.host_str()
            .map(|h| vec![format!("{}:{}", h, parsed_url.port().unwrap_or(9092))])
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("No brokers specified in URI"))?;

        let topic = parsed_url.path().trim_start_matches('/').to_string();
        if topic.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("No topic specified in URI path"));
        }

        let mut builder = Consumer::from_hosts(brokers)
            .with_topic(topic.clone());

        if let Some(gid) = group_id {
            builder = builder.with_group(gid)
                .with_fallback_offset(FetchOffset::Earliest)
                .with_offset_storage(Some(GroupOffsetStorage::Kafka));
        }

        let mut consumer = builder.create().map_err(wrap_py_err)?;
        
        let (tx, rx) = crossbeam_channel::bounded(1000);

        std::thread::spawn(move || {
            loop {
                let msets = match consumer.poll() {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(KafkaData::Error(format!("Kafka poll error: {}", e)));
                        break;
                    }
                };

                if msets.is_empty() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }

                for mset in msets.iter() {
                    for msg in mset.messages() {
                        let key_opt = if msg.key.is_empty() {
                            None
                        } else {
                            Some(msg.key.to_vec())
                        };
                        
                        if tx.send(KafkaData::Message(msg.value.to_vec(), key_opt)).is_err() {
                            return;
                        }
                    }
                }
                
                if let Err(e) = consumer.commit_consumed() {
                     let _ = tx.send(KafkaData::Error(format!("Kafka commit error: {}", e)));
                }
            }
        });

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(KafkaReader {
            receiver: Mutex::new(rx),
            keys: InternedKeys::new(py),
            status_pending,
            generate_ids,
            position: Mutex::new(0),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let receiver = slf.receiver.lock().map_err(|_| PipeError::MutexLock)?;
        
        match receiver.recv() {
            Ok(KafkaData::Message(value_bytes, key_bytes)) => {
                slf.create_envelope(py, value_bytes, key_bytes)
            },
            Ok(KafkaData::Error(e)) => Err(crate::utils::wrap_py_err(std::io::Error::other(e))),
            Err(_) => Ok(None),
        }
    }
}

impl KafkaReader {
    fn create_envelope<'py>(&self, py: Python<'py>, value_bytes: Vec<u8>, key_bytes: Option<Vec<u8>>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut pos = self.position.lock().map_err(|_| PipeError::MutexLock)?;
        let current_pos = *pos;
        *pos += 1;

        let raw_data = PyDict::new(py);
        let value = PyBytes::new(py, &value_bytes);
        raw_data.set_item("value", value)?;
        
        if let Some(key) = key_bytes {
            raw_data.set_item("key", PyBytes::new(py, &key))?;
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
        envelope.set_item(self.keys.get_errors(py), pyo3::types::PyList::empty(py))?;

        Ok(Some(envelope.into_any()))
    }
}

#[pyclass]
pub struct KafkaWriter {
    producer: Mutex<Producer>,
    topic: String,
}

#[pymethods]
impl KafkaWriter {
    #[new]
    #[pyo3(signature = (uri, acks=1, timeout=30))]
    fn new(uri: String, acks: i16, timeout: u64) -> PyResult<Self> {
        let parsed_url = Url::parse(&uri).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid Kafka URI: {}", e)))?;
        
        if parsed_url.scheme() != "kafka" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("URI scheme must be 'kafka'"));
        }

        let brokers: Vec<String> = parsed_url.host_str()
            .map(|h| vec![format!("{}:{}", h, parsed_url.port().unwrap_or(9092))])
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("No brokers specified in URI"))?;

        let topic = parsed_url.path().trim_start_matches('/').to_string();
        if topic.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("No topic specified in URI path"));
        }

        let required_acks = match acks {
            0 => RequiredAcks::None,
            1 => RequiredAcks::One,
            -1 => RequiredAcks::All,
            _ => RequiredAcks::One,
        };

        let producer = Producer::from_hosts(brokers)
            .with_ack_timeout(std::time::Duration::from_secs(timeout))
            .with_required_acks(required_acks)
            .create()
            .map_err(wrap_py_err)?;

        Ok(KafkaWriter {
            producer: Mutex::new(producer),
            topic,
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = py;
        let mut producer = self.producer.lock().map_err(|_| PipeError::MutexLock)?;

        let raw_data = if let Ok(dict) = data.cast::<PyDict>() {
            if let Some(rd) = dict.get_item("raw_data")? {
                rd
            } else {
                data.clone()
            }
        } else {
            data.clone()
        };

        let bytes = if let Ok(b) = raw_data.cast::<PyBytes>() {
            b.as_bytes().to_vec()
        } else if let Ok(s) = raw_data.cast::<PyString>() {
            s.to_str()?.as_bytes().to_vec()
        } else {
            raw_data.to_string().as_bytes().to_vec()
        };

        producer.send(&Record::from_value(&self.topic, bytes)).map_err(wrap_py_err)?;

        Ok(())
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = py;
        let mut producer = self.producer.lock().map_err(|_| PipeError::MutexLock)?;
        let iterator = entries.try_iter()?;

        let mut all_bytes: Vec<Vec<u8>> = Vec::new();
        
        for entry in iterator {
            let entry = entry?;
            let raw_data = if let Ok(dict) = entry.cast::<PyDict>() {
                if let Some(rd) = dict.get_item("raw_data")? {
                    rd
                } else {
                    entry.clone()
                }
            } else {
                entry.clone()
            };

            let bytes = if let Ok(b) = raw_data.cast::<PyBytes>() {
                b.as_bytes().to_vec()
            } else if let Ok(s) = raw_data.cast::<PyString>() {
                s.to_str()?.as_bytes().to_vec()
            } else {
                raw_data.to_string().as_bytes().to_vec()
            };
            
            all_bytes.push(bytes);
        }

        let mut batch_records = Vec::with_capacity(all_bytes.len());
        for b in &all_bytes {
            batch_records.push(Record::from_value(&self.topic, b.as_slice()));
        }

        producer.send_all(&batch_records).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

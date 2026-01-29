use crate::error::PipeError;
use crate::io::get_runtime;
use crate::utils::interning::InternedKeys;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use std::sync::Mutex;
use std::time::Duration;
use url::Url;

use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

#[derive(Debug)]
enum KafkaData {
    Message(Vec<u8>, Option<Vec<u8>>),
    Error(String),
}

/// Fast Kafka consumer that streams messages from a topic.
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
    fn new(
        py: Python<'_>,
        uri: String,
        group_id: Option<String>,
        generate_ids: bool,
    ) -> PyResult<Self> {
        let parsed_url = Url::parse(&uri).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid Kafka URI: {}", e))
        })?;

        if parsed_url.scheme() != "kafka" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI scheme must be 'kafka'",
            ));
        }

        let brokers = parsed_url
            .host_str()
            .map(|h| format!("{}:{}", h, parsed_url.port().unwrap_or(9092)))
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("No brokers specified in URI")
            })?;

        let topic = parsed_url.path().trim_start_matches('/').to_string();
        if topic.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No topic specified in URI path",
            ));
        }

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &brokers);

        if let Some(gid) = group_id {
            config.set("group.id", &gid);
            config.set("enable.auto.commit", "true");
            config.set("auto.offset.reset", "earliest");
        } else {
            // If no group is provided, we still need one for rdkafka StreamConsumer usually
            // or we could use BaseConsumer. For simplicity in a stream-oriented pipe,
            // generate a random group if none provided.
            config.set("group.id", format!("zoopipe-{}", uuid::Uuid::new_v4()));
            config.set("auto.offset.reset", "earliest");
        }

        let consumer: StreamConsumer = {
            let _guard = get_runtime().enter();
            config
                .create()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };
        consumer
            .subscribe(&[&topic])
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let (tx, rx) = crossbeam_channel::bounded(1000);

        std::thread::spawn(move || {
            let rt = get_runtime();
            rt.block_on(async move {
                let mut stream = consumer.stream();
                while let Some(msg_res) = stream.next().await {
                    match msg_res {
                        Ok(msg) => {
                            let value = msg.payload().unwrap_or_default().to_vec();
                            let key = msg.key().map(|k| k.to_vec());
                            if tx.send(KafkaData::Message(value, key)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(KafkaData::Error(format!("Kafka poll error: {}", e)));
                            break;
                        }
                    }
                }
            });
        });

        let models = py.import("zoopipe.structs")?;
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

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let receiver = slf.receiver.lock().map_err(|_| PipeError::MutexLock)?;

        let result = py.detach(|| receiver.recv());

        match result {
            Ok(KafkaData::Message(value_bytes, key_bytes)) => {
                slf.create_envelope(py, value_bytes, key_bytes)
            }
            Ok(KafkaData::Error(e)) => Err(PyRuntimeError::new_err(e)),
            Err(_) => Ok(None),
        }
    }
}

impl KafkaReader {
    fn create_envelope<'py>(
        &self,
        py: Python<'py>,
        value_bytes: Vec<u8>,
        key_bytes: Option<Vec<u8>>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
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

/// Optimized Kafka producer for high-throughput message publishing.
#[pyclass]
pub struct KafkaWriter {
    producer: FutureProducer,
    topic: String,
}

#[pymethods]
impl KafkaWriter {
    #[new]
    #[pyo3(signature = (uri, acks=1, timeout=30))]
    fn new(uri: String, acks: i16, timeout: u64) -> PyResult<Self> {
        let parsed_url = Url::parse(&uri).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid Kafka URI: {}", e))
        })?;

        if parsed_url.scheme() != "kafka" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI scheme must be 'kafka'",
            ));
        }

        let brokers = parsed_url
            .host_str()
            .map(|h| format!("{}:{}", h, parsed_url.port().unwrap_or(9092)))
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("No brokers specified in URI")
            })?;

        let topic = parsed_url.path().trim_start_matches('/').to_string();
        if topic.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No topic specified in URI path",
            ));
        }

        let acks_str = match acks {
            0 => "0",
            1 => "1",
            -1 => "all",
            _ => "1",
        };

        let producer: FutureProducer = {
            let _guard = get_runtime().enter();
            ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("request.timeout.ms", format!("{}", timeout * 1000))
                .set("acks", acks_str)
                .create()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };

        Ok(KafkaWriter { producer, topic })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = self.extract_bytes(py, data)?;
        let topic = self.topic.clone();
        let producer = self.producer.clone();

        py.detach(|| {
            get_runtime().block_on(async move {
                producer
                    .send(
                        FutureRecord::<(), [u8]>::to(&topic).payload(&bytes),
                        Duration::from_secs(5),
                    )
                    .await
                    .map_err(|(e, _)| PyRuntimeError::new_err(e.to_string()))
            })
        })?;

        Ok(())
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let iterator = entries.try_iter()?;
        let mut futures = Vec::new();

        for entry in iterator {
            let entry = entry?;
            let bytes = self.extract_bytes(py, entry)?;
            let topic = self.topic.clone();
            let producer = self.producer.clone();

            futures.push(async move {
                producer
                    .send(
                        FutureRecord::<(), [u8]>::to(&topic).payload(&bytes),
                        Duration::from_secs(5),
                    )
                    .await
            });
        }

        py.detach(|| {
            get_runtime().block_on(async move {
                let results = futures_util::future::join_all(futures).await;
                for res in results {
                    if let Err((e, _)) = res {
                        return Err(PyRuntimeError::new_err(e.to_string()));
                    }
                }
                Ok(())
            })
        })?;

        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        let _guard = get_runtime().enter();
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| PyRuntimeError::new_err(format!("Kafka flush error: {}", e)))?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        self.flush()
    }
}

impl KafkaWriter {
    fn extract_bytes(&self, py: Python<'_>, entry: Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        let raw_data = if let Ok(dict) = entry.cast::<PyDict>() {
            if let Some(rd) = dict.get_item("raw_data")? {
                rd
            } else {
                entry.clone()
            }
        } else {
            entry.clone()
        };

        if let Ok(b) = raw_data.cast::<PyBytes>() {
            Ok(b.as_bytes().to_vec())
        } else if let Ok(s) = raw_data.cast::<PyString>() {
            Ok(s.to_str()?.as_bytes().to_vec())
        } else if raw_data.is_instance_of::<PyDict>() {
            let json = py.import("json")?;
            let dumps = json.getattr("dumps")?;
            let s: String = dumps.call1((&raw_data,))?.extract()?;
            Ok(s.into_bytes())
        } else {
            Ok(raw_data.to_string().into_bytes())
        }
    }
}

# Examples

Pipe includes executable examples demonstrating various data processing scenarios. All examples are located in the [`examples/`](../examples/) directory.

## Running Examples

---

## Available Examples

### [01_basic_csv.py](../examples/01_basic_csv.py)

Basic CSV processing with validation and error handling.

**Demonstrates:**
- Reading from CSV files
- Schema validation with Pydantic
- Writing validated data to output
- Separating errors into dedicated file

**Run:**
```bash
uv run examples/01_basic_csv.py
```

---

### [02_multiprocessing.py](../examples/02_multiprocessing.py)

Parallel processing using multiple CPU cores with MultiprocessingExecutor.

**Demonstrates:**
- Multi-core parallel processing
- Configurable worker count
- Chunk-based data processing
- LZ4 compression for efficient IPC

**Run:**
```bash
uv run examples/02_multiprocessing.py
```

---

### [03_ray_executor.py](../examples/03_ray_executor.py)

Distributed processing using Ray for massive datasets across clusters.

**Demonstrates:**
- Ray-based distributed computing
- Cluster support (local or remote)
- LZ4 compression for network efficiency
- Scalable data processing

**Run:**
```bash
uv run examples/03_ray_executor.py
```

---

### [04_json_processing.py](../examples/04_json_processing.py)

Working with JSON files in both array and JSONL formats.

**Demonstrates:**
- Reading JSON arrays
- Reading JSONL (JSON Lines) format
- Writing to formatted JSON
- JSON to JSON transformations

**Run:**
```bash
uv run examples/04_json_processing.py
```

---

### [05_hooks.py](../examples/05_hooks.py)

Using the Hooks system to transform and enrich data.

**Demonstrates:**
- TimestampHook for adding timestamps
- FieldMapperHook for field renaming
- Creating custom hooks
- Combining multiple hooks in a pipeline

**Run:**
```bash
uv run examples/05_hooks.py
```

### [06_sqlite_output_hook.py](../examples/06_sqlite_output_hook.py)

Advanced use of hooks for custom persistence (e.g., SQLite) while skipping standard output.

**Demonstrates:**
- Custom persistence hooks (SQLite)
- Using `DummyOutputAdapter` when output is handled by hooks
- Hook session management (`setup` and `teardown`)

**Run:**
```bash
uv run examples/06_sqlite_output_hook.py
```

---

### [07_distributed_file_reading.py](../examples/07_distributed_file_reading.py)

"JIT Ingestion" pattern: the coordinator only sends light metadata, and workers fetch the actual data.

**Demonstrates:**
- Zero-copy data ingestion pattern
- Using hooks to fetch data on demand in parallel workers
- Efficient handling of large metadata-only input streams

**Run:**
```bash
uv run examples/07_distributed_file_reading.py
```

---

### [08_file_partitioning.py](../examples/08_file_partitioning.py)

Parallel file reading using byte-ranges and the `FilePartitioner`.

**Demonstrates:**
- `FilePartitioner` for splitting large files into byte-range chunks
- `PartitionedReaderHook` for parallel distributed file reading
- Efficiently horizontal scaling of file-based workloads

**Run:**
```bash
uv run examples/08_file_partitioning.py
```

---

### [09_async_queues.py](../examples/09_async_queues.py)

Ingestion using asynchronous queues for high-concurrency ingestion.

**Demonstrates:**
- `AsyncQueueInputAdapter` and `AsyncQueueOutputAdapter`
- Producers and Consumers in an async pipeline
- Handling streams of data asynchronously

**Run:**
```bash
uv run examples/09_async_queues.py
```

---

### [10_sync_queues.py](../examples/10_sync_queues.py)

Using standard thread-safe queues for communication between pipeline stages.

**Demonstrates:**
- `QueueInputAdapter` and `QueueOutputAdapter`
- Interleaving production and consumption in sync mode
- Sentinel-based flow control

**Run:**
```bash
uv run examples/10_sync_queues.py
```

---

### [11_handling_errors.py](../examples/11_handling_errors.py)

Deep dive into error reporting and dead-letter queues.

**Demonstrates:**
- Validation failure handling
- Custom `error_output_adapter` configuration
- Analyzing the final `FlowReport` for error details

**Run:**
```bash
uv run examples/11_handling_errors.py
```

---

### [12_no_pydantic.py](../examples/12_no_pydantic.py)

Running pipelines without Pydantic schemas (using dictionary transformations).

**Demonstrates:**
- Schema-less data processing
- Flexible data enrichment
- Handling raw dictionaries in encoders

**Run:**
```bash
uv run examples/12_no_pydantic.py
```

---

### [12_threads_io_bound.py](../examples/12_threads_io_bound.py)

Concurrent processing of IO-bound tasks using ThreadExecutor.

**Demonstrates:**
- Using `ThreadExecutor` for concurrent operations
- Simulating slow IO operations (API calls) in hooks
- Comparing concurrent performance vs sequential expectation

**Run:**
```bash
uv run examples/12_threads_io_bound.py
```

---

### [13_no_output_adapter.py](../examples/13_no_output_adapter.py)

Executing pipelines where processing happens in hooks or executors and results aren't persisted via an adapter.

**Demonstrates:**
- Pipelines without an `output_adapter`
- Pure-transformation or analysis flows
- Efficient termination of processed streams

**Run:**
```bash
uv run examples/13_no_output_adapter.py
```

---

### [13_pyarrow_adapter.py](../examples/13_pyarrow_adapter.py)

High-performance processing of Parquet files using PyArrow.

**Demonstrates:**
- `ArrowInputAdapter` and `ArrowOutputAdapter`
- Batch-based Parquet processing
- Automatic schema inference

**Run:**
```bash
uv run examples/13_pyarrow_adapter.py
```

---

### [14_adapter_hooks_fetching.py](../examples/14_adapter_hooks_fetching.py)

Advanced adapter-level hook patterns for fetching remote data.

**Demonstrates:**
- Registering hooks directly on the input adapter
- Fetching details in the pre-validation stage
- Dynamic data enrichment before the main executor runs

**Run:**
```bash
uv run examples/14_adapter_hooks_fetching.py
```

---

### [15_sqlalchemy_jit.py](../examples/15_sqlalchemy_jit.py)

"Just-In-Time" (JIT) SQL fetching pattern: the input adapter generates row IDs, and a specialized hook performs the actual fetching in bulk within the worker.

**Demonstrates:**
- `SQLInputAdapter` for lightweight metadata generation
- `SQLFetchHook` with thread-safe connection management (`setup`/`teardown`)
- `SQLOutputAdapter` for bulk persistence
- Parallel distributed fetching pattern for massive databases (100k+ rows)

**Run:**
```bash
uv run examples/scripts/generate_sql_data.py
PYTHONPATH=src uv run examples/15_sqlalchemy_jit.py
```

---

## Sample Data

All examples use the sample data files located in [`examples/data/`](../examples/data/):

- `sample_data.csv` - Sample CSV data with user information
- `sample_data.json` - Sample JSON array format
- `sample_data.jsonl` - Sample JSONL (JSON Lines) format

Output files are created in the [`examples/output_data/`](../examples/output_data/) directory after running examples.

---

## Additional Resources

For more detailed information, see:

- [Executors Documentation](executors.md) - Learn about different execution strategies
- [Adapters Documentation](adapters.md) - Input and output adapter reference
- [RFC: Architecture and Design](RFC.md) - Deep dive into Pipe's architecture

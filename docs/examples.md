# Examples

FlowSchema includes executable examples demonstrating various data processing scenarios. All examples are located in the [`examples/`](../examples/) directory.

## Running Examples

From the project root directory:

```bash
uv run examples/01_basic_csv.py
```

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

---

## Sample Data

All examples use the sample data files located in the project root:

- `sample_data.csv` - Sample CSV data
- `sample_data.json` - Sample JSON array
- `sample_data.jsonl` - Sample JSONL data

---

## Additional Resources

For more detailed information, see:

- [Executors Documentation](executors.md) - Learn about different execution strategies
- [Adapters Documentation](adapters.md) - Input and output adapter reference
- [RFC: Architecture and Design](RFC.md) - Deep dive into FlowSchema's architecture

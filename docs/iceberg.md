# Iceberg Adapters

ZooPipe provides high-performance Iceberg adapters for working with data lakes. It supports parallel writing to Iceberg tables and high-speed parallel reading with automatic file discovery.

## IcebergOutputAdapter

Write data to an Iceberg table using a high-performance Rust-powered Parquet writer and atomic commits.

### Basic Usage

```python
from pydantic import BaseModel
from zoopipe import CSVInputAdapter, IcebergOutputAdapter, Pipe

class UserSchema(BaseModel):
    user_id: str
    username: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=IcebergOutputAdapter(
        table_location="examples/output_data/iceberg_table",
        catalog_properties={"type": "hadoop"}
    ),
    schema_model=UserSchema,
)

pipe.run()
```

### Parameters

- **table_location** (`str`): The root directory of the Iceberg table.
- **catalog_properties** (`dict`, optional): Properties for the Iceberg catalog. Defaults to `{"type": "hadoop"}`.

### Features

- **High-Speed Ingestion**: Utilizes native Rust Parquet writers (~450k+ rows/s).
- **Atomic Commits**: Uses the `IcebergCoordinator` to ensure that results from all parallel workers are committed atomically in a single transaction.
- **Automatic Schema Inference**: Infers the Iceberg schema from the first batch of data.
- **Metadata Management**: Automatically generates `metadata.json` and `version-hint.text` in the `metadata/` directory.

---

## IcebergInputAdapter

Read data from an Iceberg table with efficient parallel sharding and file discovery.

### Basic Usage

```python
from zoopipe import IcebergInputAdapter, JSONOutputAdapter, Pipe, PipeManager
from zoopipe.engines.zoosync import ZoosyncPoolEngine

pipe = Pipe(
    input_adapter=IcebergInputAdapter("examples/output_data/iceberg_table"),
    output_adapter=JSONOutputAdapter("users.jsonl", format="jsonl"),
)

# Parallel reading across 4 workers
with PipeManager.parallelize_pipe(
    pipe, workers=4, engine=ZoosyncPoolEngine()
) as manager:
    manager.run()
```

### Parameters

- **table_location** (`str`): The root directory of the Iceberg table.
- **generate_ids** (`bool`, default=`True`): Whether to generate unique IDs for each record.
- **batch_size** (`int`, default=`1024`): Number of rows to read per batch.

### Features

- **Automatic File Discovery**: Scans the `data/` directory of the Iceberg table for Parquet files using a fast Rust implementation.
- **Intelligent Sharding**: Automatically distributes files among available workers for maximum parallelism.
- **Multi-File Sequential Reading**: Each worker uses the `MultiParquetReader` (Rust) to stream through its assigned files without overhead.
- **Extreme Throughput**: Capable of scanning Iceberg tables at over 1,000,000 rows/s.

---

## Performance Comparison

| Format | Write Speed (rows/s) | Read Speed (rows/s) |
|--------|----------------------|----------------------|
| **CSV** | ~350,000 | ~400,000 |
| **JSONL** | ~250,000 | ~300,000 |
| **Parquet** | ~400,000 | ~800,000 |
| **Iceberg** | **~450,000** | **~1,000,000+** |

*Benchmarks conducted on local M1 Max with 4 parallel workers.*

## Best Practices

1. **Clean Runs**: When running examples or tests, ensure you clear the `table_location` if you want a fresh start.
2. **Parallelize**: Always use `PipeManager.parallelize_pipe` with `IcebergInputAdapter` to leverage multi-core performance.
3. **Zoosync Engine**: Recommended for the best inter-process communication performance.

# DuckDB Output Adapter

Writes validated data to DuckDB databases using efficient batch processing and transaction support.

## Overview

The `DuckDBOutputAdapter` persists data to DuckDB tables using batched writes with automatic transaction management. It converts validated entries to CSV format internally for high-performance bulk inserts.

## Features

- **Batch Writing**: Accumulates entries and writes in configurable batches
- **Transaction Support**: Automatic BEGIN/COMMIT/ROLLBACK for data integrity
- **CSV-based Bulk Insert**: Uses DuckDB's optimized CSV reader for fast inserts
- **Automatic Flushing**: Ensures all data is written on close
- **Thread Configuration**: Optimized for parallel execution with PRAGMA settings

## Installation

Requires the `duckdb` optional dependency:

```bash
uv add zoopipe[duckdb]
```

Or with pip:

```bash
pip install zoopipe[duckdb]
```

## Usage

### Basic Example

```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("raw_data.csv"),
    output_adapter=DuckDBOutputAdapter(
        database="analytics.duckdb",
        table_name="processed_events"
    ),
    executor=MultiProcessingExecutor(EventSchema, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### DuckDB to DuckDB Pipeline

```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter

pipe = Pipe(
    input_adapter=DuckDBInputAdapter("source.duckdb", "raw_data", batch_size=5000),
    output_adapter=DuckDBOutputAdapter("target.duckdb", "clean_data", batch_size=5000),
    executor=RayExecutor(DataSchema)
)

with pipe:
    report = pipe.start()
    report.wait()
```

## Parameters

### Constructor Parameters

- **`database`** (str, required): Path to the DuckDB database file
- **`table_name`** (str, required): Name of the table to write to
- **`batch_size`** (int, optional): Number of rows to accumulate before flushing. Default: `1000`
- **`pre_hooks`** (list[BaseHook], optional): Pre-write hooks. Default: `None`
- **`post_hooks`** (list[BaseHook], optional): Post-write hooks. Default: `None`

## How It Works

### Write Process

1. **Entry Buffering**: Each validated entry is added to an in-memory buffer
2. **Batch Detection**: When buffer reaches `batch_size`, automatic flush is triggered
3. **CSV Conversion**: Buffer is converted to CSV format in memory
4. **Bulk Insert**: DuckDB reads CSV and inserts rows in a single transaction
5. **Buffer Clear**: Buffer is cleared after successful write

### Transaction Management

Each flush operation is wrapped in a transaction:

```sql
BEGIN TRANSACTION;
INSERT INTO table_name SELECT * FROM _rel;
COMMIT;
```

If an error occurs during insert, the transaction is rolled back:

```sql
ROLLBACK;
```

## Data Source Priority

The adapter writes data in the following priority:

1. **`validated_data`**: If present (after successful Pydantic validation)
2. **`raw_data`**: If `validated_data` is not available

This allows flexibility in processing both validated and raw entries.

## Performance Tips

- **Batch Size**: Larger batches improve throughput but increase memory usage
  - Small datasets: 1,000 - 5,000
  - Large datasets: 10,000 - 50,000
- **Thread Configuration**: The adapter automatically sets `PRAGMA threads=8`
- **Pre-create Tables**: Create the target table with proper schema before running the pipeline
- **Indexes**: Add indexes after bulk insert, not before

## Example: CSV to DuckDB with Validation

```python
from pydantic import BaseModel, Field
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter
import duckdb

class SalesRecord(BaseModel):
    order_id: int
    customer_id: int
    amount: float = Field(gt=0)
    status: str

conn = duckdb.connect("sales.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER,
        customer_id INTEGER,
        amount DOUBLE,
        status VARCHAR
    )
""")
conn.close()

pipe = Pipe(
    input_adapter=CSVInputAdapter("sales.csv"),
    output_adapter=DuckDBOutputAdapter(
        database="sales.duckdb",
        table_name="orders",
        batch_size=5000
    ),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=MultiProcessingExecutor(SalesRecord, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()

print(f"Inserted {report.success_count} records")
print(f"Errors: {report.error_count}")
```

## Example: Building Analytics Pipeline

```python
from zoopipe import Pipe
from zoopipe.executor.dask import DaskExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter
from zoopipe.hooks.builtin import TimestampHook

class EventSchema(BaseModel):
    event_id: int
    user_id: int
    event_type: str
    payload: dict

conn = duckdb.connect("analytics.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        event_type VARCHAR,
        payload JSON,
        processed_at TIMESTAMP
    )
""")
conn.close()

pipe = Pipe(
    input_adapter=JSONInputAdapter("events.jsonl", format="jsonl"),
    output_adapter=DuckDBOutputAdapter("analytics.duckdb", "events", batch_size=10000),
    executor=DaskExecutor(EventSchema),
    post_validation_hooks=[TimestampHook(field_name="processed_at")]
)

with pipe:
    report = pipe.start()
    report.wait()
```

## Flushing Behavior

### Automatic Flush

Flush is triggered automatically when:
- Buffer reaches `batch_size`
- Adapter is closed (via `close()` or context manager exit)

### Manual Flush

You can manually flush data:

```python
adapter = DuckDBOutputAdapter("db.duckdb", "table")
adapter.open()
# ... write entries ...
adapter.flush()  # Force immediate write
adapter.close()
```

## Error Handling

If a write error occurs:
1. Transaction is rolled back
2. Exception is raised
3. Buffer is NOT cleared (data is preserved)
4. You can retry or handle the error

```python
try:
    pipe.start()
except Exception as e:
    print(f"Write failed: {e}")
    # Handle error, potentially retry
```

## See Also

- [DuckDB Input Adapter](duckdb-input.md) - Reading from DuckDB
- [Hooks Documentation](hooks.md) - Transform data before writing
- [MultiProcessing Executor](../executors.md#multiprocessingexecutor) - Parallel processing
- [DuckDB Documentation](https://duckdb.org/) - Official DuckDB docs

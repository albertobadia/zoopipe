# DuckDB Input Adapter

Reads data from DuckDB databases using an efficient "Just-In-Time" (JIT) metadata pattern.

## Overview

The `DuckDBInputAdapter` generates lightweight metadata entries containing batch information (offsets or primary key ranges). The actual data fetching is delegated to workers through the `DuckDBFetchHook`, enabling parallel distributed reading of large DuckDB tables.

## Features

- **JIT Metadata Pattern**: Coordinator only generates metadata; workers fetch actual data
- **Batch Processing**: Configurable batch sizes for efficient memory usage
- **PK Range Optimization**: Automatically uses primary key ranges for numeric columns
- **Parallel Processing**: Each worker fetches its own batch independently
- **Automatic Hook Injection**: Auto-injects `DuckDBFetchHook` if not provided
- **Flexible Batching**: Falls back to OFFSET/LIMIT for non-numeric primary keys

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
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        database="analytics.duckdb",
        table_name="events",
        batch_size=1000
    ),
    output_adapter=CSVOutputAdapter("output.csv"),
    executor=MultiProcessingExecutor(EventSchema, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### Distributed Processing with Ray

```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        database="source.duckdb",
        table_name="raw_data",
        batch_size=5000,
        pk_column="event_id"
    ),
    output_adapter=DuckDBOutputAdapter("processed.duckdb", "clean_data"),
    executor=RayExecutor(DataSchema)
)
```

## Parameters

### Constructor Parameters

- **`database`** (str, required): Path to the DuckDB database file
- **`table_name`** (str, required): Name of the table to read from
- **`batch_size`** (int, optional): Number of rows per batch. Default: `1000`
- **`pk_column`** (str, optional): Primary key column name for range optimization. Default: `"id"`
- **`total_rows`** (int | None, optional): Total row count (auto-detected if `None`). Default: `None`
- **`id_generator`** (Callable, optional): Function to generate entry IDs. Default: `uuid.uuid4`
- **`pre_hooks`** (list[BaseHook], optional): Pre-validation hooks. Default: `None`
- **`post_hooks`** (list[BaseHook], optional): Post-validation hooks. Default: `None`
- **`auto_inject_fetch_hook`** (bool, optional): Automatically inject `DuckDBFetchHook`. Default: `True`

## How It Works

### Metadata Generation Phase

The adapter generates metadata entries without fetching actual data:

1. **Row Count**: Determines total rows in the table
2. **Schema Inspection**: Checks if `pk_column` is numeric
3. **Optimization Decision**:
   - **Numeric PK**: Uses PK range boundaries for efficient fetching
   - **Non-numeric PK**: Falls back to OFFSET/LIMIT

### Metadata Structure

**For Numeric Primary Keys (Optimized)**:
```python
{
    "table": "events",
    "pk_column": "event_id",
    "pk_start": 1000,
    "pk_end": 2000,
    "is_duckdb_range_metadata": True
}
```

**For OFFSET/LIMIT (Fallback)**:
```python
{
    "table": "events",
    "batch_offset": 5000,
    "batch_limit": 1000,
    "is_duckdb_range_metadata": True
}
```

### Data Fetching Phase

The `DuckDBFetchHook` (auto-injected by default) reads the metadata and fetches actual rows:

```python
# For PK ranges
SELECT * FROM events WHERE event_id >= 1000 AND event_id < 2000

# For OFFSET/LIMIT
SELECT * FROM events OFFSET 5000 LIMIT 1000
```

## Integration with DuckDBFetchHook

The `DuckDBFetchHook` is automatically registered as a pre-validation hook. It:

1. Reads the metadata from each entry
2. Connects to the DuckDB database in each worker
3. Executes the appropriate query (PK range or OFFSET/LIMIT)
4. Populates `entry['raw_data']` with fetched rows
5. Returns entries for validation

### Manual Hook Configuration

You can disable auto-injection and provide your own hooks:

```python
from zoopipe.hooks.duckdb import DuckDBFetchHook

custom_hook = DuckDBFetchHook(database="custom.duckdb")

adapter = DuckDBInputAdapter(
    database="metadata.duckdb",
    table_name="events",
    auto_inject_fetch_hook=False,
    pre_hooks=[custom_hook, OtherHook()]
)
```

## Performance Tips

- **Batch Size**: Larger batches reduce overhead but increase memory usage per worker
- **Primary Key Optimization**: Use numeric primary keys for better query performance
- **Worker Count**: Match worker count to available CPU cores or database connections
- **Parallel Workers**: More workers = more concurrent database connections

## Example with MultiProcessing

```python
from pydantic import BaseModel
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter

class EventSchema(BaseModel):
    event_id: int
    user_id: int
    event_type: str
    timestamp: str

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        database="events.duckdb",
        table_name="raw_events",
        batch_size=2000,
        pk_column="event_id"
    ),
    output_adapter=JSONOutputAdapter("processed_events.jsonl", format="jsonl"),
    executor=MultiProcessingExecutor(EventSchema, max_workers=8)
)

with pipe:
    report = pipe.start()
    report.wait()

print(f"Processed {report.success_count} events")
```

## See Also

- [DuckDB Output Adapter](duckdb-output.md) - Writing to DuckDB
- [Hooks Documentation](hooks.md) - Learn about hooks system
- [MultiProcessing Executor](../executors.md#multiprocessingexecutor) - Parallel processing
- [Ray Executor](../executors.md#rayexecutor) - Distributed processing

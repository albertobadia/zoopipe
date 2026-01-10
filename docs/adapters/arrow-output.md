# Arrow Output Adapter

The `ArrowOutputAdapter` writes validated data to Apache Parquet files using PyArrow, providing efficient columnar storage for analytics workloads.

## Features

- **Columnar Storage**: Uses Apache Parquet format for efficient compression and query performance
- **Batched Writing**: Buffers records and writes in batches for optimal performance
- **Schema Flexibility**: Auto-infers schema or accepts explicit PyArrow schemas
- **Writer Options**: Supports all PyArrow Parquet writer options (compression, row group size, etc.)

## Installation

The Arrow adapter requires PyArrow as an optional dependency:

```bash
uv add pyarrow
```

Or with pip:

```bash
pip install pyarrow
```

## Usage

### Basic Example

```python
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.arrow import ArrowOutputAdapter
from pydantic import BaseModel

class UserSchema(BaseModel):
    name: str
    age: int
    email: str

pipe = Pipe(
    input_adapter=JSONInputAdapter("users.json"),
    output_adapter=ArrowOutputAdapter("output.parquet"),
    executor=SyncFifoExecutor(UserSchema)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### With Custom Schema

```python
import pyarrow as pa
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

schema = pa.schema([
    ("name", pa.string()),
    ("age", pa.int64()),
    ("email", pa.string()),
    ("created_at", pa.timestamp("ms"))
])

output_adapter = ArrowOutputAdapter(
    "users.parquet",
    schema=schema,
    batch_size=5000
)
```

### With Compression

```python
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

output_adapter = ArrowOutputAdapter(
    "compressed_data.parquet",
    batch_size=10000,
    compression="snappy"
)
```

## Constructor Parameters

### `output`
- **Type**: `str | pathlib.Path`
- **Required**: Yes
- **Description**: Path to the output Parquet file

### `format`
- **Type**: `str`
- **Default**: `"parquet"`
- **Description**: Output format (currently only "parquet" is supported)

### `batch_size`
- **Type**: `int`
- **Default**: `1000`
- **Description**: Number of records to buffer before writing to disk

### `schema`
- **Type**: `pa.Schema | None`
- **Default**: `None`
- **Description**: PyArrow schema for the output file. If not provided, schema is inferred from the first batch

### `**writer_options`
- **Type**: Additional keyword arguments
- **Description**: Passed directly to `pyarrow.parquet.ParquetWriter`. Common options:
  - `compression`: Compression codec (`"snappy"`, `"gzip"`, `"lz4"`, `"zstd"`, `"none"`)
  - `use_dictionary`: Enable dictionary encoding (default: `True`)
  - `compression_level`: Compression level for codecs that support it

## Use Cases

### Data Analytics
Parquet is optimized for analytical queries with column pruning and predicate pushdown:

```python
output_adapter = ArrowOutputAdapter(
    "analytics_data.parquet",
    batch_size=50000,
    compression="zstd"
)
```

### Data Lakes
Write validation results directly to data lake storage:

```python
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

output_adapter = ArrowOutputAdapter(
    "/data/lake/validated/events.parquet",
    batch_size=100000,
    compression="snappy"
)
```

### ETL Pipelines
Transform CSV/JSON to efficient Parquet format:

```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiprocessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=ArrowOutputAdapter(
        "transformed.parquet",
        batch_size=10000,
        compression="snappy"
    ),
    executor=MultiprocessingExecutor(YourSchema, num_workers=4)
)
```

## Notes

- The adapter writes only the `validated_data` field from entries (or `raw_data` if validation is skipped)
- The output directory is created automatically if it doesn't exist
- Records are flushed to disk when the buffer reaches `batch_size` or when the adapter closes
- Schema is inferred from the first batch if not explicitly provided

## Reading Parquet Files

You can read the output files with PyArrow, Pandas, or DuckDB:

```python
import pyarrow.parquet as pq

table = pq.read_table("output.parquet")
df = table.to_pandas()
```

```python
import pandas as pd

df = pd.read_parquet("output.parquet")
```

```python
import duckdb

result = duckdb.query("SELECT * FROM 'output.parquet' WHERE age > 25")
```

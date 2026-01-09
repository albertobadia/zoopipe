# Arrow Input Adapter

The `ArrowInputAdapter` reads data from Apache Parquet and other Arrow-compatible formats, providing efficient columnar data reading with advanced features like partitioning, filtering, and column selection.

## Features

- **Multiple Formats**: Supports Parquet, Feather, Arrow IPC, and other Arrow formats
- **Partitioned Datasets**: Read from partitioned data lakes with automatic partition discovery
- **Column Selection**: Read only specific columns to minimize memory usage
- **Predicate Pushdown**: Filter data at the storage layer for better performance
- **Batch Processing**: Efficiently processes data in batches using PyArrow

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
from flowschema import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.arrow import ArrowInputAdapter
from flowschema.output_adapter.json import JSONOutputAdapter
from pydantic import BaseModel

class UserSchema(BaseModel):
    name: str
    age: int
    email: str

flow = FlowSchema(
    input_adapter=ArrowInputAdapter("users.parquet"),
    output_adapter=JSONOutputAdapter("output.json"),
    executor=SyncFifoExecutor(UserSchema)
)

with flow:
    report = flow.start()
    report.wait()
```

### Reading Specific Columns

```python
from flowschema.input_adapter.arrow import ArrowInputAdapter

input_adapter = ArrowInputAdapter(
    "large_dataset.parquet",
    columns=["name", "email", "age"]
)
```

### Filtering Data

```python
import pyarrow.dataset as ds
from flowschema.input_adapter.arrow import ArrowInputAdapter

filter_expr = (ds.field("age") > 25) & (ds.field("country") == "US")

input_adapter = ArrowInputAdapter(
    "users.parquet",
    filter=filter_expr
)
```

### Reading Partitioned Datasets

```python
from flowschema.input_adapter.arrow import ArrowInputAdapter

input_adapter = ArrowInputAdapter(
    "/data/lake/events/",
    partitioning="hive"
)
```

### Multiple Files

```python
from flowschema.input_adapter.arrow import ArrowInputAdapter
import pathlib

files = [
    pathlib.Path("data/part1.parquet"),
    pathlib.Path("data/part2.parquet"),
    pathlib.Path("data/part3.parquet")
]

input_adapter = ArrowInputAdapter(files)
```

## Constructor Parameters

### `source`
- **Type**: `str | pathlib.Path | list[str] | list[pathlib.Path]`
- **Required**: Yes
- **Description**: Path to a single file, directory, or list of files to read

### `format`
- **Type**: `str`
- **Default**: `"parquet"`
- **Description**: File format to read. Options include `"parquet"`, `"feather"`, `"ipc"`, `"csv"`

### `partitioning`
- **Type**: `str | ds.Partitioning | None`
- **Default**: `None`
- **Description**: Partitioning scheme for the dataset. Common values:
  - `"hive"` - Hive-style partitioning (e.g., `year=2023/month=01/`)
  - `None` - No partitioning or auto-detect

### `columns`
- **Type**: `list[str] | None`
- **Default**: `None` (read all columns)
- **Description**: List of column names to read. Reduces memory usage and improves performance

### `filter`
- **Type**: `ds.Expression | None`
- **Default**: `None`
- **Description**: PyArrow filter expression for predicate pushdown

### `**dataset_options`
- **Type**: Additional keyword arguments
- **Description**: Passed directly to `pyarrow.dataset.dataset()`. See PyArrow documentation for available options

## Use Cases

### Data Lake Processing
Read from partitioned data lakes with automatic partition filtering:

```python
import pyarrow.dataset as ds
from flowschema.input_adapter.arrow import ArrowInputAdapter

filter_expr = ds.field("date") >= "2024-01-01"

input_adapter = ArrowInputAdapter(
    "/data/lake/events/",
    partitioning="hive",
    filter=filter_expr,
    columns=["user_id", "event_type", "timestamp"]
)
```

### ETL from Parquet to JSON
Convert Parquet files to JSON with validation:

```python
from flowschema import FlowSchema
from flowschema.executor.multiprocessing import MultiprocessingExecutor
from flowschema.input_adapter.arrow import ArrowInputAdapter
from flowschema.output_adapter.json import JSONOutputAdapter

flow = FlowSchema(
    input_adapter=ArrowInputAdapter("input.parquet"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
    executor=MultiprocessingExecutor(Schema, num_workers=4)
)
```

### Memory-Efficient Processing
Read only required columns from large Parquet files:

```python
from flowschema.input_adapter.arrow import ArrowInputAdapter

input_adapter = ArrowInputAdapter(
    "huge_dataset.parquet",
    columns=["id", "name", "status"]
)
```

### Filtering at Source
Apply filters before loading data into memory:

```python
import pyarrow.dataset as ds
from flowschema.input_adapter.arrow import ArrowInputAdapter

filter_expr = (
    (ds.field("country") == "US") &
    (ds.field("created_at") >= "2024-01-01") &
    (ds.field("is_active") == True)
)

input_adapter = ArrowInputAdapter(
    "users.parquet",
    filter=filter_expr
)
```

## Advanced Features

### Working with Partitioned Data

```python
from flowschema.input_adapter.arrow import ArrowInputAdapter
import pyarrow.dataset as ds

input_adapter = ArrowInputAdapter(
    "/data/warehouse/sales/",
    partitioning=ds.partitioning(
        schema=ds.schema([
            ("year", ds.int32()),
            ("month", ds.int32())
        ]),
        flavor="hive"
    ),
    filter=ds.field("year") == 2024
)
```

### Combining Column Selection and Filtering

```python
import pyarrow.dataset as ds
from flowschema.input_adapter.arrow import ArrowInputAdapter

input_adapter = ArrowInputAdapter(
    "large_events.parquet",
    columns=["event_id", "user_id", "event_type", "timestamp"],
    filter=ds.field("event_type").isin(["click", "purchase"])
)
```

## Performance Tips

1. **Use Column Selection**: Only read columns you need to reduce memory usage
2. **Push Down Filters**: Use the `filter` parameter to reduce data loaded from disk
3. **Leverage Partitioning**: Partitioned datasets allow efficient filtering by partition keys
4. **Batch Size**: PyArrow automatically handles batching for optimal performance

## Notes

- The adapter reads data using PyArrow's `scanner` API for efficient streaming
- Each row from the Parquet file becomes an entry with `raw_data` set to a dictionary
- Filtering happens at the storage layer before data enters FlowSchema
- Supports reading from local files, directories, and lists of files
- Compatible with all Arrow-supported file formats

## Related Adapters

- [ArrowOutputAdapter](arrow-output.md) - Write validated data to Parquet files
- [CSVInputAdapter](csv-input.md) - Read from CSV files
- [JSONInputAdapter](json-input.md) - Read from JSON files

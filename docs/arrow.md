# Arrow Adapters

ZooPipe provides Apache Arrow adapters for working with high-performance columnar data in the Arrow IPC (Feather) format.

## What is Apache Arrow?

Apache Arrow is a cross-language development platform for in-memory columnar data. It provides:

- **Zero-Copy Reads**: Direct memory access without serialization
- **Columnar Format**: Optimized for analytical operations
- **Interoperability**: Share data between Python, Rust, R, Java, etc. without copying
- **Efficient Compression**: Built-in compression algorithms
- **Type Safety**: Rich type system with nested and complex types

## ArrowInputAdapter

Read data from Arrow IPC files (also known as Feather v2 files).

### Basic Usage

```python
from zoopipe import ArrowInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ArrowInputAdapter("data.arrow"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the Arrow IPC file to read
  - Supports `.arrow`, `.feather`, or `.ipc` extensions
  - Files created by Pandas, Polars, or other Arrow-compatible tools

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Reading Pandas-Generated Arrow Files

```python
import pandas as pd

df = pd.DataFrame({
    'user_id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
df.to_feather('users.arrow')

from zoopipe import ArrowInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ArrowInputAdapter("users.arrow"),
    output_adapter=CSVOutputAdapter("users.csv"),
)

with pipe:
    pipe.wait()
```

### Performance Characteristics

- **Zero-Copy**: Direct memory mapping when possible
- **Columnar Reading**: Efficient batch processing
- **Compression**: Automatic decompression (LZ4, ZSTD)
- **Type Preservation**: Rich type system conversion to Python
- **Throughput**: Very high (~1M+ rows/s) due to zero-copy design

## ArrowOutputAdapter

Write data to Arrow IPC files for efficient storage and interoperability.

### Basic Usage

```python
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=ArrowOutputAdapter("data.arrow"),
)
```

### Parameters

- **output** (`str | pathlib.Path`): Path to the Arrow IPC file to write
  - Parent directories are automatically created if they don't exist
  - Output is compatible with Pandas, Polars, and other Arrow tools

### Writing for Pandas Consumption

```python
from zoopipe import ArrowOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=ArrowOutputAdapter("data.arrow"),
)

with pipe:
    pipe.wait()

import pandas as pd
df = pd.read_feather("data.arrow")
print(df.head())
```

## Complete Examples

### CSV to Arrow Conversion

```python
import time
from pydantic import BaseModel, ConfigDict
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, MultiThreadExecutor, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=ArrowOutputAdapter("users.arrow"),
    schema_model=UserSchema,
    executor=MultiThreadExecutor(max_workers=4),
)

pipe.start()

while not pipe.report.is_finished:
    print(
        f"Processed: {pipe.report.total_processed} | "
        f"Speed: {pipe.report.items_per_second:.2f} rows/s"
    )
    time.sleep(0.5)

print(f"Wrote {pipe.report.total_processed} records to Arrow format")
```

### Arrow to JSONL Export

```python
from zoopipe import ArrowInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ArrowInputAdapter("processed_data.arrow"),
    output_adapter=JSONOutputAdapter("export.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

### Multi-Stage Pipeline with Arrow Intermediate

```python
from zoopipe import ArrowInputAdapter, ArrowOutputAdapter, CSVInputAdapter, JSONOutputAdapter, MultiThreadExecutor, Pipe

stage1 = Pipe(
    input_adapter=CSVInputAdapter("raw_data.csv"),
    output_adapter=ArrowOutputAdapter("intermediate.arrow"),
    executor=MultiThreadExecutor(max_workers=4),
)

with stage1:
    stage1.wait()

stage2 = Pipe(
    input_adapter=ArrowInputAdapter("intermediate.arrow"),
    output_adapter=JSONOutputAdapter("final.jsonl", format="jsonl"),
)

with stage2:
    stage2.wait()
```

## Arrow Format Benefits

### When to Use Arrow

1. **Interoperability**: Share data between Python, Rust, R, etc.
2. **Performance**: Zero-copy reads for analytical workloads
3. **Type Safety**: Rich type system preserves schema
4. **Compression**: Efficient storage with built-in compression
5. **Analytics**: Optimized for columnar operations

### Format Comparison

| Feature | Arrow | Parquet | CSV | JSONL |
|---------|-------|---------|-----|-------|
| **Read Speed** | ✅✅ Fastest | ✅ Fast | ⚠️ Moderate | ⚠️ Moderate |
| **Write Speed** | ✅✅ Fastest | ⚠️ Slow | ✅ Fast | ✅ Fast |
| **Compression** | ✅ Good | ✅✅ Best | ❌ No | ❌ No |
| **Schema** | ✅ Rich | ✅ Rich | ❌ No | ⚠️ Inferred |
| **Streaming** | ✅ Yes | ⚠️ Limited | ✅ Yes | ✅ Yes |
| **Zero-Copy** | ✅ Yes | ❌ No | ❌ No | ❌ No |
| **Type Safety** | ✅✅ Full | ✅✅ Full | ❌ No | ⚠️ Basic |

**Use Arrow when:**
- You need maximum read/write performance
- Sharing data between different languages/tools
- Working with analytical libraries (Pandas, Polars, Dask)
- Type preservation is important

**Use Parquet when:**
- Long-term archival storage (better compression)
- Sharing data across organizations (more portable)
- Need predicate pushdown for large files

**Use CSV/JSONL when:**
- Human readability is required
- Working with external systems that don't support Arrow
- Simple data structures without nested types

## Integration Examples

### With Pandas

```python
import pandas as pd
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=ArrowOutputAdapter("data.arrow"),
)

with pipe:
    pipe.wait()

df = pd.read_feather("data.arrow")
df['processed'] = df['value'] * 2
df.to_feather("processed.arrow")

pipe2 = Pipe(
    input_adapter=ArrowInputAdapter("processed.arrow"),
    output_adapter=CSVOutputAdapter("result.csv"),
)

with pipe2:
    pipe2.wait()
```

### With Polars

```python
import polars as pl
from zoopipe import ArrowOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=ArrowOutputAdapter("data.arrow"),
)

with pipe:
    pipe.wait()

df = pl.read_ipc("data.arrow")
result = df.filter(pl.col("age") > 18)
result.write_ipc("filtered.arrow")
```

### With DuckDB

```python
import duckdb
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("sales.csv"),
    output_adapter=ArrowOutputAdapter("sales.arrow"),
)

with pipe:
    pipe.wait()

con = duckdb.connect()
result = con.execute("""
    SELECT 
        product_id,
        SUM(revenue) as total_revenue
    FROM 'sales.arrow'
    GROUP BY product_id
    ORDER BY total_revenue DESC
    LIMIT 10
""").fetchdf()

print(result)
```

## Best Practices

### For Reading
1. **Leverage Zero-Copy**: Arrow reading is extremely fast due to memory mapping
2. **Type Awareness**: Arrow preserves complex types (lists, structs, dates)
3. **Batch Processing**: Use with `MultiThreadExecutor` for parallel processing
4. **Memory Efficient**: Streaming reads keep memory usage constant

### For Writing
1. **Use for Intermediate Storage**: Arrow is perfect for pipeline stages
2. **Compression**: Arrow automatically compresses data
3. **Interop**: Output is compatible with all Arrow-based tools
4. **Performance**: Fastest write format available in ZooPipe

## Advanced Patterns

### Data Lake Export

```python
from pathlib import Path
from zoopipe import Pipe, MultiThreadExecutor, SQLInputAdapter, ArrowOutputAdapter

tables = ['users', 'orders', 'products']

for table in tables:
    pipe = Pipe(
        input_adapter=SQLInputAdapter(
            "postgresql://user:pass@localhost/db",
            table_name=table
        ),
        output_adapter=ArrowOutputAdapter(f"data_lake/{table}.arrow"),
        executor=MultiThreadExecutor(max_workers=8),
    )
    
    with pipe:
        pipe.wait()
    
    print(f"Exported {table} to Arrow")
```

### High-Performance ETL

```python
from zoopipe import Pipe, MultiThreadExecutor, CSVInputAdapter, ArrowInputAdapter, ArrowOutputAdapter, DuckDBOutputAdapter

extract_pipe = Pipe(
    input_adapter=CSVInputAdapter("raw_data.csv"),
    output_adapter=ArrowOutputAdapter("staging.arrow"),
    executor=MultiThreadExecutor(max_workers=8, batch_size=5000),
)

with extract_pipe:
    extract_pipe.wait()

load_pipe = Pipe(
    input_adapter=ArrowInputAdapter("staging.arrow"),
    output_adapter=DuckDBOutputAdapter(
        "warehouse.duckdb",
        table_name="clean_data",
        mode="replace"
    ),
    executor=MultiThreadExecutor(max_workers=4),
)

with load_pipe:
    load_pipe.wait()
```

### Cross-Language Workflow

```python
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("python_data.csv"),
    output_adapter=ArrowOutputAdapter("shared_data.arrow"),
)

with pipe:
    pipe.wait()
```

Then in R:
```r
library(arrow)
data <- read_feather("shared_data.arrow")
processed <- data %>% filter(age > 18)
write_feather(processed, "r_processed.arrow")
```

Back in Python:
```python
pipe2 = Pipe(
    input_adapter=ArrowInputAdapter("r_processed.arrow"),
    output_adapter=JSONOutputAdapter("final.jsonl", format="jsonl"),
)

with pipe2:
    pipe2.wait()
```

## Error Handling

```python
try:
    pipe = Pipe(
        input_adapter=ArrowInputAdapter("data.arrow"),
        output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
    )
    pipe.start()
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- **Invalid Arrow File**: Corrupted or non-Arrow file
- **Schema Incompatibility**: Type conversion issues
- **Memory Limits**: File too large for available memory
- **Permission Denied**: Can't read input or write output

## Performance Tips

1. **Zero-Copy Advantage**: Arrow is the fastest format for read operations
2. **Batch Size**: Larger batches (5000-10000) work well with Arrow
3. **Multi-Threading**: Always use `MultiThreadExecutor` for large Arrow files
4. **Compression**: Arrow automatically uses LZ4 compression for optimal balance
5. **Memory Mapping**: Arrow reader uses memory mapping for efficient access
6. **Type Conversion**: Minimal overhead converting Arrow types to Python

## Schema Preservation

Arrow preserves complex schemas that other formats lose:

```python
from zoopipe import ArrowOutputAdapter, CSVInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ArrowInputAdapter("complex_data.arrow"),
    output_adapter=ArrowOutputAdapter("validated_data.arrow"),
)
```

Arrow preserves:
- Integer types (int8, int16, int32, int64, uint)
- Floating point (float32, float64)
- Temporal types (date, time, timestamp with timezone)
- Nested types (lists, structs)
- Nullable vs non-nullable columns

# Parquet Adapters

ZooPipe provides high-performance Parquet adapters for working with columnar data in the Apache Parquet format. Parquet is the industry-standard format for analytical data storage, offering excellent compression and fast analytical queries.

## What is Apache Parquet?

Apache Parquet is a columnar storage file format optimized for use with big data processing frameworks. It provides:

- **Columnar Storage**: Data is stored by column rather than by row, enabling efficient compression and encoding
- **Excellent Compression**: Typically 2-10x smaller than CSV or JSON formats
- **Predicate Pushdown**: Read only the columns you need, skipping irrelevant data
- **Type Safety**: Rich type system with nested and complex types
- **Industry Standard**: Widely supported across Spark, Pandas, Polars, DuckDB, BigQuery, Snowflake, etc.
- **Cloud Optimized**: Perfect for S3, GCS, and other cloud storage systems

## ParquetInputAdapter

Read data from Parquet files with efficient columnar access.

### Basic Usage

```python
from zoopipe import JSONOutputAdapter, ParquetInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("data.parquet"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the Parquet file to read
  - Supports local paths: `/path/to/file.parquet`
  - Supports S3 URIs: `s3://bucket/path/to/file.parquet`
  - Files created by Pandas, Polars, Spark, or other Parquet-compatible tools

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Reading Pandas-Generated Parquet Files

```python
import pandas as pd

df = pd.DataFrame({
    'user_id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
df.to_parquet('users.parquet')

from zoopipe import CSVOutputAdapter, ParquetInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("users.parquet"),
    output_adapter=CSVOutputAdapter("users.csv"),
)

with pipe:
    pipe.wait()
```

### Reading from S3

```python
from zoopipe import JSONOutputAdapter, ParquetInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("s3://my-bucket/data/users.parquet"),
    output_adapter=JSONOutputAdapter("users.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

### Performance Characteristics

- **Columnar Reading**: Efficient batch processing by column
- **Compression**: Automatic decompression (Snappy, GZIP, LZ4, ZSTD)
- **Type Preservation**: Rich type system conversion to Python
- **Column Pruning**: Only reads columns that exist in your schema
- **Throughput**: Very high (~500k-1M+ rows/s) due to columnar format

## ParquetOutputAdapter

Write data to Parquet files for efficient storage and analytics.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=ParquetOutputAdapter("data.parquet"),
)
```

### Parameters

- **path** (`str | pathlib.Path`): Path to the Parquet file to write
  - Parent directories are automatically created if they don't exist
  - Supports local paths: `/path/to/file.parquet`
  - Supports S3 URIs: `s3://bucket/path/to/file.parquet`
  - Output is compatible with Pandas, Polars, Spark, and other Parquet tools

### Writing to S3

```python
from zoopipe import CSVInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("local_data.csv"),
    output_adapter=ParquetOutputAdapter("s3://my-bucket/processed/data.parquet"),
)

with pipe:
    pipe.wait()
```

### Writing for Pandas Consumption

```python
from zoopipe import JSONInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=ParquetOutputAdapter("data.parquet"),
)

with pipe:
    pipe.wait()

import pandas as pd
df = pd.read_parquet("data.parquet")
print(df.head())
```

## Complete Examples

### CSV to Parquet Conversion

```python
import time
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=ParquetOutputAdapter("users.parquet"),
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

print(f"Wrote {pipe.report.total_processed} records to Parquet format")
```

### Parquet to JSONL Export

```python
from zoopipe import JSONOutputAdapter, ParquetInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("processed_data.parquet"),
    output_adapter=JSONOutputAdapter("export.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

### Cloud Storage Pipeline (S3 to S3)

```python
from zoopipe import MultiThreadExecutor, ParquetInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("s3://raw-data/input.parquet"),
    output_adapter=ParquetOutputAdapter("s3://processed-data/output.parquet"),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()
```

## Parquet Format Benefits

### When to Use Parquet

1. **Long-Term Storage**: Excellent compression saves storage costs
2. **Data Warehousing**: Optimized for analytical queries
3. **Big Data Processing**: Standard format for Spark, Hive, Presto
4. **Cloud Storage**: Ideal for S3, GCS, Azure Blob Storage
5. **Cross-Platform Sharing**: Widely supported across languages and tools

### Format Comparison

| Feature | Parquet | Arrow | CSV | JSONL |
|---------|---------|-------|-----|-------|
| **Compression** | ✅✅ Best | ✅ Good | ❌ No | ❌ No |
| **Read Speed** | ✅ Fast | ✅✅ Fastest | ⚠️ Moderate | ⚠️ Moderate |
| **Write Speed** | ⚠️ Moderate | ✅✅ Fastest | ✅ Fast | ✅ Fast |
| **File Size** | ✅✅ Smallest | ✅ Small | ❌ Largest | ❌ Large |
| **Schema** | ✅ Rich | ✅ Rich | ❌ No | ⚠️ Inferred |
| **Analytics** | ✅✅ Excellent | ✅ Good | ❌ Poor | ❌ Poor |
| **Human Readable** | ❌ No | ❌ No | ✅ Yes | ✅ Yes |
| **Cloud Optimized** | ✅✅ Yes | ✅ Yes | ⚠️ Limited | ⚠️ Limited |

**Use Parquet when:**
- Storing data long-term (lowest storage costs)
- Running analytical queries (best query performance)
- Sharing data with big data systems (Spark, BigQuery, Snowflake)
- Working with cloud storage (optimized for S3/GCS)
- File size is a concern (best compression)

**Use Arrow when:**
- Maximum read/write speed is critical
- Sharing data between processes in memory
- Working with analytical libraries locally

**Use CSV/JSONL when:**
- Human readability is required
- Working with external systems that don't support Parquet
- Simple streaming scenarios

## Integration Examples

### With Pandas

```python
import pandas as pd
from zoopipe import CSVInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=ParquetOutputAdapter("data.parquet"),
)

with pipe:
    pipe.wait()

df = pd.read_parquet("data.parquet")
df['processed'] = df['value'] * 2
df.to_parquet("processed.parquet")

pipe2 = Pipe(
    input_adapter=ParquetInputAdapter("processed.parquet"),
    output_adapter=CSVOutputAdapter("result.csv"),
)

with pipe2:
    pipe2.wait()
```

### With Polars

```python
import polars as pl
from zoopipe import JSONInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=ParquetOutputAdapter("data.parquet"),
)

with pipe:
    pipe.wait()

df = pl.read_parquet("data.parquet")
result = df.filter(pl.col("age") > 18)
result.write_parquet("filtered.parquet")
```

### With DuckDB

```python
import duckdb
from zoopipe import CSVInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("sales.csv"),
    output_adapter=ParquetOutputAdapter("sales.parquet"),
)

with pipe:
    pipe.wait()

con = duckdb.connect()
result = con.execute("""
    SELECT 
        product_id,
        SUM(revenue) as total_revenue
    FROM 'sales.parquet'
    GROUP BY product_id
    ORDER BY total_revenue DESC
    LIMIT 10
""").fetchdf()

print(result)
```

## Best Practices

### For Reading
1. **Leverage Columnar Format**: Parquet reading is optimized for analytical queries
2. **Type Awareness**: Parquet preserves complex types (lists, structs, dates)
3. **Batch Processing**: Use with `MultiThreadExecutor` for parallel processing
4. **Column Pruning**: Only columns in your schema are read (automatic optimization)
5. **Cloud Storage**: Use S3 URIs for data lake access

### For Writing
1. **Use for Archival**: Parquet provides best compression for long-term storage
2. **Cloud First**: Perfect for S3/cloud storage with excellent compression
3. **Analytics Ready**: Output is optimized for analytical tools
4. **Compression Savings**: Expect 2-10x size reduction vs CSV/JSON
5. **Type Safety**: Schema is preserved automatically

## Advanced Patterns

### Data Lake Export

```python
from pathlib import Path
from zoopipe import MultiThreadExecutor, ParquetOutputAdapter, Pipe, SQLInputAdapter

tables = ['users', 'orders', 'products']

for table in tables:
    pipe = Pipe(
        input_adapter=SQLInputAdapter(
            "postgresql://user:pass@localhost/db",
            table_name=table
        ),
        output_adapter=ParquetOutputAdapter(f"s3://data-lake/{table}.parquet"),
        executor=MultiThreadExecutor(max_workers=8),
    )
    
    with pipe:
        pipe.wait()
    
    print(f"Exported {table} to data lake")
```

### ETL Pipeline with Compression

```python
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=ParquetOutputAdapter("compressed_dataset.parquet"),
    executor=MultiThreadExecutor(max_workers=8, batch_size=5000),
)

with pipe:
    pipe.wait()

import os
csv_size = os.path.getsize("large_dataset.csv")
parquet_size = os.path.getsize("compressed_dataset.parquet")
compression_ratio = csv_size / parquet_size

print(f"Compression ratio: {compression_ratio:.1f}x smaller")
```

### Multi-Stage Processing

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, MultiThreadExecutor, ParquetInputAdapter, ParquetOutputAdapter, Pipe

extract_pipe = Pipe(
    input_adapter=CSVInputAdapter("raw_data.csv"),
    output_adapter=ParquetOutputAdapter("staging.parquet"),
    executor=MultiThreadExecutor(max_workers=8),
)

with extract_pipe:
    extract_pipe.wait()

load_pipe = Pipe(
    input_adapter=ParquetInputAdapter("staging.parquet"),
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

## Error Handling

```python
try:
    pipe = Pipe(
        input_adapter=ParquetInputAdapter("data.parquet"),
        output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
    )
    pipe.start()
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- **Invalid Parquet File**: Corrupted or non-Parquet file
- **Schema Incompatibility**: Type conversion issues
- **S3 Access Denied**: Check AWS credentials and bucket permissions
- **Permission Denied**: Can't read input or write output locally

## S3 Configuration

When using S3 URIs, ensure AWS credentials are configured via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

Or use AWS credential files (`~/.aws/credentials`).

## Performance Tips

1. **Compression**: Parquet automatically uses Snappy compression for optimal balance
2. **Batch Size**: Larger batches (5000-10000) work well with Parquet
3. **Multi-Threading**: Always use `MultiThreadExecutor` for large Parquet files
4. **Storage Savings**: Expect 5-10x compression vs CSV for typical datasets
5. **Cloud Performance**: Parquet's columnar format minimizes data transfer from S3
6. **Type Conversion**: Minimal overhead converting Parquet types to Python

## Schema Preservation

Parquet preserves complex schemas that other formats lose:

```python
from zoopipe import ParquetInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("complex_data.parquet"),
    output_adapter=ParquetOutputAdapter("validated_data.parquet"),
)
```

Parquet preserves:
- Integer types (int8, int16, int32, int64, uint)
- Floating point (float32, float64)
- Temporal types (date, time, timestamp with timezone)
- Nested types (lists, structs, maps)
- Nullable vs non-nullable columns
- Decimal types with precision

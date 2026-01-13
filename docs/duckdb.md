# DuckDB Adapters

ZooPipe provides high-performance DuckDB adapters for columnar data processing. DuckDB is an in-process analytical database optimized for OLAP queries and data analytics.

## DuckDBInputAdapter

Read data from DuckDB databases using either table names or custom analytical queries.

### Basic Usage

```python
from zoopipe import DuckDBInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        source="data.duckdb",
        table_name="users"
    ),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Custom Analytics Queries

```python
input_adapter = DuckDBInputAdapter(
    source="analytics.duckdb",
    query="""
        SELECT 
            user_id,
            COUNT(*) as visit_count,
            AVG(duration) as avg_duration
        FROM events
        WHERE event_date >= '2024-01-01'
        GROUP BY user_id
        HAVING COUNT(*) > 10
    """
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the DuckDB database file
  - Can be an existing database or a new file (will be created)
  - Use `:memory:` for in-memory databases

- **query** (`str | None`): Custom SQL query to execute
  - Mutually exclusive with `table_name`
  - Supports full DuckDB SQL syntax including CTEs, window functions, etc.

- **table_name** (`str | None`): Name of the table to read from
  - Mutually exclusive with `query`
  - Creates a simple `SELECT * FROM table_name` query

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Reading from Parquet via DuckDB

DuckDB can directly query Parquet files:

```python
input_adapter = DuckDBInputAdapter(
    source=":memory:",
    query="SELECT * FROM 'data.parquet' WHERE year = 2024"
)
```

### Complex Analytical Queries

```python
input_adapter = DuckDBInputAdapter(
    source="warehouse.duckdb",
    query="""
        WITH daily_stats AS (
            SELECT 
                DATE_TRUNC('day', timestamp) as day,
                product_id,
                SUM(revenue) as daily_revenue
            FROM sales
            GROUP BY day, product_id
        )
        SELECT 
            product_id,
            AVG(daily_revenue) as avg_daily_revenue,
            MAX(daily_revenue) as max_daily_revenue
        FROM daily_stats
        GROUP BY product_id
        ORDER BY avg_daily_revenue DESC
    """
)
```

## DuckDBOutputAdapter

Write data to DuckDB databases with optimized batch operations.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=DuckDBOutputAdapter(
        output="analytics.duckdb",
        table_name="processed_data",
        mode="replace"
    ),
)
```

### Parameters

- **output** (`str | pathlib.Path`): Path to the DuckDB database file
  - Parent directories are automatically created if they don't exist

- **table_name** (`str`): Name of the table to write to

- **mode** (`str`, default=`"replace"`): Write mode behavior
  - `"replace"`: Drop existing table and create new one
  - `"append"`: Append to existing table (create if doesn't exist)
  - `"fail"`: Raise error if table already exists

### Performance Example

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, MultiThreadExecutor, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=DuckDBOutputAdapter(
        output="warehouse.duckdb",
        table_name="records",
        mode="replace"
    ),
    executor=MultiThreadExecutor(max_workers=4, batch_size=2000),
)
```

## Complete Examples

### CSV to DuckDB with Validation

```python
import time
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, MultiThreadExecutor, Pipe

class SalesRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    revenue: float

pipe = Pipe(
    input_adapter=CSVInputAdapter("sales.csv"),
    output_adapter=DuckDBOutputAdapter(
        "sales.duckdb",
        table_name="orders",
        mode="replace"
    ),
    schema_model=SalesRecord,
    executor=MultiThreadExecutor(max_workers=4),
)

pipe.start()

while not pipe.report.is_finished:
    print(
        f"Processed: {pipe.report.total_processed} | "
        f"Speed: {pipe.report.items_per_second:.2f} rows/s"
    )
    time.sleep(0.5)

print(f"Loaded {pipe.report.total_processed} records into DuckDB")
```

### DuckDB Analytics to JSONL

```python
from zoopipe import DuckDBInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        "sales.duckdb",
        query="""
            SELECT 
                product_id,
                COUNT(*) as order_count,
                SUM(quantity) as total_quantity,
                SUM(revenue) as total_revenue,
                AVG(revenue) as avg_revenue
            FROM orders
            GROUP BY product_id
            ORDER BY total_revenue DESC
            LIMIT 100
        """
    ),
    output_adapter=JSONOutputAdapter("top_products.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

### Incremental Data Loading

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, MultiThreadExecutor, Pipe

today = datetime.now().strftime("%Y-%m-%d")

pipe = Pipe(
    input_adapter=CSVInputAdapter(f"daily_data_{today}.csv"),
    output_adapter=DuckDBOutputAdapter(
        "analytics.duckdb",
        table_name="daily_events",
        mode="append"
    ),
)

with pipe:
    pipe.wait()

print(f"Appended today's data to DuckDB")
```

## DuckDB Advantages

### Why Use DuckDB?

1. **Columnar Storage**: Optimized for analytical queries
2. **Compression**: Efficient storage with automatic compression
3. **SQL Analytics**: Full SQL support including window functions, CTEs, etc.
4. **Parquet Integration**: Direct query of Parquet files without loading
5. **In-Process**: No separate database server required
6. **ACID Transactions**: Full transactional support
7. **Fast Aggregations**: 10-100x faster than row-based databases for analytics

### DuckDB vs SQLite

| Feature | DuckDB | SQLite |
|---------|--------|--------|
| **Use Case** | Analytics (OLAP) | Transactions (OLTP) |
| **Column Storage** | ✅ Yes | ❌ No |
| **Analytics Speed** | ✅✅ Very Fast | ⚠️ Moderate |
| **Write Speed** | ✅ Fast | ✅✅ Very Fast |
| **Compression** | ✅ Built-in | ❌ No |
| **Parquet Support** | ✅ Native | ❌ No |
| **Window Functions** | ✅ Full | ⚠️ Limited |

Use **DuckDB** for: Analytics, reporting, data warehousing, batch processing  
Use **SQLite** for: Transaction processing, configuration storage, rapid writes

## Best Practices

### For Reading
1. **Use Analytics Queries**: Leverage DuckDB's analytical capabilities
2. **Filter Early**: Apply WHERE clauses to reduce data transfer
3. **Aggregate When Possible**: Use GROUP BY and aggregations before pipeline
4. **Index Important Columns**: Create indexes for frequently filtered columns
5. **Query Parquet Directly**: Use DuckDB to query Parquet files without loading

### For Writing
1. **Batch Writes**: Use `MultiThreadExecutor` for large datasets
2. **Choose Appropriate Mode**: 
   - `replace` for full refreshes
   - `append` for incremental loads
   - `fail` for strict schema enforcement
3. **Parent Directory**: Automatically created, no need to pre-create
4. **Schema Inference**: First record defines table schema

## Advanced Patterns

### ETL Pipeline with DuckDB

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("raw_events.csv"),
    output_adapter=DuckDBOutputAdapter(
        "warehouse.duckdb",
        table_name="events",
        mode="replace"
    ),
)

with pipe:
    pipe.wait()

aggregated_pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        "warehouse.duckdb",
        query="""
            SELECT 
                user_id,
                DATE_TRUNC('day', event_time) as day,
                COUNT(*) as event_count
            FROM events
            GROUP BY user_id, day
        """
    ),
    output_adapter=DuckDBOutputAdapter(
        "warehouse.duckdb",
        table_name="daily_user_stats",
        mode="replace"
    ),
)

with aggregated_pipe:
    aggregated_pipe.wait()
```

### Multi-Source Data Consolidation

```python
from zoopipe import CSVInputAdapter, DuckDBOutputAdapter, Pipe
from pathlib import Path

for csv_file in Path("data_sources").glob("*.csv"):
    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_file),
        output_adapter=DuckDBOutputAdapter(
            "consolidated.duckdb",
            table_name="all_data",
            mode="append"
        ),
    )
    
    with pipe:
        pipe.wait()
    
    print(f"Loaded {csv_file.name}")
```

### Querying External Files

```python
from zoopipe import DuckDBInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=DuckDBInputAdapter(
        source=":memory:",
        query="""
            SELECT 
                csv.user_id,
                csv.name,
                parquet.total_purchases
            FROM 'users.csv' csv
            JOIN 'purchases.parquet' parquet
                ON csv.user_id = parquet.user_id
            WHERE parquet.total_purchases > 1000
        """
    ),
    output_adapter=JSONOutputAdapter("high_value_users.jsonl", format="jsonl"),
)
```

## Performance Characteristics

### Reading
- **Columnar Scans**: Extremely fast for analytical queries
- **Vectorized Execution**: SIMD optimizations for aggregations
- **Parallel Query Execution**: Automatic parallelization of queries
- **Compression**: Automatic decompression during reads

### Writing
- **Batch Inserts**: Optimized for bulk loading
- **Columnar Storage**: Efficient compression during writes
- **Transaction Support**: All writes in single transaction
- **Schema Evolution**: Automatic schema creation from first record

## Error Handling

```python
try:
    pipe = Pipe(
        input_adapter=DuckDBInputAdapter(
            "data.duckdb",
            table_name="users"
        ),
        output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
    )
    pipe.start()
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- **Table Not Found**: Specified table doesn't exist in database
- **SQL Syntax Error**: Invalid query syntax
- **Type Mismatch**: Schema incompatibility during append
- **File Access**: Permission or lock issues

## Integration with DuckDB CLI

After processing data with ZooPipe, you can use DuckDB CLI for ad-hoc queries:

```bash
duckdb warehouse.duckdb

D SELECT COUNT(*) FROM events;
D SELECT product_id, SUM(revenue) FROM orders GROUP BY product_id;
D EXPORT DATABASE 'backup_dir';
```

## Performance Tips

1. **Use DuckDB for Analytics**: 10-100x faster than row-based databases for aggregations
2. **Leverage Columnar Storage**: Efficient compression and fast scans
3. **Query Parquet Directly**: Avoid intermediate conversions
4. **Batch Size**: Default 2000 rows optimal for most use cases
5. **Multi-Threading**: Use `MultiThreadExecutor` for large datasets
6. **Memory Usage**: DuckDB caches aggressively; ensure sufficient RAM for large databases

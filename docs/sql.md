# SQL Adapters

ZooPipe provides high-performance SQL database adapters built on top of [SQLx](https://github.com/launchbadge/sqlx), a pure Rust SQL toolkit. These adapters enable efficient reading from and writing to SQL databases with optimized batch operations.

## Supported Databases

Through SQLx's `Any` driver, ZooPipe supports:
- **SQLite** (most commonly used)
- **PostgreSQL**
- **MySQL**
- **MariaDB**

## SQLInputAdapter

Read data from SQL databases using either table names or custom queries.

### Basic Usage

```python
from zoopipe import JSONOutputAdapter, Pipe, SQLInputAdapter

pipe = Pipe(
    input_adapter=SQLInputAdapter(
        uri="sqlite:///path/to/database.db",
        table_name="users"
    ),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Custom Queries

```python
input_adapter = SQLInputAdapter(
    uri="postgresql://user:password@localhost/mydb",
    query="SELECT id, name, email FROM users WHERE active = true"
)
```

### Parameters

- **uri** (`str`): Database connection string
  - SQLite: `sqlite:///path/to/db.db` or `sqlite::memory:` for in-memory
  - PostgreSQL: `postgresql://user:password@host:port/database`
  - MySQL: `mysql://user:password@host:port/database`

- **query** (`str | None`): Custom SQL query to execute
  - Mutually exclusive with `table_name`
  - Allows filtering, joins, and complex queries

- **table_name** (`str | None`): Name of the table to read from
  - Mutually exclusive with `query`
  - Creates a simple `SELECT * FROM table_name` query

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Connection URI Features

SQLite URIs support additional parameters:
```python
uri = "sqlite:///path/to/db.db?mode=rwc"
```
- `mode=rwc`: Read-write-create mode (creates database if it doesn't exist)
- Parent directories are automatically created if needed

### Performance Characteristics

- Streaming row-by-row processing (low memory footprint)
- Asynchronous data fetching using Tokio
- Single database connection per reader
- Type conversion from SQL to Python types (String, Int, Float, Bool)
- NULL values are properly handled and mapped to Python `None`

## SQLOutputAdapter

Write data to SQL databases with optimized batch insert operations.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, Pipe, SQLOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("input.csv"),
    output_adapter=SQLOutputAdapter(
        uri="sqlite:///output.db",
        table_name="processed_data",
        mode="replace"
    ),
)
```

### Parameters

- **uri** (`str`): Database connection string (same format as SQLInputAdapter)

- **table_name** (`str`): Name of the table to write to

- **mode** (`str`, default=`"replace"`): Write mode behavior
  - `"replace"`: Drop existing table and create new one
  - `"append"`: Append to existing table (create if doesn't exist)
  - `"fail"`: Raise error if table already exists

### Batch Insert Optimization

The SQLWriter implements high-performance batch inserts:

- **Batch Size**: 500 rows per INSERT statement
- **Transaction**: All batches are wrapped in a single transaction
- **Automatic Chunking**: Large datasets are automatically split into optimal chunks
- **Zero-Copy Design**: Minimizes data copying between Python and Rust

### Performance Example

```python
from zoopipe import CSVInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=SQLOutputAdapter(
        uri="sqlite:///output.db?mode=rwc",
        table_name="records",
        mode="replace"
    ),
    executor=MultiThreadExecutor(max_workers=4, batch_size=2000),
)
```

This will:
1. Read CSV in parallel batches of 2000 rows
2. Process through Pydantic validation
3. Write to SQLite in optimized batches of 500 rows per INSERT
4. All within a single database transaction

### Schema Inference

Table schemas are automatically inferred from the first record:
- All columns are created as `TEXT` type
- Column names are sorted alphabetically
- Schema is locked after first write

### Transaction Behavior

- Each call to `write_batch()` uses a single transaction
- If any batch fails, the entire transaction is rolled back
- Ensures data consistency and atomicity

## Complete Example

### CSV to SQL with Validation

```python
import os
import time
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str

db_path = os.path.abspath("users.db")
pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=SQLOutputAdapter(
        f"sqlite:{db_path}?mode=rwc",
        table_name="users",
        mode="replace",
    ),
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

print(f"Finished! Wrote {pipe.report.total_processed} records to database")
```

### SQL to JSONL Export

```python
from zoopipe import JSONOutputAdapter, Pipe, SQLInputAdapter

pipe = Pipe(
    input_adapter=SQLInputAdapter(
        "postgresql://user:pass@localhost/mydb",
        query="SELECT * FROM users WHERE created_at > NOW() - INTERVAL '7 days'"
    ),
    output_adapter=JSONOutputAdapter("recent_users.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

## Best Practices

### For Reading
1. Use specific queries instead of `SELECT *` when possible
2. Add indexes on frequently queried columns
3. Consider pagination for very large datasets
4. Use read-only database connections when appropriate

### For Writing
1. Use `MultiThreadExecutor` for large datasets to maximize throughput
2. Choose appropriate batch sizes based on your data size
3. Use `mode="replace"` for complete data refreshes
4. Use `mode="append"` for incremental updates
5. Ensure database has sufficient disk space for write operations

## Error Handling

SQL adapters provide clear error messages for common issues:

```python
try:
    pipe = Pipe(
        input_adapter=SQLInputAdapter(
            uri="sqlite:///nonexistent.db",
            table_name="users"
        ),
        output_adapter=JSONOutputAdapter("output.jsonl"),
    )
    pipe.start()
except RuntimeError as e:
    print(f"Database error: {e}")
```

Common errors:
- `Connection failed`: Invalid URI or database not accessible
- `Query failed`: SQL syntax error or table doesn't exist
- `Batch insert failed`: Constraint violation or disk full
- `Failed to commit transaction`: Transaction conflict or lock timeout

## Performance Tips

1. **Connection Pooling**: Each reader/writer uses a dedicated connection
2. **Batch Size**: Default 500 rows per INSERT is optimized for most use cases
3. **Transactions**: All writes in a single transaction for consistency and speed
4. **Type Conversion**: Minimal overhead with direct Rust-to-Python type mapping
5. **Memory**: Streaming architecture keeps memory usage constant regardless of dataset size

# SQLAlchemy Input Adapter

Reads data from SQL databases using SQLAlchemy with an efficient "Just-In-Time" (JIT) metadata pattern.

## Overview

The `SQLAlchemyInputAdapter` generates lightweight metadata entries containing batch information (offsets or primary key ranges). The actual data fetching is delegated to workers through the `SQLAlchemyFetchHook`, enabling parallel distributed reading of large database tables.

## Features

- **JIT Metadata Pattern**: Coordinator only generates metadata; workers fetch actual data
- **Multi-Database Support**: Works with PostgreSQL, MySQL, SQLite, Oracle, SQL Server, and more
- **Batch Processing**: Configurable batch sizes for efficient memory usage
- **PK Range Optimization**: Automatically uses primary key ranges for numeric columns
- **Parallel Processing**: Each worker fetches its own batch independently
- **Automatic Hook Injection**: Auto-injects `SQLAlchemyFetchHook` if not provided
- **Flexible Batching**: Falls back to OFFSET/LIMIT for non-numeric primary keys

## Installation

Requires the `sqlalchemy` optional dependency:

```bash
uv add zoopipe[sqlalchemy]
```

Or with pip:

```bash
pip install zoopipe[sqlalchemy]
```

You'll also need the appropriate database driver (e.g., `psycopg2` for PostgreSQL, `pymysql` for MySQL).

## Usage

### Basic Example (PostgreSQL)

```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(
        connection_string="postgresql://user:pass@localhost/mydb",
        table_name="users",
        batch_size=1000
    ),
    output_adapter=CSVOutputAdapter("users_export.csv"),
    executor=MultiProcessingExecutor(UserSchema, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### MySQL to PostgreSQL Migration

```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(
        connection_string="mysql://user:pass@oldserver/legacy_db",
        table_name="customers",
        batch_size=5000,
        pk_column="customer_id"
    ),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="postgresql://user:pass@newserver/modern_db",
        table_name="customers"
    ),
    executor=RayExecutor(CustomerSchema)
)
```

### SQLite Example

```python
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(
        connection_string="sqlite:///local.db",
        table_name="events",
        batch_size=500
    ),
    output_adapter=JSONOutputAdapter("events.jsonl", format="jsonl"),
    executor=SyncFifoExecutor(EventSchema)
)
```

## Parameters

### Constructor Parameters

- **`connection_string`** (str, required): SQLAlchemy connection string (e.g., `postgresql://user:pass@host/db`)
- **`table_name`** (str, required): Name of the table to read from
- **`batch_size`** (int, optional): Number of rows per batch. Default: `1000`
- **`pk_column`** (str, optional): Primary key column name for range optimization. Default: `"id"`
- **`total_rows`** (int | None, optional): Total row count (auto-detected if `None`). Default: `None`
- **`id_generator`** (Callable, optional): Function to generate entry IDs. Default: `uuid.uuid4`
- **`pre_hooks`** (list[BaseHook], optional): Pre-validation hooks. Default: `None`
- **`post_hooks`** (list[BaseHook], optional): Post-validation hooks. Default: `None`
- **`auto_inject_fetch_hook`** (bool, optional): Automatically inject `SQLAlchemyFetchHook`. Default: `True`

## How It Works

### Metadata Generation Phase

The adapter generates metadata entries without fetching actual data:

1. **Row Count**: Determines total rows using `COUNT(*)`
2. **Schema Inspection**: Checks if `pk_column` is numeric (int/float)
3. **Optimization Decision**:
   - **Numeric PK**: Uses PK range boundaries for efficient fetching
   - **Non-numeric PK**: Falls back to OFFSET/LIMIT

### Metadata Structure

**For Numeric Primary Keys (Optimized)**:
```python
{
    "table": "users",
    "pk_column": "user_id",
    "pk_start": 1000,
    "pk_end": 2000,
    "is_sql_range_metadata": True
}
```

**For OFFSET/LIMIT (Fallback)**:
```python
{
    "table": "users",
    "batch_offset": 5000,
    "batch_limit": 1000,
    "is_sql_range_metadata": True
}
```

### Data Fetching Phase

The `SQLAlchemyFetchHook` (auto-injected by default) reads the metadata and fetches actual rows:

```sql
-- For PK ranges
SELECT * FROM users WHERE user_id >= 1000 AND user_id < 2000

-- For OFFSET/LIMIT
SELECT * FROM users OFFSET 5000 LIMIT 1000
```

## Integration with SQLAlchemyFetchHook

The `SQLAlchemyFetchHook` is automatically registered as a pre-validation hook. It:

1. Reads the metadata from each entry
2. Connects to the database in each worker (thread-safe)
3. Executes the appropriate query (PK range or OFFSET/LIMIT)
4. Populates `entry['raw_data']` with fetched rows as dictionaries
5. Returns entries for validation

### Manual Hook Configuration

You can disable auto-injection and provide your own hooks:

```python
from zoopipe.hooks.sqlalchemy import SQLAlchemyFetchHook

custom_hook = SQLAlchemyFetchHook(connection_string="postgresql://...")

adapter = SQLAlchemyInputAdapter(
    connection_string="postgresql://...",
    table_name="events",
    auto_inject_fetch_hook=False,
    pre_hooks=[custom_hook, CustomTransformHook()]
)
```

## Database Connection Strings

### PostgreSQL
```python
"postgresql://user:password@localhost:5432/database"
"postgresql+psycopg2://user:password@localhost/database"
```

### MySQL
```python
"mysql://user:password@localhost/database"
"mysql+pymysql://user:password@localhost/database"
```

### SQLite
```python
"sqlite:///path/to/database.db"
"sqlite:////absolute/path/to/database.db"
```

### SQL Server
```python
"mssql+pyodbc://user:password@host/database?driver=ODBC+Driver+17+for+SQL+Server"
```

### Oracle
```python
"oracle+cx_oracle://user:password@host:port/?service_name=service"
```

## Performance Tips

- **Batch Size**: Balance between memory usage and query overhead
  - Small tables: 500 - 1,000
  - Large tables: 5,000 - 10,000
- **Primary Key Optimization**: Use numeric, indexed primary keys for best performance
- **Worker Count**: More workers = more database connections; ensure your DB can handle it
- **Connection Pooling**: SQLAlchemy handles connection pooling automatically per worker

## Example: Large Table Migration

```python
from pydantic import BaseModel
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

class RecordSchema(BaseModel):
    id: int
    name: str
    email: str
    created_at: str

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(
        connection_string="postgresql://user:pass@oldhost/legacy",
        table_name="records",
        batch_size=10000,
        pk_column="id"
    ),
    output_adapter=ArrowOutputAdapter("records.parquet", format="parquet"),
    executor=MultiProcessingExecutor(RecordSchema, max_workers=8)
)

with pipe:
    report = pipe.start()
    report.wait()

print(f"Migrated {report.success_count} records")
print(f"Errors: {report.error_count}")
```

## Example: Cross-Database ETL

```python
from zoopipe import Pipe
from zoopipe.executor.dask import DaskExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter
from zoopipe.hooks.builtin import TimestampHook, FieldMapperHook

mysql_conn = "mysql://user:pass@mysql-host/source_db"
postgres_conn = "postgresql://user:pass@pg-host/target_db"

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(mysql_conn, "legacy_orders", batch_size=5000),
    output_adapter=SQLAlchemyOutputAdapter(postgres_conn, "orders", batch_size=5000),
    executor=DaskExecutor(OrderSchema),
    pre_validation_hooks=[
        FieldMapperHook({"old_column": "new_column"})
    ],
    post_validation_hooks=[
        TimestampHook(field_name="migrated_at")
    ]
)

with pipe:
    report = pipe.start()
    report.wait()
```

## Error Handling

```python
pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter("postgresql://...", "users"),
    output_adapter=output_adapter,
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=executor
)

with pipe:
    report = pipe.start()
    report.wait()

if report.error_count > 0:
    print(f"Failed records saved to errors.csv: {report.error_count}")
```

## See Also

- [SQLAlchemy Output Adapter](sqlalchemy-output.md) - Writing to SQL databases
- [Hooks Documentation](hooks.md) - Transform data during migration
- [MultiProcessing Executor](../executors.md#multiprocessingexecutor) - Parallel processing
- [Ray Executor](../executors.md#rayexecutor) - Distributed processing
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/) - Official SQLAlchemy docs

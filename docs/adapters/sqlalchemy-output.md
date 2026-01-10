# SQLAlchemy Output Adapter

Writes validated data to SQL databases using SQLAlchemy with efficient batch processing and automatic transaction management.

## Overview

The `SQLAlchemyOutputAdapter` persists data to SQL database tables using batched inserts with automatic transaction support. It works with any database supported by SQLAlchemy.

## Features

- **Multi-Database Support**: PostgreSQL, MySQL, SQLite, Oracle, SQL Server, and more
- **Batch Writing**: Accumulates entries and writes in configurable batches
- **Transaction Support**: Automatic transaction management for data integrity
- **Automatic Schema Detection**: Reflects table schema from the database
- **Bulk Inserts**: Uses SQLAlchemy's bulk insert for high performance
- **Automatic Flushing**: Ensures all data is written on close

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
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="postgresql://user:pass@localhost/mydb",
        table_name="processed_data"
    ),
    executor=MultiProcessingExecutor(DataSchema, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### MySQL Database Load

```python
from zoopipe import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter

pipe = Pipe(
    input_adapter=JSONInputAdapter("events.jsonl", format="jsonl"),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="mysql://user:pass@localhost/analytics",
        table_name="events",
        batch_size=5000
    ),
    executor=ThreadExecutor(EventSchema, max_workers=4)
)
```

### Cross-Database Migration

```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter(
        connection_string="mysql://user:pass@oldserver/legacy_db",
        table_name="orders"
    ),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="postgresql://user:pass@newserver/modern_db",
        table_name="orders",
        batch_size=10000
    ),
    executor=RayExecutor(OrderSchema)
)
```

## Parameters

### Constructor Parameters

- **`connection_string`** (str, required): SQLAlchemy connection string (e.g., `postgresql://user:pass@host/db`)
- **`table_name`** (str, required): Name of the table to write to
- **`batch_size`** (int, optional): Number of rows to accumulate before flushing. Default: `1000`
- **`pre_hooks`** (list[BaseHook], optional): Pre-write hooks. Default: `None`
- **`post_hooks`** (list[BaseHook], optional): Post-write hooks. Default: `None`

## How It Works

### Write Process

1. **Entry Buffering**: Each validated entry is added to an in-memory buffer
2. **Batch Detection**: When buffer reaches `batch_size`, automatic flush is triggered
3. **Bulk Insert**: SQLAlchemy executes a bulk insert with all buffered rows
4. **Transaction Commit**: Transaction is committed automatically
5. **Buffer Clear**: Buffer is cleared after successful write

### Transaction Management

Each flush operation uses SQLAlchemy's transaction context:

```python
with engine.begin() as conn:
    conn.execute(insert(table), buffer)
```

This ensures ACID compliance and automatic rollback on errors.

## Data Source Priority

The adapter writes data in the following priority:

1. **`validated_data`**: If present (after successful Pydantic validation)
2. **`raw_data`**: If `validated_data` is not available

This allows flexibility in processing both validated and raw entries.

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

- **Batch Size**: Larger batches improve throughput but increase memory usage
  - Small datasets: 1,000 - 5,000
  - Large datasets: 10,000 - 50,000
- **Connection Pooling**: SQLAlchemy automatically manages connection pools
- **Pre-create Tables**: Create the target table with proper schema before running the pipeline
- **Indexes**: Add indexes after bulk insert, not before (for better insert performance)
- **Worker Count**: Balance worker count with database connection limits

## Example: CSV to PostgreSQL

```python
from pydantic import BaseModel, Field
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter
from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table

class SalesRecord(BaseModel):
    order_id: int
    customer_id: int
    amount: float = Field(gt=0)
    status: str

engine = create_engine("postgresql://user:pass@localhost/sales")
metadata = MetaData()

orders_table = Table(
    'orders',
    metadata,
    Column('order_id', Integer, primary_key=True),
    Column('customer_id', Integer),
    Column('amount', Float),
    Column('status', String(50))
)

metadata.create_all(engine)

pipe = Pipe(
    input_adapter=CSVInputAdapter("sales.csv"),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="postgresql://user:pass@localhost/sales",
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

## Example: JSON to MySQL with Hooks

```python
from zoopipe import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter
from zoopipe.hooks.builtin import TimestampHook, FieldMapperHook

class UserSchema(BaseModel):
    user_id: int
    username: str
    email: str
    created_at: str

pipe = Pipe(
    input_adapter=JSONInputAdapter("users.jsonl", format="jsonl"),
    output_adapter=SQLAlchemyOutputAdapter(
        connection_string="mysql://user:pass@localhost/app_db",
        table_name="users",
        batch_size=10000
    ),
    executor=ThreadExecutor(UserSchema, max_workers=4),
    pre_validation_hooks=[
        FieldMapperHook({"old_field": "new_field"})
    ],
    post_validation_hooks=[
        TimestampHook(field_name="processed_at")
    ]
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
adapter = SQLAlchemyOutputAdapter("postgresql://...", "table")
adapter.open()
# ... write entries ...
adapter.flush()  # Force immediate write
adapter.close()
```

## Error Handling

If a write error occurs:
1. Transaction is automatically rolled back by SQLAlchemy
2. Exception is raised
3. Buffer is NOT cleared (data is preserved)
4. You can retry or handle the error

```python
from zoopipe.output_adapter.csv import CSVOutputAdapter

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=SQLAlchemyOutputAdapter("postgresql://...", "table"),
    error_output_adapter=CSVOutputAdapter("failed_records.csv"),
    executor=executor
)

with pipe:
    report = pipe.start()
    report.wait()

if report.error_count > 0:
    print(f"Failed to insert {report.error_count} records")
    print("Check failed_records.csv for details")
```

## Working with Existing Tables

The adapter automatically reflects the table schema from the database. Ensure:

1. **Table exists** before running the pipeline
2. **Columns match** your Pydantic model fields
3. **Data types** are compatible

```python
from sqlalchemy import inspect

engine = create_engine("postgresql://...")
inspector = inspect(engine)
columns = inspector.get_columns("table_name")
print(columns)
```

## See Also

- [SQLAlchemy Input Adapter](sqlalchemy-input.md) - Reading from SQL databases
- [Hooks Documentation](hooks.md) - Transform data before writing
- [MultiProcessing Executor](../executors.md#multiprocessingexecutor) - Parallel processing
- [Thread Executor](../executors.md#threadexecutor) - Concurrent I/O processing
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/) - Official SQLAlchemy docs

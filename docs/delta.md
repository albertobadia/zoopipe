# Delta Lake Adapter

ZooPipe provides experimental support for [Delta Lake](https://delta.io/), enabling high-performance reads and writes to Data Lakehouses.

## Features

- **ACID Transactions**: Writers generate data files, and a central Coordinator commits them atomically to the Delta Log (Implementation pending API stabilization).
- **Time Travel**: Read older versions of your data by specifying a version number.
- **Cloud Native**: Native support for S3, Azure Blob Storage, and GCS using Rust-based connectors.
- **Schema Evolution**: (Planned) Automatically merge schema changes.

## Installation

Delta Lake support is included in the standard `zoopipe` package, but requires cloud credentials if accessing remote storage.

## Usage

### Reading from Delta Lake

```python
from zoopipe import DeltaInputAdapter

# Read the latest version
adapter = DeltaInputAdapter(
    "s3://my-data-lake/users_table",
    storage_options={
        "AWS_ACCESS_KEY_ID": "...",
        "AWS_SECRET_ACCESS_KEY": "...",
        "AWS_REGION": "us-east-1"
    }
)

for batch in adapter.get_batches():
    print(f"Read {len(batch)} records")
```

### Writing to Delta Lake

```python
from zoopipe import DeltaOutputAdapter, Pipe

# Append mode (default)
writer = DeltaOutputAdapter(
    "s3://my-data-lake/processed_users",
    mode="append",
    storage_options={...}
)

pipe = Pipe(
    ...,
    output_adapter=writer
)
pipe.run()
```

## Architecture

ZooPipe uses the `delta-rs` (deltalake) Rust crate for protocol compliance.

1.  **Reading**: The `DeltaReader` queries the `_delta_log` to find valid Parquet files for the requested snapshot and uses Zoopipe's multi-threaded Parquet engine to read them.
2.  **Writing**: Workers write standard Parquet files to the table directory.
3.  **Committing**: The `DeltaCoordinator` collects the list of new files and creates an atomic `Add` transaction in the Delta Log.

> **Note on Version 2026.1**: Support for atomic commits is currently stubbed due to breaking changes in the underlying `deltalake` 0.30 Rust API. Files are written to storage but not yet registered in the log.

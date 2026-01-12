# CSV Input Adapter

The CSVInputAdapter reads data from CSV files with extensive configuration options.

## Features

- **High-performance native Rust parser** for optimal throughput
- Streaming processing for memory-efficient handling of large files
- Configurable encoding
- Custom delimiters and quote characters
- Skip rows (e.g., headers)
- Limit number of rows to process
- Custom field names
- Automatic ID generation for entries

## Usage

```python
from zoopipe.input_adapter.csv import CSVInputAdapter

input_adapter = CSVInputAdapter(
    filepath="data.csv",
    encoding="utf-8",
    delimiter=",",
    skip_rows=0,
    max_rows=None,
    fieldnames=None
)
```

## Parameters

- `source` (required): Path to the CSV file
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `delimiter` (optional): CSV delimiter character. Default: `","`
- `quotechar` (optional): Character used to quote fields. Default: `'"'`
- `skip_rows` (optional): Number of rows to skip from the beginning. Default: `0`
- `max_rows` (optional): Maximum number of rows to read. `None` means all rows. Default: `None`
- `fieldnames` (optional): Custom field names for the CSV. If `None`, uses the first row. Default: `None`
- `generate_ids` (optional): Automatically generate IDs for entries. Default: `True`

## Examples

### Basic Usage

```python
from zoopipe.input_adapter.csv import CSVInputAdapter

adapter = CSVInputAdapter("users.csv")
```

### Custom Configuration

```python
adapter = CSVInputAdapter(
    filepath="data.csv",
    delimiter=";",
    skip_rows=1,
    max_rows=1000,
    encoding="latin-1"
)
```

### Tab-Separated Values

```python
adapter = CSVInputAdapter(
    filepath="data.tsv",
    delimiter="\t"
)
```

---

[← Back to Adapters](adapters.md)

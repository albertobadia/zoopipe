# CSV Input Adapter

The CSVInputAdapter reads data from CSV files with extensive configuration options.

## Features

- Configurable encoding
- Custom delimiters
- Skip rows (e.g., headers)
- Limit number of rows to process
- Custom field names

## Usage

```python
from flowschema.input_adapter.csv import CSVInputAdapter

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

- `filepath` (required): Path to the CSV file
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `delimiter` (optional): CSV delimiter character. Default: `","`
- `skip_rows` (optional): Number of rows to skip from the beginning. Default: `0`
- `max_rows` (optional): Maximum number of rows to read. `None` means all rows. Default: `None`
- `fieldnames` (optional): Custom field names for the CSV. If `None`, uses the first row. Default: `None`

## Examples

### Basic Usage

```python
from flowschema.input_adapter.csv import CSVInputAdapter

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

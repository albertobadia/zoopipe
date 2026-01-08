# CSV Output Adapter

The CSVOutputAdapter writes validated entries to a CSV file.

## Features

- Automatic CSV writing
- Configurable encoding
- Custom delimiters
- Handles Entry objects automatically
- Efficient buffered writing

## Usage

```python
from flowschema.output_adapter.csv import CSVOutputAdapter

output_adapter = CSVOutputAdapter(
    filepath="output.csv",
    encoding="utf-8",
    delimiter=","
)
```

## Parameters

- `filepath` (required): Path to the output CSV file
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `delimiter` (optional): CSV delimiter character. Default: `","`

## Examples

### Basic Usage

```python
from flowschema.output_adapter.csv import CSVOutputAdapter

adapter = CSVOutputAdapter("processed_data.csv")
```

### Custom Delimiter

```python
adapter = CSVOutputAdapter(
    filepath="output.tsv",
    delimiter="\t"
)
```

### Error Output

```python
error_adapter = CSVOutputAdapter(
    filepath="errors.csv",
    delimiter="|"
)
```

---

[← Back to Adapters](adapters.md)

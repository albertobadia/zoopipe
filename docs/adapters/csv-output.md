# CSV Output Adapter

The CSVOutputAdapter writes validated entries to a CSV file.

## Features

- **High-performance native Rust writer** for optimal throughput
- **Batch writing optimization** reduces I/O overhead
- Automatic CSV writing
- Configurable encoding
- Custom delimiters and quote characters
- Handles Entry objects automatically
- Efficient buffered writing
- Optional automatic flushing

## Usage

```python
from zoopipe.output_adapter.csv import CSVOutputAdapter

output_adapter = CSVOutputAdapter(
    filepath="output.csv",
    encoding="utf-8",
    delimiter=","
)
```

## Parameters

- `output` (required): Path to the output CSV file
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `delimiter` (optional): CSV delimiter character. Default: `","`
- `quotechar` (optional): Character used to quote fields. Default: `'"'`
- `fieldnames` (optional): Explicit field names for CSV header. If `None`, inferred from first record. Default: `None`
- `autoflush` (optional): Automatically flush after each write. Default: `True`

## Examples

### Basic Usage

```python
from zoopipe.output_adapter.csv import CSVOutputAdapter

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

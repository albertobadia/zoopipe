# JSON Output Adapter

The JSONOutputAdapter writes validated entries to a JSON file in two formats: JSON arrays or JSONL (JSON Lines).

## Features

- **High-performance native Rust writer** for optimal throughput
- **Batch writing optimization** reduces I/O overhead
- Support for JSON arrays and JSONL format
- Configurable encoding
- Optional pretty-printing with indentation
- Handles complex types automatically
- Efficient buffered writing

## Usage

```python
from zoopipe.output_adapter.json import JSONOutputAdapter

output_adapter = JSONOutputAdapter(
    output="output.json",
    format="array",
    encoding="utf-8",
    include_metadata=False,
    indent=None
)
```

## Parameters

- `output` (required): Path to the output JSON file
- `format` (optional): JSON format. Options: `"array"` or `"jsonl"`. Default: `"array"`
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `indent` (optional): Number of spaces for indentation. `None` means compact output. Default: `None`

## Examples

### Writing JSON Array with Pretty Print

```python
from zoopipe.output_adapter.json import JSONOutputAdapter

output_adapter = JSONOutputAdapter(
    output="users.json",
    format="array",
    indent=2
)
```

### Writing JSONL

```python
output_adapter = JSONOutputAdapter(
    output="logs.jsonl",
    format="jsonl"
)
```

### Compact Output

```python
output_adapter = JSONOutputAdapter(
    output="output.json",
    format="array",
    indent=None
)
```

---

[← Back to Adapters](adapters.md)

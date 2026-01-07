# SchemaFlow

**SchemaFlow** is a lightweight, declarative data processing framework for Python. It allows you to build robust data pipelines by defining your data schemas with Pydantic and plugging in various input and output adapters.

Whether you are migrating data, cleaning CSVs, or processing streams, SchemaFlow provides a structured way to handle validation, transformation, and error management.

---

## 📑 Table of Contents

- [✨ Features](#-features)
- [🚀 Quick Start](#-quick-start)
  - [Installation](#installation)
  - [Basic Example](#basic-example)
- [🧩 Core Concepts](#-core-concepts)
  - [Adapters](#adapters)
  - [Executors](#executors)
  - [The Entry Object](#the-entry-object)
- [🛠 Development](#-development)
- [📄 License](#-license)

---

## ✨ Features

- **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures.
- **Pluggable Architecture**: Easily swap Input Adapters, Output Adapters, and Executors.
- **Built-in CSV Support**: Direct support for reading from and writing to CSV files with customizable encoding, delimiters, and row limits.
- **Automated Error Handling**: Configure a dedicated error output adapter to capture records that fail validation.
- **Async Ready**: Designed with async support in mind (Base adapters provided for async implementations).
- **Type Safe**: Fully type-hinted for a better developer experience.

## 🚀 Quick Start

### Installation

If you are using [uv](https://github.com/astral-sh/uv):

```bash
uv add schemaflow
```

Or using pip:

```bash
pip install schemaflow
```

### Basic Example

Here is how you can process a CSV file, validate it against a model, and save the results:

```python
from pydantic import BaseModel, ConfigDict
from schemaflow.core import SchemaFlow
from schemaflow.executor.sync_fifo import SyncFifoExecutor
from schemaflow.input_adapter.csv import CSVInputAdapter
from schemaflow.output_adapter.csv import CSVOutputAdapter

# 1. Define your schema
class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

# 2. Configure the flow
schema_flow = SchemaFlow(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"), # Optional: catch failures
    executor=SyncFifoExecutor(UserSchema),
)

# 3. Run it!
for entry in schema_flow.run():
    status = entry['status'].value
    print(f"[{status.upper()}] Row {entry['position']}: {entry['id']}")
```

## 🧩 Core Concepts

### Adapters
Adapters handle the communication with external data sources and sinks.
- **Input Adapter**: Responsible for fetching data from a source (CSV, API, Database, etc.).
  - `CSVInputAdapter`: Supports `encoding`, `delimiter`, `skip_rows`, `max_rows`, and `fieldnames`.
- **Output Adapter**: Responsible for persisting processed data to a destination.
  - `CSVOutputAdapter`: Efficiently writes entries to a CSV file.
- **Error Output Adapter**: (Optional) A specialized output adapter for records that failed validation or processing.

### Executors
Executors define *how* the data flows through the system.
- `SyncFifoExecutor`: Processes entries one by one in a synchronous, first-in-first-out manner. It uses Pydantic to validate each entry against the provided schema.

### The Entry Object
Every record in SchemaFlow is wrapped in an `EntryTypedDict`, which tracks its lifecycle:
- `id`: A unique `uuid.UUID` for the record.
- `position`: The original index/line number in the source.
- `status`: `PENDING`, `VALIDATED`, or `FAILED`.
- `raw_data`: The original dictionary extracted from the input.
- `validated_data`: The data after being parsed and dumped by your Pydantic model.
- `errors`: A list of validation errors (if any).
- `metadata`: A place to store additional context.

## 🛠 Development

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/schemaflow.git
   cd schemaflow
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

### Running Tests

```bash
pytest
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

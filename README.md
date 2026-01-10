# ZooPipe

**ZooPipe** is a lightweight, high-performance data processing framework for Python that combines the simplicity of Pydantic validation with the power of parallel processing.

Whether you're migrating data, cleaning CSVs, or processing streams, ZooPipe provides a structured way to handle validation, transformation, and error management without the complexity of big data frameworks.

---

## ✨ Key Features

- 🔍 **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures
- 🔌 **Pluggable Architecture**: Easily swap Input Adapters, Output Adapters, and Executors
- ⚡ **Parallel Processing**: Scale from single-threaded to distributed computing with `MultiprocessingExecutor`, `ThreadExecutor`, `DaskExecutor` and `RayExecutor`
- 🗜️ **High-Performance Serialization**: Uses msgpack and optional LZ4 compression for efficient inter-process communication
- 📊 **Built-in Format Support**: Direct support for CSV, JSON (array & JSONL), and Parquet files
- 🚨 **Automated Error Handling**: Dedicated error output adapter to capture records that fail validation
- 🪝 **Hooks System**: Transform and enrich data at various pipeline stages with built-in and custom hooks
- 🔄 **Async Ready**: Base adapters provided for async implementations
- 🛡️ **Type Safe**: Fully type-hinted for a better developer experience


---

## 🚀 Quick Start

### Installation

```bash
uv add zoopipe
```

Or using pip:

```bash
pip install zoopipe
```

### Simple Example

```python
from pydantic import BaseModel, ConfigDict
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(UserSchema),
)

# Start the pipewith context manager to ensure cleanup
with pipe:
    report = pipe.start()
    report.wait()

print(f"Finished! Processed {report.total_processed} items.")
```

---

## 📚 Documentation

### Getting Started
- [**Installation & First Steps**](docs/getting-started.md) - Get up and running quickly

### Core Concepts
- [**Executors**](docs/executors.md) - Learn about SyncFifoExecutor, MultiprocessingExecutor, ThreadExecutor, DaskExecutor and RayExecutor
- [**Adapters**](docs/adapters.md) - Input and Output adapters for various data sources
- [**Examples**](docs/examples.md) - Practical examples for common use cases


## 🎯 Use Cases

ZooPipe excels at:

- **Legacy Data Migrations**: Moving data between heterogeneous databases with validation
- **ETL Pipelines**: Extract, Transform, Load workflows with error handling
- **Data Cleaning**: Processing manually generated files (Excel/CSV) with inconsistent formats
- **Quality Filters**: Acting as a validation layer before loading data into Data Lakes or ML models
- **Batch Processing**: Processing large datasets with parallel execution

---

## 🧩 Architecture

ZooPipe uses a decoupled architecture based on four components:

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  Input Adapter  │─────▶│   Executor   │─────▶│ Output Adapter  │
│ (CSV, JSON,     │      │ (Validation  │      │ (CSV, JSON,     │
│  Parquet, DB,   │      │  & Transform)│      │  Parquet, DB,   │
│    API, etc)    │      │              │      │   API, etc)     │
└─────────────────┘      └──────────────┘      └─────────────────┘
                                │
                                │ (errors)
                                ▼
                         ┌──────────────┐
                         │ Error Output │
                         │   Adapter    │
                         └──────────────┘
```

- **Input Adapter**: Reads data from sources (CSV, JSON, Parquet, SQL, API)
- **Executor**: Validates with Pydantic and processes data (sequential or parallel)
- **Output Adapter**: Persists validated data
- **Error Output Adapter**: Captures failed validations (Dead Letter Queue)

Learn more in the [Architecture RFC](docs/RFC.md).

---

## 🔧 Executors

ZooPipe provides three execution strategies:

| Executor | Best For | Parallelism |
|----------|----------|-------------|
| `SyncFifoExecutor` | Small datasets, debugging | Single-threaded |
| `MultiprocessingExecutor` | Large datasets on single machine | Multi-process (CPU cores) |
| `ThreadExecutor` | IO-bound tasks (network/DB) | Multi-thread |
| `DaskExecutor` | ETL pipelines, Dask users | Dask cluster |
| `RayExecutor` | Massive datasets, distributed | Ray cluster |

See the [Executors documentation](docs/executors.md) for detailed information.

---

## 🛠 Development

### Setup

```bash
git clone https://github.com/albertobadia/zoopipe.git
cd zoopipe
uv sync
```

### Running Tests

```bash
uv run pytest -v
```

### Linting

```bash
./lint.sh
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📧 Contact

**Alberto Daniel Badia**  
Email: alberto_badia@enlacepatagonia.com  
GitHub: [@albertobadia](https://github.com/albertobadia)

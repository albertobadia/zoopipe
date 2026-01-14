# ZooPipe

**ZooPipe** is a lean, ultra-high-performance data processing engine for Python. It leverages a **100% Rust core** to handle I/O and orchestration, while keeping the flexibility of Python for schema validation (via Pydantic) and custom data enrichment (via Hooks).

---

## ✨ Key Features

- 🚀 **100% Native Rust Engine**: The core execution loop, including CSV and JSON parsing/writing, is implemented in Rust for maximum throughput.
- 🔍 **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures naturally.
- 🪝 **Python Hooks**: Transform and enrich data at any stage using standard Python functions or classes.
- ⚡ **Zero-Copy Intent**: Minimal overhead between the Rust processing engine and Python validation/hooks.
- 🚨 **Automated Error Routing**: Native support for routing failed records to a dedicated error output.
- 📊 **Multiple Format Support**: Optimized readers/writers for CSV, JSONL, and SQL databases (via SQLx with batch inserts).
- 🔧 **Pluggable Executors**: Choose between single-threaded or multi-threaded execution strategies.

---

## 🚀 Quick Start

### Installation

```bash
uv build
uv run maturin develop --release
```

### Simple Example

```python
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    schema_model=UserSchema,
)

pipe.start()
pipe.wait()

print(f"Finished! Processed {pipe.report.total_processed} items.")
```

---

## 📚 Documentation

### Core Concepts

- [**Executors Guide**](docs/executors.md) - Choose and configure execution strategies

### Input/Output Adapters

#### File Formats

- [**CSV Adapters**](docs/csv.md) - High-performance CSV reading and writing
- [**JSON Adapters**](docs/json.md) - JSONL and JSON array format support
- [**Parquet Adapters**](docs/parquet.md) - Columnar storage for analytics and data lakes
- [**Arrow Adapters**](docs/arrow.md) - Apache Arrow IPC format for zero-copy interoperability

#### Databases

- [**SQL Adapters**](docs/sql.md) - Read from and write to SQL databases with batch optimization
- [**DuckDB Adapters**](docs/duckdb.md) - Analytical database for OLAP workloads

#### Advanced

- [**Python Generator Adapters**](docs/pygen.md) - In-memory streaming and testing
- [**Cloud Storage (S3)**](docs/cloud-storage.md) - Read and write data from Amazon S3 and compatible services

---

## 🛠 Architecture

ZooPipe is designed as a thin Python wrapper around a powerful Rust core:

1. **Python Layer**: Configuration, Pydantic models, and custom Hooks.
2. **Rust Core**: 
   - **Adapters**: High-speed CSV/JSON/SQL Readers and Writers with optimized batch operations.
   - **NativePipe**: Orchestrates the loop, fetching chunks, calling a consolidated Python batch processor, and routing result batches.
   - **Executors**: Single-threaded or multi-threaded batch processing strategies.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# ZooPipe

**ZooPipe** is a lean, ultra-high-performance data processing engine for Python. It leverages a **100% Rust core** to handle I/O and orchestration, while keeping the flexibility of Python for schema validation (via Pydantic) and custom data enrichment (via Hooks).

---

## ✨ Key Features

- 🚀 **100% Native Rust Engine**: The core execution loop, including CSV and JSON parsing/writing, is implemented in Rust for maximum throughput.
- 🔍 **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures naturally.
- 🪝 **Python Hooks**: Transform and enrich data at any stage using standard Python functions or classes.
- 🚨 **Automated Error Routing**: Native support for routing failed records to a dedicated error output.
- 📊 **Multiple Format Support**: Optimized readers/writers for CSV, JSONL, and SQL databases (via SQLx with batch inserts).
- 🔧 **Pluggable Executors**: Choose between single-threaded or multi-threaded execution strategies.

---

## ⚡ Performance & Benchmarks

Why ZooPipe? Because **vectorization isn't always the answer.**

Tools like **Pandas** and **Polars** are incredible for analytical workloads (groupby, sum, joins) where operations can be vectorized in C/Rust. However, real-world Data Engineering often involves "chaotic ETL": messy custom rules, API calls per row, hashing, conditional cleanup, and complex normalization that forcedly drop down to Python loops.

**In these "Heavy ETL" scenarios, ZooPipe outperforms Vectorized DataFrames by 3x-8x.**


### Benchmark: Heavy ETL (15M+ Rows, 10GB CSV)
*Scenario: SHA256 Hashing, Normalization, Filtering, Enrichment per row.*

> **System**: Macbook Pro M1 2020 (8GB RAM). 

| Tool | Time (s) | Speed (Rows/s) | Peak RAM (MB) |
|---|---|---|---|
| **ZooPipe (4 workers)** | **~45s** | **~356k** | **~85 MB** |
| ZooPipe (1 worker)* | ~89s | ~180k | ~34 MB |
| Pure Python | ~145s | ~110k | ~25 MB |
| Pydantic | ~180s | ~89k | ~31 MB |
| Polars | ~370s | ~43k | ~2500 MB |
| Pandas | ~1830s | ~9k | ~3400 MB |

> *\*ZooPipe (1 worker) ran a lighter workload (timestamp only) validation, used as baseline for raw throughput.*

> **Key Takeaway**: ZooPipe's "Python-First Architecture" with parallel streaming (`PipeManager`) avoids the serialization overhead that cripples Polars/Pandas when using Python UDFs (`map_elements`/`apply`), and uses **97% less RAM**.


---

## 🚀 Quick Start

### Installation

```bash
pip install zoopipe
```
Or using uv:
```bash
uv add zoopipe
```
Or from source (uv recommended):
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
    email: str


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

### Parallel Processing (Multi-Process)

Automatically split a large file across multiple workers (processes) to bypass the GIL:

```python
from zoopipe import PipeManager

# Create your pipe as usual...
pipe = Pipe(...)

# Automatically parallelize across 4 workers
manager = PipeManager.parallelize_pipe(pipe, workers=4)
manager.start()
manager.join()
```

---

## 📚 Documentation

### Core Concepts

- [**Executors Guide**](docs/executors.md) - Choose and configure execution strategies
- [**Hooks Guide**](#hooks) - Transform and enrich data using Python hooks

#### Hooks

Hooks are Python classes that allow you to intercept, transform, and enrich data at different stages of the pipeline.

**[📘 Read the full Hooks Guide](docs/hooks.md)** to learn about lifecycle methods (`setup`, `execute`, `teardown`), state management, and advanced patterns like cursor pagination.

### Quick Example

```python
from zoopipe import BaseHook

class MyHook(BaseHook):
    def execute(self, entries, store):
        for entry in entries:
            entry["raw_data"]["checked"] = True
        return entries
```

> [!IMPORTANT]
> If you are using a `schema_model`, the pipeline will output the contents of `validated_data` for successful records.
> - To modify data **before** validation, use `pre_validation_hooks` and modify `entry["raw_data"]`.
> - To modify data **after** validation (and ensure it reaches the output), use `post_validation_hooks` and modify `entry["validated_data"]`.

### Input/Output Adapters

#### File Formats

- [**CSV Adapters**](docs/csv.md) - High-performance CSV reading and writing
- [**JSON Adapters**](docs/json.md) - JSONL and JSON array format support
- [**Excel Adapters**](docs/excel.md) - Read and write Excel (.xlsx) files
- [**Parquet Adapters**](docs/parquet.md) - Columnar storage for analytics and data lakes
- [**Arrow Adapters**](docs/arrow.md) - Apache Arrow IPC format for high-throughput interoperability

#### Databases

- [**SQL Adapters**](docs/sql.md) - Read from and write to SQL databases with batch optimization
- [**SQL Pagination**](docs/sql.md#sqlpaginationinputadapter) - High-performance cursor-style pagination for large tables
- [**DuckDB Adapters**](docs/duckdb.md) - Analytical database for OLAP workloads

#### Messaging Systems

- [**Kafka Adapters**](docs/kafka.md) - High-throughput messaging

#### Advanced

- [**Python Generator Adapters**](docs/pygen.md) - In-memory streaming and testing
- [**Cloud Storage (S3)**](docs/cloud-storage.md) - Read and write data from Amazon S3 and compatible services
- [**PipeManager**](docs/pipemanager.md) - Run multiple pipes in parallel for distributed processing

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

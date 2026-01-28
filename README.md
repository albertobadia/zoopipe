<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/albertobadia/zoopipe/main/docs/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/albertobadia/zoopipe/main/docs/assets/logo-light.svg">
    <img alt="ZooPipe Logo" src="https://raw.githubusercontent.com/albertobadia/zoopipe/main/docs/assets/logo-light.svg" width="600">
  </picture>
</p>

**ZooPipe** is a lean, ultra-high-performance data processing engine for Python. It leverages a **100% Rust core** to handle I/O and orchestration, while keeping the flexibility of Python for schema validation (via Pydantic) and custom data enrichment (via Hooks).

<p align="center">
  <a href="https://www.python.org/downloads/"><img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10+-blue.svg"></a>
  <a href="https://opensource.org/licenses/MIT"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-green.svg"></a>
  <a href="https://pypi.org/project/zoopipe/"><img alt="PyPI" src="https://img.shields.io/pypi/v/zoopipe"></a>
  <img alt="Downloads" src="https://img.shields.io/pypi/dm/zoopipe">
  <a href="https://github.com/albertobadia/zoopipe/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/albertobadia/zoopipe/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://zoopipe.readthedocs.io/"><img alt="ReadTheDocs" src="https://img.shields.io/readthedocs/zoopipe"></a>
</p>

---

Read the [docs](https://zoopipe.readthedocs.io/) for more information.

## ✨ Key Features

- 🚀 **100% Native Rust Engine**: The core execution loop, including CSV and JSON parsing/writing, is implemented in Rust for maximum throughput.
- 🔍 **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures naturally.
- 🪝 **Python Hooks**: Transform and enrich data at any stage using standard Python functions or classes.
- 🚨 **Automated Error Routing**: Native support for routing failed records to a dedicated error output.
- 📊 **Multiple Format Support**: Optimized readers/writers for CSV, JSONL, and SQL databases.
- 🔧 **Two-Tier Parallelism**: Orchestrate across processes or clusters with **Engines** (Local, Ray, Dask), and scale throughput at the node level with Rust **Executors**.
- ☁️ **Cloud Native**: Native S3, GCS, and Azure support, plus zero-config distributed execution on **Ray** or **Dask** clusters.

---

## ⚡ Performance & Benchmarks

Why ZooPipe? Because **vectorization isn't always the answer.**

Tools like **Pandas** and **Polars** are incredible for analytical workloads (groupby, sum, joins) where operations can be vectorized in C/Rust. However, real-world Data Engineering often involves "chaotic ETL": messy custom rules, API calls per row, hashing, conditional cleanup, and complex normalization that forcedly drop down to Python loops.

**In these "Heavy ETL" scenarios, ZooPipe outperforms Vectorized DataFrames by 3x-8x.**

![Benchmark Chart](https://raw.githubusercontent.com/albertobadia/zoopipe/main/docs/assets/benchmark.svg)

> **Key Takeaway**: ZooPipe's "Python-First Architecture" with parallel streaming (`PipeManager`) avoids the serialization overhead that cripples Polars/Pandas when using Python UDFs (`map_elements`/`apply`), and uses **97% less RAM**.

### ⚖️ Is this unfair to Pandas/Polars?

**Yes and No.**

- **Unfair**: If your workload is purely analytical (e.g., `GROUP BY`, `SUM`, `JOIN`), **Polars and Pandas will likely destroy ZooPipe** because they can use vectorized C/Rust operations on whole columns at once.
- **Fair**: In real-world Data Engineering, many pipelines are "chaotic". They require custom hashing, API calls per row, conditional normalization, or complex Pydantic validation. **In these "Python-UDF heavy" scenarios, vectorization breaks down**, and ZooPipe shines by orchestrating parallel Python execution efficiently without the DataFrame overhead.

### ❓ When to use what?

| Use **ZooPipe** When... | Use **Pandas / Polars** When... |
|---|---|
| 🏗️ You have complex, custom Python logic per row (hash, clean, validate). | 🧮 You are doing aggregations (SUM, AVG) or Relational Algebra (JOIN, GROUP BY). |
| 🔄 You are processing streaming data or files larger than RAM. | 💾 Your dataset fits comfortably in RAM (or use LazyFrames). |
| 🛡️ You need strict schema validation (Pydantic) and error handling. | 🔬 You are doing data exploration or statistical analysis. |
| 🚀 You want to mix Rust I/O performance with Python flexibility. | ⚡ Your entire pipeline can be expressed in vectorized expressions. |


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

# Run the pipe (streaming processing)
pipe.run()

print(f"Finished! Processed {pipe.report.total_processed} items.")
```
```

Automatically split large files or manage multiple independent workflows:

```python
from zoopipe import PipeManager, MultiProcessEngine

# Create your pipe as usual (Pipe is purely declarative)
pipe = Pipe(...)

# Automatically parallelize across 4 workers
# MultiProcessEngine() for local, RayEngine() or DaskEngine() for clusters
# Automatically parallelize across 4 workers
manager = PipeManager.parallelize_pipe(
    pipe, 
    workers=4, 
    engine=MultiProcessEngine() 
)

# Start, wait, and coordinate (e.g. merge files) automatically
manager.run()
```
```

---

## 📚 Documentation

### Core Concepts


#### Hooks

Hooks are Python classes that allow you to intercept, transform, and enrich data at different stages of the pipeline.

**[📘 Read the full Hooks Guide](https://github.com/albertobadia/zoopipe/blob/main/docs/hooks.md)** to learn about lifecycle methods (`setup`, `execute`, `teardown`), state management, and advanced patterns like cursor pagination.

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

#### Executors

Executors control how ZooPipe scales **up** within a single node using Rust-managed threads. They are the engine under the hood that drives high throughput.

**[📘 Read the full Executors Guide](https://github.com/albertobadia/zoopipe/blob/main/docs/executors.md)** to understand the difference between `SingleThreadExecutor` (debug/ordered) and `MultiThreadExecutor` (high-throughput).

### Input/Output Adapters

#### File Formats

- [**CSV Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/csv.md) - High-performance CSV reading and writing
- [**JSON Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/json.md) - JSONL and JSON array format support
- [**Excel Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/excel.md) - Read and write Excel (.xlsx) files
- [**Parquet Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/parquet.md) - Columnar storage for analytics and data lakes
- [**Arrow Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/arrow.md) - Apache Arrow IPC format for high-throughput interoperability

#### Databases

- [**SQL Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/sql.md) - Read from and write to SQL databases with batch optimization
- [**SQL Pagination**](https://github.com/albertobadia/zoopipe/blob/main/docs/sql.md#sqlpaginationinputadapter) - High-performance cursor-style pagination for large tables

#### Messaging Systems

- [**Kafka Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/kafka.md) - High-throughput messaging

#### Advanced

- [**Python Generator Adapters**](https://github.com/albertobadia/zoopipe/blob/main/docs/pygen.md) - In-memory streaming and testing
- [**Cloud Storage (S3)**](https://github.com/albertobadia/zoopipe/blob/main/docs/cloud-storage.md) - Read and write data from Amazon S3 and compatible services
- [**PipeManager**](https://github.com/albertobadia/zoopipe/blob/main/docs/pipemanager.md) - Run multiple pipes in parallel for distributed processing
- [**Ray Guide**](https://github.com/albertobadia/zoopipe/blob/main/docs/ray.md) - Zero-config distributed execution on Ray clusters
- [**Dask Guide**](https://github.com/albertobadia/zoopipe/blob/main/docs/dask.md) - Zero-config distributed execution on Dask clusters

---

## 🛠 Architecture

ZooPipe is designed as a thin Python wrapper around a powerful Rust core, featuring a two-tier parallel architecture:

1. **Orchestration Tier (Python Engines)**: 
   - Manage distribution across processes or nodes (e.g., `MultiProcessEngine`).
   - Handles data sharding, process lifecycle, and metrics aggregation.
2. **Execution Tier (Rust BatchExecutors)**: 
   - **Internal Throughput**: High-speed processing within a single process.
   - **Adapters**: Native CSV/JSON/SQL Readers and Writers.
   - **NativePipe**: Orchestrates the loop, fetching chunks and routing result batches.
   - **Executors**: Multi-threaded Rust strategies to bypass the GIL within a node.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# ZooPipe Examples

This directory contains examples demonstrating various features of ZooPipe.

## Running Examples
Make sure you have ZooPipe installed:
```bash
uv run maturin develop
```

Then run any example:
```bash
uv run python examples/01_basic_csv.py
```

## Available Examples

### 01_basic_csv.py
Basic CSV processing with Pydantic validation and hooks.

```bash
uv run python examples/01_basic_csv.py
```

### 02_jsonl_to_csv.py
Convert JSONL to CSV format with schema validation.

```bash
uv run python examples/02_jsonl_to_csv.py
```

### 03_executor_comparison.py
Compare performance between SingleThreadExecutor and MultiThreadExecutor.

```bash
uv run python examples/03_executor_comparison.py
```

This example demonstrates:
- Using `SingleThreadExecutor` for baseline performance
- Using `MultiThreadExecutor` with different worker counts
- Performance comparison and throughput metrics

### 04_csv_to_duckdb.py
Write CSV data to a DuckDB analytical database.

```bash
uv run python examples/04_csv_to_duckdb.py
```

This example demonstrates:
- Using `DuckDBOutputAdapter` for columnar storage
- Optimized for analytical queries and aggregations
- Fast batch loading into DuckDB

### 05_duckdb_to_jsonl.py
Export data from DuckDB to JSONL format.

```bash
uv run python examples/05_duckdb_to_jsonl.py
```

This example demonstrates:
- Using `DuckDBInputAdapter` to read from analytical databases
- Executing analytical queries with DuckDB SQL
- Exporting query results to JSONL

### 06_csv_to_arrow.py
Convert CSV data to Apache Arrow IPC format.

```bash
uv run python examples/06_csv_to_arrow.py
```

This example demonstrates:
- Using `ArrowOutputAdapter` for high-performance columnar storage
- Zero-copy interoperability with Pandas, Polars, R, etc.
- Efficient compression and fast writes

### 07_arrow_to_jsonl.py
Read Arrow IPC files and export to JSONL.

```bash
uv run python examples/07_arrow_to_jsonl.py
```

This example demonstrates:
- Using `ArrowInputAdapter` for ultra-fast reads
- Zero-copy memory access
- Converting columnar data to row-based JSONL

### 08_csv_to_sql.py
Write CSV data to a SQL database with optimized batch inserts.

```bash
uv run python examples/08_csv_to_sql.py
```

This example demonstrates:
- Using `SQLOutputAdapter` to write to SQLite databases
- Batch insert optimization for high-performance writes
- Database table creation with configurable modes (`replace`, `append`, `fail`)

### 09_sql_to_jsonl.py
Read data from SQL databases and export to JSONL format.

```bash
uv run python examples/09_sql_to_jsonl.py
```

This example demonstrates:
- Using `SQLInputAdapter` to read from SQLite databases
- Support for custom queries or table names
- Streaming large datasets from SQL to JSON

### 10_csv_to_generator.py
Process CSV data and stream results through a Python generator.

```bash
uv run python examples/10_csv_to_generator.py
```

This example demonstrates:
- Using `PyGeneratorOutputAdapter` for in-memory streaming
- Consuming pipeline results as a Python iterable
- Real-time processing without writing to disk
- Separating valid and error outputs into different generators

### 11_csv_to_parquet.py
Convert CSV data to Apache Parquet format for efficient storage.

```bash
uv run python examples/11_csv_to_parquet.py
```

This example demonstrates:
- Using `ParquetOutputAdapter` for columnar storage
- Excellent compression (5-10x smaller than CSV)
- Optimized for analytical queries and data warehousing
- Compatible with Pandas, Polars, Spark, and DuckDB

### 12_parquet_to_jsonl.py
Read Parquet files and export to JSONL format.

```bash
uv run python examples/12_parquet_to_jsonl.py
```

This example demonstrates:
- Using `ParquetInputAdapter` for fast columnar reads
- Converting Parquet data to JSONL for portability
- Type preservation from Parquet schema
- High-performance reading with zero-copy optimizations

## Sample Data

The `sample_data/` directory contains example CSV and JSONL files for testing.
The `output_data/` directory will contain the processed results.

## Directory Structure
- `sample_data/`: Contains input files for the examples.
- `output_data/`: Where processed results are saved.
- `models.py`: Shared Pydantic models.

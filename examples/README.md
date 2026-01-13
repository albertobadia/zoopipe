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

### 04_hooks_enrichment.py
Using Python hooks to enrich data before validation.

### 05_error_handling.py
Advanced error handling and routing to an error output.

## Sample Data

The `sample_data/` directory contains example CSV and JSONL files for testing.
The `output_data/` directory will contain the processed results.

## Directory Structure
- `sample_data/`: Contains input files for the examples.
- `output_data/`: Where processed results are saved.
- `models.py`: Shared Pydantic models.

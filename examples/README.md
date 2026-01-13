# ZooPipe Examples

This directory contains examples of how to use ZooPipe with its 100% Rust core.

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

1. **[01_basic_csv.py](01_basic_csv.py)**: Basic CSV to CSV processing with Pydantic validation.
2. **[02_jsonl_to_csv.py](02_jsonl_to_csv.py)**: Reading JSONL files and writing to CSV.
3. **[03_hooks_enrichment.py](03_hooks_enrichment.py)**: Using Python hooks to enrich data before validation.
4. **[04_error_handling.py](04_error_handling.py)**: Advanced error handling and routing to an error output.

## Directory Structure
- `sample_data/`: Contains input files for the examples.
- `output_data/`: Where processed results are saved.
- `models.py`: Shared Pydantic models.

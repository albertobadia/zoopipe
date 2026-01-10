# Pipe Examples

This directory contains executable examples demonstrating various Pipe features.

## Running Examples

All examples can be run from the project root directory:

```bash
uv run examples/01_basic_csv.py
```

## Available Examples

### 01–14 Examples
Detailed descriptions of all examples can be found in the [Documentation](https://github.com/albertobadia/zoopipe/blob/main/docs/examples.md).

## ⚡ Quick Run (All Examples)
You can run all examples sequentially with:
```bash
./run_examples.sh
```

## 📂 Available Examples
- **01 Basic CSV**: CSV processing, Pydantic validation, error handling.
- **02 Multiprocessing**: Multi-core parallel processing.
- **03 Ray**: Distributed computing across clusters.
- **04 JSON**: JSON Array and JSONL parsing/writing.
- **05 Hooks**: Data transformation and enrichment.
- **06 SQLite Hook**: Custom persistence via hooks.
- **07 JIT Ingestion**: Zero-copy parallel data fetching.
- **08 Partitioning**: Large file split into byte-range chunks.
- **09 Async Queues**: High-concurrency async ingestion.
- **10 Sync Queues**: Thread-safe queue communication.
- **11 Handling Errors**: Error analysis and DLQs.
- **12 Schema-less**: Dictionaries without Pydantic.
- **12 Threads**: IO-bound concurrent operations.
- **13 No Output**: Hooks/Executor-only persistence.
- **13 PyArrow**: High-speed Parquet processing.
- **14 Remote Fetch**: Adapter-level fetching hooks.

## Sample Data

Examples use the sample data files in `examples/data/`:
- `sample_data.csv` - Sample CSV with user records
- `sample_data.json` - Sample JSON array
- `sample_data.jsonl` - Sample JSONL data

## Output Files

Running the examples will create output files in `examples/output_data/`:
- `output.csv` - Validated CSV output
- `output.json` - JSON output
- `output.jsonl` - JSONL output
- `output_with_hooks.json` - JSON output with hook metadata
- `errors.csv` - CSV validation errors
- `errors.json` - JSON validation errors
- `errors.jsonl` - JSONL validation errors

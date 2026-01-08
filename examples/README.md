# FlowSchema Examples

This directory contains executable examples demonstrating various FlowSchema features.

## Running Examples

All examples can be run from the project root directory:

```bash
uv run examples/01_basic_csv.py
```

## Available Examples

### 01_basic_csv.py
Basic CSV processing with validation and error handling. Demonstrates:
- Reading from CSV
- Validating with Pydantic schema
- Writing validated data to output CSV
- Writing errors to separate CSV

### 02_multiprocessing.py
Parallel processing using MultiprocessingExecutor. Demonstrates:
- Multi-core processing
- Configurable worker count
- Chunk-based processing
- LZ4 compression for inter-process communication

### 03_ray_executor.py
Distributed processing using Ray. Demonstrates:
- Ray-based distributed computing
- Cluster support
- LZ4 compression for network efficiency

### 04_json_processing.py
Working with JSON files. Demonstrates:
- Reading JSON arrays
- Reading JSONL (JSON Lines) format
- Writing to JSON with formatting
- JSON to JSON transformations

### 05_hooks.py
Using the Hooks system. Demonstrates:
- TimestampHook for adding timestamps
- FieldMapperHook for field renaming
- Custom hooks for data transformation
- Combining multiple hooks

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

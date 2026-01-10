#!/bin/bash
set -e

export PYTHONPATH=src

# Find all python files in examples/ starting with a number, sorted numerically
for file in $(ls examples/[0-9]*.py | sort -V); do
    echo "=========================================="
    echo "Running $file"
    echo "=========================================="
    uv run python "$file"
    echo ""
done

echo "All examples executed successfully!"

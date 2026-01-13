from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter

__all__ = [
    "BaseOutputAdapter",
    "CSVOutputAdapter",
    "JSONOutputAdapter",
    "DuckDBOutputAdapter",
]

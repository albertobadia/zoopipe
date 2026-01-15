from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.arrow import ArrowInputAdapter
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.input_adapter.parquet import ParquetInputAdapter
from zoopipe.input_adapter.pygen import PyGeneratorInputAdapter
from zoopipe.input_adapter.sql import SQLInputAdapter
from zoopipe.manager import PipeManager
from zoopipe.output_adapter.arrow import ArrowOutputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter
from zoopipe.output_adapter.parquet import ParquetOutputAdapter
from zoopipe.output_adapter.pygen import PyGeneratorOutputAdapter
from zoopipe.output_adapter.sql import SQLOutputAdapter
from zoopipe.report import (
    EntryStatus,
    EntryTypedDict,
    FlowReport,
    FlowStatus,
    get_logger,
)
from zoopipe.zoopipe_rust_core import MultiThreadExecutor, SingleThreadExecutor

NAMES_TO_PYTYPE = {
    "ArrowInputAdapter": ArrowInputAdapter,
    "CSVInputAdapter": CSVInputAdapter,
    "DuckDBInputAdapter": DuckDBInputAdapter,
    "JSONInputAdapter": JSONInputAdapter,
    "PyGeneratorInputAdapter": PyGeneratorInputAdapter,
    "SQLInputAdapter": SQLInputAdapter,
    "ParquetInputAdapter": ParquetInputAdapter,
    "ArrowOutputAdapter": ArrowOutputAdapter,
    "CSVOutputAdapter": CSVOutputAdapter,
    "DuckDBOutputAdapter": DuckDBOutputAdapter,
    "JSONOutputAdapter": JSONOutputAdapter,
    "PyGeneratorOutputAdapter": PyGeneratorOutputAdapter,
    "SQLOutputAdapter": SQLOutputAdapter,
    "ParquetOutputAdapter": ParquetOutputAdapter,
    "PipeManager": PipeManager,
    "FlowReport": FlowReport,
    "FlowStatus": FlowStatus,
    "BaseHook": BaseHook,
    "HookStore": HookStore,
    "EntryStatus": EntryStatus,
    "EntryTypedDict": EntryTypedDict,
    "get_logger": get_logger,
    "SingleThreadExecutor": SingleThreadExecutor,
    "MultiThreadExecutor": MultiThreadExecutor,
}


__all__ = [NAMES_TO_PYTYPE]

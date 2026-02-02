from typing import Any, Dict

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.coordinators.iceberg import IcebergCoordinator
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import IcebergWriter


class IcebergOutputAdapter(BaseOutputAdapter):
    """
    Adapter for writing to Iceberg tables.
    Uses Rust-based IcebergWriter for high-performance data writing.
    """

    def __init__(
        self, table_location: str, catalog_properties: Dict[str, str] = None, **kwargs
    ):
        self.table_location = table_location
        self.catalog_properties = catalog_properties or {}
        self._writer = None

    def write_batch(self, data: Any) -> None:
        if not self._writer:
            raise RuntimeError("Writer not initialized. Use within a Pipe.")

        self._writer.write_batch(data)

    def close(self) -> Any:
        if self._writer:
            return self._writer.close()
        return "[]"

    def get_native_writer(self) -> Any:
        return IcebergWriter(table_location=self.table_location)

    def get_coordinator(self) -> BaseCoordinator | None:
        return IcebergCoordinator(
            table_location=self.table_location,
            catalog_properties=self.catalog_properties,
        )

from typing import Any, Dict, List, Optional

from zoopipe.coordinators.delta import DeltaCoordinator
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import DeltaWriter


class DeltaOutputAdapter(BaseOutputAdapter):
    """
    Writes data to a Delta Lake table.

    Produces Parquet files and returns AddActions to be committed
    transactionally by the DeltaCoordinator.
    """

    def __init__(
        self,
        table_uri: str,
        mode: str = "append",
        storage_options: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            table_uri: Target Delta table URI.
            mode: Write mode ("append" is currently supported).
            storage_options: Cloud storage credentials.
        """
        self.table_uri = table_uri
        self.mode = mode
        self.storage_options = storage_options
        self.schema = schema
        self._writer: DeltaWriter | None = None

    def _get_rust_writer(self) -> DeltaWriter:
        if not self._writer:
            self._writer = DeltaWriter(
                self.table_uri,
                self.storage_options,
            )
        return self._writer

    def get_native_writer(self) -> Any:
        return self._get_rust_writer()

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        writer = self._get_rust_writer()
        writer.write_batch(batch)

    def close(self) -> Any:
        if self._writer:
            return self._writer.close()
        return None

    def get_coordinator(self) -> Optional[DeltaCoordinator]:
        """
        Returns the DeltaCoordinator to handle the transaction commit.
        """
        return DeltaCoordinator(
            self.table_uri,
            self.mode,
            self.storage_options,
            self.schema,
        )

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import SQLWriter


class SQLOutputAdapter(BaseOutputAdapter):
    """
    Writes data into SQL databases via bulk inserts.

    Manages database transactions and performs batch insertions using
    optimized SQL writers in the Rust core.
    """

    def __init__(
        self,
        uri: str,
        table_name: str,
        mode: str = "replace",
        batch_size: int = 500,
    ):
        """
        Initialize the SQLOutputAdapter.

        Args:
            uri: Database URI.
            table_name: Name of the table to write to.
            mode: Write mode ('replace', 'append', or 'fail').
            batch_size: Number of records to insert per transaction.
        """
        self.uri = uri
        self.table_name = table_name
        self.mode = mode
        self.batch_size = batch_size

    def get_native_writer(self) -> SQLWriter:
        return SQLWriter(
            self.uri,
            self.table_name,
            mode=self.mode,
            batch_size=self.batch_size,
        )


__all__ = ["SQLOutputAdapter"]

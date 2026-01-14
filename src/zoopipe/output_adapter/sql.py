from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import SQLWriter


class SQLOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        uri: str,
        table_name: str,
        mode: str = "replace",
        batch_size: int = 500,
    ):
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

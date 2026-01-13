from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import SQLWriter


class SQLOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        uri: str,
        table_name: str,
        mode: str = "replace",
    ):
        self.uri = uri
        self.table_name = table_name
        self.mode = mode

    def get_native_writer(self) -> SQLWriter:
        return SQLWriter(
            self.uri,
            self.table_name,
            mode=self.mode,
        )


__all__ = ["SQLOutputAdapter"]

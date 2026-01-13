from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import SQLReader


class SQLInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        uri: str,
        query: str | None = None,
        table_name: str | None = None,
        generate_ids: bool = True,
    ):
        self.uri = uri
        self.generate_ids = generate_ids

        if query is None and table_name is None:
            raise ValueError("Either query or table_name must be provided")

        if query is not None and table_name is not None:
            raise ValueError("Only one of query or table_name should be provided")

        if query is not None:
            self.query = query
        else:
            self.query = f"SELECT * FROM {table_name}"

    def get_native_reader(self) -> SQLReader:
        return SQLReader(
            self.uri,
            self.query,
            generate_ids=self.generate_ids,
        )


__all__ = ["SQLInputAdapter"]

import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import DuckDBReader


class DuckDBInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        query: str | None = None,
        table_name: str | None = None,
        generate_ids: bool = True,
    ):
        self.source_path = str(source)
        self.generate_ids = generate_ids

        if query is None and table_name is None:
            raise ValueError("Either query or table_name must be provided")

        if query is not None and table_name is not None:
            raise ValueError("Only one of query or table_name should be provided")

        if query is not None:
            self.query = query
        else:
            self.query = f"SELECT * FROM {table_name}"

    def get_native_reader(self) -> DuckDBReader:
        return DuckDBReader(
            self.source_path,
            self.query,
            generate_ids=self.generate_ids,
        )


__all__ = ["DuckDBInputAdapter"]

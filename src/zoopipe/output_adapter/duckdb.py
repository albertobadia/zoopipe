import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import DuckDBWriter


class DuckDBOutputAdapter(BaseOutputAdapter):
    """
    Persists data into DuckDB database files.

    Supports replacing or appending to existing tables, leveraging DuckDB's
    transactional integrity and high-speed storage.
    """

    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        table_name: str,
        mode: str = "replace",
    ):
        """
        Initialize the DuckDBOutputAdapter.

        Args:
            output: Path to the DuckDB database file.
            table_name: Name of the table to write to.
            mode: Write mode ('replace', 'append', or 'fail').
        """
        self.output_path = str(output)
        self.table_name = table_name
        self.mode = mode

        if mode not in ["replace", "append", "fail"]:
            raise ValueError("mode must be 'replace', 'append', or 'fail'")

    def get_native_writer(self) -> DuckDBWriter:
        pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        return DuckDBWriter(
            self.output_path,
            self.table_name,
            mode=self.mode,
        )


__all__ = ["DuckDBOutputAdapter"]

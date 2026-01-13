import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import DuckDBWriter


class DuckDBOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        table_name: str,
        mode: str = "replace",
    ):
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

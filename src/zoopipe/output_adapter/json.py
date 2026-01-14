import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import JSONWriter


class JSONOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        format: str = "array",
        indent: int | None = None,
    ):
        self.output_path = str(output)
        self.format = format
        self.indent = indent

    def get_native_writer(self) -> JSONWriter:
        pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        return JSONWriter(
            self.output_path,
            format=self.format,
            indent=self.indent,
        )


__all__ = ["JSONOutputAdapter"]

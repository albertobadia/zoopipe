import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import JSONWriter


class JSONOutputAdapter(BaseOutputAdapter):
    """
    Serializes data to JSON format, supporting both array and
    line-delimited (JSONL) outputs.

    Equipped with a fast Rust-powered serializer that can indent results or
    output them in a compact single-line per record format.
    """

    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        format: str = "array",
        indent: int | None = None,
    ):
        """
        Initialize the JSONOutputAdapter.

        Args:
            output: Path where the JSON file will be created.
            format: JSON format ('array' for a single JSON array, or
                'lines' for JSONLines).
            indent: Optional indentation for pretty-printing.
        """
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

import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import JSONReader


class JSONInputAdapter(BaseInputAdapter):
    """
    Reads data from JSON or JSONLines (.jsonl) files.

    It supports both standard JSON arrays and line-delimited records.
    The adapter uses a fast Rust-based parser that streams data efficiently,
    making it suitable for very large datasets.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
    ):
        """
        Initialize the JSONInputAdapter.

        Args:
            source: Path to the JSONLines file.
        """
        self.source_path = str(source)

    def get_native_reader(self) -> JSONReader:
        return JSONReader(self.source_path)


__all__ = ["JSONInputAdapter"]

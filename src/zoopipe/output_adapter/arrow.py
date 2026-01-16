import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import ArrowWriter


class ArrowOutputAdapter(BaseOutputAdapter):
    """
    Writes data to disk in Apache Arrow IPC (feather) format.

    This adapter automatically handles parent directory creation and uses
    optimized Rust code for fast serialization.
    """

    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
    ):
        """
        Initialize the ArrowOutputAdapter.

        Args:
            output: destination file path (string or Path).
        """
        self.output_path = str(output)

    def get_native_writer(self) -> ArrowWriter:
        pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        return ArrowWriter(self.output_path)


__all__ = ["ArrowOutputAdapter"]

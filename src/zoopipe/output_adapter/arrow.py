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

    def split(self, workers: int) -> typing.List["ArrowOutputAdapter"]:
        """
        Split the output adapter into `workers` partitions.
        Generates filenames like `filename_part_1.arrow`.
        """
        path = pathlib.Path(self.output_path)
        stem = path.stem
        suffix = path.suffix
        parent = path.parent

        shards = []
        for i in range(workers):
            part_name = f"{stem}_part_{i + 1}{suffix}"
            part_path = parent / part_name
            shards.append(self.__class__(output=str(part_path)))
        return shards

    def get_native_writer(self) -> ArrowWriter:
        return ArrowWriter(self.output_path)


__all__ = ["ArrowOutputAdapter"]

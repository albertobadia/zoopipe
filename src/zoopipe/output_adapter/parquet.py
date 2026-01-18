import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import ParquetWriter


class ParquetOutputAdapter(BaseOutputAdapter):
    """
    Writes data to Apache Parquet files.

    Provides highly efficient columnar storage using the Arrow ecosystem,
    making it ideal for large-scale analytical processing.
    """

    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
    ):
        """
        Initialize the ParquetOutputAdapter.

        Args:
            path: Path where the Parquet file will be created.
        """
        self.path = str(path)

    def split(self, workers: int) -> typing.List["ParquetOutputAdapter"]:
        """
        Split the output adapter into `workers` partitions.
        Generates filenames like `filename_part_1.parquet`.
        """
        path = pathlib.Path(self.path)
        stem = path.stem
        suffix = path.suffix
        parent = path.parent

        shards = []
        for i in range(workers):
            part_name = f"{stem}_part_{i + 1}{suffix}"
            part_path = parent / part_name
            shards.append(self.__class__(path=str(part_path)))
        return shards

    def get_native_writer(self) -> ParquetWriter:
        return ParquetWriter(self.path)


__all__ = ["ParquetOutputAdapter"]

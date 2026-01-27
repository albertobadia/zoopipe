import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.utils.path import shard_file_path
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
        shard_paths = shard_file_path(self.path, workers)
        return [self.__class__(path=p) for p in shard_paths]

    def get_native_writer(self) -> ParquetWriter:
        return ParquetWriter(self.path)


__all__ = ["ParquetOutputAdapter"]

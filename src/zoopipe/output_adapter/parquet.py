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

    def get_native_writer(self) -> ParquetWriter:
        return ParquetWriter(self.path)


__all__ = ["ParquetOutputAdapter"]

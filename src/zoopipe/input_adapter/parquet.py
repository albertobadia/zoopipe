import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import ParquetReader


class ParquetInputAdapter(BaseInputAdapter):
    """
    Reads records from Apache Parquet files.

    Utilizes the Arrow ecosystem for efficient columnar data reading and
    multi-threaded loading.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        generate_ids: bool = True,
        batch_size: int = 1024,
    ):
        """
        Initialize the ParquetInputAdapter.

        Args:
            source: Path to the Parquet file.
            generate_ids: Whether to generate unique IDs for each record.
            batch_size: Number of records to read at once from the file.
        """
        self.source_path = str(source)
        self.generate_ids = generate_ids
        self.batch_size = batch_size

    def get_native_reader(self) -> ParquetReader:
        return ParquetReader(
            self.source_path,
            generate_ids=self.generate_ids,
            batch_size=self.batch_size,
        )


__all__ = ["ParquetInputAdapter"]

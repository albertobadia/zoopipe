import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import CSVReader


class CSVInputAdapter(BaseInputAdapter):
    """
    A high-performance CSV reader supporting both local and S3 sources.

    Uses a multi-threaded parser in the Rust core to ensure fast data ingestion
    without blocking the Python GIL.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        delimiter: str = ",",
        quotechar: str = '"',
        skip_rows: int = 0,
        fieldnames: list[str] | None = None,
        generate_ids: bool = True,
    ):
        """
        Initialize the CSVInputAdapter.

        Args:
            source: Path to the CSV file or S3 URI.
            delimiter: Column separator.
            quotechar: Character used for quoting fields.
            skip_rows: Number of rows to skip at the beginning.
            fieldnames: Optional list of column names.
            generate_ids: Whether to generate unique IDs for each record.
        """
        self.source_path = str(source)
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.skip_rows = skip_rows
        self.fieldnames = fieldnames
        self.generate_ids = generate_ids

    def get_native_reader(self) -> CSVReader:
        return CSVReader(
            self.source_path,
            delimiter=ord(self.delimiter),
            quote=ord(self.quotechar),
            skip_rows=self.skip_rows,
            fieldnames=self.fieldnames,
            generate_ids=self.generate_ids,
        )


__all__ = ["CSVInputAdapter"]

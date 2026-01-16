import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import CSVWriter


class CSVOutputAdapter(BaseOutputAdapter):
    """
    Writes pipeline results to CSV files.

    Handles directory creation and uses a buffered writer in Rust to ensure
    high-throughput performance.
    """

    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        delimiter: str = ",",
        quotechar: str = '"',
        fieldnames: list[str] | None = None,
    ):
        """
        Initialize the CSVOutputAdapter.

        Args:
            output: Path where the CSV file will be created.
            delimiter: Column separator.
            quotechar: Character used for quoting fields.
            fieldnames: Optional list of column names for the header.
        """
        self.output_path = str(output)
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.fieldnames = fieldnames

    def get_native_writer(self) -> CSVWriter:
        pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        return CSVWriter(
            self.output_path,
            delimiter=ord(self.delimiter),
            quote=ord(self.quotechar),
            fieldnames=self.fieldnames,
        )


__all__ = ["CSVOutputAdapter"]

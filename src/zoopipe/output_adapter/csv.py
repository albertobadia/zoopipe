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

    def split(self, workers: int) -> typing.List["CSVOutputAdapter"]:
        """
        Split the output adapter into `workers` partitions.
        Generates filenames like `filename_part_1.csv`.
        """
        path = pathlib.Path(self.output_path)
        stem = path.stem
        suffix = path.suffix
        parent = path.parent

        shards = []
        for i in range(workers):
            part_name = f"{stem}_part_{i + 1}{suffix}"
            part_path = parent / part_name
            shards.append(
                self.__class__(
                    output=str(part_path),
                    delimiter=self.delimiter,
                    quotechar=self.quotechar,
                    fieldnames=self.fieldnames,
                )
            )
        return shards

    def get_native_writer(self) -> CSVWriter:
        return CSVWriter(
            self.output_path,
            delimiter=ord(self.delimiter),
            quote=ord(self.quotechar),
            fieldnames=self.fieldnames,
        )


__all__ = ["CSVOutputAdapter"]

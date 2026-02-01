import csv
import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.utils.path import calculate_byte_ranges
from zoopipe.zoopipe_rust_core import CSVReader, get_file_size


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
        limit: int | None = None,
        start_byte: int = 0,
        end_byte: int | None = None,
    ):
        super().__init__()
        self.source_path = str(source)
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.skip_rows = skip_rows
        self.fieldnames = fieldnames
        self.generate_ids = generate_ids
        self.limit = limit
        self.start_byte = start_byte
        self.end_byte = end_byte

    def split(self, workers: int) -> typing.List["CSVInputAdapter"]:
        """
        Split the CSV input into `workers` byte-range shards.
        """
        file_size = get_file_size(self.source_path)
        ranges = calculate_byte_ranges(file_size, workers)

        # Ensure we have fieldnames if not explicitly provided
        final_fieldnames = self.fieldnames
        if final_fieldnames is None:
            if self.source_path.startswith("s3://"):
                final_fieldnames = self.get_native_reader().headers
            else:
                with open(self.source_path, "r") as f:
                    reader = csv.reader(
                        f, delimiter=self.delimiter, quotechar=self.quotechar
                    )
                    try:
                        final_fieldnames = next(reader)
                    except StopIteration:
                        final_fieldnames = []

        shards = []
        for start, end in ranges:
            shard = self.__class__(
                source=self.source_path,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                skip_rows=self.skip_rows,
                fieldnames=final_fieldnames,
                generate_ids=self.generate_ids,
                limit=self.limit,
                start_byte=start,
                end_byte=end,
            )
            shard.required_columns = self.required_columns
            shards.append(shard)
        return shards

    def get_native_reader(self) -> CSVReader:
        return CSVReader(
            self.source_path,
            delimiter=ord(self.delimiter),
            quote=ord(self.quotechar),
            skip_rows=self.skip_rows,
            fieldnames=self.fieldnames,
            generate_ids=self.generate_ids,
            limit=self.limit,
            start_byte=self.start_byte,
            end_byte=self.end_byte,
            projection=self.required_columns,
        )

    @staticmethod
    def count_rows(
        source: str | pathlib.Path,
        delimiter: str = ",",
        quotechar: str = '"',
        has_header: bool = True,
    ) -> int:
        """
        Efficiently count the number of rows in a CSV file using the Rust core.

        Args:
            source: Path to the CSV file.
            delimiter: Column separator. (Default: ',')
            quotechar: Character used for quoting. (Default: '"')
            has_header: Whether the file has a header row to ignore in user count
                (if used in context where header matters, though CSVReader.count_rows
                name implies all records).
                Actually pass this to rust to decide if first row is record or header.

        Returns:
            Number of rows (records).
        """
        return CSVReader.count_rows(
            str(source),
            ord(delimiter),
            ord(quotechar),
            has_header,
        )


__all__ = ["CSVInputAdapter"]

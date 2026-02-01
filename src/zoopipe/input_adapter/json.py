import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.utils.path import calculate_byte_ranges
from zoopipe.zoopipe_rust_core import JSONReader, get_file_size


class JSONInputAdapter(BaseInputAdapter):
    """
    Reads data from JSON or JSONLines (.jsonl) files.

    It supports both standard JSON arrays and line-delimited records.
    The adapter uses a fast Rust-based parser that streams data efficiently,
    making it suitable for very large datasets.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        start_byte: int = 0,
        end_byte: int | None = None,
    ):
        super().__init__()
        self.source_path = str(source)
        self.start_byte = start_byte
        self.end_byte = end_byte

    @property
    def can_split(self) -> bool:
        """Only allow splitting for JSONLines/NDJSON formats."""
        path = self.source_path.lower()
        return path.endswith(".jsonl") or path.endswith(".ndjson")

    def split(self, workers: int) -> typing.List["JSONInputAdapter"]:
        """
        Split the JSON input into `workers` byte-range shards.
        """
        file_size = get_file_size(self.source_path)
        ranges = calculate_byte_ranges(file_size, workers)
        shards = []
        for start, end in ranges:
            shard = self.__class__(
                source=self.source_path,
                start_byte=start,
                end_byte=end,
            )
            shard.required_columns = self.required_columns
            shards.append(shard)
        return shards

    def get_native_reader(self) -> JSONReader:
        return JSONReader(
            self.source_path,
            start_byte=self.start_byte,
            end_byte=self.end_byte,
            projection=self.required_columns,
        )


__all__ = ["JSONInputAdapter"]

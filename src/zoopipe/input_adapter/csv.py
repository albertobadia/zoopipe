import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import CSVReader


class CSVInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        delimiter: str = ",",
        quotechar: str = '"',
        skip_rows: int = 0,
        fieldnames: list[str] | None = None,
        generate_ids: bool = True,
    ):
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

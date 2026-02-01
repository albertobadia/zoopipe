import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import ExcelReader


class ExcelInputAdapter(BaseInputAdapter):
    """
    Reads Excel files (.xlsx, .xls, .ods, .xlsb) using the Calamine engine.

    Provides high-performance, memory-efficient parsing of various spreadsheet
    formats directly from Rust. Supports sheet selection by name or index,
    skipping header rows, and custom field mapping.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        sheet: typing.Union[str, int, None] = None,
        skip_rows: int = 0,
        fieldnames: typing.Optional[typing.List[str]] = None,
        generate_ids: bool = True,
    ):
        super().__init__()
        self.source_path = str(source)
        self.sheet = sheet
        self.skip_rows = skip_rows
        self.fieldnames = fieldnames
        self.generate_ids = generate_ids

    def get_native_reader(self) -> ExcelReader:
        return ExcelReader(
            self.source_path,
            sheet=self.sheet,
            skip_rows=self.skip_rows,
            fieldnames=self.fieldnames,
            generate_ids=self.generate_ids,
            projection=self.required_columns,
        )


__all__ = ["ExcelInputAdapter"]

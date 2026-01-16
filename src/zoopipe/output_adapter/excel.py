import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import ExcelWriter


class ExcelOutputAdapter(BaseOutputAdapter):
    """
    Creates Excel files (.xlsx) from pipeline entries.

    Provides a simple way to export processed data to spreadsheets, with
    support for custom worksheet names and column headers.
    """

    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
        sheet_name: typing.Optional[str] = None,
        fieldnames: typing.Optional[typing.List[str]] = None,
    ):
        """
        Initialize the ExcelOutputAdapter.

        Args:
            path: Path where the Excel file will be created.
            sheet_name: Optional name for the worksheet.
            fieldnames: Optional list of column names for the header.
        """
        self.path = str(path)
        self.sheet_name = sheet_name
        self.fieldnames = fieldnames

    def get_native_writer(self) -> ExcelWriter:
        return ExcelWriter(
            self.path,
            sheet_name=self.sheet_name,
            fieldnames=self.fieldnames,
        )


__all__ = ["ExcelOutputAdapter"]

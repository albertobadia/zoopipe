import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import ExcelWriter


class ExcelOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
        sheet_name: typing.Optional[str] = None,
        fieldnames: typing.Optional[typing.List[str]] = None,
    ):
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

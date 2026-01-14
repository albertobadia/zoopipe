import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import JSONReader


class JSONInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
    ):
        self.source_path = str(source)

    def get_native_reader(self) -> JSONReader:
        return JSONReader(self.source_path)


__all__ = ["JSONInputAdapter"]

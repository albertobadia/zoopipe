import pathlib
import typing

from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import ParquetWriter


class ParquetOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
    ):
        self.path = str(path)

    def get_native_writer(self) -> ParquetWriter:
        return ParquetWriter(self.path)


__all__ = ["ParquetOutputAdapter"]

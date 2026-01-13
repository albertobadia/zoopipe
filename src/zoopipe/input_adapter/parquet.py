import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import ParquetReader


class ParquetInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        generate_ids: bool = True,
    ):
        self.source_path = str(source)
        self.generate_ids = generate_ids

    def get_native_reader(self) -> ParquetReader:
        return ParquetReader(
            self.source_path,
            generate_ids=self.generate_ids,
        )


__all__ = ["ParquetInputAdapter"]

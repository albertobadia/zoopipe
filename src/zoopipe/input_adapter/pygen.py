import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import PyGeneratorReader


class PyGeneratorInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        iterable: typing.Iterable[typing.Any],
        generate_ids: bool = True,
    ):
        self.iterable = iterable
        self.generate_ids = generate_ids

    def get_native_reader(self) -> PyGeneratorReader:
        return PyGeneratorReader(
            self.iterable,
            generate_ids=self.generate_ids,
        )


__all__ = ["PyGeneratorInputAdapter"]

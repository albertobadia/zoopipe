import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import PyGeneratorReader


class PyGeneratorInputAdapter(BaseInputAdapter):
    """
    Bridges Python iterables and generators into the pipeline.

    Allows using any custom Python logic or in-memory data as a source
    for the pipeline.
    """

    def __init__(
        self,
        iterable: typing.Iterable[typing.Any],
        generate_ids: bool = True,
    ):
        """
        Initialize the PyGeneratorInputAdapter.

        Args:
            iterable: Any Python iterable or generator yielding dictionaries.
            generate_ids: Whether to generate unique IDs for each record.
        """
        self.iterable = iterable
        self.generate_ids = generate_ids

    def get_native_reader(self) -> PyGeneratorReader:
        return PyGeneratorReader(
            self.iterable,
            generate_ids=self.generate_ids,
        )


__all__ = ["PyGeneratorInputAdapter"]

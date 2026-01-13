from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import PyGeneratorWriter


class PyGeneratorOutputAdapter(BaseOutputAdapter):
    def __init__(self, queue_size: int = 1000):
        self._writer = PyGeneratorWriter(queue_size=queue_size)

    def get_native_writer(self) -> PyGeneratorWriter:
        return self._writer

    def __iter__(self):
        return self._writer


__all__ = ["PyGeneratorOutputAdapter"]

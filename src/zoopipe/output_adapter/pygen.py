from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import PyGeneratorWriter


class PyGeneratorOutputAdapter(BaseOutputAdapter):
    """
    Exposes pipeline results as a Python generator.

    This adapter provides a bridge back to Python code, allowing you to
    iterate over the processed results as they become available.
    """

    def __init__(self, queue_size: int = 1000):
        """
        Initialize the PyGeneratorOutputAdapter.

        Args:
            queue_size: Buffer size for the internal queue.
        """
        self._writer = PyGeneratorWriter(queue_size=queue_size)

    def get_native_writer(self) -> PyGeneratorWriter:
        return self._writer

    def __iter__(self):
        return self._writer


__all__ = ["PyGeneratorOutputAdapter"]

import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import ArrowReader


class ArrowInputAdapter(BaseInputAdapter):
    """
    Reads records from Apache Arrow IPC (feather) files.

    Provides high-speed sequential access to Arrow data with minimal
    serialization overhead.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        generate_ids: bool = True,
    ):
        """
        Initialize the ArrowInputAdapter.

        Args:
            source: Path to the Arrow file.
            generate_ids: Whether to generate unique IDs for each record.
        """
        self.source_path = str(source)
        self.generate_ids = generate_ids

    def get_native_reader(self) -> ArrowReader:
        return ArrowReader(
            self.source_path,
            generate_ids=self.generate_ids,
        )


__all__ = ["ArrowInputAdapter"]

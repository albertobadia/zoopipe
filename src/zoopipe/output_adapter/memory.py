from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class MemoryOutputAdapter(BaseOutputAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[EntryTypedDict] = []

    def write(self, entry: EntryTypedDict) -> None:
        self.results.append(entry)


__all__ = ["MemoryOutputAdapter"]

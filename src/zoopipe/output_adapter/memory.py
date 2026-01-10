from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class MemoryOutputAdapter(BaseOutputAdapter):
    def __init__(self, pre_hooks=None, post_hooks=None) -> None:
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.results: list[EntryTypedDict] = []

    def write(self, entry: EntryTypedDict) -> None:
        self.results.append(entry)


__all__ = ["MemoryOutputAdapter"]

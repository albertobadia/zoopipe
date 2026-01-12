import typing

from zoopipe.output_adapter.base import BaseOutputAdapter


class MemoryOutputAdapter(BaseOutputAdapter):
    def __init__(self, pre_hooks=None, post_hooks=None) -> None:
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.results: list[dict[str, typing.Any]] = []

    def write(self, data: dict[str, typing.Any]) -> None:
        self.results.append(data)


__all__ = ["MemoryOutputAdapter"]

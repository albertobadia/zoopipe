import typing

from zoopipe.output_adapter.base import BaseOutputAdapter


class DummyOutputAdapter(BaseOutputAdapter):
    def write(self, data: dict[str, typing.Any]) -> None:
        pass

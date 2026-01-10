from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class DummyOutputAdapter(BaseOutputAdapter):
    def write(self, entry: EntryTypedDict) -> None:
        pass

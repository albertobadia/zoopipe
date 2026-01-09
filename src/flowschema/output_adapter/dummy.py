from flowschema.models.core import EntryTypedDict
from flowschema.output_adapter.base import BaseOutputAdapter


class DummyOutputAdapter(BaseOutputAdapter):
    def write(self, entry: EntryTypedDict) -> None:
        pass

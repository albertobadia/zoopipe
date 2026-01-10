from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.dummy import DummyOutputAdapter


def test_dummy_output_adapter():
    adapter = DummyOutputAdapter()

    entry = {
        "id": "test-id",
        "raw_data": {"name": "Alice"},
        "validated_data": {"name": "Alice"},
        "position": 0,
        "status": EntryStatus.VALIDATED,
        "errors": [],
        "metadata": {},
    }

    with adapter:
        adapter.write(entry)

    assert True

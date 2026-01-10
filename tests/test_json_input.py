import pathlib

from pydantic import BaseModel, ConfigDict

from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.models.core import EntryStatus


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


def test_json_input_adapter_array():
    sample_file = (
        pathlib.Path(__file__).parent.parent / "examples" / "data" / "sample_data.json"
    )

    adapter = JSONInputAdapter(sample_file, format="array")

    with adapter:
        entries = list(adapter.generator)

    assert len(entries) == 5

    assert entries[0]["raw_data"]["name"] == "Alice"
    assert entries[0]["raw_data"]["age"] == 30
    assert entries[0]["status"] == EntryStatus.PENDING
    assert entries[0]["position"] == 0

    assert entries[4]["raw_data"]["name"] == "Eve"
    assert entries[4]["position"] == 4


def test_json_input_adapter_jsonl():
    sample_file = (
        pathlib.Path(__file__).parent.parent / "examples" / "data" / "sample_data.jsonl"
    )

    adapter = JSONInputAdapter(sample_file, format="jsonl")

    with adapter:
        entries = list(adapter.generator)

    assert len(entries) == 5

    assert entries[0]["raw_data"]["name"] == "Alice"
    assert entries[0]["raw_data"]["age"] == 30
    assert entries[0]["status"] == EntryStatus.PENDING
    assert entries[0]["position"] == 0

    assert entries[4]["raw_data"]["name"] == "Eve"
    assert entries[4]["position"] == 4


def test_json_input_adapter_max_items():
    sample_file = (
        pathlib.Path(__file__).parent.parent / "examples" / "data" / "sample_data.json"
    )

    adapter = JSONInputAdapter(sample_file, format="array", max_items=3)

    with adapter:
        entries = list(adapter.generator)

    assert len(entries) == 3
    assert entries[0]["raw_data"]["name"] == "Alice"
    assert entries[2]["raw_data"]["name"] == "Charlie"

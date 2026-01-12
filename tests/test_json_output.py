import json
import uuid

from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.json import JSONOutputAdapter


def test_json_output_adapter_array(tmp_path):
    output_file = tmp_path / "output.json"

    adapter = JSONOutputAdapter(output_file, format="array")

    entries = [
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Alice", "age": 30},
            position=0,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Bob", "age": 25},
            position=1,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
    ]

    with adapter:
        for entry in entries:
            # In the real Pipe, Pipe._handle_entry extracts the data
            data = entry.get("validated_data") or entry.get("raw_data")
            adapter.write(data)

    with open(output_file) as f:
        result = json.load(f)

    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 30
    assert result[1]["name"] == "Bob"
    assert result[1]["age"] == 25


def test_json_output_adapter_jsonl(tmp_path):
    output_file = tmp_path / "output.jsonl"

    adapter = JSONOutputAdapter(output_file, format="jsonl")

    entries = [
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Alice", "age": 30},
            position=0,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Bob", "age": 25},
            position=1,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
    ]

    with adapter:
        for entry in entries:
            data = entry.get("validated_data") or entry.get("raw_data")
            adapter.write(data)

    with open(output_file) as f:
        lines = f.readlines()

    assert len(lines) == 2
    obj1 = json.loads(lines[0])
    obj2 = json.loads(lines[1])

    assert obj1["name"] == "Alice"
    assert obj1["age"] == 30
    assert obj2["name"] == "Bob"
    assert obj2["age"] == 25


# test_json_output_adapter_with_metadata removed as it's no longer supported


def test_json_output_adapter_round_trip(tmp_path):
    output_file = tmp_path / "output.json"

    from zoopipe.input_adapter.json import JSONInputAdapter

    write_adapter = JSONOutputAdapter(output_file, format="array")

    entries = [
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Alice", "age": 30},
            position=0,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
        EntryTypedDict(
            id=uuid.uuid4(),
            raw_data=None,
            validated_data={"name": "Bob", "age": 25},
            position=1,
            status=EntryStatus.VALIDATED,
            errors=[],
            metadata={},
        ),
    ]

    with write_adapter:
        for entry in entries:
            data = entry.get("validated_data") or entry.get("raw_data")
            write_adapter.write(data)

    read_adapter = JSONInputAdapter(output_file, format="array")

    with read_adapter:
        read_entries = list(read_adapter.generator)

    assert len(read_entries) == 2
    assert read_entries[0]["raw_data"]["name"] == "Alice"
    assert read_entries[1]["raw_data"]["name"] == "Bob"

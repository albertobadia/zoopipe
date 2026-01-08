import json
import pathlib
import tempfile
import uuid

from flowschema.models.core import EntryStatus, EntryTypedDict
from flowschema.output_adapter.json import JSONOutputAdapter


def test_json_output_adapter_array():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = pathlib.Path(tmpdir) / "output.json"

        adapter = JSONOutputAdapter(output_file, format="array")

        entries = [
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Alice", "age": 30},
                position=0,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Bob", "age": 25},
                position=1,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
        ]

        with adapter:
            for entry in entries:
                adapter.write(entry)

        with open(output_file) as f:
            result = json.load(f)

        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30
        assert result[1]["name"] == "Bob"
        assert result[1]["age"] == 25


def test_json_output_adapter_jsonl():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = pathlib.Path(tmpdir) / "output.jsonl"

        adapter = JSONOutputAdapter(output_file, format="jsonl")

        entries = [
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Alice", "age": 30},
                position=0,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Bob", "age": 25},
                position=1,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
        ]

        with adapter:
            for entry in entries:
                adapter.write(entry)

        with open(output_file) as f:
            lines = f.readlines()

        assert len(lines) == 2
        obj1 = json.loads(lines[0])
        obj2 = json.loads(lines[1])

        assert obj1["name"] == "Alice"
        assert obj1["age"] == 30
        assert obj2["name"] == "Bob"
        assert obj2["age"] == 25


def test_json_output_adapter_with_metadata():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = pathlib.Path(tmpdir) / "output.json"

        adapter = JSONOutputAdapter(output_file, format="array", include_metadata=True)

        entry_id = uuid.uuid4()
        entries = [
            EntryTypedDict(
                id=entry_id,
                raw_data=None,
                validated_data={"name": "Alice", "age": 30},
                position=0,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
        ]

        with adapter:
            for entry in entries:
                adapter.write(entry)

        with open(output_file) as f:
            result = json.load(f)

        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30
        assert result[0]["__id"] == str(entry_id)
        assert result[0]["__status"] == "validated"
        assert result[0]["__position"] == 0


def test_json_output_adapter_round_trip():
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = pathlib.Path(tmpdir) / "output.json"

        from flowschema.input_adapter.json import JSONInputAdapter

        write_adapter = JSONOutputAdapter(output_file, format="array")

        entries = [
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Alice", "age": 30},
                position=0,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
            EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=None,
                validated_data={"name": "Bob", "age": 25},
                position=1,
                status=EntryStatus.VALIDATED,
                errors=[],
            ),
        ]

        with write_adapter:
            for entry in entries:
                write_adapter.write(entry)

        read_adapter = JSONInputAdapter(output_file, format="array")

        with read_adapter:
            read_entries = list(read_adapter.generator)

        assert len(read_entries) == 2
        assert read_entries[0]["raw_data"]["name"] == "Alice"
        assert read_entries[1]["raw_data"]["name"] == "Bob"

import enum
import uuid

import lz4.frame
import msgpack
from pydantic import BaseModel

from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.models.core import EntryStatus, EntryTypedDict


def _packer_default(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, enum.Enum):
        return obj.value
    return obj


class MockEnum(enum.Enum):
    A = "value_a"
    B = "value_b"


class MockModel(BaseModel):
    name: str


def test_packer_default_uuid():
    test_uuid = uuid.uuid4()
    packed = msgpack.packb(test_uuid, default=_packer_default)
    unpacked = msgpack.unpackb(packed)
    assert unpacked == str(test_uuid)


def test_packer_default_enum():
    test_enum = MockEnum.A
    packed = msgpack.packb(test_enum, default=_packer_default)
    unpacked = msgpack.unpackb(packed)
    assert unpacked == "value_a"


def test_round_trip_entry_typed_dict():
    entry_id = uuid.uuid4()
    entry: EntryTypedDict = {
        "id": entry_id,
        "position": 1,
        "status": EntryStatus.PENDING,
        "raw_data": {"name": "test"},
        "validated_data": None,
        "errors": [],
        "metadata": {},
    }

    packed = msgpack.packb(entry, default=_packer_default)
    unpacked = msgpack.unpackb(packed)

    assert unpacked["id"] == str(entry_id)
    assert unpacked["status"] == "pending"
    assert unpacked["raw_data"]["name"] == "test"


def test_executor_process_chunk():
    chunk_data = [{"raw_data": {"name": "Alice"}}, {"raw_data": {"name": "Bob"}}]
    packed_chunk = msgpack.packb(chunk_data, default=_packer_default)

    results = MultiProcessingExecutor._process_chunk(MockModel, None, packed_chunk)

    assert len(results) == 2
    assert results[0]["status"] == EntryStatus.VALIDATED
    assert results[0]["validated_data"]["name"] == "Alice"
    assert results[1]["status"] == EntryStatus.VALIDATED
    assert results[1]["validated_data"]["name"] == "Bob"


def test_executor_process_chunk_lz4():
    chunk_data = [{"raw_data": {"name": "Alice"}}, {"raw_data": {"name": "Bob"}}]
    packed_chunk = msgpack.packb(chunk_data, default=_packer_default)
    compressed_chunk = lz4.frame.compress(packed_chunk)

    results = MultiProcessingExecutor._process_chunk(MockModel, "lz4", compressed_chunk)

    assert len(results) == 2
    assert results[0]["status"] == EntryStatus.VALIDATED
    assert results[0]["validated_data"]["name"] == "Alice"

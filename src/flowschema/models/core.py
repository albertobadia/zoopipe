import enum
import typing
import uuid


class EntryStatus(enum.Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class EntryTypedDict(typing.TypedDict):
    id: uuid.UUID
    position: int | None
    status: EntryStatus
    raw_data: dict[str, typing.Any]
    validated_data: dict[str, typing.Any]
    errors: list[dict[str, typing.Any]]
    metadata: dict[str, typing.Any]

import enum
import typing


class EntryStatus(enum.Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class EntryTypedDict(typing.TypedDict):
    id: typing.Any
    position: int | None
    status: EntryStatus
    raw_data: dict[str, typing.Any]
    validated_data: dict[str, typing.Any] | None
    errors: list[dict[str, typing.Any]]
    metadata: dict[str, typing.Any]

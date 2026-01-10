import enum
import json
import uuid

from pydantic import BaseModel, ValidationError

from zoopipe.models.core import EntryStatus, EntryTypedDict


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, enum.Enum):
            return obj.value
        return super().default(obj)


def validate_entry(
    schema_model: type[BaseModel] | None, entry: EntryTypedDict
) -> EntryTypedDict:
    if schema_model is None:
        entry["status"] = EntryStatus.VALIDATED
        return entry

    try:
        validated_data = schema_model.model_validate(entry["raw_data"])
        entry["validated_data"] = validated_data.model_dump()
        entry["status"] = EntryStatus.VALIDATED
        return entry
    except ValidationError as e:
        entry["status"] = EntryStatus.FAILED
        entry["errors"] = [
            {"loc": err["loc"], "msg": err["msg"], "type": err["type"]}
            for err in e.errors()
        ]
        return entry
    except Exception as e:
        entry["status"] = EntryStatus.FAILED
        entry["errors"] = [{"message": str(e), "type": type(e).__name__}]
        return entry

from pydantic import BaseModel, ValidationError

from flowschema.models.core import EntryStatus, EntryTypedDict


def validate_entry(
    schema_model: type[BaseModel], entry: EntryTypedDict
) -> EntryTypedDict:
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

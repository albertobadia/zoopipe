from pydantic import BaseModel

from flowschema.models.core import EntryStatus, EntryTypedDict


def validate_entry(
    schema_model: type[BaseModel], entry: EntryTypedDict
) -> EntryTypedDict:
    try:
        validated_data = schema_model.model_validate(entry["raw_data"])
        entry["validated_data"] = validated_data.model_dump()
        entry["status"] = EntryStatus.VALIDATED
        return entry
    except Exception as e:
        entry["status"] = EntryStatus.FAILED
        entry["errors"] = [{"message": str(e), "type": type(e).__name__}]
        return entry

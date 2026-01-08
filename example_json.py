import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.output_adapter.json import JSONOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


def main():
    print("=== JSON Array Example ===")
    schema_flow = FlowSchema(
        input_adapter=JSONInputAdapter("sample_data.json", format="array"),
        output_adapter=JSONOutputAdapter("output.json", format="array", indent=2),
        error_output_adapter=JSONOutputAdapter("errors.json", format="array"),
        executor=SyncFifoExecutor(UserSchema),
    )

    for entry in schema_flow.run():
        status = entry["status"].value
        entry_message = entry.get("validated_data", entry.get("raw_data"))
        message = f"Position {entry['position']}: {entry_message}"
        print(f"[{status.upper()}] {message}")

    print("\n=== JSONL Example ===")
    schema_flow_jsonl = FlowSchema(
        input_adapter=JSONInputAdapter("sample_data.jsonl", format="jsonl"),
        output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
        executor=SyncFifoExecutor(UserSchema),
    )

    for entry in schema_flow_jsonl.run():
        status = entry["status"].value
        entry_message = entry.get("validated_data", entry.get("raw_data"))
        message = f"Position {entry['position']}: {entry_message}"
        print(f"[{status.upper()}] {message}")


if __name__ == "__main__":
    main()

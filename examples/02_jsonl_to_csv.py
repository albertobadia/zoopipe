from pydantic import BaseModel

from zoopipe import Pipe
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


class LogSchema(BaseModel):
    level: str
    message: str
    timestamp: str


def main():
    # Ensure directories exist
    import pathlib

    pathlib.Path("examples/output_data").mkdir(parents=True, exist_ok=True)

    input_path = "examples/sample_data/logs.jsonl"
    output_path = "examples/output_data/logs_processed.csv"

    # Create dummy JSONL for demo if not exists
    if not pathlib.Path(input_path).exists():
        pathlib.Path("examples/sample_data").mkdir(parents=True, exist_ok=True)
        with open(input_path, "w") as f:
            f.write(
                '{"level": "INFO", "message": "Started", '
                '"timestamp": "2024-01-01T00:00:00Z"}\n'
            )
            f.write(
                '{"level": "ERROR", "message": "Failed", '
                '"timestamp": "2024-01-01T00:00:01Z"}\n'
            )

    print("--- Starting JSONL to CSV Pipeline ---")

    pipe = Pipe(
        input_adapter=JSONInputAdapter(input_path),
        output_adapter=CSVOutputAdapter(output_path),
        schema_model=LogSchema,
    )

    pipe.start()
    pipe.wait()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

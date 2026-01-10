import json
import os

from zoopipe import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.json import JSONInputAdapter


def setup_data():
    os.makedirs("examples/data", exist_ok=True)

    data = [{"id": i, "name": f"Item {i}", "value": i * 10} for i in range(10)]
    with open("examples/data/no_output.json", "w") as f:
        json.dump(data, f)


def main():
    setup_data()

    # Minimal Pipe: No schema model, no output adapter
    # It will just process the input and show the report
    executor = ThreadExecutor(schema_model=None, max_workers=2)
    input_adapter = JSONInputAdapter("examples/data/no_output.json")

    print("Starting pipewithout Pydantic schema and without output adapter...")
    with Pipe(
        input_adapter=input_adapter,
        executor=executor,
    ) as pipe:
        report = pipe.start()
        report.wait()

    print("\nPipecompleted!")
    print(f"Total processed: {report.total_processed}")
    print(f"Success count: {report.success_count}")
    print(f"Error count: {report.error_count}")
    print(f"Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()

import json
import os

from zoopipe import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter


def setup_data():
    os.makedirs("examples/data", exist_ok=True)
    os.makedirs("examples/output_data", exist_ok=True)

    data = [{"id": i, "name": f"Item {i}", "value": i * 10} for i in range(10)]
    with open("examples/data/no_pydantic.json", "w") as f:
        json.dump(data, f)


def main():
    setup_data()

    # We use ThreadExecutor without a schema_model
    executor = ThreadExecutor(schema_model=None, max_workers=2)

    input_adapter = JSONInputAdapter("examples/data/no_pydantic.json")
    output_adapter = JSONOutputAdapter("examples/output_data/no_pydantic_output.json")

    print("Starting pipewithout Pydantic schema...")
    with Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        executor=executor,
    ) as pipe:
        report = pipe.start()
        report.wait()

    print("\nPipecompleted!")
    print(f"Total processed: {report.total_processed}")
    print(f"Success count: {report.success_count}")
    print(f"Error count: {report.error_count}")

    if os.path.exists("examples/output_data/no_pydantic_output.json"):
        with open("examples/output_data/no_pydantic_output.json", "r") as f:
            output_data = json.load(f)
            print(f"\nExample output entry: {output_data[0]}")


if __name__ == "__main__":
    main()

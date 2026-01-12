import pathlib
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.rust import RustBatchExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def main():
    output_adapter = JSONOutputAdapter(
        "examples/output_data/big_users.jsonl", format="jsonl"
    )

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=output_adapter,
        executor=RustBatchExecutor(UserSchema, batch_size=10000),
    )

    report = pipe.start()

    ouput_path = pathlib.Path(output_adapter.output_path)

    while not report.is_finished:
        time.sleep(1)
        print(f"Progress: {report.total_processed}/{report.total_processed}")
        print(f"Speed: {report.items_per_second:.2f} rows/s")
        print(f"Size: {ouput_path.stat().st_size / 1024 / 1024:.2f} MB")

    print(f"\nFinal Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()

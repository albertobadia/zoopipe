import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def main():
    # We use GeneratorOutputAdapter to iterate over results as they are produced
    output_adapter = CSVOutputAdapter(
        "examples/output_data/big_users.csv", autoflush=True
    )

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=output_adapter,
        # error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=SyncFifoExecutor(UserSchema),
    )

    # Start the pipein the background
    report = pipe.start()

    # Iterate over the results from the adapter
    # for entry in output_adapter:
    #     status = entry["status"].value
    #     progress = report.total_processed
    #     print(
    #         f"[{status.upper()}] Row {entry['position']} "
    #         f"(Progress: {progress}): {entry['id']}"
    #     )

    while not report.is_finished:
        time.sleep(0.1)
        print(f"Progress: {report.total_processed}/{report.total_processed}")

    print(f"\nFinal Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()

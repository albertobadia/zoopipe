import os
import time

from examples.schemas import UserSchema
from zoopipe import CSVInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter


def main():
    db_path = os.path.abspath("examples/output_data/users_data.db")
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=SQLOutputAdapter(
            f"sqlite:{db_path}?mode=rwc",
            table_name="users_data",
            mode="replace",
        ),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4),
    )

    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"Processed: {pipe.report.total_processed} | "
            f"Speed: {pipe.report.items_per_second:.2f} rows/s | "
            f"Ram Usage: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

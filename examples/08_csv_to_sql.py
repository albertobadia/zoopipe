import os

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


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

    pipe.run()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

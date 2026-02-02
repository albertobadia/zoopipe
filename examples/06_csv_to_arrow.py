from pydantic import BaseModel, ConfigDict

from zoopipe import ArrowOutputAdapter, CSVInputAdapter, MultiThreadExecutor, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=ArrowOutputAdapter("examples/output_data/users_processed.arrow"),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4),
    )

    pipe.run()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVOutputAdapter, JSONInputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=JSONInputAdapter("examples/output_data/users_processed.zst"),
        output_adapter=CSVOutputAdapter("examples/output_data/users_processed.csv"),
        schema_model=UserSchema,
    )

    pipe.run()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

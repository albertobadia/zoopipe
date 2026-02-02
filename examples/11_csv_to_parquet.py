from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, ParquetOutputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=ParquetOutputAdapter("examples/output_data/users_data.parquet"),
        schema_model=UserSchema,
    )

    pipe.run()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

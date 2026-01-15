import time

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, Pipe, PyGeneratorOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    input_adapter = CSVInputAdapter("examples/sample_data/users_data.csv")
    output_adapter = PyGeneratorOutputAdapter(queue_size=10)
    error_adapter = PyGeneratorOutputAdapter(queue_size=10)

    pipe = Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        error_output_adapter=error_adapter,
        schema_model=UserSchema,
    )

    print("\n--- Starting Pipeline ---")
    pipe.start()
    time.sleep(1)
    print("--- Starting to consume ---")

    print(f"\nReport: {pipe.report}")

    for result in output_adapter:
        print(result)


if __name__ == "__main__":
    main()

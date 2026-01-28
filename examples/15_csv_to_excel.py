from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    ExcelInputAdapter,
    ExcelOutputAdapter,
    JSONOutputAdapter,
    Pipe,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    input_csv = "examples/sample_data/demo_users.csv"
    with open(input_csv, "w") as f:
        f.write("user_id,username,email\n")
        for i in range(100):
            f.write(f"{i},user_{i},user_{i}@example.com\n")

    print(f"Step 1: CSV ({input_csv}) -> Excel")
    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(input_csv),
        output_adapter=ExcelOutputAdapter(
            "examples/output_data/users_intermediate.xlsx",
            sheet_name="Users",
        ),
        schema_model=UserSchema,
    )

    pipe1.run()

    print(f"Converted {pipe1.report.success_count} records to Excel")

    print("\nStep 2: Excel -> JSONL")
    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(
            "examples/output_data/users_intermediate.xlsx",
            sheet="Users",
        ),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_from_excel.jsonl",
            format="jsonl",
        ),
        schema_model=UserSchema,
    )

    pipe2.run()

    print(f"Converted {pipe2.report.success_count} records to JSONL")
    print("\nPipeline Finished!")


if __name__ == "__main__":
    main()

import datetime

from pydantic import BaseModel, ConfigDict

from zoopipe import BaseHook, ExcelInputAdapter, ExcelOutputAdapter, HookStore, Pipe


class TimeStampHook(BaseHook):
    def execute(self, entries: list[dict], store: HookStore) -> list[dict]:
        for entry in entries:
            entry["validated_data"]["processed_at"] = datetime.datetime.now()
        return entries


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=ExcelInputAdapter(
            "examples/sample_data/users_data.xlsx",
            sheet=0,
        ),
        output_adapter=ExcelOutputAdapter(
            "examples/output_data/users_processed.xlsx",
            sheet_name="ProcessedUsers",
        ),
        schema_model=UserSchema,
        post_validation_hooks=[TimeStampHook()],
        report_update_interval=10,
    )

    pipe.run()

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()

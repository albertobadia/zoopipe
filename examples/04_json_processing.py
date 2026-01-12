import pathlib
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.rust import RustBatchExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


pipe = Pipe(
    input_adapter=JSONInputAdapter("examples/output_data/big_users.json"),
    output_adapter=CSVOutputAdapter("examples/output_data/big_users_2.csv"),
    executor=RustBatchExecutor(UserSchema, batch_size=10000),
)
output_file_path = pathlib.Path(pipe.output_adapter.output_path)
report = pipe.start()


while not report.is_finished:
    time.sleep(1)
    print(
        f"Progress: {report.total_processed} rows, {report.items_per_second:.2f} rows/s"
    )
    print(f"size: {output_file_path.stat().st_size / 1024 / 1024:.2f} MB")
print(f"Final: {report}")

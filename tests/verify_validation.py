import pathlib

from pydantic import BaseModel, ConfigDict

from zoopipe.core import Pipe
from zoopipe.executor.rust import RustBatchExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    age: int


def test_rust_validation_filtering(tmp_path):
    # Create CSV with extra columns
    csv_file = tmp_path / "extra_keys.csv"
    with open(csv_file, "w") as f:
        f.write("name,age,extra_field,another_one\n")
        for i in range(5):
            f.write(f"User{i},{20 + i},secret,123\n")

    input_adapter = CSVInputAdapter(str(csv_file))
    output_adapter = MemoryOutputAdapter()

    pipe = Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        executor=RustBatchExecutor(UserSchema, batch_size=2),
    )

    report = pipe.start()
    report.wait()

    assert report.total_processed == 5
    assert len(output_adapter.results) == 5

    print(
        "DEBUG: First result:",
        output_adapter.results[0] if output_adapter.results else "Empty",
    )

    for res in output_adapter.results:
        assert "name" in res
        assert "age" in res
        assert "extra_field" not in res
        assert "another_one" not in res


if __name__ == "__main__":
    # Simulate tmp_path
    import shutil
    import tempfile

    tmp_dir = pathlib.Path(tempfile.mkdtemp())
    try:
        test_rust_validation_filtering(tmp_dir)
        print("Verification Passed!")
    finally:
        shutil.rmtree(tmp_dir)

from pydantic import BaseModel, ConfigDict

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


def test_csv_output_adapter(tmp_path):
    class Person(BaseModel):
        model_config = ConfigDict(extra="ignore")
        name: str
        age: int

    output_file = tmp_path / "output.csv"

    input_adapter = CSVInputAdapter("examples/data/sample_data.csv", max_rows=5)
    output_adapter = CSVOutputAdapter(output_file)
    executor = SyncFifoExecutor(Person)

    pipe = Pipe(
        input_adapter=input_adapter, executor=executor, output_adapter=output_adapter
    )

    report = pipe.start()
    report.wait()
    results_count = report.total_processed

    assert results_count > 0
    assert output_file.exists()

    import csv

    with open(output_file, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == results_count
        if len(rows) > 0:
            assert "name" in rows[0]
            assert "age" in rows[0]
            assert "id" in rows[0]
            assert "status" in rows[0]
            assert "position" in rows[0]
            assert "metadata" in rows[0]

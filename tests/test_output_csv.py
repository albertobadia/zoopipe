from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


def test_csv_output_adapter(tmp_path):
    class Person(BaseModel):
        model_config = ConfigDict(extra="ignore")
        name: str
        age: int

    output_file = tmp_path / "output.csv"

    # Simple input adapter using the real sample_data.csv if it has name/age
    # Or just use the first few lines of sample_data.csv

    input_adapter = CSVInputAdapter("sample_data.csv", max_rows=5)
    output_adapter = CSVOutputAdapter(output_file)
    executor = SyncFifoExecutor(Person)

    flow = FlowSchema(
        input_adapter=input_adapter, executor=executor, output_adapter=output_adapter
    )

    results = list(flow.run())

    assert len(results) > 0
    assert output_file.exists()

    # Read output and verify
    import csv

    with open(output_file, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == len(results)
        if len(rows) > 0:
            assert "name" in rows[0]
            assert "age" in rows[0]

import logging

from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter
from flowschema.output_adapter.generator import GeneratorOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    name: str
    last_name: str
    age: int
    description: str


def test_multiprocessing_executor(tmp_path):
    sample_csv = tmp_path / "test_mp_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")

    gen_adapter = GeneratorOutputAdapter()
    error_csv = tmp_path / "test_mp_errors.csv"
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter(str(sample_csv)),
        output_adapter=gen_adapter,
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        executor=MultiProcessingExecutor(InputModel, max_workers=2, chunksize=2),
    )
    report = schema_flow.start()
    output_data = list(gen_adapter)
    report.wait()

    assert len(output_data) == 3
    assert output_data[0]["status"].value == "validated"
    assert output_data[0]["validated_data"]["name"] == "Alice"

    print("Multiprocessing test passed!")
    print(output_data)


if __name__ == "__main__":
    import pytest

    pytest.main([__file__])

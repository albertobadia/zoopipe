from pydantic import BaseModel

from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter


class UserSchema(BaseModel):
    user_id: int
    name: str


def test_pygen_adapters():
    def user_gen():
        yield {"user_id": 1, "name": "Alice"}
        yield {"user_id": 2, "name": "Bob"}
        yield {"user_id": 3, "name": "Charlie"}

    output_adapter = PyGeneratorOutputAdapter()

    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(user_gen()),
        output_adapter=output_adapter,
        schema_model=UserSchema,
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed == 3
    assert pipe.report.success_count == 3

    results = list(output_adapter)
    assert len(results) == 3
    assert results[0]["name"] == "Alice"
    assert results[1]["name"] == "Bob"
    assert results[2]["name"] == "Charlie"


def test_pygen_errors():
    def user_gen():
        yield {"user_id": 1, "name": "Alice"}
        yield {"user_id": "invalid", "name": "Bob"}

    output_adapter = PyGeneratorOutputAdapter()
    error_adapter = PyGeneratorOutputAdapter()

    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(user_gen()),
        output_adapter=output_adapter,
        error_output_adapter=error_adapter,
        schema_model=UserSchema,
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed == 2
    assert pipe.report.success_count == 1
    assert pipe.report.error_count == 1

    success_results = list(output_adapter)
    error_results = list(error_adapter)

    assert len(success_results) == 1
    assert success_results[0]["name"] == "Alice"

    assert len(error_results) == 1
    assert error_results[0]["name"] == "Bob"

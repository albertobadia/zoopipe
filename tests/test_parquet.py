import json

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    JSONOutputAdapter,
    ParquetInputAdapter,
    ParquetOutputAdapter,
    Pipe,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_csv_to_parquet_to_jsonl(tmp_path):
    input_csv = tmp_path / "input.csv"
    intermediate_parquet = tmp_path / "storage.parquet"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ParquetOutputAdapter(str(intermediate_parquet)),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 2
    assert intermediate_parquet.exists()

    pipe2 = Pipe(
        input_adapter=ParquetInputAdapter(str(intermediate_parquet)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2

    with open(output_jsonl, "r") as f:
        lines = f.readlines()
        rows = [json.loads(line) for line in lines]

    assert len(rows) == 2
    rows.sort(key=lambda x: x["username"])

    assert rows[0]["username"] == "alice"
    assert int(rows[0]["age"]) == 30
    assert rows[1]["username"] == "bob"
    assert int(rows[1]["age"]) == 25

def test_parquet_row_group_splitting(tmp_path):
    path = str(tmp_path / "groups.parquet")
    
    # ParquetWriter has a fixed row group size of 8192.
    # Let's write 10,000 rows to get 2 row groups.
    # We use a large enough batch to trigger multiple row groups (8192 is the threshold)
    data = [{"user_id": str(i), "username": f"user_{i}", "age": i} for i in range(10000)]
    
    writer = ParquetOutputAdapter(path)
    # Manual write for testing
    native_writer = writer.get_native_writer()
    native_writer.write_batch(data)
    native_writer.close()
    
    # Verify we can use ParquetReader directly to get info
    from zoopipe.zoopipe_rust_core import ParquetReader
    info = ParquetReader.get_row_groups_info(path)
    assert len(info) >= 2, f"Should have at least 2 row groups, got {len(info)}"
    assert sum(info) == 10000
    
    # Test splitting
    adapter = ParquetInputAdapter(path)
    shards = adapter.split(workers=2)
    
    assert len(shards) == 2
    assert shards[0].row_groups is not None
    assert shards[1].row_groups is not None
    
    # Verify reading from shards
    total_read = 0
    for shard in shards:
        reader = shard.get_native_reader()
        # Read in large chunks
        while True:
            batch = reader.read_batch(1000)
            if not batch:
                break
            total_read += len(batch)
            
    assert total_read == 10000

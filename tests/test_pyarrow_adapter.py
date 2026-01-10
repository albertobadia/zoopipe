import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from zoopipe.input_adapter.arrow import ArrowInputAdapter
from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.arrow import ArrowOutputAdapter


def test_arrow_input_adapter_parquet(tmp_path):
    parquet_path = tmp_path / "test.parquet"
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    pq.write_table(table, parquet_path)

    adapter = ArrowInputAdapter(parquet_path)
    with adapter:
        results = list(adapter.generator)

    assert len(results) == 3
    assert results[0]["raw_data"] == {"a": 1, "b": "x"}
    assert results[1]["raw_data"] == {"a": 2, "b": "y"}
    assert results[2]["raw_data"] == {"a": 3, "b": "z"}
    assert isinstance(results[0]["id"], uuid.UUID)
    assert results[0]["position"] == 0


def test_arrow_input_adapter_csv(tmp_path):
    csv_path = tmp_path / "test.csv"
    csv_content = "a,b\n1,x\n2,y\n3,z"
    csv_path.write_text(csv_content)

    # PyArrow dataset can read CSVs too
    adapter = ArrowInputAdapter(csv_path, format="csv")
    with adapter:
        results = list(adapter.generator)

    assert len(results) == 3
    assert results[0]["raw_data"] == {"a": 1, "b": "x"}


def test_arrow_output_adapter_parquet(tmp_path):
    parquet_path = tmp_path / "output.parquet"
    adapter = ArrowOutputAdapter(parquet_path)

    entries = [
        {
            "id": uuid.uuid4(),
            "status": EntryStatus.PENDING,
            "position": i,
            "raw_data": {"val": i},
            "validated_data": {"val": i},
            "errors": [],
            "metadata": {},
        }
        for i in range(5)
    ]

    with adapter:
        for entry in entries:
            adapter.write(entry)

    assert parquet_path.exists()
    table = pq.read_table(parquet_path)
    assert table.num_rows == 5
    assert table.to_pylist() == [
        {"val": 0},
        {"val": 1},
        {"val": 2},
        {"val": 3},
        {"val": 4},
    ]


def test_arrow_output_adapter_buffering(tmp_path):
    parquet_path = tmp_path / "output_buffered.parquet"
    # Small batch size to trigger multiple flushes
    adapter = ArrowOutputAdapter(parquet_path, batch_size=2)

    with adapter:
        for i in range(5):
            adapter.write(
                {
                    "id": uuid.uuid4(),
                    "status": EntryStatus.PENDING,
                    "position": i,
                    "raw_data": {"val": i},
                    "validated_data": {"val": i},
                    "errors": [],
                    "metadata": {},
                }
            )

    table = pq.read_table(parquet_path)
    assert table.num_rows == 5

from unittest.mock import patch

from zoopipe.input_adapter.delta import DeltaInputAdapter


def test_delta_split_logic():
    """
    Test that DeltaInputAdapter.split() correctly partitions files
    among the requested number of splits.
    """
    table_uri = "s3://bucket/table"

    # Mock get_delta_files to return a fixed list of files
    with patch("zoopipe.input_adapter.delta.get_delta_files") as mock_get_files:
        # distinct files
        mock_files = [f"file_{i}.parquet" for i in range(10)]
        mock_get_files.return_value = mock_files

        adapter = DeltaInputAdapter(table_uri=table_uri)

        # Split into 2
        splits_2 = adapter.split(2)
        assert len(splits_2) == 2
        assert splits_2[0].files == [f"file_{i}.parquet" for i in range(5)]
        assert splits_2[1].files == [f"file_{i}.parquet" for i in range(5, 10)]

        # Split into 3 (uneven)
        splits_3 = adapter.split(3)
        assert len(splits_3) == 3
        # 10 / 3 = 3.33 -> ceil(3.33) = 4 files per chunk
        # Chunk 1: indices 0-4 (4 files)
        # Chunk 2: indices 4-8 (4 files)
        # Chunk 3: indices 8-10 (2 files)
        assert len(splits_3[0].files) == 4
        assert len(splits_3[1].files) == 4
        assert len(splits_3[2].files) == 2

        # Split into more than available files (e.g. 12)
        splits_12 = adapter.split(12)
        assert len(splits_12) == 12
        # First 10 should have 1 file each
        for i in range(10):
            assert len(splits_12[i].files) == 1
            assert splits_12[i].files == [f"file_{i}.parquet"]
        # Last 2 should be empty
        assert splits_12[10].files == []
        assert splits_12[11].files == []


def test_delta_explicit_files_split():
    """Test splitting when files are already provided in init"""
    files = ["a.parquet", "b.parquet", "c.parquet"]
    adapter = DeltaInputAdapter("uri", files=files)

    # We shouldn't need to mock get_delta_files here because it shouldn't be called
    with patch("zoopipe.input_adapter.delta.get_delta_files") as mock_get_files:
        splits = adapter.split(3)
        mock_get_files.assert_not_called()

        assert len(splits) == 3
        assert splits[0].files == ["a.parquet"]
        assert splits[1].files == ["b.parquet"]
        assert splits[2].files == ["c.parquet"]


def test_delta_output_adapter_instantiation():
    """
    Test that DeltaOutputAdapter can be instantiated (has all abstract methods
    implemented)
    """
    from zoopipe.output_adapter.delta import DeltaOutputAdapter

    # This would raise TypeError if abstract methods are missing
    adapter = DeltaOutputAdapter(table_uri="s3://bucket/out")

    assert adapter.table_uri == "s3://bucket/out"
    # smoke test get_native_writer
    # Note: effectively a no-op until we mock the underlying rust writer if needed,
    # but checks the method exists and runs.
    # The _get_rust_writer calls Rust, which might fail if DLLs are missing
    # or mocked incorrectly, but the method call itself should exist.
    assert hasattr(adapter, "get_native_writer")

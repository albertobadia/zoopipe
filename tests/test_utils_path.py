from zoopipe.utils.path import calculate_byte_ranges, shard_file_path


def test_shard_file_path_with_multiple_workers():
    path = "/data/output.csv"
    workers = 3

    result = shard_file_path(path, workers)

    assert len(result) == 3
    assert result[0] == "/data/output_part_1.csv"
    assert result[1] == "/data/output_part_2.csv"
    assert result[2] == "/data/output_part_3.csv"


def test_shard_file_path_with_single_worker():
    path = "/data/output.csv"
    workers = 1

    result = shard_file_path(path, workers)

    assert result == ["/data/output.csv"]


def test_shard_file_path_with_zero_workers():
    path = "/data/output.csv"
    workers = 0

    result = shard_file_path(path, workers)

    assert result == ["/data/output.csv"]


def test_shard_file_path_preserves_directory_structure():
    path = "/var/data/nested/folder/file.parquet"
    workers = 2

    result = shard_file_path(path, workers)

    assert result[0] == "/var/data/nested/folder/file_part_1.parquet"
    assert result[1] == "/var/data/nested/folder/file_part_2.parquet"


def test_shard_file_path_handles_different_extensions():
    path = "output.json"
    workers = 2

    result = shard_file_path(path, workers)

    assert result[0] == "output_part_1.json"
    assert result[1] == "output_part_2.json"


def test_shard_file_path_handles_no_extension():
    path = "/data/outputfile"
    workers = 2

    result = shard_file_path(path, workers)

    assert result[0] == "/data/outputfile_part_1"
    assert result[1] == "/data/outputfile_part_2"


def test_calculate_byte_ranges_with_multiple_workers():
    file_size = 1000
    workers = 4

    result = calculate_byte_ranges(file_size, workers)

    assert len(result) == 4
    assert result[0] == (0, 250)
    assert result[1] == (250, 500)
    assert result[2] == (500, 750)
    assert result[3] == (750, None)


def test_calculate_byte_ranges_with_single_worker():
    file_size = 1000
    workers = 1

    result = calculate_byte_ranges(file_size, workers)

    assert result == [(0, None)]


def test_calculate_byte_ranges_with_zero_workers():
    file_size = 1000
    workers = 0

    result = calculate_byte_ranges(file_size, workers)

    assert result == [(0, None)]


def test_calculate_byte_ranges_handles_uneven_file_size():
    file_size = 1001
    workers = 3

    result = calculate_byte_ranges(file_size, workers)

    assert len(result) == 3
    assert result[0] == (0, 333)
    assert result[1] == (333, 666)
    assert result[2] == (666, None)


def test_calculate_byte_ranges_small_file():
    file_size = 10
    workers = 3

    result = calculate_byte_ranges(file_size, workers)

    assert len(result) == 3
    assert result[0] == (0, 3)
    assert result[1] == (3, 6)
    assert result[2] == (6, None)


def test_calculate_byte_ranges_boundary_condition():
    file_size = 0
    workers = 2

    result = calculate_byte_ranges(file_size, workers)

    assert len(result) == 2
    assert result[0] == (0, 0)
    assert result[1] == (0, None)

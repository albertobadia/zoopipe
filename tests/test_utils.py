import pathlib

from zoopipe.utils import get_file_chunk_entrypoints


def test_get_csv_chunk_entrypoints(tmp_path: pathlib.Path):
    csv_file = tmp_path / "test.csv"
    content = b"line1\nline2\nline3\nline4\n"
    csv_file.write_bytes(content)

    chunk_bytes = 7
    entrypoints = get_file_chunk_entrypoints(csv_file, chunk_bytes)

    assert entrypoints == [5, 11, 17, 23]

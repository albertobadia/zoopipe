import pathlib


def get_file_chunk_entrypoints(
    path: pathlib.Path, chunk_bytes: int, separator: bytes = b"\n"
) -> list[int]:
    """
    Finds byte offsets in a file that correspond to the start of a line.

    This utility is used to partition large files into chunks that can be
    processed in parallel, ensuring that each chunk starts at a valid
    record boundary.

    Args:
        path: Path to the file.
        chunk_bytes: Approximate size of each chunk in bytes.
        separator: Record separator (defaults to newline).

    Returns:
        A list of byte offsets for each chunk starting point.
    """
    file_bytes = path.stat().st_size
    current_byte = 0
    entrypoints = []

    with path.open("rb") as f:
        while current_byte < file_bytes:
            f.seek(current_byte)
            line = f.readline()
            if separator in line:
                entrypoints.append(current_byte + len(line.split(separator)[0]))
            current_byte += chunk_bytes

    return entrypoints

import pathlib


def get_file_chunk_entrypoints(
    path: pathlib.Path, chunk_bytes: int, separator: bytes = b"\n"
) -> list[int]:
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

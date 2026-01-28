import pathlib
from typing import List, Optional, Tuple


def shard_file_path(path: str, workers: int) -> List[str]:
    """
    Generate partitioned filenames for output shards.
    e.g. filename.csv -> [filename_part_1.csv, filename_part_2.csv, ...]
    """
    if workers <= 1:
        return [path]

    p = pathlib.Path(path)
    stem = p.stem
    suffix = p.suffix
    parent = p.parent

    return [str(parent / f"{stem}_part_{i + 1}{suffix}") for i in range(workers)]


def calculate_byte_ranges(
    file_size: int, workers: int
) -> List[Tuple[int, Optional[int]]]:
    """
    Calculate start and end byte offsets for splitting a file into shards.
    """
    if workers <= 1:
        return [(0, None)]

    chunk_size = file_size // workers
    ranges = []
    for i in range(workers):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < workers - 1 else None
        ranges.append((start, end))
    return ranges

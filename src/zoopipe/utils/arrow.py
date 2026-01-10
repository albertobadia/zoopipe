import typing

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


def parse_parquet(
    stream: typing.BinaryIO,
) -> typing.Generator[dict[str, typing.Any], None, None]:
    if pq is None:
        raise ImportError("pyarrow is required for parquet parsing")

    table = pq.read_table(stream)
    for row in table.to_pylist():
        yield row


__all__ = ["parse_parquet"]

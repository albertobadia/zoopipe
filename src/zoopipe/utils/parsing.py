import io
import typing

from zoopipe.zoopipe_rust_core import CSVReader as RustCSVReader
from zoopipe.zoopipe_rust_core import JSONReader as RustJSONReader


def parse_csv(
    stream: typing.BinaryIO,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    skip_rows: int = 0,
    fieldnames: list[str] | None = None,
    generate_ids: bool = True,
    **csv_options,
) -> typing.Any:
    if hasattr(stream, "name") and isinstance(stream.name, str):
        reader = RustCSVReader(
            stream.name,
            delimiter=ord(delimiter),
            quote=ord(quotechar),
            skip_rows=skip_rows,
            fieldnames=fieldnames,
            generate_ids=generate_ids,
        )
    else:
        data = stream.read()
        reader = RustCSVReader.from_bytes(
            data,
            delimiter=ord(delimiter),
            quote=ord(quotechar),
            skip_rows=skip_rows,
            fieldnames=fieldnames,
            generate_ids=generate_ids,
        )
    return reader


def parse_json(
    stream: typing.BinaryIO,
    format: str = "array",
    prefix: str = "item",
    encoding: str = "utf-8",
) -> typing.Any:
    if hasattr(stream, "name") and isinstance(stream.name, str):
        reader = RustJSONReader(stream.name)
    else:
        data = stream.read()
        reader = RustJSONReader.from_bytes(data)
    return reader


def parse_content(
    content: typing.Union[bytes, io.BytesIO], file_format: str, **options
) -> typing.Generator[dict[str, typing.Any], None, None]:
    if isinstance(content, bytes):
        stream = io.BytesIO(content)
    else:
        stream = content

    if file_format == "csv":
        yield from parse_csv(stream, **options)
    elif file_format == "json" or file_format == "jsonl":
        fmt = options.get("format", file_format)
        yield from parse_json(stream, format=fmt, **options)
    elif file_format == "parquet":
        from zoopipe.utils.arrow import parse_parquet

        yield from parse_parquet(stream)
    else:
        yield {"content": stream.read()}


def detect_format(filename: str) -> str | None:
    ext = filename.lower().split(".")[-1]
    if ext == "csv":
        return "csv"
    if ext == "json":
        return "json"
    if ext == "jsonl":
        return "jsonl"
    if ext in ["parquet", "pq"]:
        return "parquet"
    return None


__all__ = ["parse_csv", "parse_json", "parse_content", "detect_format"]

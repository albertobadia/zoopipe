import csv
import io
import json
import typing

import ijson


def parse_csv(
    stream: typing.BinaryIO,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    skip_rows: int = 0,
    fieldnames: list[str] | None = None,
    **csv_options,
) -> typing.Generator[dict[str, typing.Any], None, None]:
    text_stream = io.TextIOWrapper(stream, encoding=encoding, newline="")
    for _ in range(skip_rows):
        next(text_stream, None)

    reader = csv.DictReader(
        text_stream,
        delimiter=delimiter,
        quotechar=quotechar,
        fieldnames=fieldnames,
        **csv_options,
    )
    for row in reader:
        yield dict(row)


def parse_json(
    stream: typing.BinaryIO,
    format: str = "array",
    prefix: str = "item",
    encoding: str = "utf-8",
) -> typing.Generator[dict[str, typing.Any], None, None]:
    if format == "jsonl":
        text_stream = io.TextIOWrapper(stream, encoding=encoding)
        for line in text_stream:
            line = line.strip()
            if line:
                yield json.loads(line)
    else:
        items = ijson.items(stream, prefix)
        for item in items:
            if isinstance(item, dict):
                yield item
            else:
                yield {"value": item}


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

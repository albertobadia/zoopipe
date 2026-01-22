import os
import subprocess

from pydantic import BaseModel

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
)

MINIO_CONTAINER = "zoopipe-minio-test"
S3_BUCKET = "test-bucket-compression"


class UserSchema(BaseModel):
    user_id: int
    username: str


def setup_minio():
    """
    Setup MinIO: create bucket and prepare environment.
    """
    # 1. Set alias
    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "alias",
            "set",
            "myminio",
            "http://localhost:9000",
            "zoopipe",
            "zoopipepassword",
        ],
        check=True,
    )

    # 2. Create the bucket (ignore error if already exists)
    subprocess.run(
        ["docker", "exec", MINIO_CONTAINER, "mc", "mb", f"myminio/{S3_BUCKET}"],
        check=False,
    )


def set_aws_env():
    os.environ["AWS_ACCESS_KEY_ID"] = "zoopipe"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "zoopipepassword"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ALLOW_HTTP"] = "true"


def test_s3_read_gzipped_csv(tmp_path):
    setup_minio()
    set_aws_env()

    # 1. Create gzipped csv locally
    import gzip

    local_gz = tmp_path / "users_raw.csv.gz"
    with gzip.open(local_gz, "wt") as f:
        f.write("user_id,username\n1,alice\n2,bob")

    # 2. Copy to container
    subprocess.run(
        ["docker", "cp", str(local_gz), f"{MINIO_CONTAINER}:/tmp/users_raw.csv.gz"],
        check=True,
    )

    # 3. Upload to S3 (bucket)
    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "cp",
            "/tmp/users_raw.csv.gz",
            f"myminio/{S3_BUCKET}/users_raw.csv.gz",
        ],
        check=True,
    )

    # 4. Read with Adapter
    adapter = CSVInputAdapter(f"s3://{S3_BUCKET}/users_raw.csv.gz")
    reader = adapter.get_native_reader()

    items = reader.read_batch(10)
    assert items is not None
    assert len(items) == 2
    assert items[0]["raw_data"]["username"] == "alice"


def test_s3_write_read_roundtrip_zstd(tmp_path):
    setup_minio()
    set_aws_env()

    # 1. Write to S3 using Pipe (Local CSV -> S3 Zstd)
    input_file = tmp_path / "input.csv"
    input_file.write_text("user_id,username\n10,zstd_user\n20,another_one")

    s3_output_uri = f"s3://{S3_BUCKET}/output_roundtrip.csv.zst"

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_file)),
        output_adapter=CSVOutputAdapter(s3_output_uri),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    # 2. Read back from S3
    adapter_in = CSVInputAdapter(s3_output_uri)
    reader = adapter_in.get_native_reader()
    items = reader.read_batch(10)

    assert items is not None
    assert len(items) == 2
    assert items[0]["raw_data"]["username"] == "zstd_user"

import subprocess

import pytest

from zoopipe import CSVInputAdapter

MINIO_CONTAINER = "zoopipe-minio-test"
S3_BUCKET = "test-bucket"


def setup_minio_data():
    """
    Setup MinIO data using the 'mc' tool inside the container.
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
        ["docker", "exec", MINIO_CONTAINER, "mc", "mb", "myminio/test-bucket"],
        check=False,
    )

    # 3. Write data to a temporary file in the container
    csv_content = "user_id,username\n1,alice\n2,bob"
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            MINIO_CONTAINER,
            "sh",
            "-c",
            "cat > /tmp/users_data.csv",
        ],
        input=csv_content.encode(),
        check=True,
    )

    # 4. Copy to MinIO bucket
    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "cp",
            "/tmp/users_data.csv",
            "myminio/test-bucket/users.csv",
        ],
        check=True,
    )


def test_s3_csv_integration():
    """
    Test reading a CSV from MinIO/S3 using CSVInputAdapter.
    """
    setup_minio_data()

    # The S3 URI format expected by CSVInputAdapter
    # Note: We need to ensure the adapter knows how to talk to our local MinIO
    # Typically this involves AWS_ENDPOINT_URL or similar environment variables
    # if the Rust core uses standard AWS SDK logic.

    s3_uri = f"s3://{S3_BUCKET}/users.csv"

    # We might need to set environment variables for the Rust core to find MinIO
    # properly instead of trying to hit real AWS.
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "zoopipe"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "zoopipepassword"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ALLOW_HTTP"] = "true"  # Important for local testing

    adapter = CSVInputAdapter(s3_uri)

    # Since we are using S3, we use the Rust core directly through get_native_reader
    reader = adapter.get_native_reader()

    # We might need to wait a small bit for MinIO to index the file if it's not instant
    # though usually direct writes to /data work well after a restart or if MinIO
    # is watching the filesystem, but MinIO doesn't always watch.
    # If this fails, we might need a restart or use another way.

    # Alternative: use a presigned URL if the adapter
    # supports it (it just uses the string as path)

    try:
        items = reader.read_batch(10)
        assert items is not None
        assert len(items) == 2
        assert items[0]["raw_data"]["username"] == "alice"
        assert items[1]["raw_data"]["username"] == "bob"
    except Exception as e:
        pytest.fail(f"S3 integration failed: {e}")

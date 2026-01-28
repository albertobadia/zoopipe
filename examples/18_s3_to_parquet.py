import time

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe


# Definition of the schema for validation
class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str
    age: int


def main():
    """
    Example demonstrating a Cloud-Native ETL pipeline:
    1. Reads a Zstd-compressed CSV from Amazon S3
    2. Validates data using Pydantic
    3. Writes the output as a Parquet file back to S3

    Note: Requires AWS credentials configured in the environment.
    """

    # ZooPipe automatically handles s3://, gs://, and az:// URIs
    # and transparently decompresses .zst / .gz files.
    input_uri = "s3://my-bucket/raw/users_2026.csv.zst"
    output_uri = "s3://my-bucket/processed/users.parquet"

    pipe = Pipe(
        input_adapter=CSVInputAdapter(input_uri),
        output_adapter=ParquetOutputAdapter(output_uri),
        schema_model=UserSchema,
        # Use MultiThreadExecutor for cloud-native workloads to bypass the GIL
        executor=MultiThreadExecutor(max_workers=8, batch_size=5000),
    )

    print(f"Starting Cloud ETL: {input_uri} -> {output_uri}")
    pipe.start()

    try:
        while not pipe.report.is_finished:
            print(
                f"Processed: {pipe.report.total_processed} | "
                f"Speed: {pipe.report.items_per_second:.2f} rows/s | "
                f"RAM: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB"
            )
            time.sleep(1)
    except KeyboardInterrupt:
        pipe.stop()

    print("\nProcessing complete!")
    print(pipe.report)


if __name__ == "__main__":
    # Note: This is an illustrative example.
    # To run it, ensure you have access to the specified S3 resources.
    # main()
    print("Example 18: Cloud-Native S3 to Parquet with Compression ready.")
    print("Update the URIs in the script and uncomment main() to run.")

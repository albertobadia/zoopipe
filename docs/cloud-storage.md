# Cloud Storage Support

ZooPipe provides seamless cloud storage integration through S3-compatible object storage. All file-based adapters support reading from and writing to Amazon S3 (and S3-compatible services) using simple URI syntax.

## Overview

ZooPipe integrates with cloud storage via the [object_store](https://docs.rs/object_store/) Rust crate, providing:

- **S3 Support**: Direct integration with Amazon S3
- **URI-Based Access**: Use `s3://bucket/path/file` URIs just like local paths
- **Automatic Handling**: No code changes needed beyond URI format
- **Compatible Services**: Works with AWS S3, MinIO, Wasabi, and other S3-compatible services
- **Format Support**: Available for CSV, JSON, Arrow, and Parquet adapters

## Supported Adapters

All file-based adapters support S3 URIs:

- ✅ **CSVInputAdapter** / **CSVOutputAdapter**
- ✅ **JSONInputAdapter** / **JSONOutputAdapter**
- ✅ **ArrowInputAdapter** / **ArrowOutputAdapter**
- ✅ **ParquetInputAdapter** / **ParquetOutputAdapter**

## Configuration

### AWS Credentials

ZooPipe uses standard AWS credential configuration via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_key_here
export AWS_REGION=us-east-1
```

Or use AWS credential files (`~/.aws/credentials`):

```ini
[default]
aws_access_key_id = your_access_key_here
aws_secret_access_key = your_secret_key_here
region = us-east-1
```

### URI Format

S3 URIs follow the standard format:

```
s3://bucket-name/path/to/file.ext
```

Examples:
- `s3://my-data-bucket/raw/users.csv`
- `s3://analytics/processed/2024/01/sales.parquet`
- `s3://exports/customers.jsonl`

## Usage by Adapter Type

### CSV Adapters with S3

#### Reading from S3

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("s3://my-bucket/input/data.csv"),
    output_adapter=CSVOutputAdapter("processed_data.csv"),
)

with pipe:
    pipe.wait()
```

#### Writing to S3

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("local_data.csv"),
    output_adapter=CSVOutputAdapter("s3://my-bucket/output/processed.csv"),
)

with pipe:
    pipe.wait()
```

#### S3 to S3

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, MultiThreadExecutor, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("s3://source-bucket/raw/data.csv"),
    output_adapter=CSVOutputAdapter("s3://dest-bucket/processed/data.csv"),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()
```

### JSON Adapters with S3

#### Reading JSONL from S3

```python
from zoopipe import CSVOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("s3://logs-bucket/app/events.jsonl"),
    output_adapter=CSVOutputAdapter("events.csv"),
)

with pipe:
    pipe.wait()
```

#### Writing JSONL to S3

```python
from zoopipe import CSVInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=JSONOutputAdapter(
        "s3://exports-bucket/users.jsonl",
        format="jsonl"
    ),
)

with pipe:
    pipe.wait()
```

### Parquet Adapters with S3

#### Reading Parquet from S3

```python
from zoopipe import JSONOutputAdapter, ParquetInputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("s3://data-lake/users.parquet"),
    output_adapter=JSONOutputAdapter("users.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

#### Writing Parquet to S3

```python
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=ParquetOutputAdapter("s3://data-lake/processed.parquet"),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()
```

#### Data Lake Pattern

```python
from zoopipe import MultiThreadExecutor, ParquetInputAdapter, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ParquetInputAdapter("s3://raw-data/input.parquet"),
    output_adapter=ParquetOutputAdapter("s3://processed-data/output.parquet"),
    executor=MultiThreadExecutor(max_workers=8, batch_size=5000),
)

with pipe:
    pipe.wait()

print(f"Processed {pipe.report.total_processed} records in data lake")
```

### Arrow Adapters with S3

#### Reading Arrow from S3

```python
from zoopipe import ArrowInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ArrowInputAdapter("s3://analytics/cache/data.arrow"),
    output_adapter=CSVOutputAdapter("local_export.csv"),
)

with pipe:
    pipe.wait()
```

#### Writing Arrow to S3

```python
from zoopipe import ArrowOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("events.jsonl"),
    output_adapter=ArrowOutputAdapter("s3://cache-bucket/events.arrow"),
)

with pipe:
    pipe.wait()
```

## Complete Examples

### ETL Pipeline: Local to S3

```python
import time
from pydantic import BaseModel, ConfigDict, EmailStr
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: EmailStr
    age: int

pipe = Pipe(
    input_adapter=CSVInputAdapter("local_users.csv"),
    output_adapter=ParquetOutputAdapter("s3://data-warehouse/users.parquet"),
    error_output_adapter=ParquetOutputAdapter("s3://data-warehouse/errors.parquet"),
    schema_model=UserSchema,
    executor=MultiThreadExecutor(max_workers=8),
)

pipe.start()

while not pipe.report.is_finished:
    print(
        f"Processed: {pipe.report.total_processed} | "
        f"Speed: {pipe.report.items_per_second:.2f} rows/s"
    )
    time.sleep(0.5)

print(f"Successfully uploaded {pipe.report.total_processed} records to S3")
```

### Data Lake Export: Database to S3

```python
from zoopipe import MultiThreadExecutor, ParquetOutputAdapter, Pipe, SQLInputAdapter

tables = ['users', 'orders', 'products', 'transactions']

for table in tables:
    pipe = Pipe(
        input_adapter=SQLInputAdapter(
            "postgresql://user:pass@localhost/production",
            table_name=table
        ),
        output_adapter=ParquetOutputAdapter(
            f"s3://data-lake/raw/{table}.parquet"
        ),
        executor=MultiThreadExecutor(max_workers=8),
    )
    
    with pipe:
        pipe.wait()
    
    print(f"Exported {table} to data lake")
```

### S3 to S3 Transformation

```python
from pydantic import BaseModel, field_validator
from zoopipe import MultiThreadExecutor, ParquetInputAdapter, ParquetOutputAdapter, Pipe

class SalesRecord(BaseModel):
    order_id: str
    revenue: float
    
    @field_validator('revenue')
    def positive_revenue(cls, v):
        if v <= 0:
            raise ValueError("Revenue must be positive")
        return v

pipe = Pipe(
    input_adapter=ParquetInputAdapter("s3://raw-data/sales.parquet"),
    output_adapter=ParquetOutputAdapter("s3://clean-data/sales.parquet"),
    error_output_adapter=ParquetOutputAdapter("s3://errors/sales_errors.parquet"),
    schema_model=SalesRecord,
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()

print(f"Processed: {pipe.report.total_processed}")
print(f"Errors: {pipe.report.total_errors}")
```

### Multi-Format Cloud Pipeline

```python
from zoopipe import JSONInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("s3://logs/app-events.jsonl"),
    output_adapter=ParquetOutputAdapter("s3://analytics/events.parquet"),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()

print("Converted JSONL logs to Parquet analytics format in S3")
```

## Best Practices

### Performance

1. **Use Parquet for S3**: Columnar format minimizes data transfer and storage costs
2. **Multi-Threading**: Always use `MultiThreadExecutor` for cloud operations
3. **Batch Size**: Larger batches (5000-10000) reduce network round-trips
4. **Choose the Right Region**: Ensure your compute and S3 bucket are in the same AWS region
5. **Compression**: Parquet and Arrow automatically compress, reducing transfer time

### Cost Optimization

1. **Parquet over CSV/JSON**: 5-10x smaller files = lower storage and transfer costs
2. **Batch Processing**: Process multiple files in one session to amortize connection overhead
3. **Same Region**: Avoid cross-region transfer fees
4. **Lifecycle Policies**: Use S3 lifecycle policies to archive old data
5. **Monitor Transfer**: Large pipelines benefit from monitoring data transfer costs

### Security

1. **IAM Roles**: Prefer IAM roles over access keys when running on EC2/ECS
2. **Least Privilege**: Grant only necessary S3 permissions (s3:GetObject, s3:PutObject)
3. **Bucket Policies**: Use bucket policies to restrict access
4. **Encryption**: Enable S3 encryption at rest (SSE-S3, SSE-KMS)
5. **VPC Endpoints**: Use VPC endpoints for private S3 access from AWS

### Error Handling

1. **Network Retries**: S3 operations automatically retry on transient failures
2. **Error Output**: Always use `error_output_adapter` for production pipelines
3. **Monitor Failures**: Check `pipe.report.total_errors` after completion
4. **Separate Error Buckets**: Write errors to different S3 bucket for analysis

## Common Patterns

### Daily Batch Processing

```python
from datetime import datetime
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

today = datetime.now().strftime("%Y-%m-%d")

pipe = Pipe(
    input_adapter=CSVInputAdapter(f"s3://raw-data/logs/{today}.csv"),
    output_adapter=ParquetOutputAdapter(f"s3://processed/logs/{today}.parquet"),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()
```

### Multi-Source Consolidation

```python
from pathlib import Path
from zoopipe import CSVInputAdapter, MultiThreadExecutor, ParquetOutputAdapter, Pipe

sources = [
    "s3://source-a/data.csv",
    "s3://source-b/data.csv",
    "s3://source-c/data.csv",
]

for i, source in enumerate(sources):
    pipe = Pipe(
        input_adapter=CSVInputAdapter(source),
        output_adapter=ParquetOutputAdapter(
            f"s3://consolidated/part_{i:03d}.parquet"
        ),
        executor=MultiThreadExecutor(max_workers=4),
    )
    
    with pipe:
        pipe.wait()
    
    print(f"Processed source {i+1}/{len(sources)}")
```

### Versioned Data Lake

```python
from datetime import datetime
from zoopipe import MultiThreadExecutor, ParquetOutputAdapter, Pipe, SQLInputAdapter

version = datetime.now().strftime("%Y%m%d_%H%M%S")

pipe = Pipe(
    input_adapter=SQLInputAdapter(
        "postgresql://user:pass@localhost/db",
        table_name="users"
    ),
    output_adapter=ParquetOutputAdapter(
        f"s3://data-lake/users/v_{version}/data.parquet"
    ),
    executor=MultiThreadExecutor(max_workers=8),
)

with pipe:
    pipe.wait()

print(f"Created version {version} in data lake")
```

## S3-Compatible Services

ZooPipe works with any S3-compatible service by configuring the endpoint:

### MinIO

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1
```

### Wasabi

```bash
export AWS_ACCESS_KEY_ID=your_wasabi_key
export AWS_SECRET_ACCESS_KEY=your_wasabi_secret
export AWS_ENDPOINT_URL=https://s3.wasabisys.com
export AWS_REGION=us-east-1
```

### DigitalOcean Spaces

```bash
export AWS_ACCESS_KEY_ID=your_spaces_key
export AWS_SECRET_ACCESS_KEY=your_spaces_secret
export AWS_ENDPOINT_URL=https://nyc3.digitaloceanspaces.com
export AWS_REGION=us-east-1
```

## Troubleshooting

### Common Errors

#### Access Denied

```
Error: Access Denied (403)
```

**Solution**: Check AWS credentials and S3 bucket permissions

```bash
aws s3 ls s3://your-bucket/
```

#### Bucket Not Found

```
Error: NoSuchBucket
```

**Solution**: Verify bucket name and region

```bash
aws s3 mb s3://your-bucket --region us-east-1
```

#### Connection Timeout

```
Error: Connection timeout
```

**Solution**: Check network connectivity and region configuration

```bash
export AWS_REGION=us-east-1
```

### Debugging

Enable detailed logging to debug S3 issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("s3://my-bucket/data.csv"),
    output_adapter=CSVOutputAdapter("output.csv"),
)

with pipe:
    pipe.wait()
```

### Testing S3 Access

Test S3 credentials before running pipelines:

```bash
aws s3 ls s3://your-bucket/

aws s3 cp test.txt s3://your-bucket/test.txt

aws s3 rm s3://your-bucket/test.txt
```

## Cost Estimation

Approximate AWS S3 costs (us-east-1, as of 2024):

- **Storage**: $0.023/GB/month
- **PUT Requests**: $0.005 per 1,000 requests
- **GET Requests**: $0.0004 per 1,000 requests
- **Data Transfer Out**: $0.09/GB (first 10TB)

**Example**: Processing 1TB CSV → Parquet daily:
- Input: 1TB CSV = $23/month storage
- Output: ~100GB Parquet = $2.30/month storage
- Savings: ~$20/month + faster queries + lower transfer costs

## Security Checklist

- [ ] Use IAM roles instead of access keys when possible
- [ ] Enable S3 bucket versioning for critical data
- [ ] Enable S3 server-side encryption (SSE-S3 or SSE-KMS)
- [ ] Configure S3 bucket policies with least privilege
- [ ] Use VPC endpoints for private S3 access
- [ ] Enable S3 access logging for audit trails
- [ ] Rotate access keys regularly
- [ ] Use separate buckets for raw, processed, and error data
- [ ] Enable MFA delete for critical buckets
- [ ] Monitor S3 access with CloudTrail

## Next Steps

- Review [Parquet Adapters](parquet.md) for best cloud storage format
- Review [Arrow Adapters](arrow.md) for high-performance temporary storage
- Review [CSV Adapters](csv.md) for source data ingestion
- Review [JSON Adapters](json.md) for log processing

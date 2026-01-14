use std::sync::Arc;
use object_store::{ObjectStore, local::LocalFileSystem, aws::AmazonS3Builder};
use url::Url;
use crate::error::PipeError;

pub struct StorageController {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl StorageController {
    pub fn new(path: &str) -> Result<Self, PipeError> {
        if path.starts_with("s3://") {
            let url = Url::parse(path).map_err(|e| PipeError::Other(e.to_string()))?;
            let bucket = url.host_str().ok_or_else(|| PipeError::Other("Invalid S3 bucket".into()))?;
            
            let builder = AmazonS3Builder::from_env()
                .with_bucket_name(bucket);
            
            let s3 = builder.build().map_err(|e| PipeError::Other(e.to_string()))?;
            
            Ok(Self {
                inner: Arc::new(s3),
                prefix: url.path().trim_start_matches('/').to_string(),
            })
        } else {
            let local = LocalFileSystem::new();
            Ok(Self {
                inner: Arc::new(local),
                prefix: path.to_string(),
            })
        }
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.inner.clone()
    }

    pub fn path(&self) -> &str {
        &self.prefix
    }
}

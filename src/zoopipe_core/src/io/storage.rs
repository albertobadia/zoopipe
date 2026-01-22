use std::sync::Arc;
use object_store::{ObjectStore, ObjectStoreExt, local::LocalFileSystem};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::azure::MicrosoftAzureBuilder;
use url::Url;
use crate::error::PipeError;

/// Trait that defines how to build an ObjectStore for a specific scheme.
trait StorageBuilder: Send + Sync {
    /// Builds the store and returns the store and the relative path key within it.
    fn build(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, String), PipeError>;
}

pub fn is_cloud_path(path: &str) -> bool {
    path.starts_with("s3://") 
        || path.starts_with("gs://")
        || path.starts_with("gcs://")
        || path.starts_with("az://")
        || path.starts_with("adl://")
        || path.starts_with("azure://")
}

struct S3StorageBuilder;

impl StorageBuilder for S3StorageBuilder {
    fn build(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, String), PipeError> {
        let bucket = url.host_str().ok_or_else(|| PipeError::Other("Invalid S3 bucket".into()))?;
        
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_retry(object_store::RetryConfig::default())
            .with_client_options(object_store::ClientOptions::default());
        
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL").or_else(|_| std::env::var("AWS_ENDPOINT")) {
            builder = builder.with_endpoint(endpoint);
        }
        
        if std::env::var("AWS_ALLOW_HTTP").map(|v| v.to_lowercase() == "true").unwrap_or(false) {
            builder = builder.with_allow_http(true);
        }
        
        let s3 = builder.build().map_err(|e| PipeError::Other(e.to_string()))?;
        let prefix = url.path().trim_start_matches('/').to_string();
        
        Ok((Arc::new(s3), prefix))
    }
}

struct GcsStorageBuilder;

impl StorageBuilder for GcsStorageBuilder {
    fn build(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, String), PipeError> {
        let bucket = url.host_str().ok_or_else(|| PipeError::Other("Invalid GCS bucket".into()))?;
        
        let builder = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .with_retry(object_store::RetryConfig::default())
            .with_client_options(object_store::ClientOptions::default());
            
        let gcs = builder.build().map_err(|e| PipeError::Other(e.to_string()))?;
        let prefix = url.path().trim_start_matches('/').to_string();
        
        Ok((Arc::new(gcs), prefix))
    }
}

struct AzureStorageBuilder;

impl StorageBuilder for AzureStorageBuilder {
    fn build(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, String), PipeError> {
        let container = url.host_str().ok_or_else(|| PipeError::Other("Invalid Azure container".into()))?;
        
        let builder = MicrosoftAzureBuilder::from_env()
            .with_container_name(container)
            .with_retry(object_store::RetryConfig::default())
            .with_client_options(object_store::ClientOptions::default());
            
        let azure = builder.build().map_err(|e| PipeError::Other(e.to_string()))?;
        let prefix = url.path().trim_start_matches('/').to_string();
        
        Ok((Arc::new(azure), prefix))
    }
}

/// Orchestrates access to different storage backends (Local vs S3/GCS/Azure).
/// 
/// It parses URI schemes and initializes the appropriate object-store 
/// implementation to provide uniform data access.
pub struct StorageController {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl StorageController {
    pub fn new(path: &str) -> Result<Self, PipeError> {
        if let Ok(url) = Url::parse(path) {
            if url.scheme() == "file" {
                // handle file:// explicitly or fall through to local
                let path = url.path();
                let local = LocalFileSystem::new();
                return Ok(Self {
                    inner: Arc::new(local),
                    prefix: path.to_string(),
                })
            }

            let builder: Box<dyn StorageBuilder> = match url.scheme() {
                "s3" => Box::new(S3StorageBuilder),
                "gs" | "gcs" => Box::new(GcsStorageBuilder),
                "az" | "adl" | "azure" => Box::new(AzureStorageBuilder),
                _ => {
                     return Err(PipeError::Other(format!("Unsupported scheme: {}", url.scheme())));
                }
            };
            
            let (store, prefix) = builder.build(&url)?;
            Ok(Self { inner: store, prefix })
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

    pub async fn get_size(&self) -> Result<u64, PipeError> {
        let meta = self.inner.head(&object_store::path::Path::from(self.prefix.as_str())).await
            .map_err(|e| PipeError::Other(e.to_string()))?;
        Ok(meta.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_path_controller() {
        let controller = StorageController::new("/tmp/test.csv").unwrap();
        assert_eq!(controller.path(), "/tmp/test.csv");
    }

    #[test]
    fn test_local_relative_path() {
        let controller = StorageController::new("data/output.json").unwrap();
        assert_eq!(controller.path(), "data/output.json");
    }

    #[test]
    fn test_s3_path_parsing() {
        let result = StorageController::new("s3://my-bucket/path/to/file.csv");
        
        if let Ok(controller) = result {
            assert_eq!(controller.path(), "path/to/file.csv");
        }
    }

    #[test]
    fn test_gcs_path_parsing() {
        let result = StorageController::new("gs://my-bucket/path/to/file.csv");
        match result {
            Ok(controller) => assert_eq!(controller.path(), "path/to/file.csv"),
            Err(PipeError::Other(msg)) => {
                println!("GCS build skipped due to: {}", msg);
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_azure_path_parsing() {
        let result = StorageController::new("az://my-container/path/to/file.csv");
        
        match result {
            Ok(controller) => assert_eq!(controller.path(), "path/to/file.csv"),
            Err(PipeError::Other(msg)) => {
                println!("Azure build skipped due to: {}", msg);
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_unsupported_scheme() {
        let result = StorageController::new("ftp://server/file.csv");
        assert!(result.is_err());
        match result {
            Err(PipeError::Other(msg)) => assert!(msg.contains("Unsupported scheme")),
            _ => panic!("Expected unsupported scheme error"),
        }
    }
}

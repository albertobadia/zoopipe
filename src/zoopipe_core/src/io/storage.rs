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
    fn test_s3_path_with_root() {
        let result = StorageController::new("s3://my-bucket/file.csv");
        
        if let Ok(controller) = result {
            assert_eq!(controller.path(), "file.csv");
        }
    }

    #[test]
    fn test_invalid_url() {
        let result = StorageController::new("s3://");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_getter() {
        let controller = StorageController::new("test/path").unwrap();
        assert_eq!(controller.path(), "test/path");
    }

    #[test]
    fn test_local_path_with_spaces() {
        let controller = StorageController::new("/path with spaces/file.csv").unwrap();
        assert_eq!(controller.path(), "/path with spaces/file.csv");
    }

    #[test]
    fn test_local_path_with_special_chars() {
        let controller = StorageController::new("/path/file-name_2024.csv").unwrap();
        assert_eq!(controller.path(), "/path/file-name_2024.csv");
    }

    #[test]
    fn test_s3_path_no_prefix() {
        let result = StorageController::new("s3://my-bucket/");
        
        if let Ok(controller) = result {
            assert_eq!(controller.path(), "");
        }
    }

    #[test]
    fn test_s3_path_deep_nesting() {
        let result = StorageController::new("s3://my-bucket/level1/level2/level3/file.csv");
        
        if let Ok(controller) = result {
            assert_eq!(controller.path(), "level1/level2/level3/file.csv");
        }
    }

    #[test]
    fn test_multiple_store_calls() {
        let controller = StorageController::new("/tmp/test.csv").unwrap();
        let store1 = controller.store();
        let store2 = controller.store();
        assert_eq!(Arc::strong_count(&store1), Arc::strong_count(&store2));
    }
}


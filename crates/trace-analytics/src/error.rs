use thiserror::Error;

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("ObjectStore error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

pub type Result<T> = std::result::Result<T, TraceError>;

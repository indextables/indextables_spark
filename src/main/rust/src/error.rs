use thiserror::Error;

#[derive(Error, Debug)]
pub enum TantivyError {
    #[error("Tantivy core error: {0}")]
    Core(#[from] tantivy::TantivyError),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Query error: {0}")]
    QueryError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
}
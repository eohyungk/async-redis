use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),
    
    #[error("Stream processing error: {0}")]
    Processing(String),
}

pub type Result<T> = std::result::Result<T, StreamError>;

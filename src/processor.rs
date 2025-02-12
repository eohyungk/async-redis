use async_trait::async_trait;
use redis::streams::StreamId;
use tracing::{info, instrument};

use crate::error::Result;

#[async_trait]
pub trait StreamProcessor: Send + Sync {
    async fn process_message(&self, stream_id: &str, message: StreamId) -> Result<()>;
}

pub struct BasicProcessor;

#[async_trait]
impl StreamProcessor for BasicProcessor {
    #[instrument(skip(self))]
    async fn process_message(&self, stream_id: &str, message: StreamId) -> Result<()> {
        info!(
            stream_id = %stream_id,
            message_id = %message.id,
            "Processing stream message"
        );
        
        for (key, value) in message.map.iter() {
            let value: String = redis::from_redis_value(value)?;
            info!(field = %key, value = %value, "Message field");
        }
        
        Ok(())
    }
}
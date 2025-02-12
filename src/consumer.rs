// src/consumer.rs
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use tracing::{info, error, instrument};

use crate::error::Result;
use crate::processor::StreamProcessor;
use crate::config::StreamSettings;

pub struct StreamConsumer<P: StreamProcessor> {
    client: redis::Client,
    settings: StreamSettings,
    processor: P,
}

impl<P: StreamProcessor> StreamConsumer<P> {
    pub fn new(client: redis::Client, settings: StreamSettings, processor: P) -> Self {
        Self {
            client,
            settings,
            processor,
        }
    }
    
    #[instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        
        info!(
            stream = %self.settings.name,
            batch_size = %self.settings.batch_size,
            "Starting stream consumer"
        );
        
        loop {
            let batch_size = self.settings.batch_size as usize;
            let block_ms = self.settings.block_ms as usize;
            let opts = StreamReadOptions::default()
                .count(batch_size)
                .block(block_ms);

            match con.xread_options::<&str, &str, StreamReadReply>(
                &[self.settings.name.as_str()],
                &["$"],                          
                &opts
            ).await {
                Ok(reply) => {
                    for stream_key in reply.keys {
                        for message in stream_key.ids {
                            if let Err(e) = self.processor.process_message(&stream_key.key, message).await {
                                error!(error = %e, "Error processing message");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error reading from stream");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
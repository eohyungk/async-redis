use tokio::time::sleep;
use std::time::Duration;
use redis::AsyncCommands;
use redis_stream_demo::{Settings, StreamConsumer, BasicProcessor, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize consumer
    let settings = Settings::new()?;
    let client = redis::Client::open(settings.redis.url.clone())?;
    let consumer = StreamConsumer::new(
        client.clone(),
        settings.stream.clone(),
        BasicProcessor
    );

    // Start consumer in background
    let _consumer = tokio::spawn(async move {
        consumer.run().await
    });

    // Produce messages
    let mut con = client.get_multiplexed_async_connection().await?;
    for i in 1..=5 {
        let _: String = con.xadd(
            "my_stream",
            "*",
            &[("message", format!("Test message {}", i))],
        ).await?;
        sleep(Duration::from_secs(1)).await;
    }

    sleep(Duration::from_secs(5)).await;
    Ok(())
}
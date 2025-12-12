use redis::AsyncCommands;
use futures_util::StreamExt;

pub struct RedisLogPublisher {
    client: redis::Client,
}

impl RedisLogPublisher {
    pub fn new(url: &str) -> Self {
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        Self { client }
    }

    pub async fn publish(&self, job_id: &str, message: &str) -> Result<(), redis::RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let channel = format!("job:{}:logs", job_id);
        // Publish returns the number of clients that received the message
        let _: i32 = conn.publish(channel, message).await?;
        Ok(())
    }

    pub async fn subscribe(&self, job_id: &str) -> Result<impl futures_util::Stream<Item = String>, redis::RedisError> {
        let conn = self.client.get_async_pubsub().await?; 
        let mut pubsub = conn;
        let channel = format!("job:{}:logs", job_id);
        
        pubsub.subscribe(channel).await?;
        
        let stream = pubsub.into_on_message().map(|msg| {
            let payload: String = msg.get_payload().unwrap_or_default();
            payload
        });

        Ok(stream)
    }
}

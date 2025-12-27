use redis::AsyncCommands;
use futures_util::StreamExt;

#[derive(Clone)]
pub struct RedisLogPublisher {
    client: redis::Client,
}

impl RedisLogPublisher {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    pub async fn publish(&self, job_id: &str, message: &str) -> Result<(), redis::RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("job:{}:logs:stream", job_id);
        
        // Use Redis Streams (XADD) with MAXLEN to prevent infinite growth
        // MAXLEN ~ 10000 log lines or handled by TTL elsewhere. 
        // Here we just limit to 10000 to be safe.
        let _: String = conn.xadd_maxlen(
            &key, 
            redis::streams::StreamMaxlen::Approx(10000), 
            "*", 
            &[("data", message)]
        ).await?;
        
        // Set TTL on the stream key so it expires eventually (e.g., 24h)
        let _: () = conn.expire(&key, 86400).await?;

        Ok(())
    }

    pub async fn subscribe(
        &self, 
        job_id: &str, 
        last_id: Option<String>
    ) -> Result<impl futures_util::Stream<Item = Result<(String, String), redis::RedisError>>, redis::RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("job:{}:logs:stream", job_id);
        let start_id = last_id.unwrap_or_else(|| "0-0".to_string());

        let stream = async_stream::try_stream! {
            let mut current_id = start_id;
            loop {
                // POLLING MODE: Safe for Multiplexed Connections
                // We do NOT use .block() here to avoid deadlocking the shared connection.
                let opts = redis::streams::StreamReadOptions::default()
                    .count(10);

                let result: Option<redis::streams::StreamReadReply> = conn.xread_options(
                    &[&key], 
                    &[&current_id], 
                    &opts
                ).await?;

                let mut had_data = false;
                if let Some(reply) = result {
                    for key_res in reply.keys {
                        for id in key_res.ids {
                            had_data = true;
                            current_id = id.id.clone(); // Update cursor
                            if let Some(val) = id.map.get("data") {
                                if let redis::Value::BulkString(bytes) = val {
                                    let msg = String::from_utf8_lossy(bytes).to_string();
                                    yield (current_id.clone(), msg);
                                } else if let redis::Value::SimpleString(s) = val {
                                     yield (current_id.clone(), s.clone());
                                }
                            }
                        }
                    }
                }
                
                if !had_data {
                    // Sleep to prevent tight loop if no data
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        };

        Ok(stream)
    }
}

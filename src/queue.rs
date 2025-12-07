use async_trait::async_trait;
use redis::AsyncCommands;
use crate::models::JobContext;
use tracing::{info, warn};

// FIX: Added `+ Send + Sync` to the Boxed Error.
// This is critical for tokio::spawn to accept the Result containing this error across threads.
#[async_trait]
pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, job: JobContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn dequeue(&self) -> Result<Option<JobContext>, Box<dyn std::error::Error + Send + Sync>>;
    async fn acknowledge(&self, job: &JobContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn restore_stranded(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>;
}

// --- REDIS IMPLEMENTATION ---
pub struct RedisQueue {
    client: redis::Client,
    queue_name: String,
    processing_queue_name: String,
}

impl RedisQueue {
    pub fn new(url: &str) -> Self {
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        Self {
            client,
            queue_name: "zorp_jobs".to_string(),
            processing_queue_name: "zorp_jobs:processing".to_string(),
        }
    }
}

#[async_trait]
impl JobQueue for RedisQueue {
    async fn enqueue(&self, job: JobContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let payload = serde_json::to_string(&job)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        // LPUSH: Push to the left side of the list
        let _: () = conn.lpush(&self.queue_name, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<JobContext>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        // Reliable Queue: BRPOPLPUSH source destination timeout
        // Moves item from tail of source to head of destination safely.
        let payload: Option<String> = conn.brpoplpush(&self.queue_name, &self.processing_queue_name, 0.0).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        match payload {
            Some(payload_str) => {
                let job: JobContext = serde_json::from_str(&payload_str)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn acknowledge(&self, job: &JobContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
         let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
         // We must use the exact string representation to remove it.
         let payload = serde_json::to_string(job)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
         // LREM key count value. count > 0: Remove elements equal to value moving from head to tail.
         let removed: i64 = conn.lrem(&self.processing_queue_name, 1, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
         if removed == 0 {
             warn!("Could not acknowledge job {}. It might have been already removed or modified.", job.id);
         }
         
         Ok(())
    }

    async fn restore_stranded(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let mut count = 0;
        loop {
            // RPOPLPUSH from processing back to queue.
            // Returns the element being popped. If None, list is empty.
            let item: Option<String> = conn.rpoplpush(&self.processing_queue_name, &self.queue_name).await
                 .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
            match item {
                Some(_) => count += 1,
                None => break,
            }
        }
        
        if count > 0 {
            info!("Restored {} stranded jobs from '{}' to '{}'", count, self.processing_queue_name, self.queue_name);
        }
        
        Ok(count)
    }
}

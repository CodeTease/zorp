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
    async fn update_heartbeat(&self, job_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn monitor_stranded_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>;
    async fn ping(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// --- REDIS IMPLEMENTATION ---
pub struct RedisQueue {
    client: redis::Client,
    queue_name: String, // Default/Normal queue
    queue_high: String,
    queue_low: String,
    processing_queue_name: String,
}

impl RedisQueue {
    pub fn new(url: &str) -> Self {
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        Self {
            client,
            queue_name: "zorp_jobs_normal".to_string(),
            queue_high: "zorp_jobs_high".to_string(),
            queue_low: "zorp_jobs_low".to_string(),
            processing_queue_name: "zorp_jobs:processing".to_string(),
        }
    }

    fn get_queue_for_priority(&self, priority: Option<&str>) -> &str {
        match priority {
            Some("high") | Some("HIGH") => &self.queue_high,
            Some("low") | Some("LOW") => &self.queue_low,
            _ => &self.queue_name,
        }
    }

    async fn process_payload(&self, payload: Option<String>, conn: &mut redis::aio::MultiplexedConnection) 
        -> Result<Option<JobContext>, Box<dyn std::error::Error + Send + Sync>> 
    {
        match payload {
            Some(payload_str) => {
                let job: JobContext = serde_json::from_str(&payload_str)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                
                // Set initial heartbeat immediately to prevent race with monitor
                let key = format!("zorp:heartbeat:{}", job.id);
                let _: () = conn.set_ex(key, "1", 30).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                Ok(Some(job))
            }
            None => Ok(None),
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
        
        let target_queue = self.get_queue_for_priority(job.priority.as_deref());

        // LPUSH: Push to the left side of the list
        let _: () = conn.lpush(target_queue, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<JobContext>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        // Priority Dequeue Logic:
        // 1. Check HIGH (Non-blocking rpoplpush)
        // 2. Check NORMAL (Non-blocking rpoplpush)
        // 3. Check LOW (Blocking brpoplpush with short timeout to circle back to HIGH)

        // Try HIGH
        // Note: rpoplpush is not available in redis crate async commands directly as a method sometimes?
        // Checking docs/usage: cmd("RPOPLPUSH").arg(src).arg(dst)
        let payload: Option<String> = redis::cmd("RPOPLPUSH")
            .arg(&self.queue_high)
            .arg(&self.processing_queue_name)
            .query_async(&mut conn).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if payload.is_some() {
             return self.process_payload(payload, &mut conn).await;
        }

        // Try NORMAL
        let payload: Option<String> = redis::cmd("RPOPLPUSH")
            .arg(&self.queue_name)
            .arg(&self.processing_queue_name)
            .query_async(&mut conn).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if payload.is_some() {
             return self.process_payload(payload, &mut conn).await;
        }

        // Try LOW (Blocking with timeout 1s)
        let payload: Option<String> = conn.brpoplpush(&self.queue_low, &self.processing_queue_name, 1.0).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        self.process_payload(payload, &mut conn).await
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
        let dlq_name = format!("{}:dlq", self.queue_name);

        loop {
            // 1. Pop from processing queue (simulating RPOPLPUSH but with inspection)
            // Note: redis::AsyncCommands::rpop takes 2 args in 0.32 (key, count)
            let item: Option<String> = conn.rpop(&self.processing_queue_name, None).await
                 .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            match item {
                Some(payload) => {
                    let mut job: JobContext = match serde_json::from_str(&payload) {
                        Ok(j) => j,
                        Err(e) => {
                            warn!("Poison Pill found! Failed to parse stranded job: {}. Moving to DLQ.", e);
                            let _: () = conn.lpush(&dlq_name, &payload).await
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                            continue;
                        }
                    };

                    job.retry_count += 1;
                    
                    if job.retry_count > 3 {
                         warn!("Job {} exceeded max retries ({}). Moving to DLQ.", job.id, job.retry_count);
                         let new_payload = serde_json::to_string(&job).unwrap();
                         let _: () = conn.lpush(&dlq_name, new_payload).await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    } else {
                         let new_payload = serde_json::to_string(&job).unwrap();
                         // Push back to appropriate queue based on priority
                         let target_queue = self.get_queue_for_priority(job.priority.as_deref());
                         let _: () = conn.lpush(target_queue, new_payload).await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                         count += 1;
                    }
                },
                None => break, // List is empty
            }
        }
        
        if count > 0 {
            info!("Restored {} stranded jobs from '{}'", count, self.processing_queue_name);
        }
        
        Ok(count)
    }

    async fn update_heartbeat(&self, job_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let key = format!("zorp:heartbeat:{}", job_id);
        // Set heartbeat with 30s expiration
        let _: () = conn.set_ex(key, "1", 30).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        Ok(())
    }

    async fn monitor_stranded_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Get all processing jobs
        let jobs: Vec<String> = conn.lrange(&self.processing_queue_name, 0, -1).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut restored_count = 0;

        for payload in jobs {
            let job: JobContext = match serde_json::from_str(&payload) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to parse job in processing queue: {}. Skipping.", e);
                    continue;
                }
            };

            let key = format!("zorp:heartbeat:{}", job.id);
            let exists: bool = conn.exists(&key).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            if !exists {
                info!("Heartbeat missing for job {}. Restoring to queue.", job.id);
                
                // Transactional move: Remove from processing and push to queue
                // LREM removes matching 'payload'.
                let removed: i64 = conn.lrem(&self.processing_queue_name, 1, &payload).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                if removed > 0 {
                    let target_queue = self.get_queue_for_priority(job.priority.as_deref());
                    let _: () = conn.lpush(target_queue, &payload).await
                         .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    restored_count += 1;
                }
            }
        }
        
        if restored_count > 0 {
            info!("Monitor: Restored {} stranded jobs.", restored_count);
        }

        Ok(restored_count)
    }

    async fn ping(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let _: String = redis::cmd("PING").query_async(&mut conn).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        Ok(())
    }
}

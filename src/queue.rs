use async_trait::async_trait;
use redis::AsyncCommands;
use crate::models::{JobContext, UploadTask};
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
    
    // Upload Queue
    async fn enqueue_upload(&self, task: UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn enqueue_upload_dlq(&self, task: UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn dequeue_upload(&self) -> Result<Option<UploadTask>, Box<dyn std::error::Error + Send + Sync>>;
}

// --- REDIS IMPLEMENTATION ---
pub struct RedisQueue {
    client: redis::Client,
    queue_name: String, // Default/Normal queue
    queue_high: String,
    queue_low: String,
    processing_queue_name: String,
    upload_queue_name: String,
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
            upload_queue_name: "zorp_uploads".to_string(),
        }
    }

    async fn enqueue_upload_dlq(&self, task: UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let payload = serde_json::to_string(&task)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let dlq_name = format!("{}:dlq", self.upload_queue_name);
        
        let _: () = conn.lpush(&dlq_name, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
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
                
                // 1. Store payload for retrieval by monitor (if it expires)
                let payload_key = format!("zorp:job_payload:{}", job.id);
                let _: () = conn.set(&payload_key, &payload_str).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // 2. Set initial heartbeat (ZSET)
                let now = chrono::Utc::now().timestamp();
                let score = now + 30;
                let _: () = conn.zadd("zorp:heartbeats", &job.id, score).await
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
    
    async fn enqueue_upload_dlq(&self, task: UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let payload = serde_json::to_string(&task)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let dlq_name = format!("{}:dlq", self.upload_queue_name);
        
        let _: () = conn.lpush(&dlq_name, payload).await
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
            
         // 1. Remove from processing list
         let removed: i64 = conn.lrem(&self.processing_queue_name, 1, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
         // 2. Remove heartbeat (ZSET)
         let _: () = conn.zrem("zorp:heartbeats", &job.id).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

         // 3. Remove sidecar payload
         let payload_key = format!("zorp:job_payload:{}", job.id);
         let _: () = conn.del(&payload_key).await
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

        // Use ZSET for efficient range queries (O(log N))
        let now = chrono::Utc::now().timestamp();
        // Expiration = now + 30s
        let score = now + 30;
        let key = "zorp:heartbeats";
        
        let _: () = conn.zadd(key, job_id, score).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        Ok(())
    }

    async fn monitor_stranded_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        info!("❤️  Monitor: Checking for stranded jobs (Heartbeat Check)...");
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let now = chrono::Utc::now().timestamp();
        let key = "zorp:heartbeats";
        let dlq_name = format!("{}:dlq", self.queue_name);

        // Get expired jobs: score < now
        // ZRANGEBYSCORE key -inf (now - 1)
        let expired_job_ids: Vec<String> = conn.zrangebyscore(key, "-inf", now).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut restored_count = 0;

        for job_id in expired_job_ids {
            // Find the job payload in processing queue? 
            // The processing queue is a List. Searching a list is O(N).
            // However, we need the payload to restore it. 
            // Optimization: Store job payload in a separate HASH or assume we can find it?
            // If we don't have the payload, we can't restore it easily unless we stored it elsewhere.
            // But wait, the previous implementation did `lrange` (O(N)) and checked heartbeat for each.
            // Now we have the ID of the expired job. We need to find its payload in the `processing` list and remove it.
            // `LREM` removes by value (payload). We don't have the payload, only ID.
            
            // To make this O(1) or O(log N), we should have stored the payload in a K/V sidecar 
            // OR we iterate the processing queue (which is what we wanted to avoid).
            
            // Alternative: The `processing` queue isn't strictly needed if we use the ZSET as the "processing" state?
            // But `processing` list is useful for "at least once" if Redis crashes (persisted list).
            // A common pattern is `RPOPLPUSH source processing`. 
            
            // If we want to avoid O(N) scan of the list, we can keep a side mapping: job_id -> payload.
            // Let's implement that: When processing, set `zorp:job:{id}` -> payload.
            // Then we can retrieve it here.
            
            // But for now, let's look at `process_payload` in `RedisQueue` where we set the initial heartbeat.
            // We should also set the sidecar key there?
            // Existing `process_payload` does: `conn.set_ex(key, "1", 30)`.
            // We can change that to `conn.set(format!("zorp:job_payload:{}", job.id), payload_str)`.
            
            // Let's try to find the payload using the sidecar key approach.
            // Assuming we will modify `process_payload` next.
            
            let payload_key = format!("zorp:job_payload:{}", job_id);
            let payload: Option<String> = conn.get(&payload_key).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
            if let Some(payload_str) = payload {
                // Parse to check/update retry count
                let mut job: JobContext = match serde_json::from_str(&payload_str) {
                    Ok(j) => j,
                    Err(e) => {
                        warn!("Failed to parse expired job {}: {}", job_id, e);
                         // Clean up bad state
                        let _: () = conn.zrem(key, &job_id).await.unwrap_or(());
                        continue;
                    }
                };
                
                // Remove from processing queue (LREM)
                // We use the payload string we just fetched.
                let removed: i64 = conn.lrem(&self.processing_queue_name, 1, &payload_str).await
                     .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                
                // Clean up heartbeat & payload key
                let _: () = conn.zrem(key, &job_id).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let _: () = conn.del(&payload_key).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                if removed == 0 {
                    // Job was NOT in processing queue. It finished successfully but heartbeat lingered (race condition).
                    // We already cleaned up the heartbeat above. Do NOTHING else.
                    info!("Job {} finished successfully (not in processing), cleaning up stale heartbeat.", job.id);
                    continue;
                }

                info!("Heartbeat expired for job {}. Restoring...", job_id);

                // POISON PILL LOGIC
                job.retry_count += 1;
                
                let new_payload = serde_json::to_string(&job).unwrap();
                
                if job.retry_count > 3 {
                     warn!("Job {} exceeded max retries ({}). Moving to DLQ.", job.id, job.retry_count);
                     let _: () = conn.lpush(&dlq_name, new_payload).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                } else {
                     let target_queue = self.get_queue_for_priority(job.priority.as_deref());
                     let _: () = conn.lpush(target_queue, new_payload).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                     restored_count += 1;
                }
            } else {
                 // Payload missing? Maybe it finished and heartbeat is stale?
                 // Just remove from ZSET.
                 let _: () = conn.zrem(key, &job_id).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
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

    async fn enqueue_upload(&self, task: UploadTask) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let payload = serde_json::to_string(&task)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let _: () = conn.lpush(&self.upload_queue_name, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
    }

    async fn dequeue_upload(&self) -> Result<Option<UploadTask>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        // Blocking pop with timeout 1s
        // We use explicit return type to clarify what we expect from BRPOP
        let result: Option<(String, String)> = conn.brpop(&self.upload_queue_name, 1.0).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        if let Some((_, payload)) = result {
             let task: UploadTask = serde_json::from_str(&payload)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
             Ok(Some(task))
        } else {
             Ok(None)
        }
    }
}

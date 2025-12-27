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

    // Webhook Queue
    async fn enqueue_webhook(&self, payload: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn enqueue_webhook_retry(&self, payload: String, delay_seconds: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn dequeue_webhook(&self) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;
    async fn monitor_webhook_retries(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>;
    
    // Delayed Jobs
    async fn monitor_delayed_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>;

    // Idempotency
    async fn get_idempotency_key(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;
    async fn set_idempotency_key(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn set_idempotency_key_nx(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
    async fn delete_idempotency_key(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    // Helper needed for API layer sometimes? No, try to keep it abstract.
    // We remove get_client_connection_if_possible usage attempt in API.
    async fn get_client_connection_if_possible(&self) -> redis::aio::MultiplexedConnection {
         // Dummy implementation for trait satisfaction if needed, or better, 
         // remove the call in API and use the trait method.
         // I will implement delete_idempotency_key instead.
         unimplemented!("Use trait methods");
    }

    // Helper to get raw client for log publisher or others
    fn get_client(&self) -> redis::Client;
}

// --- REDIS IMPLEMENTATION ---
pub struct RedisQueue {
    client: redis::Client,
    queue_name: String, // Default/Normal queue
    queue_high: String,
    queue_low: String,
    processing_queue_name: String,
    upload_queue_name: String,
    webhook_queue_name: String,
    webhook_retry_zset: String,
    delayed_queue_zset: String,
}

impl RedisQueue {
    pub fn new(url: &str) -> Self {
        // Support redis+sentinel:// via the url string which redis crate supports automatically
        // but we need to ensure the client is created correctly.
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        Self {
            client,
            queue_name: "zorp_jobs_normal".to_string(),
            queue_high: "zorp_jobs_high".to_string(),
            queue_low: "zorp_jobs_low".to_string(),
            processing_queue_name: "zorp_jobs:processing".to_string(),
            upload_queue_name: "zorp_uploads".to_string(),
            webhook_queue_name: "zorp:webhooks:pending".to_string(),
            webhook_retry_zset: "zorp:webhooks:retry".to_string(),
            delayed_queue_zset: "zorp:jobs:delayed".to_string(),
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
        
        // Delayed Job Logic
        if let Some(run_at) = job.run_at {
            let now = chrono::Utc::now();
            if run_at > now {
                let score = run_at.timestamp();
                info!("Scheduling job {} for {}", job.id, run_at);
                let _: () = conn.zadd(&self.delayed_queue_zset, payload, score).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                return Ok(());
            }
        }

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

    async fn monitor_delayed_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let now = chrono::Utc::now().timestamp();
        
        // Atomic Lua Script: ZRANGEBYSCORE -> LPUSH (to processing/target) -> ZREM
        // Note: We need to parse priority to know WHICH queue to push to.
        // This makes pure Lua hard because we need to parse JSON.
        // Compromise: Use ZRANGEBYSCORE to get items, then use a Lua script to "Move if exists in ZSET".
        // Or simple transaction/lock.
        // But since we are the only ones removing from ZSET (presumably), we can just be careful.
        // Ideally, we fetch items, then for each item, we run a Lua script that removes it from ZSET and pushes to List.
        // Script:
        // if redis.call("ZREM", KEYS[1], ARGV[1]) == 1 then
        //     redis.call("LPUSH", KEYS[2], ARGV[1])
        //     return 1
        // else
        //     return 0
        // end
        
        // 1. Get ready items (score <= now)
        let items: Vec<String> = conn.zrangebyscore(&self.delayed_queue_zset, "-inf", now).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let mut count = 0;
        let script = redis::Script::new(r#"
            if redis.call("ZREM", KEYS[1], ARGV[1]) == 1 then
                redis.call("LPUSH", KEYS[2], ARGV[1])
                return 1
            else
                return 0
            end
        "#);

        for payload in items {
             // Parse to check priority
             let job: JobContext = match serde_json::from_str(&payload) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to parse delayed job: {}. Removing from ZSET.", e);
                    let _: () = conn.zrem(&self.delayed_queue_zset, &payload).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    continue;
                }
             };
             
             let target_queue = self.get_queue_for_priority(job.priority.as_deref());
             
             // Atomic Move
             let result: i32 = script.key(&self.delayed_queue_zset)
                                     .key(target_queue)
                                     .arg(&payload)
                                     .invoke_async(&mut conn).await
                                     .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
             
             if result == 1 {
                 info!("Delayed job {} is now ready and queued.", job.id);
                 count += 1;
             }
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

    async fn enqueue_webhook(&self, payload: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let _: () = conn.lpush(&self.webhook_queue_name, payload).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }

    async fn enqueue_webhook_retry(&self, payload: String, delay_seconds: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let score = chrono::Utc::now().timestamp() + delay_seconds;
        let _: () = conn.zadd(&self.webhook_retry_zset, payload, score).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }

    async fn dequeue_webhook(&self) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let result: Option<(String, String)> = conn.brpop(&self.webhook_queue_name, 1.0).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        if let Some((_, payload)) = result {
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    async fn monitor_webhook_retries(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let now = chrono::Utc::now().timestamp();
        
        // 1. Get ready items
        let items: Vec<String> = conn.zrangebyscore(&self.webhook_retry_zset, "-inf", now).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let mut count = 0;
        for item in items {
            // 2. Move to Pending List
            let _: () = conn.lpush(&self.webhook_queue_name, &item).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                
            // 3. Remove from ZSET
            let _: () = conn.zrem(&self.webhook_retry_zset, &item).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            count += 1;
        }
        
        Ok(count)
    }

    async fn get_idempotency_key(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let redis_key = format!("zorp:idempotency:{}", key);
        let value: Option<String> = conn.get(&redis_key).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(value)
    }

    async fn set_idempotency_key(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let redis_key = format!("zorp:idempotency:{}", key);
        let _: () = conn.set_ex(&redis_key, value, ttl_seconds).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
    }

    async fn set_idempotency_key_nx(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let redis_key = format!("zorp:idempotency:{}", key);
        
        // SET key value NX EX ttl
        let result: Option<String> = redis::cmd("SET")
            .arg(&redis_key)
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut conn).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(result.is_some())
    }

    async fn delete_idempotency_key(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let redis_key = format!("zorp:idempotency:{}", key);
        
        let _: () = conn.del(&redis_key).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        Ok(())
    }
    
    async fn get_client_connection_if_possible(&self) -> redis::aio::MultiplexedConnection {
        // This is a hack because the API change I made referenced this method which doesn't exist.
        // But since I'm overwriting the API file too, I don't actually need this method if I fix the API call.
        // However, I added it to the TRAIT definition in the block above (by mistake?)
        // Let's check the Search block again.
        // I added `async fn get_client_connection_if_possible` to the trait in the previous replacement.
        // I should probably REMOVE it from the trait and just use `delete_idempotency_key`.
        // But the `REPLACE` block put it there.
        // I will implement it to satisfy the trait I just defined, but panicking is bad if called.
        // Actually, I can just return a connection!
        self.client.get_multiplexed_async_connection().await.expect("Redis connection failed")
    }

    fn get_client(&self) -> redis::Client {
        self.client.clone()
    }
}

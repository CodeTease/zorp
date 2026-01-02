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
    upload_queue_name: String,
    webhook_queue_name: String,
    webhook_retry_zset: String,
    delayed_queue_zset: String,
    consumer_group: String,
    consumer_name: String,
}

impl RedisQueue {
    pub fn new(url: &str) -> Self {
        // Support redis+sentinel:// via the url string which redis crate supports automatically
        // but we need to ensure the client is created correctly.
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
        let consumer_name = format!("worker_{}_{}", hostname, uuid::Uuid::new_v4());
        let consumer_group = "zorp_workers".to_string();

        let queue = Self {
            client,
            queue_name: "zorp_jobs_normal".to_string(),
            queue_high: "zorp_jobs_high".to_string(),
            queue_low: "zorp_jobs_low".to_string(),
            upload_queue_name: "zorp_uploads".to_string(),
            webhook_queue_name: "zorp:webhooks:pending".to_string(),
            webhook_retry_zset: "zorp:webhooks:retry".to_string(),
            delayed_queue_zset: "zorp:jobs:delayed".to_string(),
            consumer_group,
            consumer_name,
        };

        // Initialize Consumer Groups
        // We use a synchronous connection to block until groups are created.
        // This avoids race conditions where workers start before groups exist.
        if let Ok(mut conn) = queue.client.get_connection() {
            let streams = [&queue.queue_high, &queue.queue_name, &queue.queue_low];
            let group = &queue.consumer_group;

            for stream in streams {
                // XGROUP CREATE stream group $ MKSTREAM
                let _: redis::RedisResult<()> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(stream)
                    .arg(group)
                    .arg("$")
                    .arg("MKSTREAM")
                    .query(&mut conn);
                // Ignore error (likely BUSYGROUP)
            }
        } else {
             eprintln!("Failed to connect to Redis synchronously for Group Init. Workers may fail if groups do not exist.");
        }

        queue
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

    async fn process_stream_entry(&self, stream_name: &str, id: String, map: std::collections::HashMap<String, redis::Value>, conn: &mut redis::aio::MultiplexedConnection) 
        -> Result<Option<JobContext>, Box<dyn std::error::Error + Send + Sync>> 
    {
        // Redis Value to String helper
        let get_string = |val: &redis::Value| -> Option<String> {
            match val {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
                _ => None,
            }
        };

        if let Some(val) = map.get("payload") {
            if let Some(payload_str) = get_string(val) {
                let mut job: JobContext = serde_json::from_str(&payload_str)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                
                // Populate stream info for ACK
                job.stream_id = Some(id.clone());
                job.stream_name = Some(stream_name.to_string());

                // Store mapping for Heartbeat (update_heartbeat needs to find Stream ID via Job ID)
                // zorp:job_stream_ref:{job_id} -> {stream_name}:{stream_id}
                // Expires in 24 hours (generous upper bound) to avoid leakage
                let map_key = format!("zorp:job_stream_ref:{}", job.id);
                let map_val = format!("{}:{}", stream_name, id);
                let _: () = conn.set_ex(&map_key, map_val, 86400).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                return Ok(Some(job));
            }
        }
        
        warn!("Stream entry {} in {} missing payload field or invalid format.", id, stream_name);
        Ok(None)
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
                // Note: Delayed jobs are stored in ZSET. 
                // When ready, monitor_delayed_jobs must XADD them.
                let _: () = conn.zadd(&self.delayed_queue_zset, payload, score).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                return Ok(());
            }
        }

        let target_queue = self.get_queue_for_priority(job.priority.as_deref());

        // XADD: Add to stream
        // XADD key * payload value
        let _: () = redis::cmd("XADD")
            .arg(target_queue)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async(&mut conn).await
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
        
        // XREADGROUP GROUP group consumer BLOCK 2000 STREAMS high normal low > > >
        let _streams = &[&self.queue_high, &self.queue_name, &self.queue_low];
        let _ids = &[">", ">", ">"];

        // We use redis::streams::StreamReadReply but it's easier to parse raw response or use the crate helpers.
        // The return structure of XREADGROUP is complex.
        // It returns Array of Arrays.
        // Inner Array: [StreamName, [Entries]]
        // Entry: [ID, [Field, Value, ...]]

        type StreamResponse = redis::streams::StreamReadReply;
        
        let result: Option<StreamResponse> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.consumer_group)
            .arg(&self.consumer_name)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(2000) // 2s block
            .arg("STREAMS")
            .arg(&self.queue_high)
            .arg(&self.queue_name)
            .arg(&self.queue_low)
            .arg(">")
            .arg(">")
            .arg(">")
            .query_async(&mut conn).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if let Some(response) = result {
            for stream_key in response.keys {
                let stream_name = stream_key.key;
                if let Some(entry) = stream_key.ids.first() {
                     let id = entry.id.clone();
                     let map = entry.map.clone();
                     
                     return self.process_stream_entry(&stream_name, id, map, &mut conn).await;
                }
            }
        }
        
        Ok(None)
    }

    async fn acknowledge(&self, job: &JobContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
         let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
         if let (Some(stream_id), Some(stream_name)) = (&job.stream_id, &job.stream_name) {
             // XACK key group id
             let ack_count: i64 = redis::cmd("XACK")
                .arg(stream_name)
                .arg(&self.consumer_group)
                .arg(stream_id)
                .query_async(&mut conn).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

             if ack_count == 0 {
                 warn!("Could not acknowledge job {} from stream. It might have been already ACKed.", job.id);
             } else {
                 // info!("Acknowledged job {} from {}", job.id, stream_name);
                 
                 // Clean up heartbeat mapping
                 let map_key = format!("zorp:job_stream_ref:{}", job.id);
                 let _: () = conn.del(&map_key).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
             }
         } else {
             warn!("Job {} has no stream info. Cannot acknowledge in Redis Streams.", job.id);
         }
         
         Ok(())
    }

    async fn restore_stranded(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let mut total_restored = 0;
        let dlq_name = format!("{}:dlq", self.queue_name);
        let min_idle_time = 60_000; // 60 seconds

        // Check all streams
        for stream_name in &[&self.queue_high, &self.queue_name, &self.queue_low] {
            // XAUTOCLAIM key group consumer min-idle-time start-id count
            // We iterate until we get 0 messages
            let mut start_id = "0-0".to_string();
            
            loop {
                let result: redis::streams::StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                    .arg(stream_name)
                    .arg(&self.consumer_group)
                    .arg(&self.consumer_name) // Claim to self, then process
                    .arg(min_idle_time)
                    .arg(&start_id)
                    .arg("COUNT")
                    .arg(10)
                    .query_async(&mut conn).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // result.ids contains the stream keys (StreamId)
                // Note: redis crate StreamAutoClaimReply structure:
                // pub next_stream_id: String,
                // pub claimed: Vec<StreamId>,
                
                let messages = result.claimed;
                if messages.is_empty() {
                    break;
                }
                
                start_id = result.next_stream_id;

                for entry in messages {
                    let id = entry.id;
                    let map = entry.map;
                    
                    // Helper to extract string from Redis Value
                    let get_string = |val: &redis::Value| -> Option<String> {
                        match val {
                            redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
                            _ => None,
                        }
                    };

                    if let Some(val) = map.get("payload") {
                        if let Some(payload_str) = get_string(val) {
                            match serde_json::from_str::<JobContext>(&payload_str) {
                                Ok(mut job) => {
                                     job.retry_count += 1;
                                     let new_payload = serde_json::to_string(&job).unwrap();

                                     // Re-enqueue FIRST (Data Safety)
                                     if job.retry_count > 3 {
                                         warn!("Job {} (Stream ID {}) exceeded max retries. Moving to DLQ.", job.id, id);
                                         let _: () = conn.lpush(&dlq_name, new_payload).await
                                             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                                     } else {
                                         // Re-enqueue as new message to reset timeout/retry logic cleanly
                                         let _: () = redis::cmd("XADD")
                                            .arg(stream_name)
                                            .arg("*")
                                            .arg("payload")
                                            .arg(new_payload)
                                            .query_async(&mut conn).await
                                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                                         
                                         total_restored += 1;
                                     }

                                     // ACK old message ONLY after successful re-queue
                                     let _: () = redis::cmd("XACK")
                                        .arg(stream_name)
                                        .arg(&self.consumer_group)
                                        .arg(&id)
                                        .query_async(&mut conn).await
                                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                                },
                                Err(e) => {
                                    warn!("Poison Pill found! Failed to parse stranded job (Stream ID {}): {}. Moving to DLQ.", id, e);
                                    // Move raw payload to DLQ
                                    let _: () = conn.lpush(&dlq_name, &payload_str).await
                                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                                    
                                    // ACK to remove from PEL
                                    let _: () = redis::cmd("XACK")
                                        .arg(stream_name)
                                        .arg(&self.consumer_group)
                                        .arg(&id)
                                        .query_async(&mut conn).await
                                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if total_restored > 0 {
            info!("Restored {} stranded jobs via XAUTOCLAIM", total_restored);
        }
        
        Ok(total_restored)
    }

    async fn monitor_delayed_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.client.get_multiplexed_async_connection().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let now = chrono::Utc::now().timestamp();
        
        // 1. Get ready items (score <= now)
        let items: Vec<String> = conn.zrangebyscore(&self.delayed_queue_zset, "-inf", now).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            
        let mut count = 0;
        // Atomic Lua Script: ZREM -> XADD
        // KEYS[1]: ZSET Key
        // KEYS[2]: Target Stream Key
        // ARGV[1]: Payload
        let script = redis::Script::new(r#"
            if redis.call("ZREM", KEYS[1], ARGV[1]) == 1 then
                redis.call("XADD", KEYS[2], "*", "payload", ARGV[1])
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
             
        let map_key = format!("zorp:job_stream_ref:{}", job_id);
        let ref_val: Option<String> = conn.get(&map_key).await
             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
             
        if let Some(val) = ref_val {
            if let Some((stream_name, stream_id)) = val.split_once(':') {
                // XCLAIM key group consumer 0 ID JUSTID
                // Updates idle time to now (resets it)
                 let _: () = redis::cmd("XCLAIM")
                    .arg(stream_name)
                    .arg(&self.consumer_group)
                    .arg(&self.consumer_name)
                    .arg(0) // min-idle-time 0 means force claim (update)
                    .arg(stream_id)
                    .arg("JUSTID") // Do not return message
                    .query_async(&mut conn).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
        }
        
        Ok(())
    }

    async fn monitor_stranded_jobs(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        // This method is now redundant because `restore_stranded` does the work.
        // However, the trait defines both. 
        // Usually `monitor_stranded_jobs` was the "Heartbeat Check".
        // `restore_stranded` was the "Queue Check".
        // Now they are unified in `XAUTOCLAIM`.
        // I'll alias this to `restore_stranded`.
        self.restore_stranded().await
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
        self.client.get_multiplexed_async_connection().await.expect("Redis connection failed")
    }

    fn get_client(&self) -> redis::Client {
        self.client.clone()
    }
}

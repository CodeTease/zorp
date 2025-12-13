use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions, DownloadFromContainerOptions, ListContainersOptions,
    StopContainerOptions, PruneContainersOptions,
};
use bollard::image::{CreateImageOptions, PruneImagesOptions};
use bollard::volume::PruneVolumesOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use futures_util::TryStreamExt; 
use futures_util::StreamExt;    
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Semaphore;
use tokio::fs::File;
use tokio::io::AsyncWriteExt; // Removed AsyncReadExt
use tracing::{info, warn, error};
use crate::models::{JobContext, JobRegistry};
use crate::db::{DbPool, sql_placeholder};
use crate::queue::JobQueue;
use crate::metrics;
use crate::streaming::RedisLogPublisher;
use bollard::network::CreateNetworkOptions;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;

use aws_sdk_s3;

const ZORP_NETWORK_NAME: &str = "zorp-net";

// --- CANCEL JOB LOGIC ---
pub async fn cancel_job(
    docker: &Docker,
    registry: &JobRegistry,
    job_id: &str,
    db: &DbPool
) -> Result<bool, String> {
    let container_name = {
        let mut reg = registry.write().await;
        reg.remove(job_id)
    };

    if let Some(container_id) = container_name {
        info!("Cancellation requested for job {}. Stopping container {} (Grace period: 10s)...", job_id, container_id);

        // Graceful Cancellation: Stop container with timeout (SIGTERM -> SIGKILL)
        let stop_opts = StopContainerOptions { t: 10 }; 
        match docker.stop_container(&container_id, Some(stop_opts)).await {
            Ok(_) => {
                info!("Container {} stopped successfully.", container_id);
            }
            Err(e) => {
                warn!("Failed to gracefully stop container {}: {}. Attempting forced kill...", container_id, e);
                // Fallback to kill if stop fails (e.g. timeout or error)
                if let Err(k_e) = docker.kill_container::<String>(&container_id, None).await {
                     warn!("Failed to kill container {}: {}", container_id, k_e);
                }
            }
        }

        // Update DB status to CANCELLED
        let query = format!("UPDATE jobs SET status = 'CANCELLED' WHERE id = {}", sql_placeholder(1));
        if let Err(e) = sqlx::query(&query).bind(job_id).execute(db).await {
            error!("Failed to update job {} status to CANCELLED: {}", job_id, e);
        }

        return Ok(true);
    }

    // Fallback: Check if it's in DB as RUNNING but not in registry (Zombie?)
    // Or maybe just return false saying "Job not found running".
    warn!("Cancellation requested for job {}, but it was not found in active registry.", job_id);
    Ok(false)
}

// --- THE ZOMBIE REAPER & GARBAGE COLLECTOR ---
pub fn spawn_reaper_task(docker: Docker, db: DbPool, registry: JobRegistry) {
    tokio::spawn(async move {
        info!("逐 Zombie Reaper & Garbage Collector task started.");
        loop {
            // Reap Zombies (every 60s)
            reap_zombies(&docker, &db).await;
            
            // Garbage Collection (Temp files & Pruning)
            garbage_collection(&docker, &registry).await;

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });
}

async fn reap_zombies(docker: &Docker, db: &DbPool) {
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);

    let options = ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    };

    if let Ok(containers) = docker.list_containers(Some(options)).await {
        for c in containers {
            let job_id = c.labels.as_ref()
                .and_then(|l| l.get("job_id"))
                .cloned();

            if let Some(jid) = job_id {
                 let query = format!("SELECT status FROM jobs WHERE id = {}", sql_placeholder(1));
                 let row = sqlx::query_as::<_, (String,)>(&query)
                     .bind(&jid)
                     .fetch_optional(db).await;

                 let should_kill = match row {
                     Ok(Some((status,))) => status != "RUNNING" && status != "QUEUED",
                     Ok(None) => true, // Job not in DB? Zombie.
                     Err(_) => false,
                 };

                 if should_kill {
                     if c.state.as_deref() == Some("running") {
                         warn!("逐 Reaper: Found active zombie {} (Job {}). Terminating...", c.id.as_deref().unwrap_or("?"), jid);
                         if let Some(id) = c.id.as_ref() {
                            let _ = docker.kill_container::<String>(id, None).await;
                         }
                     }
                     if let Some(id) = c.id.as_ref() {
                        let _ = docker.remove_container(id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                     }
                 }
            }
        }
    }
}

async fn garbage_collection(docker: &Docker, registry: &JobRegistry) {
    // 1. Clean up stale /tmp files (logs & artifacts) older than 1 hour
    let tmp_dir = std::path::Path::new("/tmp");
    if let Ok(mut entries) = tokio::fs::read_dir(tmp_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("zorp-") {
                    // Extract ID from zorp-{id}.log or zorp-artifacts-{id}.tar
                    let job_id = if name.starts_with("zorp-artifacts-") {
                        name.trim_start_matches("zorp-artifacts-").trim_end_matches(".tar")
                    } else {
                        name.trim_start_matches("zorp-").trim_end_matches(".log")
                    };

                    // Check if job is still running
                    if registry.read().await.contains_key(job_id) {
                         // Job is running, SKIP deletion even if old (long running job)
                         continue;
                    }

                    if let Ok(metadata) = entry.metadata().await {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(age) = SystemTime::now().duration_since(modified) {
                                if age > std::time::Duration::from_secs(3600) {
                                    info!("🧹 GC: Removing stale file {:?}", path);
                                    let _ = tokio::fs::remove_file(path).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Prune Docker Resources
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);
    
    // Prune containers
    let _ = docker.prune_containers(Some(PruneContainersOptions { filters: filters.clone() })).await;

    // Prune images (dangling)
    let _ = docker.prune_images(Some(PruneImagesOptions::<String> { filters: std::collections::HashMap::new() })).await; 
    
    // Prune volumes
    let _ = docker.prune_volumes(Some(PruneVolumesOptions::<String> { filters: std::collections::HashMap::new() })).await;
}

// --- DISPATCHER LOGIC ---

pub struct Dispatcher {
    docker: Docker,
    db: DbPool,
    http_client: reqwest::Client,
    limiter: Arc<Semaphore>,
    job_registry: JobRegistry,
    s3_client: Option<aws_sdk_s3::Client>,
    s3_bucket: Option<String>,
    secret_key: String,
    queue: Arc<dyn JobQueue>,
}

impl Dispatcher {
    pub fn new(
        docker: Docker, 
        db: DbPool, 
        http_client: reqwest::Client, 
        max_concurrent: usize,
        job_registry: JobRegistry,
        s3_client: aws_sdk_s3::Client,
        s3_bucket: String,
        secret_key: String,
        queue: Arc<dyn JobQueue>,
    ) -> Self {
        Self {
            docker,
            db,
            http_client,
            limiter: Arc::new(Semaphore::new(max_concurrent)),
            job_registry,
            s3_client: Some(s3_client),
            s3_bucket: Some(s3_bucket),
            secret_key,
            queue,
        }
    }

    pub async fn dispatch(&self, job: JobContext) {
        let permit = self.limiter.clone().acquire_owned().await.unwrap();
        let docker = self.docker.clone();
        let db = self.db.clone();
        let http_client = self.http_client.clone();
        let job_registry = self.job_registry.clone();
        let s3_client = self.s3_client.clone();
        let s3_bucket = self.s3_bucket.clone();
        let secret_key = self.secret_key.clone();
        let queue = self.queue.clone();
        
        tokio::spawn(async move {
            let _permit = permit; 
            metrics::inc_running();
            metrics::dec_queued(); // It was queued, now running

            let start_time = Instant::now();
            let container_name = format!("zorp-{}", job.id);
            
            // Redis Publisher for logs
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
            let log_publisher = RedisLogPublisher::new(&redis_url);

            // Register job for cancellation
            {
                let mut reg = job_registry.write().await;
                reg.insert(job.id.clone(), container_name.clone());
            }

            // Dynamic SQL: UPDATE jobs SET status = 'RUNNING' WHERE id = $1
            let update_running = format!("UPDATE jobs SET status = 'RUNNING' WHERE id = {}", sql_placeholder(1));
            let _ = sqlx::query(&update_running)
                .bind(&job.id)
                .execute(&db).await;

            info!("[{}] Status: RUNNING", job.id);
            let _ = log_publisher.publish(&job.id, &format!("INFO: Job {} started.\n", job.id)).await;

            // Start Heartbeat Loop
            let queue_for_heartbeat = queue.clone();
            let job_id_for_heartbeat = job.id.clone();
            // We use a cancellation token or just abort handle for the heartbeat task
            let heartbeat_handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    if let Err(e) = queue_for_heartbeat.update_heartbeat(&job_id_for_heartbeat).await {
                         warn!("[{}] Failed to update heartbeat: {}", job_id_for_heartbeat, e);
                    }
                }
            });

            let mut final_status = "FINISHED";
            let mut final_exit_code = 0;
            // captured_logs is now replaced by file streaming, but we keep a variable for webhook/db fallback
            let mut webhook_logs = String::new();
            let mut log_file_path = String::new();
            let mut final_artifact_url = String::new();

            let env_vars: Option<Vec<String>> = job.env.as_ref().map(|map| {
                map.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
            });

            // Resource Limits (Hard Limits or Defaults)
            let default_mem_mb = std::env::var("ZORP_DEFAULT_MEMORY_MB")
                .ok().and_then(|v| v.parse::<i64>().ok()).unwrap_or(256);
            let default_cpu_cores = std::env::var("ZORP_DEFAULT_CPU_CORES")
                .ok().and_then(|v| v.parse::<f32>().ok()).unwrap_or(1.0);

            let memory = job.limits.as_ref()
                .and_then(|l| l.memory_mb)
                .unwrap_or(default_mem_mb) * 1024 * 1024;
            
            let cpu_quota = (job.limits.as_ref()
                .and_then(|l| l.cpu_cores)
                .unwrap_or(default_cpu_cores) * 100000.0) as i64;

            // Ensure Network Exists
            if let Err(_) = docker.inspect_network::<String>(ZORP_NETWORK_NAME, None).await {
                let _ = docker.create_network(CreateNetworkOptions {
                    name: ZORP_NETWORK_NAME,
                    driver: "bridge",
                    check_duplicate: true,
                    ..Default::default()
                }).await;
            }

            if let Err(e) = ensure_image(&docker, &job.image).await {
                error!("[{}] Image pull failed: {}", job.id, e);
                let _ = log_publisher.publish(&job.id, &format!("ERROR: Image pull failed: {}\n", e)).await;
                final_status = "FAILED";
                final_exit_code = -1;
                webhook_logs = format!("System Error: Image pull failed. {}", e);
            } else {
                let mut labels = std::collections::HashMap::new();
                labels.insert("managed_by".to_string(), "zorp".to_string());
                labels.insert("job_id".to_string(), job.id.clone());

                let config = Config {
                    image: Some(job.image.clone()),
                    cmd: Some(job.commands.clone()),
                    env: env_vars,
                    labels: Some(labels),
                    host_config: Some(HostConfig {
                        auto_remove: Some(false), 
                        memory: Some(memory), 
                        cpu_quota: Some(cpu_quota),
                        // Security Context
                        readonly_rootfs: Some(true),
                        cap_drop: Some(vec!["ALL".to_string()]),
                        pids_limit: Some(100),
                        // Network Isolation
                        network_mode: Some(ZORP_NETWORK_NAME.to_string()),
                        // Extra Hosts: Block metadata service
                        extra_hosts: Some(vec!["169.254.169.254:127.0.0.1".to_string()]),
                        ..Default::default()
                    }),
                    // Configurable User
                    user: job.user.clone(),
                    tty: Some(false),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                };

                match docker.create_container(
                    Some(CreateContainerOptions { name: container_name.as_str(), platform: None }), 
                    config
                ).await {
                    Ok(_) => {
                        // --- RETRY LOGIC START ---
                        let mut start_attempts = 0;
                        let max_retries = 3;
                        let mut start_result = Err(bollard::errors::Error::DockerResponseServerError {
                            status_code: 500,
                            message: "Initial attempt".to_string(),
                        }); // Dummy error to init

                        while start_attempts < max_retries {
                             start_result = docker.start_container(&container_name, None::<StartContainerOptions<String>>).await;
                             if start_result.is_ok() {
                                 break;
                             }
                             start_attempts += 1;
                             warn!("[{}] Start failed (Attempt {}/{}): {:?}", job.id, start_attempts, max_retries, start_result.as_ref().err());
                             tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(start_attempts as u32))).await;
                        }
                        // --- RETRY LOGIC END ---

                        if let Err(e) = start_result {
                             error!("[{}] Start failed after retries: {}", job.id, e);
                             let _ = log_publisher.publish(&job.id, &format!("ERROR: Container start failed: {}\n", e)).await;
                             final_status = "FAILED";
                             final_exit_code = -2;
                             webhook_logs = format!("Error: Start failed: {}", e);
                        } else {
                            // Wait logic with Timeout
                            let mut wait_stream = docker.wait_container(
                                &container_name, 
                                None::<WaitContainerOptions<String>>
                            );

                            let timeout_fut = async {
                                if let Some(secs) = job.timeout_seconds {
                                    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                                    true
                                } else {
                                    // Never finish if no timeout
                                    std::future::pending::<bool>().await
                                }
                            };

                            tokio::select! {
                                res = wait_stream.next() => {
                                    match res {
                                        Some(Ok(r)) => {
                                            final_exit_code = r.status_code as i32;
                                            info!("[{}] Container exited with {}", job.id, final_exit_code);
                                            let _ = log_publisher.publish(&job.id, &format!("INFO: Container exited with code {}\n", final_exit_code)).await;
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] Wait stream error: {}", job.id, e);
                                            let _ = log_publisher.publish(&job.id, &format!("ERROR: Wait stream error: {}\n", e)).await;
                                            final_status = "FAILED";
                                            final_exit_code = -99;
                                        }
                                        None => {
                                            warn!("[{}] Wait stream closed unexpectedly", job.id);
                                            // Try inspect to be sure
                                            if let Ok(inspect) = docker.inspect_container(&container_name, None).await {
                                                if let Some(state) = inspect.state {
                                                    final_exit_code = state.exit_code.unwrap_or(-98) as i32;
                                                }
                                            }
                                        }
                                    }
                                }
                                _ = timeout_fut => {
                                    // Timeout
                                    info!("[{}] Timeout reached. Killing...", job.id);
                                    let _ = log_publisher.publish(&job.id, "ERROR: Timeout reached. Killing container.\n").await;
                                    let _ = docker.kill_container::<String>(&container_name, None).await;
                                    final_status = "TIMED_OUT";
                                }
                            }

                            // Collect logs to file (and stream)
                            log_file_path = stream_logs_to_file_and_broadcast(&docker, &container_name, &job.id, &log_publisher).await;

                            // Collect Artifacts
                            if let Some(art_path) = &job.artifacts_path {
                                let _ = log_publisher.publish(&job.id, "INFO: Collecting artifacts...\n").await;
                                let options = Some(DownloadFromContainerOptions { path: art_path.clone() });
                                let mut stream = docker.download_from_container(&container_name, options);

                                let art_file_path = format!("/tmp/zorp-artifacts-{}.tar", job.id);
                                let path_ref = std::path::Path::new(&art_file_path);

                                if let Ok(mut file) = File::create(path_ref).await {
                                    let mut success = true;
                                    while let Some(chunk_res) = stream.next().await {
                                        match chunk_res {
                                            Ok(chunk) => {
                                                if file.write_all(&chunk).await.is_err() {
                                                    success = false; break;
                                                }
                                            }
                                            Err(_) => { success = false; break; }
                                        }
                                    }

                                    if success {
                                        if let (Some(s3), Some(bucket)) = (s3_client.as_ref(), s3_bucket.as_ref()) {
                                            let key = format!("artifacts/{}.tar", job.id);
                                             match aws_sdk_s3::primitives::ByteStream::from_path(path_ref).await {
                                                 Ok(bs) => {
                                                     if let Ok(_) = s3.put_object().bucket(bucket).key(&key).body(bs).send().await {
                                                         final_artifact_url = format!("s3://{}/{}", bucket, key);
                                                         info!("[{}] Artifact uploaded: {}", job.id, final_artifact_url);
                                                         let _ = log_publisher.publish(&job.id, &format!("INFO: Artifact uploaded to {}\n", final_artifact_url)).await;
                                                     }
                                                 },
                                                 Err(e) => error!("Failed to read artifact file: {}", e)
                                             }
                                        }
                                    }
                                    let _ = tokio::fs::remove_file(path_ref).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                         error!("[{}] Create failed: {}", job.id, e);
                         let _ = log_publisher.publish(&job.id, &format!("ERROR: Create container failed: {}\n", e)).await;
                        final_status = "FAILED";
                        final_exit_code = -3;
                        webhook_logs = format!("Error: Container creation failed: {}", e);
                    }
                }

                let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }

            // Clean up registry
            {
                let mut reg = job_registry.write().await;
                reg.remove(&job.id);
            }

            if final_status != "TIMED_OUT" && final_status != "FAILED" && final_exit_code != 0 {
                 final_status = "FAILED";
            } else if final_exit_code != 0 {
                 final_status = "FAILED";
            }

            if final_status == "FAILED" || final_status == "TIMED_OUT" || final_status == "CANCELLED" {
                metrics::inc_failed();
            } else {
                metrics::inc_completed();
            }
            metrics::dec_running();

            let duration = start_time.elapsed().as_secs_f64();

            // --- LOGS PERSISTENCE (MANDATORY S3) ---
            let mut final_log_ref = String::new();

            // If we have a log file, process it
            if !log_file_path.is_empty() {
                if let (Some(s3), Some(bucket)) = (s3_client.as_ref(), s3_bucket.as_ref()) {
                    let key = format!("logs/{}.txt", job.id);
                    let path_ref = std::path::Path::new(&log_file_path);

                    match aws_sdk_s3::primitives::ByteStream::from_path(path_ref).await {
                        Ok(byte_stream) => {
                            info!("[{}] Uploading logs to S3...", job.id);
                            match s3.put_object().bucket(bucket).key(&key).body(byte_stream).send().await {
                                Ok(_) => {
                                    final_log_ref = format!("s3://{}/{}", bucket, key);
                                    webhook_logs = final_log_ref.clone(); // Webhook gets the URL
                                    info!("[{}] S3 upload successful.", job.id);
                                }
                                Err(e) => {
                                    error!("S3 upload failed: {}", e);
                                    final_status = "FAILED"; // Mark job as failed if logs cannot be preserved
                                    webhook_logs = format!("System Error: Log upload failed. {}", e);
                                }
                            }
                        },
                        Err(e) => {
                            error!("Failed to read log file for S3 upload: {}", e);
                            final_status = "FAILED";
                        }
                    }
                } else {
                    error!("S3 Client not available despite being mandatory!");
                    final_status = "FAILED";
                }

                // Remove temp file
                let _ = tokio::fs::remove_file(&log_file_path).await;
            } else if final_status != "FAILED" && final_status != "CANCELLED" {
                // No logs generated?
                warn!("[{}] No logs were generated or captured.", job.id);
            }

            // Dynamic SQL: UPDATE jobs SET status = $1, exit_code = $2, logs = $3, artifact_url = $4 WHERE id = $5
            let update_final = format!(
                "UPDATE jobs SET status = {}, exit_code = {}, logs = {}, artifact_url = {} WHERE id = {}",
                sql_placeholder(1), sql_placeholder(2), sql_placeholder(3), sql_placeholder(4), sql_placeholder(5)
            );

            if let Err(e) = sqlx::query(&update_final)
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&final_log_ref)
                .bind(&final_artifact_url)
                .bind(&job.id)
                .execute(&db).await 
            {
                error!("[{}] DB Update failed: {}", job.id, e);
            }

            info!("[{}] Status: {} (Exit: {}). Time: {:.2}s", job.id, final_status, final_exit_code, duration);

            // Acknowledge the job in the queue (Remove from processing queue)
            // Stop heartbeat
            heartbeat_handle.abort();

            if let Err(e) = queue.acknowledge(&job).await {
                error!("[{}] Failed to acknowledge job in queue: {}", job.id, e);
            } else {
                info!("[{}] Job acknowledged in queue.", job.id);
            }

            if let Some(url) = job.callback_url {
                let payload = serde_json::json!({
                    "job_id": job.id,
                    "status": final_status,
                    "exit_code": final_exit_code,
                    "duration_seconds": duration,
                    "logs": webhook_logs
                });

                // SIGNATURE GENERATION
                let mut headers = reqwest::header::HeaderMap::new();
                if let Ok(payload_str) = serde_json::to_string(&payload) {
                     type HmacSha256 = Hmac<Sha256>;
                     if let Ok(mut mac) = HmacSha256::new_from_slice(secret_key.as_bytes()) {
                         mac.update(payload_str.as_bytes());
                         let result = mac.finalize();
                         let signature = hex::encode(result.into_bytes());
                         if let Ok(hv) = reqwest::header::HeaderValue::from_str(&signature) {
                             headers.insert("X-Zorp-Signature", hv);
                         }
                     }
                }

                let send_result = http_client.post(&url)
                    .headers(headers.clone())
                    .json(&payload)
                    .send()
                    .await;
                
                if let Err(e) = send_result {
                     warn!("[{}] Webhook failed (1/2): {}. Retrying...", job.id, e);
                     tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                     let _ = http_client.post(&url)
                         .headers(headers)
                         .json(&payload)
                         .send()
                         .await
                         .map_err(|e2| error!("[{}] Webhook failed (2/2): {}", job.id, e2));
                }
            }
        });
    }
}

async fn ensure_image(docker: &Docker, image: &str) -> Result<(), bollard::errors::Error> {
    let image = if !image.contains(':') { format!("{}:latest", image) } else { image.to_string() };
    if docker.inspect_image(&image).await.is_ok() { 
        return Ok(()); 
    }
    let mut stream = docker.create_image(Some(CreateImageOptions { from_image: image.clone(), ..Default::default() }), None, None);
    while let Some(_) = stream.try_next().await? {}
    Ok(())
}

async fn stream_logs_to_file_and_broadcast(docker: &Docker, name: &str, job_id: &str, publisher: &RedisLogPublisher) -> String {
    let filename = format!("/tmp/zorp-{}.log", job_id);
    let path = std::path::Path::new(&filename);

    let mut file = match File::create(&path).await {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to create temp log file: {}", e);
            return String::new();
        }
    };

    let options = Some(LogsOptions::<String> {
        stdout: true, stderr: true, follow: true, tail: "all".to_string(), ..Default::default()
    });
    let mut stream = docker.logs(name, options);

    while let Ok(Some(log)) = stream.try_next().await {
        let msg = match log {
            LogOutput::StdOut { message } | LogOutput::Console { message } => message,
            LogOutput::StdErr { message } => message,
            _ => continue,
        };
        
        // Write to file
        if let Err(e) = file.write_all(&msg).await {
             error!("Failed to write to log file: {}", e);
        }

        // Broadcast to real-time subscribers via Redis
        // We convert bytes to string lossily
        let log_str = String::from_utf8_lossy(&msg).to_string();
        let _ = publisher.publish(job_id, &log_str).await;
    }

    filename
}

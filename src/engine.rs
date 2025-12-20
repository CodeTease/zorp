use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions, DownloadFromContainerOptions, ListContainersOptions,
    StopContainerOptions, PruneContainersOptions,
};
use bollard::image::{CreateImageOptions, PruneImagesOptions};
use bollard::volume::{PruneVolumesOptions, CreateVolumeOptions, ListVolumesOptions}; // Added CreateVolumeOptions, ListVolumesOptions
use bollard::models::HostConfig;
use bollard::system::EventsOptions;
use bollard::Docker;
use futures_util::TryStreamExt; 
use futures_util::StreamExt;    
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Semaphore;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncSeekExt, SeekFrom}; // Added AsyncSeekExt, SeekFrom
use tracing::{info, warn, error};
use crate::models::{JobContext, JobRegistry, UploadTask};
use crate::db::{DbPool, sql_placeholder};
use crate::queue::JobQueue;
use crate::metrics;
use crate::streaming::RedisLogPublisher;
use bollard::network::CreateNetworkOptions;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;
use uuid::Uuid;

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
    // 1. Event Stream Listener (Real-time)
    let docker_events = docker.clone();
    let db_events = db.clone();
    
    tokio::spawn(async move {
        info!("👂 Docker Event Listener started.");
        let mut filters = std::collections::HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        filters.insert("event".to_string(), vec!["die".to_string()]);
        filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);

        let options = EventsOptions {
            filters,
            ..Default::default()
        };

        let mut stream = docker_events.events(Some(options));

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(event) => {
                    if let Some(actor) = event.actor {
                        if let Some(attributes) = actor.attributes {
                            if let Some(job_id) = attributes.get("job_id") {
                                info!("⚡ Event Listener: Detected death of job container {}", job_id);
                                
                                // Immediate status update
                                let query = format!("SELECT status FROM jobs WHERE id = {}", sql_placeholder(1));
                                let row = sqlx::query_as::<_, (String,)>(&query)
                                    .bind(job_id)
                                    .fetch_optional(&db_events).await;

                                if let Ok(Some((status,))) = row {
                                    if status == "RUNNING" {
                                        warn!("⚡ Event Listener: Job {} died unexpectedly. Updating DB...", job_id);
                                        let update_query = format!("UPDATE jobs SET status = 'FAILED', exit_code = -999 WHERE id = {}", sql_placeholder(1));
                                        let _ = sqlx::query(&update_query)
                                            .bind(job_id)
                                            .execute(&db_events).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error in Docker event stream: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // 2. Fallback Poller (Every 60s)
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

            // Check if it's a DEBUG container
            let is_debug = c.labels.as_ref()
                .and_then(|l| l.get("zorp_debug"))
                .map(|v| v == "true")
                .unwrap_or(false);

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
                     // If it is DEBUG, we don't kill it immediately here. 
                     // Garbage collection will handle it based on TTL.
                     // But if it is actively running and marked as finished in DB, we SHOULD stop it.
                     if c.state.as_deref() == Some("running") {
                         warn!("逐 Reaper: Found active zombie {} (Job {}). Terminating...", c.id.as_deref().unwrap_or("?"), jid);
                         if let Some(id) = c.id.as_ref() {
                            let _ = docker.kill_container::<String>(id, None).await;
                         }
                     }
                     
                     // Removal: Only if NOT debug. Debug containers stay until GC.
                     if !is_debug {
                         if let Some(id) = c.id.as_ref() {
                            let _ = docker.remove_container(id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                         }
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

    // 2. Clean up Debug Containers (> 30 mins)
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["zorp_debug=true".to_string()]);
    
    let options = ListContainersOptions {
        all: true,
        filters: filters.clone(),
        ..Default::default()
    };

    if let Ok(containers) = docker.list_containers(Some(options)).await {
        for c in containers {
             if let Some(created) = c.created {
                 // Docker timestamp is usually unix timestamp
                 let created_time = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(created as u64);
                 if let Ok(age) = SystemTime::now().duration_since(created_time) {
                     if age > std::time::Duration::from_secs(1800) { // 30 mins
                         if let Some(id) = c.id {
                             info!("🧹 GC: Removing expired debug container {}", id);
                             let _ = docker.remove_container(&id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                         }
                     }
                 }
             }
        }
    }

    // 3. Prune Docker Resources
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);
    
    // Prune containers (Only stopped ones that are not debug and not recent)
    // Actually prune_containers is aggressive. We should rely on auto-remove or explicit remove.
    // The previous implementation used prune which might be too broad if we want to keep debug ones.
    // But we filtered by label managed_by=zorp. 
    // We should probably filter out zorp_debug=true for pruning.
    // Unfortunately bollard/docker prune filters are limited (label=key=value).
    // So we'll skip global prune for now to protect debug containers, relying on explicit removal in dispatch and reap_zombies.
    // let _ = docker.prune_containers(Some(PruneContainersOptions { filters: filters.clone() })).await;

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
                if job.debug {
                    labels.insert("zorp_debug".to_string(), "true".to_string());
                }

                // --- HOT VOLUMES (CACHING) ---
                let mut binds = Vec::new();
                if let Some(cache_key) = &job.cache_key {
                    let volume_name = format!("zorp-cache-{}", cache_key);
                    
                    // Check if volume exists, if not create it
                    let mut filters = std::collections::HashMap::new();
                    filters.insert("name".to_string(), vec![volume_name.clone()]);
                    let list_opts = ListVolumesOptions { filters };
                    
                    let exists = match docker.list_volumes(Some(list_opts)).await {
                         Ok(res) => res.volumes.map(|v| !v.is_empty()).unwrap_or(false),
                         Err(_) => false,
                    };

                    if !exists {
                         let create_opts = CreateVolumeOptions {
                             name: volume_name.clone(),
                             labels: labels.clone(), // Tag volume with same labels (managed_by=zorp)
                             ..Default::default()
                         };
                         if let Err(e) = docker.create_volume(create_opts).await {
                             warn!("[{}] Failed to create cache volume {}: {}", job.id, volume_name, e);
                         } else {
                             info!("[{}] Created cache volume: {}", job.id, volume_name);
                         }
                    }

                    if let Some(paths) = &job.cache_paths {
                        for path in paths {
                            binds.push(format!("{}:{}", volume_name, path));
                        }
                    }
                }

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
                        binds: Some(binds), // Added Binds for Volumes
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
                            // Spawn log streaming in a separate task
                            let docker_log = docker.clone();
                            let container_name_log = container_name.clone();
                            let job_id_log = job.id.clone();
                            let log_publisher_log = log_publisher.clone();
                            
                            let log_handle = tokio::spawn(async move {
                                stream_logs_to_file_and_broadcast(&docker_log, &container_name_log, &job_id_log, &log_publisher_log).await
                            });

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

                            // Wait for log streaming to finish
                            log_file_path = match log_handle.await {
                                Ok(path) => path,
                                Err(e) => {
                                    error!("Log stream task panicked or cancelled: {}", e);
                                    String::new()
                                }
                            };

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
                                        // Decoupled Artifact Upload
                                        if let Some(bucket) = s3_bucket.as_ref() {
                                            let key = format!("artifacts/{}.tar", job.id);
                                            final_artifact_url = format!("s3://{}/{}", bucket, key);
                                            
                                            let task = UploadTask {
                                                job_id: job.id.clone(),
                                                file_path: art_file_path.clone(),
                                                upload_type: "artifact".to_string(),
                                                s3_key: key,
                                            };
                                            
                                            info!("[{}] Enqueuing artifact upload...", job.id);
                                            if let Err(e) = queue.enqueue_upload(task).await {
                                                error!("[{}] Failed to enqueue artifact upload: {}", job.id, e);
                                            }
                                        }
                                    }
                                    // Do NOT remove file here. UploadWorker will remove it.
                                    // let _ = tokio::fs::remove_file(path_ref).await;
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

                if !job.debug {
                    let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                } else {
                    info!("[{}] Debug mode enabled. Container kept.", job.id);
                }
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

            // --- JOB CHAINING (DAG) ---
            if final_exit_code == 0 && final_status != "FAILED" && final_status != "TIMED_OUT" && final_status != "CANCELLED" {
                if !job.on_success.is_empty() {
                    info!("[{}] Triggering {} downstream jobs...", job.id, job.on_success.len());
                    for next_req in &job.on_success {
                        let next_id = Uuid::new_v4().to_string();
                        // Persist to DB first
                         let query = format!(
                            "INSERT INTO jobs (id, status, image, commands, callback_url, user_id) VALUES ({}, 'QUEUED', {}, {}, {}, {})",
                            sql_placeholder(1), sql_placeholder(2), sql_placeholder(3), sql_placeholder(4), sql_placeholder(5)
                        );

                        // Inherit user_id from parent if not specified
                        let next_user_id = next_req.user.clone().or(job.user.clone());

                        let insert_result = sqlx::query(&query)
                            .bind(&next_id)
                            .bind(&next_req.image)
                            .bind(serde_json::to_string(&next_req.commands).unwrap_or_default())
                            .bind(&next_req.callback_url)
                            .bind(&next_user_id)
                            .execute(&db).await;
                        
                        if let Ok(_) = insert_result {
                            let next_context = JobContext {
                                id: next_id.clone(),
                                image: next_req.image.clone(),
                                commands: next_req.commands.clone(),
                                env: next_req.env.clone(),
                                limits: next_req.limits.clone().map(Arc::new),
                                callback_url: next_req.callback_url.clone(),
                                timeout_seconds: next_req.timeout_seconds,
                                artifacts_path: next_req.artifacts_path.clone(),
                                user: next_user_id,
                                cache_key: next_req.cache_key.clone(),
                                cache_paths: next_req.cache_paths.clone(),
                                on_success: next_req.on_success.clone(),
                                debug: next_req.debug,
                                priority: next_req.priority.clone(),
                                retry_count: 0,
                            };
                            
                            if let Err(e) = queue.enqueue(next_context).await {
                                error!("[{}] Failed to enqueue chained job {}: {}", job.id, next_id, e);
                            } else {
                                info!("[{}] Enqueued chained job: {}", job.id, next_id);
                            }
                        } else {
                             error!("[{}] Failed to persist chained job {}: {:?}", job.id, next_id, insert_result.err());
                        }
                    }
                }
            }

            if final_status == "FAILED" || final_status == "TIMED_OUT" || final_status == "CANCELLED" {
                metrics::inc_failed(final_status);
            } else {
                metrics::inc_completed();
            }
            metrics::dec_running();

            let duration = start_time.elapsed().as_secs_f64();

            // --- LOGS PERSISTENCE (DECOUPLED VIA REDIS QUEUE) ---
            // If we have a log file, enqueue for upload
            if !log_file_path.is_empty() {
                let key = format!("logs/{}.txt", job.id);
                // Construct S3 URL proactively for Webhook, even if not uploaded yet (Optimistic)
                if let Some(bucket) = &s3_bucket {
                    webhook_logs = format!("s3://{}/{}", bucket, key);
                }

                let task = UploadTask {
                    job_id: job.id.clone(),
                    file_path: log_file_path.clone(),
                    upload_type: "log".to_string(),
                    s3_key: key,
                };

                info!("[{}] Enqueuing log upload...", job.id);
                if let Err(e) = queue.enqueue_upload(task).await {
                    error!("[{}] Failed to enqueue log upload: {}", job.id, e);
                    // If enqueue fails, we might leave the file in /tmp to be GC'd later, 
                    // or we could mark job as failed. 
                    // Per prompt: "failure in upload should not fail job logic".
                    // But if we can't even enqueue, logs are lost.
                }
            } else if final_status != "FAILED" && final_status != "CANCELLED" {
                warn!("[{}] No logs were generated or captured.", job.id);
            }

            // --- ARTIFACT PERSISTENCE (DECOUPLED) ---
            // NOTE: Artifact upload was originally inside the main block.
            // We should also decouple it if we want to follow the pattern, 
            // but the artifacts are downloaded streamingly to a file in /tmp.
            // The original code uploaded it immediately. 
            // We can check if `final_artifact_url` is set.
            // Wait, the original code for artifacts:
            // "if let Ok(_) = s3.put_object()... final_artifact_url = ..."
            // I need to change that part too if I want full decoupling.
            // However, the prompt specifically mentioned "Upload log and artifact... sequential".
            // Let's modify the artifact part above (I need to find where I left it).
            // Actually, I missed modifying the artifact part in the previous search block because it was earlier.
            // I will address artifact upload decoupling in a separate replacement or assume logs are the main bottleneck.
            // But let's look at `final_artifact_url` usage below.
            
            // Dynamic SQL: UPDATE jobs SET status = $1, exit_code = $2, logs = $3, artifact_url = $4 WHERE id = $5 AND status != 'CANCELLED'
            // OPTIMISTIC LOCKING: We prevent overwriting 'CANCELLED' status.
            
            let update_final = format!(
                "UPDATE jobs SET status = {}, exit_code = {}, logs = {}, artifact_url = {} WHERE id = {} AND status != 'CANCELLED'",
                sql_placeholder(1), sql_placeholder(2), sql_placeholder(3), sql_placeholder(4), sql_placeholder(5)
            );

            match sqlx::query(&update_final)
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&webhook_logs) 
                .bind(&final_artifact_url)
                .bind(&job.id)
                .execute(&db).await 
            {
                Ok(result) => {
                    if result.rows_affected() == 0 {
                         warn!("[{}] Job was CANCELLED (or not found) during execution. Final update skipped.", job.id);
                         final_status = "CANCELLED"; // Correct local status for webhook/metrics if needed
                    }
                },
                Err(e) => {
                    error!("[{}] DB Update failed: {}", job.id, e);
                }
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
    
    let mut total_bytes_written: u64 = 0;
    let mut current_file_size: u64 = 0;
    
    // Limits
    const LOG_ROTATION_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB
    const LOG_HARD_LIMIT: u64 = 50 * 1024 * 1024; // 50MB (Total throughput)

    while let Ok(Some(log)) = stream.try_next().await {
        let msg = match log {
            LogOutput::StdOut { message } | LogOutput::Console { message } => message,
            LogOutput::StdErr { message } => message,
            _ => continue,
        };
        
        let len = msg.len() as u64;
        total_bytes_written += len;
        current_file_size += len;

        // Protection 1: Hard Limit on Total Throughput
        if total_bytes_written > LOG_HARD_LIMIT {
            let _ = publisher.publish(job_id, "\n[CRITICAL] LOG QUOTA EXCEEDED. KILLING CONTAINER.\n").await;
            error!("[{}] Log Hard Limit ({} MB) exceeded. Killing container...", job_id, LOG_HARD_LIMIT / 1024 / 1024);
            
            // Kill the container
            if let Err(e) = docker.kill_container::<String>(name, None).await {
                 error!("[{}] Failed to kill container after log overflow: {}", job_id, e);
            }
            break; 
        }

        // Protection 2: Rotation / Truncation
        if current_file_size > LOG_ROTATION_THRESHOLD {
            // Seek to beginning and reset length
            if let Err(e) = file.set_len(0).await {
                error!("Failed to truncate log file: {}", e);
            }
            if let Err(e) = file.seek(SeekFrom::Start(0)).await {
                error!("Failed to seek log file: {}", e);
            }
            current_file_size = 0;
            let warning = format!("\n--- [LOG TRUNCATED DUE TO SIZE LIMIT > {}MB] ---\n", LOG_ROTATION_THRESHOLD / 1024 / 1024);
            if let Err(e) = file.write_all(warning.as_bytes()).await {
                 error!("Failed to write warning to log file: {}", e);
            }
            current_file_size += warning.len() as u64;
        }

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

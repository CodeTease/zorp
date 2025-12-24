use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions, DownloadFromContainerOptions, ListContainersOptions,
    StopContainerOptions, NetworkingConfig,
};
use bollard::image::CreateImageOptions;
use bollard::volume::{CreateVolumeOptions, ListVolumesOptions}; 
use bollard::models::{HostConfig, EndpointSettings};
use bollard::Docker;
use futures_util::TryStreamExt; 
use futures_util::StreamExt;    
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncSeekExt, SeekFrom}; 
use tracing::{info, warn, error};
use crate::models::{JobContext, JobRegistry, UploadTask, ServiceRequest};
use crate::db::DbPool;
use crate::queue::JobQueue;
use crate::metrics;
use crate::streaming::RedisLogPublisher;
use bollard::network::CreateNetworkOptions;
use uuid::Uuid;
use crate::security;
use crate::cache;
use std::collections::HashMap;

use aws_sdk_s3;

const ZORP_NETWORK_NAME: &str = "zorp-net";

fn check_image_policy(image: &str) -> Result<(), String> {
    let allowed_images = std::env::var("ZORP_ALLOWED_IMAGES").unwrap_or_default();
    if allowed_images.is_empty() {
        return Ok(());
    }
    
    use regex::Regex;
    
    for pattern in allowed_images.split(',') {
        let pattern = pattern.trim();
        if pattern.is_empty() { continue; }
        
        if pattern.starts_with("regex:") {
            let re_str = pattern.trim_start_matches("regex:");
            if let Ok(re) = Regex::new(re_str) {
                if re.is_match(image) {
                    return Ok(());
                }
            } else {
                 warn!("Invalid regex policy: {}", re_str);
            }
        } else {
            if pattern.ends_with('*') {
                let prefix = pattern.trim_end_matches('*');
                if image.starts_with(prefix) {
                    return Ok(());
                }
            } else if image == pattern {
                return Ok(());
            }
        }
    }

    Err(format!("Image '{}' is not allowed by ZORP_ALLOWED_IMAGES policy.", image))
}

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

        let stop_opts = StopContainerOptions { t: 10 }; 
        match docker.stop_container(&container_id, Some(stop_opts)).await {
            Ok(_) => {
                info!("Container {} stopped successfully.", container_id);
            }
            Err(e) => {
                warn!("Failed to gracefully stop container {}: {}. Attempting forced kill...", container_id, e);
                if let Err(k_e) = docker.kill_container::<String>(&container_id, None).await {
                     warn!("Failed to kill container {}: {}", container_id, k_e);
                }
            }
        }

        let mut q_builder = sqlx::query_builder::QueryBuilder::new("UPDATE jobs SET status = 'CANCELLED' WHERE id = ");
        q_builder.push_bind(job_id);

        if let Err(e) = q_builder.build().execute(db).await {
            error!("Failed to update job {} status to CANCELLED: {}", job_id, e);
        }

        return Ok(true);
    }

    warn!("Cancellation requested for job {}, but it was not found in active registry.", job_id);
    Ok(false)
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
            let mut q_running = sqlx::query_builder::QueryBuilder::new("UPDATE jobs SET status = 'RUNNING' WHERE id = ");
            q_running.push_bind(&job.id);
            
            let _ = q_running.build().execute(&db).await;

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

            // Global Max Limits (Security)
            let max_mem_mb = std::env::var("ZORP_MAX_MEMORY_MB")
                .ok().and_then(|v| v.parse::<i64>().ok()).unwrap_or(2048); // Default 2GB max
            let max_cpu_cores = std::env::var("ZORP_MAX_CPU_CORES")
                .ok().and_then(|v| v.parse::<f32>().ok()).unwrap_or(2.0); // Default 2 cores max

            let mut req_mem_mb = job.limits.as_ref()
                .and_then(|l| l.memory_mb)
                .unwrap_or(default_mem_mb);
            
            let mut req_cpu_cores = job.limits.as_ref()
                .and_then(|l| l.cpu_cores)
                .unwrap_or(default_cpu_cores);

            // Enforce Max Limits
            if req_mem_mb > max_mem_mb {
                 warn!("[{}] Requested Memory {}MB exceeds global limit {}MB. Clamping.", job.id, req_mem_mb, max_mem_mb);
                 req_mem_mb = max_mem_mb;
            }
            if req_cpu_cores > max_cpu_cores {
                 warn!("[{}] Requested CPU {} exceeds global limit {}. Clamping.", job.id, req_cpu_cores, max_cpu_cores);
                 req_cpu_cores = max_cpu_cores;
            }

            info!("[{}] Enforcing Resource Limits: Mem={}MB, CPU={}", job.id, req_mem_mb, req_cpu_cores);

            let memory = req_mem_mb * 1024 * 1024;
            let cpu_quota = (req_cpu_cores * 100000.0) as i64;

            // Ensure Network Exists
            if let Err(_) = docker.inspect_network::<String>(ZORP_NETWORK_NAME, None).await {
                let _ = docker.create_network(CreateNetworkOptions {
                    name: ZORP_NETWORK_NAME,
                    driver: "bridge",
                    check_duplicate: true,
                    ..Default::default()
                }).await;
            }

            // --- SPIN UP SERVICE CONTAINERS ---
            let mut service_containers = Vec::new();
            let mut service_error = None;

            for service in &job.services {
                info!("[{}] Starting service container: {} (Image: {})", job.id, service.alias, service.image);
                
                if let Err(e) = ensure_image(&docker, &service.image).await {
                    error!("[{}] Service image pull failed: {}", job.id, e);
                    service_error = Some(format!("Service {} failed: {}", service.alias, e));
                    break;
                }

                let service_name = format!("zorp-svc-{}-{}", job.id, service.alias);
                let mut svc_endpoints = HashMap::new();
                svc_endpoints.insert(ZORP_NETWORK_NAME.to_string(), EndpointSettings {
                    aliases: Some(vec![service.alias.clone()]),
                    ..Default::default()
                });

                let svc_env: Option<Vec<String>> = service.env.as_ref().map(|map| {
                    map.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
                });

                let svc_config = Config {
                    image: Some(service.image.clone()),
                    cmd: service.command.clone(),
                    env: svc_env,
                    hostname: Some(service.alias.clone()),
                    networking_config: Some(NetworkingConfig {
                        endpoints_config: svc_endpoints,
                    }),
                    host_config: Some(HostConfig {
                        network_mode: Some(ZORP_NETWORK_NAME.to_string()),
                        auto_remove: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                match docker.create_container(
                    Some(CreateContainerOptions { name: service_name.as_str(), platform: None }), 
                    svc_config
                ).await {
                    Ok(_) => {
                        if let Err(e) = docker.start_container::<String>(&service_name, None).await {
                             error!("[{}] Failed to start service {}: {}", job.id, service.alias, e);
                             service_error = Some(format!("Failed to start service {}: {}", service.alias, e));
                             break;
                        } else {
                            service_containers.push(service_name);
                        }
                    },
                    Err(e) => {
                        error!("[{}] Failed to create service {}: {}", job.id, service.alias, e);
                        service_error = Some(format!("Failed to create service {}: {}", service.alias, e));
                        break;
                    }
                }
            }

            // Input Validation & Policy Check
            // --- HOT VOLUMES (CACHING) ---
            // Move cache logic OUTSIDE of validation/image check blocks to ensure it scopes correctly for later use
            let mut binds = Vec::new();
            let host_cache_path = if let Some(cache_key) = &job.cache_key {
                let path = std::path::PathBuf::from(format!("/tmp/zorp-cache/{}", cache_key));
                
                // Restore Cache from S3
                if let (Some(s3_client), Some(bucket)) = (&s3_client, &s3_bucket) {
                    if let Err(e) = cache::restore_cache(s3_client, bucket, cache_key, &path).await {
                        warn!("[{}] Failed to restore cache: {}", job.id, e);
                    } else {
                        info!("[{}] Cache restored successfully.", job.id);
                    }
                }

                // Create dir if not exists (in case S3 restore failed or was empty)
                if !path.exists() {
                        let _ = tokio::fs::create_dir_all(&path).await;
                }

                if let Some(paths) = &job.cache_paths {
                    for container_path in paths {
                        // Use Bind Mount from Host Path
                        binds.push(format!("{}:{}", path.to_string_lossy(), container_path));
                    }
                }
                Some(path)
            } else {
                None
            };
            
            let mut validation_error = None;
            if let Some(url) = &job.callback_url {
                if !url.starts_with("http://") && !url.starts_with("https://") {
                     validation_error = Some("Callback URL must start with http:// or https://".to_string());
                }
            }

            if let Some(err) = service_error {
                 error!("[{}] Service Startup Failed: {}", job.id, err);
                 let _ = log_publisher.publish(&job.id, &format!("ERROR: Service Startup Failed: {}\n", err)).await;
                 final_status = "FAILED";
                 final_exit_code = -1;
                 webhook_logs = format!("Service Error: {}", err);
            } else if let Some(err) = validation_error {
                 error!("[{}] Input Validation Failed: {}", job.id, err);
                 let _ = log_publisher.publish(&job.id, &format!("ERROR: Input Validation Failed: {}\n", err)).await;
                 final_status = "FAILED";
                 final_exit_code = -1;
                 webhook_logs = format!("Security Error: {}", err);
            } else if let Err(policy_err) = check_image_policy(&job.image) {
                 error!("[{}] Image Policy Violation: {}", job.id, policy_err);
                 let _ = log_publisher.publish(&job.id, &format!("ERROR: Image Policy Violation: {}\n", policy_err)).await;
                 final_status = "FAILED";
                 final_exit_code = -1;
                 webhook_logs = format!("Security Error: {}", policy_err);
            } else if let Err(e) = ensure_image(&docker, &job.image).await {
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
                        pids_limit: Some(std::env::var("ZORP_PIDS_LIMIT").ok().and_then(|v| v.parse().ok()).unwrap_or(100)),
                        // Tini init for proper signal handling (PID 1 zombie reaping)
                        init: Some(true),
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
                                                retry_count: 0,
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

            // Cleanup Service Containers
            for svc_name in service_containers {
                info!("[{}] Cleaning up service: {}", job.id, svc_name);
                let _ = docker.remove_container(&svc_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
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

            // --- SAVE CACHE ---
            if let (Some(host_path), Some(cache_key)) = (host_cache_path, &job.cache_key) {
                if final_status != "FAILED" && final_status != "TIMED_OUT" && final_status != "CANCELLED" {
                    if let (Some(s3_client), Some(bucket)) = (&s3_client, &s3_bucket) {
                         if let Err(e) = cache::save_cache(s3_client, bucket, cache_key, &host_path).await {
                             warn!("[{}] Failed to save cache: {}", job.id, e);
                         } else {
                             info!("[{}] Cache saved successfully.", job.id);
                         }
                    }
                }
                // Cleanup local cache dir to save space
                let _ = tokio::fs::remove_dir_all(&host_path).await;
            }

            // --- JOB CHAINING (DAG) ---
            if final_exit_code == 0 && final_status != "FAILED" && final_status != "TIMED_OUT" && final_status != "CANCELLED" {
                if !job.on_success.is_empty() {
                    info!("[{}] Triggering {} downstream jobs...", job.id, job.on_success.len());
                    for next_req in &job.on_success {
                        let next_id = Uuid::new_v4().to_string();
                        // Persist to DB first
                        // Inherit user_id from parent if not specified
                        let next_user_id = next_req.user.clone().or(job.user.clone());
                        let cmds_json = serde_json::to_string(&next_req.commands).unwrap_or_default();

                        let mut q_insert = sqlx::query_builder::QueryBuilder::new("INSERT INTO jobs (id, status, image, commands, callback_url, user_id) VALUES (");
                        q_insert.push_bind(&next_id);
                        q_insert.push(", 'QUEUED', ");
                        q_insert.push_bind(&next_req.image);
                        q_insert.push(", ");
                        q_insert.push_bind(cmds_json);
                        q_insert.push(", ");
                        q_insert.push_bind(&next_req.callback_url);
                        q_insert.push(", ");
                        q_insert.push_bind(&next_user_id);
                        q_insert.push(")");

                        let insert_result = q_insert.build().execute(&db).await;
                        
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
                                services: next_req.services.clone(), // Propagate services
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
                    retry_count: 0,
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
            
            let mut q_final = sqlx::query_builder::QueryBuilder::new("UPDATE jobs SET status = ");
            q_final.push_bind(final_status);
            q_final.push(", exit_code = ");
            q_final.push_bind(final_exit_code);
            q_final.push(", logs = ");
            q_final.push_bind(&webhook_logs);
            q_final.push(", artifact_url = ");
            q_final.push_bind(&final_artifact_url);
            q_final.push(" WHERE id = ");
            q_final.push_bind(&job.id);
            q_final.push(" AND status != 'CANCELLED'");

            match q_final.build().execute(&db).await {
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
                // Use the new Webhook Queue instead of direct call
                let webhook_payload = serde_json::json!({
                    "job_id": job.id,
                    "status": final_status,
                    "exit_code": final_exit_code,
                    "duration_seconds": duration,
                    "logs": webhook_logs,
                    "callback_url": url,
                    "attempt": 0
                });

                if let Ok(json_str) = serde_json::to_string(&webhook_payload) {
                    info!("[{}] Enqueuing webhook callback...", job.id);
                    if let Err(e) = queue.enqueue_webhook(json_str).await {
                        error!("[{}] Failed to enqueue webhook: {}", job.id, e);
                    }
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

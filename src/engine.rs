use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions, DownloadFromContainerOptions, ListContainersOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use futures_util::TryStreamExt; 
use futures_util::StreamExt;    
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tracing::{info, warn, error};
use crate::models::{JobContext, JobRegistry};
use crate::db::{DbPool, sql_placeholder};

#[cfg(feature = "s3_logging")]
use aws_sdk_s3;

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
        info!("Cancellation requested for job {}. Killing container {}...", job_id, container_id);

        match docker.kill_container::<String>(&container_id, None).await {
            Ok(_) => {
                info!("Container {} killed successfully.", container_id);
            }
            Err(e) => {
                // It might have already finished
                warn!("Failed to kill container {}: {}", container_id, e);
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

// --- THE ZOMBIE REAPER ---
pub fn spawn_reaper_task(docker: Docker, db: DbPool) {
    tokio::spawn(async move {
        info!("逐 Zombie Reaper task started (Interval: 1m).");
        loop {
            reap_zombies(&docker, &db).await;
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

// --- DISPATCHER LOGIC ---

pub struct Dispatcher {
    docker: Docker,
    db: DbPool,
    http_client: reqwest::Client,
    limiter: Arc<Semaphore>,
    job_registry: JobRegistry,

    #[cfg(feature = "s3_logging")]
    s3_client: Option<aws_sdk_s3::Client>,
    #[cfg(feature = "s3_logging")]
    s3_bucket: Option<String>,
}

impl Dispatcher {
    pub fn new(
        docker: Docker, 
        db: DbPool, 
        http_client: reqwest::Client, 
        max_concurrent: usize,
        job_registry: JobRegistry,
        #[cfg(feature = "s3_logging")] s3_client: Option<aws_sdk_s3::Client>,
        #[cfg(feature = "s3_logging")] s3_bucket: Option<String>,
    ) -> Self {
        Self {
            docker,
            db,
            http_client,
            limiter: Arc::new(Semaphore::new(max_concurrent)),
            job_registry,
            #[cfg(feature = "s3_logging")]
            s3_client,
            #[cfg(feature = "s3_logging")]
            s3_bucket,
        }
    }

    pub async fn dispatch(&self, job: JobContext) {
        let permit = self.limiter.clone().acquire_owned().await.unwrap();
        let docker = self.docker.clone();
        let db = self.db.clone();
        let http_client = self.http_client.clone();
        let job_registry = self.job_registry.clone();
        
        #[cfg(feature = "s3_logging")]
        let s3_client = self.s3_client.clone();
        #[cfg(feature = "s3_logging")]
        let s3_bucket = self.s3_bucket.clone();
        
        tokio::spawn(async move {
            let _permit = permit; 
            let start_time = Instant::now();
            let container_name = format!("zorp-{}", job.id);

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

            let mut final_status = "FINISHED";
            let mut final_exit_code = 0;
            // captured_logs is now replaced by file streaming, but we keep a variable for webhook/db fallback
            let mut webhook_logs = String::new();
            let mut log_file_path = String::new();
            let mut final_artifact_url = String::new();

            let env_vars: Option<Vec<String>> = job.env.map(|map| {
                map.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
            });

            let memory = job.limits.as_ref()
                .and_then(|l| l.memory_mb)
                .map(|mb| mb * 1024 * 1024);
            
            let cpu_quota = job.limits.as_ref()
                .and_then(|l| l.cpu_cores)
                .map(|cores| (cores * 100000.0) as i64);

            if let Err(e) = ensure_image(&docker, &job.image).await {
                error!("[{}] Image pull failed: {}", job.id, e);
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
                        memory: memory.or(Some(1024 * 1024 * 512)), 
                        cpu_quota: cpu_quota.or(Some(100000)),
                        // Security Context
                        readonly_rootfs: Some(true),
                        cap_drop: Some(vec!["ALL".to_string()]),
                        pids_limit: Some(100),
                        // We might need to keep CapAdd empty or minimal.
                        // If user needs specific caps, they aren't in JobRequest yet.
                        ..Default::default()
                    }),
                    // Run as non-root (nobody:nogroup usually 65534:65534 or 1000:1000)
                    // But depends on image. If image doesn't have user, might fail?
                    // "Non-root User" prompt. "Configure Docker to run process with UID != 0".
                    user: Some("1000:1000".to_string()),
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
                        if let Err(e) = docker.start_container(&container_name, None::<StartContainerOptions<String>>).await {
                             error!("[{}] Start failed: {}", job.id, e);
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
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] Wait stream error: {}", job.id, e);
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
                                    let _ = docker.kill_container::<String>(&container_name, None).await;
                                    final_status = "TIMED_OUT";
                                }
                            }

                            // Collect logs to file
                            log_file_path = stream_logs_to_file(&docker, &container_name, &job.id).await;

                            // Collect Artifacts
                            if let Some(art_path) = &job.artifacts_path {
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
                                        #[cfg(feature = "s3_logging")]
                                        if let (Some(s3), Some(bucket)) = (s3_client.as_ref(), s3_bucket.as_ref()) {
                                            let key = format!("artifacts/{}.tar", job.id);
                                             match aws_sdk_s3::primitives::ByteStream::from_path(path_ref).await {
                                                 Ok(bs) => {
                                                     if let Ok(_) = s3.put_object().bucket(bucket).key(&key).body(bs).send().await {
                                                         final_artifact_url = format!("s3://{}/{}", bucket, key);
                                                         info!("[{}] Artifact uploaded: {}", job.id, final_artifact_url);
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
                        final_status = "FAILED";
                        final_exit_code = -3;
                        webhook_logs = format!("Error: Container creation failed: {}", e);
                    }
                }

                let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }

            // Clean up registry
            let removed_self = {
                let mut reg = job_registry.write().await;
                reg.remove(&job.id).is_some()
            };

            if !removed_self && final_status != "TIMED_OUT" {
                final_status = "CANCELLED";
            }

            let duration = start_time.elapsed().as_secs_f64();

            // --- LOGS PERSISTENCE ---
            let mut final_log_ref = webhook_logs.clone();

            // If we have a log file, process it
            if !log_file_path.is_empty() {
                // If S3 enabled, try upload
                #[cfg(feature = "s3_logging")]
                {
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
                                    Err(e) => error!("S3 upload failed: {}", e)
                                }
                            },
                            Err(e) => error!("Failed to read log file for S3 upload: {}", e)
                        }
                    }
                }

                // If final_log_ref is still empty (no S3 or failed), read file content
                if final_log_ref.is_empty() {
                     match File::open(&log_file_path).await {
                         Ok(mut file) => {
                             // Truncate to 1MB to avoid OOM
                             let mut content = String::new();
                             let mut handle = file.take(1024 * 1024); // 1MB
                             if let Ok(_) = handle.read_to_string(&mut content).await {
                                 if content.len() >= 1024 * 1024 {
                                     content.push_str("\n[Logs truncated due to size limit]");
                                 }
                                 final_log_ref = content.clone();
                                 webhook_logs = content;
                             }
                         },
                         Err(e) => error!("Failed to open log file back: {}", e)
                     }
                }

                // Remove temp file
                let _ = tokio::fs::remove_file(&log_file_path).await;
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

            if let Some(url) = job.callback_url {
                let payload = serde_json::json!({
                    "job_id": job.id,
                    "status": final_status,
                    "exit_code": final_exit_code,
                    "duration_seconds": duration,
                    "logs": webhook_logs
                });

                let send_result = http_client.post(&url).json(&payload).send().await;
                if let Err(e) = send_result {
                     warn!("[{}] Webhook failed (1/2): {}. Retrying...", job.id, e);
                     tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                     let _ = http_client.post(&url).json(&payload).send().await
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

async fn stream_logs_to_file(docker: &Docker, name: &str, job_id: &str) -> String {
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
        if let Err(e) = file.write_all(&msg).await {
             error!("Failed to write to log file: {}", e);
             break;
        }
    }

    filename
}
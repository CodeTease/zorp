use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use futures_util::TryStreamExt; 
use futures_util::StreamExt;    
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, warn, error};
use crate::models::JobContext;

#[cfg(feature = "s3_logging")]
use aws_sdk_s3;

// --- THE ZOMBIE REAPER ---
// Scans and cleans up "hanging" containers or inconsistent DB states upon restart.
pub async fn startup_reaper(docker: &Docker, db: &SqlitePool) {
    info!("💀 Zombie Reaper is scanning for lost souls...");

    // 1. Fetch RUNNING jobs from DB
    let running_jobs = sqlx::query_as::<_, (String,)>("SELECT id FROM jobs WHERE status = 'RUNNING'")
        .fetch_all(db)
        .await;

    match running_jobs {
        Ok(jobs) => {
            if jobs.is_empty() {
                info!("💀 Reaper: No zombies found. Clean start.");
                return;
            }

            for (job_id,) in jobs {
                let container_name = format!("zorp-{}", job_id);
                info!("💀 Reaper: Checking potential zombie: {}", container_name);

                // Check if container actually exists in Docker
                match docker.inspect_container(&container_name, None).await {
                    Ok(inspect) => {
                        let state = inspect.state.unwrap_or_default();
                        if state.running.unwrap_or(false) {
                            // Still running -> Kill it to reset state
                            warn!("💀 Reaper: Found active zombie {}. Terminating...", job_id);
                            let _ = docker.kill_container::<String>(&container_name, None).await;
                        }
                        // Remove container artifacts
                        let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                    }
                    Err(_) => {
                        // Container not found in Docker -> DB state is stale
                        warn!("💀 Reaper: Ghost job {} found in DB but not in Docker.", job_id);
                    }
                }

                // Update DB to FAILED
                let _ = sqlx::query("UPDATE jobs SET status = 'FAILED', logs = ? WHERE id = ?")
                    .bind("System Crash: Job terminated by Zombie Reaper during startup.")
                    .bind(&job_id)
                    .execute(db).await;
            }
        }
        Err(e) => error!("💀 Reaper failed to query DB: {}", e),
    }
}

// --- DISPATCHER LOGIC ---

pub struct Dispatcher {
    docker: Docker,
    db: SqlitePool,
    http_client: reqwest::Client,
    limiter: Arc<Semaphore>,

    #[cfg(feature = "s3_logging")]
    s3_client: Option<aws_sdk_s3::Client>,
    #[cfg(feature = "s3_logging")]
    s3_bucket: Option<String>,
}

impl Dispatcher {
    pub fn new(
        docker: Docker, 
        db: SqlitePool, 
        http_client: reqwest::Client, 
        max_concurrent: usize,
        #[cfg(feature = "s3_logging")] s3_client: Option<aws_sdk_s3::Client>,
        #[cfg(feature = "s3_logging")] s3_bucket: Option<String>,
    ) -> Self {
        Self {
            docker,
            db,
            http_client,
            limiter: Arc::new(Semaphore::new(max_concurrent)),
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
        
        #[cfg(feature = "s3_logging")]
        let s3_client = self.s3_client.clone();
        #[cfg(feature = "s3_logging")]
        let s3_bucket = self.s3_bucket.clone();
        
        tokio::spawn(async move {
            let _permit = permit; 
            let start_time = Instant::now();
            let container_name = format!("zorp-{}", job.id);

            let _ = sqlx::query("UPDATE jobs SET status = 'RUNNING' WHERE id = ?")
                .bind(&job.id)
                .execute(&db).await;

            info!("[{}] Status: RUNNING", job.id);

            let mut final_status = "FINISHED";
            let mut final_exit_code = 0;
            let captured_logs: String;

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
                captured_logs = format!("System Error: Image pull failed. {}", e);
            } else {
                let config = Config {
                    image: Some(job.image.clone()),
                    cmd: Some(job.commands.clone()),
                    env: env_vars,
                    host_config: Some(HostConfig {
                        auto_remove: Some(false), 
                        memory: memory.or(Some(1024 * 1024 * 512)), 
                        cpu_quota: cpu_quota.or(Some(100000)),
                        ..Default::default()
                    }),
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
                             captured_logs = format!("Error: Start failed: {}", e);
                        } else {
                            // Wait logic
                            let mut wait_stream = docker.wait_container(
                                &container_name, 
                                None::<WaitContainerOptions<String>>
                            );

                            match wait_stream.next().await {
                                Some(Ok(res)) => {
                                    final_exit_code = res.status_code as i32;
                                    info!("[{}] Container exited with {}", job.id, final_exit_code);
                                }
                                Some(Err(e)) => {
                                    error!("[{}] Wait stream error: {}", job.id, e);
                                    final_status = "FAILED"; 
                                    final_exit_code = -99;
                                }
                                None => {
                                    warn!("[{}] Wait stream closed unexpectedly", job.id);
                                    if let Ok(inspect) = docker.inspect_container(&container_name, None).await {
                                        if let Some(state) = inspect.state {
                                            final_exit_code = state.exit_code.unwrap_or(-98) as i32;
                                        }
                                    }
                                }
                            }

                            captured_logs = collect_logs(&docker, &container_name).await;
                        }
                    }
                    Err(e) => {
                         error!("[{}] Create failed: {}", job.id, e);
                        final_status = "FAILED";
                        final_exit_code = -3;
                        captured_logs = format!("Error: Container creation failed: {}", e);
                    }
                }

                let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }

            let duration = start_time.elapsed().as_secs_f64();

            // --- LOGS PERSISTENCE ---
            let mut final_log_ref = captured_logs.clone();

            #[cfg(feature = "s3_logging")]
            if let (Some(s3), Some(bucket)) = (s3_client.as_ref(), s3_bucket.as_ref()) {
                let key = format!("logs/{}.txt", job.id);
                let byte_stream = aws_sdk_s3::primitives::ByteStream::from(captured_logs.clone().into_bytes());
                
                info!("[{}] Uploading logs to S3 bucket '{}' with key '{}'", job.id, bucket, key);

                match s3.put_object().bucket(bucket).key(&key).body(byte_stream).send().await {
                    Ok(_) => {
                        final_log_ref = format!("s3://{}/{}", bucket, key);
                        info!("[{}] S3 upload successful.", job.id);
                    }
                    Err(e) => {
                        error!("[{}] S3 upload failed: {}. Falling back to database.", job.id, e);
                        // final_log_ref is already set to captured_logs
                    }
                }
            }
            
            if let Err(e) = sqlx::query("UPDATE jobs SET status = ?, exit_code = ?, logs = ? WHERE id = ?")
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&final_log_ref)
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
                    "logs": captured_logs
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

async fn collect_logs(docker: &Docker, name: &str) -> String {
    let options = Some(LogsOptions::<String> {
        stdout: true, stderr: true, follow: true, tail: "all".to_string(), ..Default::default()
    });
    let mut stream = docker.logs(name, options);
    let mut buffer = String::new();
    while let Ok(Some(log)) = stream.try_next().await {
        let msg = match log {
            LogOutput::StdOut { message } | LogOutput::Console { message } => String::from_utf8_lossy(&message).to_string(),
            LogOutput::StdErr { message } => String::from_utf8_lossy(&message).to_string(),
            _ => String::new(),
        };
        buffer.push_str(&msg);
    }
    buffer
}
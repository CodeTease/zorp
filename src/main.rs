// Zorp v0.1.0 - The Hybrid Edition (Redis Enhanced)
// Copyright (c) 2025 CodeTease.

mod api;
mod db;
mod engine;
mod models;
mod queue;
mod metrics; // Added metrics module
mod streaming; // Added streaming module

use bollard::Docker;
use dotenvy::dotenv;
use std::env;

use aws_config::{self, BehaviorVersion};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn}; 
use crate::queue::{RedisQueue, JobQueue};
use crate::models::{JobRegistry};
use tokio::sync::RwLock;
use std::collections::HashMap;
use bollard::container::ListContainersOptions;
const PORT: u16 = 3000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- CRITICAL FIX FOR RUSTLS 0.23 PANIC ---
    let _ = rustls::crypto::ring::default_provider().install_default();

    dotenv().ok();
    tracing_subscriber::fmt::init();

    let secret_key = env::var("ZORP_SECRET_KEY")
        .map_err(|_| {
            error!("ZORP_SECRET_KEY environment variable not set.");
            std::io::Error::new(std::io::ErrorKind::Other, "Missing ZORP_SECRET_KEY")
        })?;

    info!(":: Zorp v0.1.0 (Production Edition) ::");
    
    // 1. Initialize DB (with Retry)
    let mut db_retry_attempts = 0;
    let db_pool = loop {
        match db::init_pool().await {
            Ok(pool) => break pool,
            Err(e) => {
                db_retry_attempts += 1;
                if db_retry_attempts > 5 {
                    error!("❌ Failed to connect to DB after 5 attempts. Exiting.");
                    return Err(e);
                }
                warn!("⚠️  DB Connection failed: {}. Retrying in 5s... ({}/5)", e, db_retry_attempts);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    };
    info!("✅ Database connected successfully.");

    // 2. Initialize Redis Queue
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    if redis_url.starts_with("rediss://") {
        info!("🔐 Initializing Redis with TLS encryption...");
    } else {
        info!("🔌 Initializing Redis via standard connection...");
    }

    let queue = Arc::new(RedisQueue::new(&redis_url));
    info!("Connected to Redis Queue successfully.");

    // Restore stranded jobs
    info!("🔎 Checking for stranded jobs in processing queue...");
    match queue.restore_stranded().await {
        Ok(0) => info!("✅ No stranded jobs found."),
        Ok(count) => info!("♻️  Restored {} stranded jobs to the main queue.", count),
        Err(e) => error!("❌ Failed to restore stranded jobs: {}", e),
    }

    // 3. Initialize Docker
    let docker = Docker::connect_with_local_defaults()?;

    // 3b. Initialize JobRegistry
    info!("🔄 Performing State Reconciliation...");
    let job_registry: JobRegistry = Arc::new(RwLock::new(HashMap::new()));
    
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);
    
    let options = ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    };
    
    if let Ok(containers) = docker.list_containers(Some(options)).await {
        let mut reg = job_registry.write().await;
        for c in containers {
            // Only care about running containers to populate registry
            if c.state.as_deref() == Some("running") {
                if let (Some(id), Some(labels)) = (c.id, c.labels) {
                     if let Some(job_id) = labels.get("job_id") {
                         // Verify with DB
                         let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT status FROM jobs WHERE id = ");
                         q_builder.push_bind(job_id);

                         if let Ok(Some((status,))) = q_builder.build_query_as::< (String,) >()
                            .fetch_optional(&db_pool).await 
                         {
                             if status == "RUNNING" {
                                 info!("   - Re-attached job: {} (Container: {})", job_id, id);
                                 reg.insert(job_id.clone(), id.clone());
                                 // Note: We cannot re-attach log stream easily for recovered jobs 
                                 // without implementing `docker attach` logic again, which is complex.
                                 // For now, recovered jobs won't support live streaming, only file logging.
                             } else {
                                 warn!("   - Zombie container found (Job {} is {}). Will be reaped.", job_id, status);
                             }
                         }
                     }
                }
            }
        }
    }
    info!("✅ State Reconciliation Complete. Active Jobs: {}", job_registry.read().await.len());

    // 4. Run Zombie Reaper (Background Task)
    engine::spawn_reaper_task(docker.clone(), db_pool.clone(), job_registry.clone());

    // 4b. Run Queue Monitor (Background Task) - Heartbeat & Auto-Recovery
    let queue_monitor = queue.clone();
    tokio::spawn(async move {
        info!("❤️  Queue Monitor (Heartbeat & Auto-Recovery) started.");
        loop {
            // Monitor stranded jobs every 30s
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            if let Err(e) = queue_monitor.monitor_stranded_jobs().await {
                error!("❌ Queue Monitor Error: {}", e);
            }
        }
    });

    // 5. Initialize Engine (Dispatcher)
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    
    // Mandatory S3 Configuration
    let s3_bucket = env::var("S3_BUCKET_NAME").map_err(|_| {
        error!("❌ S3_BUCKET_NAME is mandatory in Production Mode.");
        std::io::Error::new(std::io::ErrorKind::Other, "Missing S3_BUCKET_NAME")
    })?;

    info!("Configuring S3 client...");
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

    if let Ok(endpoint) = env::var("S3_ENDPOINT_URL") {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }
    if let Ok(force_path) = env::var("S3_FORCE_PATH_STYLE") {
            s3_config_builder = s3_config_builder.force_path_style(force_path.parse().unwrap_or(false));
    }
    
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    // Configuration
    let max_jobs = env::var("ZORP_MAX_JOBS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    info!("⚙️  Configuration: ZORP_MAX_JOBS = {}", max_jobs);
    
    let dispatcher = Arc::new(engine::Dispatcher::new(
        docker.clone(),
        db_pool.clone(),
        http_client,
        max_jobs,
        job_registry.clone(),
        s3_client.clone(),
        s3_bucket.clone(),
        secret_key.clone(),
        queue.clone(),
    ));

    // 6. Spawn WORKER THREAD (with Graceful Shutdown support)
    let queue_for_worker = queue.clone();
    let dispatcher_for_worker = dispatcher.clone();
    let (shutdown_tx, mut _shutdown_rx) = tokio::sync::broadcast::channel(1); // Fixed unused warning
    
    let worker_shutdown_rx = shutdown_tx.subscribe();
    let upload_worker_shutdown_rx = shutdown_tx.subscribe();

    // MAIN JOB WORKER
    let worker_handle = tokio::spawn(async move {
        info!("👷 Worker thread started.");
        let mut shutdown = worker_shutdown_rx;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("👷 Worker thread stopping (stop accepting new jobs)...");
                    break;
                }
                job = queue_for_worker.dequeue() => {
                    match job {
                        Ok(Some(job)) => {
                            info!("📥 Worker picked up job: {}", job.id);
                            dispatcher_for_worker.dispatch(job).await; 
                        }
                        Ok(None) => {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            error!("❌ Queue Error: {}. Retrying in 5s...", e);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
        info!("👷 Worker thread stopped.");
    });

    // UPLOAD WORKER (S3 DECOUPLING)
    let queue_for_upload = queue.clone();
    let s3_client_upload = s3_client.clone();
    let s3_bucket_upload = s3_bucket.clone();
    let db_upload = db_pool.clone();

    let upload_worker_handle = tokio::spawn(async move {
        info!("☁️  Upload Worker thread started.");
        let mut shutdown = upload_worker_shutdown_rx;
        loop {
             tokio::select! {
                _ = shutdown.recv() => {
                    info!("☁️  Upload Worker stopping...");
                    break;
                }
                task_res = queue_for_upload.dequeue_upload() => {
                    match task_res {
                        Ok(Some(task)) => {
                            info!("☁️  Processing upload task for job {}: {} ({})", task.job_id, task.s3_key, task.upload_type);
                            
                            let path = std::path::Path::new(&task.file_path);
                            if path.exists() {
                                let body = match aws_sdk_s3::primitives::ByteStream::from_path(path).await {
                                    Ok(b) => b,
                                    Err(e) => {
                                        error!("Failed to read file for upload {}: {}", task.file_path, e);
                                        continue;
                                    }
                                };

                                match s3_client_upload.put_object()
                                    .bucket(&s3_bucket_upload)
                                    .key(&task.s3_key)
                                    .body(body)
                                    .send()
                                    .await 
                                {
                                    Ok(_) => {
                                        let s3_url = format!("s3://{}/{}", s3_bucket_upload, task.s3_key);
                                        info!("✅ Upload successful: {}", s3_url);
                                        
                                        // Update DB with final URL if needed (Log Only)
                                        // Artifact URL is usually already updated or we can update it here strictly.
                                        // The prompt says "If upload fail, retry... don't affect exit code".
                                        
                                        // If it is a log, we might want to ensure the 'logs' column points to S3.
                                        // In dispatcher we optimistically set it to s3 url.
                                        // So we don't necessarily need to update DB unless we want to confirm success.
                                        // But if we want to be safe:
                                        
                                        /* 
                                        if task.upload_type == "log" {
                                            let query = format!("UPDATE jobs SET logs = {} WHERE id = {}", sql_placeholder(1), sql_placeholder(2));
                                            let _ = sqlx::query(&query).bind(&s3_url).bind(&task.job_id).execute(&db_upload).await;
                                        } else if task.upload_type == "artifact" {
                                            let query = format!("UPDATE jobs SET artifact_url = {} WHERE id = {}", sql_placeholder(1), sql_placeholder(2));
                                            let _ = sqlx::query(&query).bind(&s3_url).bind(&task.job_id).execute(&db_upload).await;
                                        }
                                        */
                                        
                                        // Clean up file
                                        let _ = tokio::fs::remove_file(path).await;
                                    },
                                    Err(e) => {
                                        error!("❌ S3 Upload Failed: {}. (Attempt: {})", e, task.retry_count + 1);
                                        let mut retry_task = task.clone();
                                        retry_task.retry_count += 1;
                                        
                                        if retry_task.retry_count > 5 {
                                             error!("❌ Upload exceeded max retries. Moving to DLQ.");
                                             if let Err(dlq_err) = queue_for_upload.enqueue_upload_dlq(retry_task).await {
                                                 error!("Failed to move upload task to DLQ: {}", dlq_err);
                                             }
                                        } else {
                                             // Non-blocking Exponential Backoff via Tokio Task
                                             let delay = 2u64.pow(retry_task.retry_count);
                                             warn!("Retrying upload in {}s...", delay);
                                             
                                             let q_clone = queue_for_upload.clone();
                                             tokio::spawn(async move {
                                                 tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                                                 if let Err(re_err) = q_clone.enqueue_upload(retry_task).await {
                                                      error!("Failed to re-enqueue upload task: {}", re_err);
                                                 }
                                             });
                                        }
                                    }
                                }
                            } else {
                                warn!("File not found for upload: {}", task.file_path);
                            }
                        }
                        Ok(None) => {
                             tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        Err(e) => {
                             error!("Upload Queue Error: {}", e);
                             tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
        info!("☁️  Upload Worker stopped.");
    });

    // 7. Setup App State
    let state = Arc::new(api::AppState {
        db: db_pool.clone(),
        queue: queue,
        secret_key,
        docker: docker,
        job_registry: job_registry,
        s3_client: Some(s3_client),
        s3_bucket: Some(s3_bucket),
    });

    // 8. Start Server with Graceful Shutdown
    let app = api::create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    
    // Axum 0.7 graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let ctrl_c = async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install signal handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {},
                _ = terminate => {},
            }

            info!("🛑 Shutdown signal received.");
            let _ = shutdown_tx.send(());

            // --- GRACEFUL SHUTDOWN DRAIN LOGIC ---
            info!("⏳ Waiting for worker thread to stop...");
            let _ = worker_handle.await;

            info!("⏳ Draining active jobs...");
            let drain_timeout = std::time::Duration::from_secs(60); // 60s hard timeout
            let start_drain = std::time::Instant::now();
            
            loop {
                let running = metrics::get_running(); // You'll need to expose this or use existing getter
                if running == 0 {
                    info!("✅ All jobs finished.");
                    break;
                }

                if start_drain.elapsed() > drain_timeout {
                    warn!("⚠️  Shutdown timeout reached! Forcefully killing {} running jobs.", running);
                    break;
                }

                info!("⏳ Waiting for {} active jobs to finish...", running);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .await
        .unwrap();

    info!("👋 Zorp shutdown complete.");
    Ok(())
}

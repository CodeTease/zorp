// Zorp v0.1.0 - The Hybrid Edition (Redis Enhanced)
// Copyright (c) 2025 CodeTease.

mod api;
mod db;
mod engine;
mod models;
mod queue;
mod metrics;
mod streaming;
mod infrastructure;
mod workers;
mod security;

use dotenvy::dotenv;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn}; 
use crate::queue::JobQueue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- CRITICAL FIX FOR RUSTLS 0.23 PANIC ---
    let _ = rustls::crypto::ring::default_provider().install_default();

    dotenv().ok();

    if env::var("ZORP_LOG_FORMAT").unwrap_or_default().to_lowercase() == "json" {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt::init();
    }

    info!(":: Zorp v0.1.0 (Production Edition) ::");
    
    // 1. Setup Infrastructure (DB, Redis, Docker, S3)
    let infra = infrastructure::setup().await?;
    
    // 2. Start Background Workers
    // Reaper (with Leader Election)
    workers::reaper::spawn(
        infra.docker.clone(), 
        infra.db_pool.clone(), 
        infra.job_registry.clone(),
        infra.queue.clone(),
        infra.s3_client.clone(),
        infra.s3_bucket.clone()
    );

    // Queue Monitor
    let queue_monitor = infra.queue.clone();
    tokio::spawn(async move {
        info!("❤️  Queue Monitor (Heartbeat & Auto-Recovery) started.");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            if let Err(e) = queue_monitor.monitor_stranded_jobs().await {
                tracing::error!("❌ Queue Monitor Error: {}", e);
            }
        }
    });

    // 3. Initialize Engine (Dispatcher)
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    
    let secret_key = env::var("ZORP_SECRET_KEY")
        .expect("ZORP_SECRET_KEY environment variable not set.");

    // Configuration
    let max_jobs = env::var("ZORP_MAX_JOBS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    info!("⚙️  Configuration: ZORP_MAX_JOBS = {}", max_jobs);
    
    let dispatcher = Arc::new(engine::Dispatcher::new(
        infra.docker.clone(),
        infra.db_pool.clone(),
        http_client,
        max_jobs,
        infra.job_registry.clone(),
        infra.s3_client.clone(),
        infra.s3_bucket.clone(),
        secret_key.clone(),
        infra.queue.clone(),
    ));

    // 4. Spawn WORKER THREADS (with Graceful Shutdown support)
    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    let upload_worker_shutdown_rx = shutdown_tx.subscribe();

    let worker_count: usize = env::var("ZORP_WORKER_COUNT")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap_or(1);
    
    info!("🚀 Spawning {} worker threads...", worker_count);

    let mut worker_handles = Vec::new();

    for i in 0..worker_count {
        let queue_for_worker = infra.queue.clone();
        let dispatcher_for_worker = dispatcher.clone();
        let mut shutdown = shutdown_tx.subscribe();
        
        let handle = tokio::spawn(async move {
            info!("👷 Worker #{} thread started.", i + 1);
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("👷 Worker #{} thread stopping...", i + 1);
                        break;
                    }
                    job = queue_for_worker.dequeue() => {
                        match job {
                            Ok(Some(job)) => {
                                info!("📥 Worker #{} picked up job: {}", i + 1, job.id);
                                dispatcher_for_worker.dispatch(job).await; 
                            }
                            Ok(None) => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                tracing::error!("❌ Queue Error (Worker #{}): {}. Retrying in 5s...", i + 1, e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            }
                        }
                    }
                }
            }
            info!("👷 Worker #{} thread stopped.", i + 1);
        });
        worker_handles.push(handle);
    }

    // 5. Upload Worker (Moved to separate module)
    workers::upload::spawn(
        infra.queue.clone(), 
        infra.s3_client.clone(), 
        infra.s3_bucket.clone(), 
        infra.db_pool.clone(), 
        upload_worker_shutdown_rx
    );

    // 6. Setup App State
    let state = Arc::new(api::AppState {
        db: infra.db_pool.clone(),
        queue: infra.queue,
        secret_key,
        docker: infra.docker,
        job_registry: infra.job_registry,
        s3_client: Some(infra.s3_client),
        s3_bucket: Some(infra.s3_bucket),
    });

    // 7. Start Server with Graceful Shutdown
    let app = api::create_router(state);
    let port = PORT; // Use const from existing or just 3000
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
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
            info!("⏳ Waiting for worker threads to stop...");
            for handle in worker_handles {
                let _ = handle.await;
            }

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

const PORT: u16 = 3000;

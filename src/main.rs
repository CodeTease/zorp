// Zorp v0.1.0
// The Smart Ephemeral CI/CD Dispatcher (Log Collector Edition).
// Copyright (c) 2025 CodeTease.

use axum::{
    extract::{Path, State, Json},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePoolOptions, Sqlite, SqlitePool, Row};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

// --- CONFIGURATION ---
const PORT: u16 = 3000;
const MAX_CONCURRENT_JOBS: usize = 20;
const DB_URL: &str = "sqlite://zorp.db";

// --- DATA STRUCTURES ---

#[derive(Debug, Deserialize)]
struct JobLimits {
    memory_mb: Option<i64>,
    cpu_cores: Option<f32>,
}

#[derive(Debug, Deserialize)]
struct JobRequest {
    image: String,
    commands: Vec<String>,
    env: Option<HashMap<String, String>>,
    limits: Option<JobLimits>,
    callback_url: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct JobStatus {
    id: String,
    status: String,
    exit_code: Option<i32>,
    image: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct JobContext {
    id: String,
    image: String,
    commands: Vec<String>,
    env: Option<HashMap<String, String>>,
    limits: Option<Arc<JobLimits>>,
    callback_url: Option<String>,
}

struct AppState {
    docker: Docker,
    db: SqlitePool,
    http_client: reqwest::Client,
    limiter: Arc<Semaphore>,
}

// --- DATABASE SETUP ---

async fn init_db() -> Result<SqlitePool, Box<dyn std::error::Error>> {
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        tracing::info!("Creating database: {}", DB_URL);
        Sqlite::create_database(DB_URL).await?;
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(DB_URL).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            exit_code INTEGER,
            image TEXT NOT NULL,
            commands TEXT NOT NULL,
            callback_url TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        "#
    )
    .execute(&pool).await?;

    Ok(pool)
}

// --- ENGINE LOGIC ---

impl AppState {
    fn new(pool: SqlitePool) -> Result<Self, Box<dyn std::error::Error>> {
        let docker = Docker::connect_with_local_defaults()?;
        let http_client = reqwest::Client::new();
        
        Ok(Self {
            docker,
            db: pool,
            http_client,
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS)),
        })
    }

    async fn dispatch_job(&self, job: JobContext) {
        let permit = self.limiter.clone().acquire_owned().await.unwrap();
        let docker = self.docker.clone();
        let db = self.db.clone();
        let http_client = self.http_client.clone();
        
        tokio::spawn(async move {
            let _permit = permit;
            let start_time = Instant::now();
            let container_name = format!("zorp-{}", job.id);

            // UPDATE STATUS: RUNNING
            let _ = sqlx::query("UPDATE jobs SET status = 'RUNNING' WHERE id = ?")
                .bind(&job.id)
                .execute(&db).await;

            tracing::info!("[{}] Status: RUNNING", job.id);

            let mut final_status = "FINISHED";
            let mut final_exit_code = 0;
            let mut captured_logs = String::new(); // [NEW] Buffer to hold logs

            // 1. Env & Limits Setup
            let env_vars: Option<Vec<String>> = job.env.map(|map| {
                map.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
            });

            let memory = job.limits.as_ref()
                .and_then(|l| l.memory_mb)
                .map(|mb| mb * 1024 * 1024);
            
            let cpu_quota = job.limits.as_ref()
                .and_then(|l| l.cpu_cores)
                .map(|cores| (cores * 100000.0) as i64);

            // 2. Pull Image
            if let Err(e) = ensure_image(&docker, &job.image).await {
                tracing::error!("[{}] Image error: {}", job.id, e);
                final_status = "FAILED";
                final_exit_code = -1;
                captured_logs = format!("System Error: {}", e);
            } else {
                // 3. Configure & Create
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
                    tty: Some(true),
                    ..Default::default()
                };

                match docker.create_container(
                    Some(CreateContainerOptions { name: container_name.as_str(), platform: None }), 
                    config
                ).await {
                    Ok(_) => {
                        // 4. Start
                        if docker.start_container(&container_name, None::<StartContainerOptions<String>>).await.is_ok() {
                            
                            // [NEW] Capture Logs asynchronously
                            // We collect logs BEFORE waiting for exit to ensure we don't miss anything if it's fast
                            captured_logs = collect_logs(&docker, &container_name).await;

                            // Wait for exit
                            let wait_res = docker.wait_container(&container_name, None::<WaitContainerOptions<String>>)
                                .try_collect::<Vec<_>>()
                                .await;
                            
                            if let Ok(results) = wait_res {
                                if let Some(res) = results.first() {
                                    final_exit_code = res.status_code as i32;
                                }
                            }
                        } else {
                            final_status = "FAILED";
                            final_exit_code = -2;
                            captured_logs = "Error: Container failed to start.".to_string();
                        }
                    }
                    Err(e) => {
                        final_status = "FAILED";
                        final_exit_code = -3;
                        captured_logs = format!("Error: Container creation failed: {}", e);
                    }
                }

                // Cleanup
                let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }

            let duration = start_time.elapsed().as_secs_f64();

            // UPDATE DB
            let _ = sqlx::query("UPDATE jobs SET status = ?, exit_code = ? WHERE id = ?")
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&job.id)
                .execute(&db).await;

            tracing::info!("[{}] Status: {} (Exit: {}). Time: {:.2}s", job.id, final_status, final_exit_code, duration);

            // 5. WEBHOOK CALLBACK (With LOGS!)
            if let Some(url) = job.callback_url {
                let payload = serde_json::json!({
                    "job_id": job.id,
                    "status": final_status,
                    "exit_code": final_exit_code,
                    "duration_seconds": duration,
                    "logs": captured_logs // [NEW] The proof!
                });

                let _ = http_client.post(&url)
                    .json(&payload)
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await
                    .map_err(|e| tracing::warn!("[{}] Webhook failed: {:?}", job.id, e));
            }
        });
    }
}

async fn ensure_image(docker: &Docker, image: &str) -> Result<(), bollard::errors::Error> {
    let image = if !image.contains(':') { format!("{}:latest", image) } else { image.to_string() };
    if docker.inspect_image(&image).await.is_ok() { return Ok(()); }
    let mut stream = docker.create_image(Some(CreateImageOptions { from_image: image.clone(), ..Default::default() }), None, None);
    while let Some(_) = stream.try_next().await? {}
    Ok(())
}

// [NEW] Log Collector Helper
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
        // In ra console server để debug
        // print!("{}", msg); 
        // Và lưu vào buffer
        buffer.push_str(&msg);
    }
    buffer
}

// --- HTTP HANDLERS ---

async fn health_check() -> &'static str {
    "Zorp v0.1.0 (Log Collector) is running."
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JobRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let job_id = Uuid::new_v4().to_string();
    
    let insert_result = sqlx::query(
        "INSERT INTO jobs (id, status, image, commands, callback_url) VALUES (?, 'QUEUED', ?, ?, ?)"
    )
    .bind(&job_id)
    .bind(&payload.image)
    .bind(serde_json::to_string(&payload.commands).unwrap_or_default())
    .bind(&payload.callback_url)
    .execute(&state.db).await;

    match insert_result {
        Ok(_) => {
            let context = JobContext {
                id: job_id.clone(),
                image: payload.image,
                commands: payload.commands,
                env: payload.env,
                limits: payload.limits.map(Arc::new),
                callback_url: payload.callback_url,
            };

            state.dispatch_job(context).await;

            (StatusCode::ACCEPTED, Json(serde_json::json!({
                "status": "queued",
                "job_id": job_id
            })))
        },
        Err(e) => {
            tracing::error!("Database error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Failed to persist job"})))
        }
    }
}

async fn handle_get_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let row = sqlx::query_as::<_, JobStatus>(
        "SELECT id, status, exit_code, image, created_at FROM jobs WHERE id = ?"
    )
    .bind(job_id)
    .fetch_optional(&state.db).await;

    match row {
        Ok(Some(job)) => (StatusCode::OK, Json(serde_json::to_value(job).unwrap())),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Job not found"}))),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Database error"})))
    }
}

// --- MAIN ENTRYPOINT ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!(":: Zorp v0.1.0 (Log Collector) ::");
    
    let db_pool = init_db().await?;
    let state = Arc::new(AppState::new(db_pool)?);

    let app = Router::new()
        .route("/", get(health_check))
        .route("/dispatch", post(handle_dispatch))
        .route("/job/:id", get(handle_get_job))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    tracing::info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

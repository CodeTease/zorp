// Zorp v0.1.0 - The Bulletproof Edition
// Copyright (c) 2025 CodeTease.

use axum::{
    async_trait,
    extract::{FromRequestParts, Path, State, Json, Query},
    http::{header, request::Parts, StatusCode},
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
use futures_util::StreamExt;    
use serde::{Deserialize, Serialize};
use sqlx::{migrate::MigrateDatabase, sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Sqlite, SqlitePool, Row, ConnectOptions}; 
use std::collections::HashMap;
use std::env;
use dotenvy::dotenv;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, warn, error};
use uuid::Uuid;
use std::str::FromStr;

// --- CONFIGURATION ---
const PORT: u16 = 3000;
const MAX_CONCURRENT_JOBS: usize = 50; 
const DB_URL: &str = "sqlite://zorp.db";

// --- AUTHENTICATION ---
struct Auth;

#[async_trait]
impl FromRequestParts<Arc<AppState>> for Auth
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        if let Some(auth_header) = parts.headers.get(header::AUTHORIZATION) {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(token) = auth_str.strip_prefix("Bearer ") {
                    if token == state.secret_key {
                        return Ok(Self);
                    }
                }
            }
        }
        Err((StatusCode::UNAUTHORIZED, "Unauthorized"))
    }
}

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
    secret_key: String,
}

// --- DATABASE SETUP ---

async fn init_db() -> Result<SqlitePool, Box<dyn std::error::Error>> {
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        info!("Creating database: {}", DB_URL);
        Sqlite::create_database(DB_URL).await?;
    }

    // TỐI ƯU HÓA SQLITE CHO CONCURRENCY
    let options = SqliteConnectOptions::from_str(DB_URL)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal) // Write-Ahead Logging
        .busy_timeout(std::time::Duration::from_secs(5))
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(50) 
        .connect_with(options).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            exit_code INTEGER,
            image TEXT NOT NULL,
            commands TEXT NOT NULL,
            logs TEXT,
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
    fn new(pool: SqlitePool, secret_key: String) -> Result<Self, Box<dyn std::error::Error>> {
        let docker = Docker::connect_with_local_defaults()?;
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        
        Ok(Self {
            docker,
            db: pool,
            http_client,
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS)),
            secret_key,
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

            if let Err(e) = sqlx::query("UPDATE jobs SET status = ?, exit_code = ?, logs = ? WHERE id = ?")
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&captured_logs)
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

// --- HTTP HANDLERS ---

async fn health_check() -> &'static str {
    "Zorp v0.1.0 is running."
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    _: Auth,
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
            error!("Database error during dispatch: {}", e);
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
        Ok(Some(job)) => (StatusCode::OK, Json(serde_json::to_value(job).unwrap_or_default())),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Job not found"}))),
        Err(e) => {
            error!("DB error get_job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Database error"})))
        }
    }
}

async fn handle_get_job_logs(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result = sqlx::query("SELECT logs FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_optional(&state.db)
        .await;

    match result {
        Ok(Some(row)) => {
            let logs: Option<String> = row.get("logs");
            (StatusCode::OK, Json(serde_json::json!({ "logs": logs.unwrap_or_default() })))
        },
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Job not found" }))),
        Err(e) => {
            error!("DB error get_logs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" })))
        }
    }
}

#[derive(Deserialize)]
struct JobsQuery {
    limit: Option<i64>,
    status: Option<String>,
}

async fn handle_list_jobs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<JobsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT id, status, exit_code, image, created_at FROM jobs");

    if let Some(status) = query.status {
        q_builder.push(" WHERE status = ");
        q_builder.push_bind(status);
    }

    q_builder.push(" ORDER BY created_at DESC LIMIT ");
    q_builder.push_bind(query.limit.unwrap_or(20));

    let result = q_builder.build_query_as::<JobStatus>()
        .fetch_all(&state.db)
        .await;

    match result {
        Ok(jobs) => (StatusCode::OK, Json(serde_json::to_value(jobs).unwrap_or_default())),
        Err(e) => {
            error!("DB error list_jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Database error"})))
        }
    }
}

// --- MAIN ENTRYPOINT ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    let secret_key = env::var("ZORP_SECRET_KEY")
        .map_err(|_| {
            error!("ZORP_SECRET_KEY environment variable not set.");
            std::io::Error::new(std::io::ErrorKind::Other, "Missing ZORP_SECRET_KEY")
        })?;

    info!(":: Zorp v0.1.0 ::");
    
    let db_pool = init_db().await?;
    let state = Arc::new(AppState::new(db_pool, secret_key)?);

    let app = Router::new()
        .route("/", get(health_check))
        .route("/dispatch", post(handle_dispatch))
        .route("/job/:id", get(handle_get_job))
        .route("/job/:id/logs", get(handle_get_job_logs))
        .route("/jobs", get(handle_list_jobs))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
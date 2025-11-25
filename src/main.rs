// Zorp
// The ephemeral CI/CD dispatcher.
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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

// --- CONFIGURATION ---
const PORT: u16 = 3000;
const MAX_CONCURRENT_JOBS: usize = 20; 
const DOCKER_SOCKET: &str = "unix:///var/run/docker.sock";
const DB_URL: &str = "sqlite://zorp.db";

// --- DATA STRUCTURES ---

#[derive(Debug, Deserialize)]
struct JobRequest {
    image: String,
    commands: Vec<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct JobStatus {
    id: String,
    status: String,      // QUEUED, RUNNING, FINISHED, FAILED
    exit_code: Option<i32>,
    image: String,
    created_at: String,  // Simple ISO string for now
}

struct AppState {
    docker: Docker,
    db: SqlitePool,
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

    // Create table if not exists (No migration files needed for single binary)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            exit_code INTEGER,
            image TEXT NOT NULL,
            commands TEXT NOT NULL,
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
        let docker = Docker::connect_with_socket(DOCKER_SOCKET, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self {
            docker,
            db: pool,
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS)),
        })
    }

    async fn dispatch_job(&self, id: String, image: String, commands: Vec<String>) {
        let permit = self.limiter.clone().acquire_owned().await.unwrap();
        let docker = self.docker.clone();
        let db = self.db.clone();
        
        let commands_str = serde_json::to_string(&commands).unwrap_or_default();

        tokio::spawn(async move {
            let _permit = permit; 
            let start_time = Instant::now();
            let container_name = format!("zorp-{}", id);

            // UPDATE STATUS: RUNNING
            let _ = sqlx::query("UPDATE jobs SET status = 'RUNNING' WHERE id = ?")
                .bind(&id)
                .execute(&db).await;

            tracing::info!("[{}] Status: RUNNING", id);

            let mut final_status = "FINISHED";
            let mut final_exit_code = 0;

            // 1. Pull Image
            if let Err(e) = ensure_image(&docker, &image).await {
                tracing::error!("[{}] Image error: {}", id, e);
                final_status = "FAILED";
                final_exit_code = -1;
            } else {
                // 2. Configure & Run
                let config = Config {
                    image: Some(image.clone()),
                    cmd: Some(commands),
                    host_config: Some(HostConfig {
                        auto_remove: Some(false), 
                        memory: Some(1024 * 1024 * 512), 
                        cpu_quota: Some(100000), 
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
                        if docker.start_container(&container_name, None::<StartContainerOptions<String>>).await.is_ok() {
                            // Stream logs (omitted for brevity in this update, focusing on DB)
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
                        }
                    }
                    Err(_) => {
                        final_status = "FAILED";
                        final_exit_code = -3;
                    }
                }

                // Cleanup
                let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }

            // UPDATE STATUS: FINISHED/FAILED
            let _ = sqlx::query("UPDATE jobs SET status = ?, exit_code = ? WHERE id = ?")
                .bind(final_status)
                .bind(final_exit_code)
                .bind(&id)
                .execute(&db).await;

            tracing::info!("[{}] Status: {} (Exit: {}). Time: {:.2?}s", id, final_status, final_exit_code, start_time.elapsed());
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

// --- HTTP HANDLERS ---

async fn health_check() -> &'static str {
    "Zorp is running (with Persistence)."
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JobRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let job_id = Uuid::new_v4().to_string();
    
    // Save to DB immediately as QUEUED
    let cmd_str = serde_json::to_string(&payload.commands).unwrap();
    let insert_result = sqlx::query(
        "INSERT INTO jobs (id, status, image, commands) VALUES (?, 'QUEUED', ?, ?)"
    )
    .bind(&job_id)
    .bind(&payload.image)
    .bind(&cmd_str)
    .execute(&state.db).await;

    match insert_result {
        Ok(_) => {
            // Trigger async worker
            state.dispatch_job(job_id.clone(), payload.image, payload.commands).await;

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
    // Query DB for status
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
    tracing::info!(":: Zorp (v0.1.0) - Persistence Enabled ::");
    
    // Init Database first
    let db_pool = init_db().await?;
    
    let state = Arc::new(AppState::new(db_pool)?);

    let app = Router::new()
        .route("/", get(health_check))
        .route("/dispatch", post(handle_dispatch))
        .route("/job/:id", get(handle_get_job)) // New Endpoint
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    tracing::info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
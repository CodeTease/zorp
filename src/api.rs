use axum::{
    async_trait,
    extract::{FromRequestParts, Path, State, Json, Query},
    http::{header, request::Parts, StatusCode},
    routing::{get, post, delete},
    Router,
};
use axum::response::{IntoResponse, Response}; 
use axum::response::sse::{Event, Sse};
use futures_util::stream::Stream;
use std::sync::Arc;
use sqlx::Row;
use serde::{Deserialize};
use uuid::Uuid;
use tracing::{error, info, warn};
use bollard::Docker;
use crate::models::{JobRequest, JobContext, JobStatus, JobRegistry};
use crate::queue::JobQueue;
use crate::db::{DbPool, sql_placeholder};
use crate::engine;
use crate::metrics;
use crate::streaming::RedisLogPublisher;
use futures_util::StreamExt;

use aws_sdk_s3;
use axum::body::Body;
use std::convert::Infallible;

// --- SHARED STATE ---
pub struct AppState {
    pub db: DbPool,
    pub queue: Arc<dyn JobQueue>,
    pub secret_key: String,
    pub docker: Docker,
    pub job_registry: JobRegistry,
    pub s3_client: Option<aws_sdk_s3::Client>,
    pub s3_bucket: Option<String>,
}

// --- AUTH MIDDLEWARE ---
struct Auth;

#[async_trait]
impl FromRequestParts<Arc<AppState>> for Auth {
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

// --- HANDLERS ---

async fn health_check() -> &'static str {
    #[cfg(feature = "postgres")]
    {
        return "Zorp v0.1.0 is running (PostgreSQL + Redis).";
    }
    
    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    {
        return "Zorp v0.1.0 is running (SQLite + Redis).";
    }
}

async fn handle_metrics() -> String {
    metrics::get_metrics()
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    _: Auth,
    Json(payload): Json<JobRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let job_id = Uuid::new_v4().to_string();
    
    // 1. Persist to DB first (Status: QUEUED)
    // Dynamic SQL: INSERT INTO jobs (...) VALUES ($1, 'QUEUED', $2, $3, $4)
    let query = format!(
        "INSERT INTO jobs (id, status, image, commands, callback_url) VALUES ({}, 'QUEUED', {}, {}, {})",
        sql_placeholder(1), // id
        sql_placeholder(2), // image
        sql_placeholder(3), // commands
        sql_placeholder(4)  // callback_url
    );

    let insert_result = sqlx::query(&query)
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
                timeout_seconds: payload.timeout_seconds,
                artifacts_path: payload.artifacts_path,
                user: payload.user,
            };

            // 2. Push to Redis Queue
            match state.queue.enqueue(context).await {
                Ok(_) => {
                    info!("Job {} enqueued successfully via Redis", job_id);
                    metrics::inc_queued();
                    (StatusCode::ACCEPTED, Json(serde_json::json!({
                        "status": "queued",
                        "job_id": job_id,
                        "message": "Job added to processing queue"
                    })))
                },
                Err(e) => {
                    error!("Redis error for job {}: {}", job_id, e);
                    
                    // Fallback update if queue fails
                    // Dynamic SQL: UPDATE jobs SET status = 'FAILED', logs = $1 WHERE id = $2
                    let update_query = format!(
                        "UPDATE jobs SET status = 'FAILED', logs = {} WHERE id = {}",
                        sql_placeholder(1), sql_placeholder(2)
                    );

                    let _ = sqlx::query(&update_query)
                        .bind(format!("System Error: Queue unavailable - {}", e))
                        .bind(&job_id)
                        .execute(&state.db).await;

                    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Queue unavailable"})))
                }
            }
        },
        Err(e) => {
            error!("Database error during dispatch: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Failed to persist job"})))
        }
    }
}

async fn handle_cancel_job(
    State(state): State<Arc<AppState>>,
    _: Auth,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match engine::cancel_job(&state.docker, &state.job_registry, &job_id, &state.db).await {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({
            "status": "cancelled",
            "job_id": job_id,
            "message": "Job cancellation initiated successfully"
        }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "Job not found running",
            "message": "The job is not currently running in the active registry."
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e})))
    }
}

async fn handle_get_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Dynamic SQL: SELECT ... FROM jobs WHERE id = $1
    let query = format!(
        "SELECT id, status, exit_code, image, created_at FROM jobs WHERE id = {}",
        sql_placeholder(1)
    );

    let row = sqlx::query_as::<_, JobStatus>(&query)
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

#[allow(unused_imports)] 
use futures_util::TryStreamExt; 

async fn handle_get_job_logs(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Response {
    // Dynamic SQL: SELECT logs FROM jobs WHERE id = $1
    let query = format!("SELECT logs FROM jobs WHERE id = {}", sql_placeholder(1));

    let result = sqlx::query(&query)
        .bind(&job_id)
        .fetch_optional(&state.db)
        .await;

    match result {
        Ok(Some(row)) => {
            let logs: Option<String> = row.get("logs");
            let log_content = logs.unwrap_or_default();
            
            if log_content.starts_with("s3://") {
                if let (Some(s3), Some(bucket)) = (&state.s3_client, &state.s3_bucket) {
                    let key = log_content.replace(&format!("s3://{}/", bucket), "");
                    info!("Streaming log for job {} from S3 key {}", job_id, key);
                    
                    match s3.get_object().bucket(bucket).key(key).send().await {
                        Ok(mut output) => {
                            let mut body = Vec::new();
                            while let Some(chunk) = output.body.try_next().await.unwrap() {
                                body.extend_from_slice(&chunk);
                            }
                            return Body::from(body).into_response();
                        },
                        Err(e) => {
                            error!("S3 get_object failed for job {}: {}", job_id, e);
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Failed to retrieve logs from S3" }))).into_response();
                        }
                    }
                } else {
                     return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "S3 configuration missing" }))).into_response();
                }
            }

            (StatusCode::OK, Json(serde_json::json!({ "logs": log_content }))).into_response()
        },
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "Job not found" }))).into_response(),
        Err(e) => {
            error!("DB error get_logs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response()
        }
    }
}

// --- SSE LOG STREAMING ---
async fn handle_stream_logs(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    info!("Client connected to stream logs for job: {}", job_id);

    // Redis Subscriber
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let publisher = RedisLogPublisher::new(&redis_url);

    let stream = async_stream::stream! {
        match publisher.subscribe(&job_id).await {
            Ok(mut rx) => {
                 while let Some(msg) = rx.next().await {
                     yield Ok(Event::default().data(msg));
                 }
                 yield Ok(Event::default().data("[Stream Ended: Redis Connection Closed]"));
            },
            Err(e) => {
                error!("Failed to subscribe to redis log channel for {}: {}", job_id, e);
                yield Ok(Event::default().data("[Error: Failed to connect to log stream]"));
            }
        }
    };

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
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
    
    #[cfg(feature = "postgres")]
    type BuildDb = sqlx::Postgres;

    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    type BuildDb = sqlx::Sqlite;

    let mut q_builder = sqlx::query_builder::QueryBuilder::<BuildDb>::new("SELECT id, status, exit_code, image, created_at FROM jobs");

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

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(health_check))
        .route("/metrics", get(handle_metrics))
        .route("/dispatch", post(handle_dispatch))
        .route("/job/:id", delete(handle_cancel_job).get(handle_get_job))
        .route("/job/:id/logs", get(handle_get_job_logs))
        .route("/job/:id/stream", get(handle_stream_logs))
        .route("/jobs", get(handle_list_jobs))
        .with_state(state)
}

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequestParts, Path, State, Json, Query},
    http::{header, request::Parts, StatusCode},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use sqlx::{Row, SqlitePool};
use serde::{Deserialize};
use uuid::Uuid;
use tracing::{error, info};
use crate::models::{JobRequest, JobContext, JobStatus};
use crate::engine::Dispatcher;

#[cfg(feature = "s3_logging")]
use aws_sdk_s3;

// --- SHARED STATE ---
pub struct AppState {
    pub db: SqlitePool,
    pub dispatcher: Arc<Dispatcher>,
    pub secret_key: String,

    #[cfg(feature = "s3_logging")]
    pub s3_client: Option<aws_sdk_s3::Client>,
    #[cfg(feature = "s3_logging")]
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

            state.dispatcher.dispatch(context).await;

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

use axum::response::{IntoResponse, Response};

async fn handle_get_job_logs(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Response {
    let result = sqlx::query("SELECT logs FROM jobs WHERE id = ?")
        .bind(&job_id)
        .fetch_optional(&state.db)
        .await;

    match result {
        Ok(Some(row)) => {
            let logs: Option<String> = row.get("logs");
            let log_content = logs.unwrap_or_default();
            
            #[cfg(feature = "s3_logging")]
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

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(health_check))
        .route("/dispatch", post(handle_dispatch))
        .route("/job/:id", get(handle_get_job))
        .route("/job/:id/logs", get(handle_get_job_logs))
        .route("/jobs", get(handle_list_jobs))
        .with_state(state)
}


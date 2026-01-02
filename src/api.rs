use axum::{
    extract::{FromRequestParts, Path, State, Json, Query},
    http::{header, request::Parts, StatusCode, HeaderMap},
    routing::{get, post, delete},
    Router,
    middleware,
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
use crate::models::{JobRequest, JobContext, JobStatus, JobRegistry, ApiKey};
use crate::queue::JobQueue;
use crate::db::DbPool;
use crate::engine;
use crate::metrics;
use crate::workflow;
use crate::matrix;
use crate::streaming::RedisLogPublisher;
use futures_util::StreamExt;
use sha2::{Sha256, Digest};

use aws_sdk_s3;
use axum::body::Body;
use std::convert::Infallible;

use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use validator::Validate;

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

// --- AUTH CONTEXT ---
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub user_id: Option<String>,
    pub permissions: Vec<String>,
    pub role: String, // e.g. "admin", "viewer", "system"
}

// --- AUTH MIDDLEWARE ---
struct Auth(AuthContext);

impl FromRequestParts<Arc<AppState>> for Auth {
    type Rejection = (StatusCode, &'static str);
    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        if let Some(auth_header) = parts.headers.get(header::AUTHORIZATION) {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(token) = auth_str.strip_prefix("Bearer ") {
                    // 1. Check Legacy/Root Key
                    if token == state.secret_key {
                        return Ok(Auth(AuthContext {
                            user_id: None,
                            permissions: vec!["*".to_string()],
                            role: "admin".to_string(),
                        }));
                    }

                    // 2. Check Database API Keys (Hashed)
                    let mut hasher = Sha256::new();
                    hasher.update(token.as_bytes());
                    let result = hasher.finalize();
                    let key_hash = hex::encode(result);

                    // Dynamic SQL: SELECT * FROM api_keys WHERE key_hash = $1
                    let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT * FROM api_keys WHERE key_hash = ");
                    q_builder.push_bind(key_hash);

                    if let Ok(Some(api_key)) = q_builder.build_query_as::<ApiKey>()
                        .fetch_optional(&state.db).await 
                    {
                        // Parse permissions
                        let permissions = api_key.permissions
                            .map(|p| p.split(',').map(|s| s.trim().to_string()).collect())
                            .unwrap_or_default();

                        return Ok(Auth(AuthContext {
                            user_id: Some(api_key.user_id),
                            permissions,
                            role: "user".to_string(), // In future, fetch user role
                        }));
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

async fn handle_healthz(State(state): State<Arc<AppState>>) -> (StatusCode, Json<serde_json::Value>) {
    // 1. Check Database
    let db_status = sqlx::query("SELECT 1").execute(&state.db).await;
    
    // 2. Check Redis
    let redis_status = state.queue.ping().await;

    // 3. Check Docker
    let docker_status = state.docker.version().await;

    if db_status.is_ok() && redis_status.is_ok() && docker_status.is_ok() {
        (StatusCode::OK, Json(serde_json::json!({
            "status": "ok",
            "db": "connected",
            "redis": "connected",
            "docker": "connected"
        })))
    } else {
        let db_err = db_status.as_ref().err();
        let redis_err = redis_status.as_ref().err();
        let docker_err = docker_status.as_ref().err();
        
        error!("Health check failed: DB={:?}, Redis={:?}, Docker={:?}", db_err, redis_err, docker_err);
        
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "status": "error",
            "db": if db_status.is_ok() { "connected" } else { "disconnected" },
            "redis": if redis_status.is_ok() { "connected" } else { "disconnected" },
            "docker": if docker_status.is_ok() { "connected" } else { "disconnected" }
        })))
    }
}

async fn handle_metrics() -> String {
    metrics::get_metrics()
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    Auth(auth): Auth,
    headers: HeaderMap,
    Json(payload): Json<JobRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Input Validation
    if let Err(e) = payload.validate() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "Validation failed", 
            "details": format!("{:?}", e)
        })));
    }

    // RBAC Check
    if auth.role != "admin" && !auth.permissions.contains(&"dispatch".to_string()) && !auth.permissions.contains(&"*".to_string()) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Insufficient permissions"})));
    }

    let expanded_jobs = matrix::expand_job_request(&payload);
    let mut created_job_ids = Vec::new();
    let mut errors = Vec::new();

    // Idempotency: We only support it for single job requests for now.
    // If it's a matrix job (expanded > 1), we skip idempotency or log a warning.
    let idempotency_key = headers.get("X-Idempotency-Key").and_then(|h| h.to_str().ok());

    if expanded_jobs.len() > 1 && idempotency_key.is_some() {
        warn!("Idempotency key ignored for matrix job request. It is only supported for single jobs.");
    }

    // Pre-generate ID for single job case to use in lock
    let single_job_id = if expanded_jobs.len() == 1 { Some(Uuid::new_v4().to_string()) } else { None };
    
    // If single job and idempotency key present, acquire lock FIRST
    if let (Some(key), Some(job_id)) = (idempotency_key, &single_job_id) {
         match state.queue.set_idempotency_key_nx(key, job_id, 86400).await {
             Ok(true) => {
                 // Lock acquired, proceed.
             },
             Ok(false) => {
                 // Key exists, return old ID
                 match state.queue.get_idempotency_key(key).await {
                     Ok(Some(existing_job_id)) => {
                         info!("Idempotency hit for key: {}. Returning existing job_id: {}", key, existing_job_id);
                         return (StatusCode::OK, Json(serde_json::json!({
                            "status": "queued",
                            "job_id": existing_job_id,
                            "message": "Job returned from idempotency cache"
                        })));
                     },
                     Ok(None) => {}, // Expired? Proceed with new ID
                     Err(e) => {
                         error!("Idempotency fetch failed: {}", e);
                         return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Idempotency check failed"})));
                     }
                 }
             },
             Err(e) => {
                 error!("Idempotency set failed: {}", e);
                 // Proceed cautiously
             }
         }
    }

    let user_id = auth.user_id.or(payload.user.clone()); 
    let expanded_len = expanded_jobs.len();

    for job_req in expanded_jobs {
        // Use pre-generated ID if single, otherwise new ID
        let job_id = if expanded_len == 1 { single_job_id.clone().unwrap() } else { Uuid::new_v4().to_string() };

        // 1. Persist to DB
        let mut q_builder = sqlx::query_builder::QueryBuilder::new("INSERT INTO jobs (id, status, image, commands, callback_url, user_id, enable_network, run_at) VALUES (");
        q_builder.push_bind(&job_id);
        q_builder.push(", 'QUEUED', ");
        q_builder.push_bind(&job_req.image);
        q_builder.push(", ");
        q_builder.push_bind(serde_json::to_string(&job_req.commands).unwrap_or_default());
        q_builder.push(", ");
        q_builder.push_bind(&job_req.callback_url);
        q_builder.push(", ");
        q_builder.push_bind(&user_id);
        q_builder.push(", ");
        q_builder.push_bind(job_req.enable_network);
        q_builder.push(", ");
        q_builder.push_bind(job_req.run_at);
        q_builder.push(")");

        if let Err(e) = q_builder.build().execute(&state.db).await {
            error!("Database error during dispatch for job {}: {}", job_id, e);
            errors.push(format!("Failed to persist job {}: {}", job_id, e));
            continue; 
        }

        let context = JobContext {
            id: job_id.clone(),
            image: job_req.image,
            commands: job_req.commands,
            env: job_req.env,
            limits: job_req.limits.map(Arc::new),
            callback_url: job_req.callback_url,
            timeout_seconds: job_req.timeout_seconds,
            artifacts_path: job_req.artifacts_path,
            user: user_id.clone(),
            cache_key: job_req.cache_key,
            cache_paths: job_req.cache_paths,
            services: job_req.services,
            on_success: job_req.on_success,
            debug: job_req.debug,
            priority: job_req.priority,
            retry_count: 0,
            enable_network: job_req.enable_network,
            run_at: job_req.run_at,
            stream_id: None,
            stream_name: None,
        };

        // 2. Push to Redis Queue
        match state.queue.enqueue(context).await {
            Ok(_) => {
                info!("Job {} enqueued successfully via Redis", job_id);
                created_job_ids.push(job_id.clone());
                metrics::inc_queued();
            },
            Err(e) => {
                error!("Redis error for job {}: {}", job_id, e);
                // Mark as failed in DB
                let _ = sqlx::query("UPDATE jobs SET status = 'FAILED', logs = ? WHERE id = ?")
                    .bind(format!("System Error: Queue unavailable - {}", e))
                    .bind(&job_id)
                    .execute(&state.db).await;
                
                errors.push(format!("Failed to enqueue job {}: {}", job_id, e));
            }
        }
    }

    // Idempotency key is ALREADY set at the top for single jobs.
    // If we fail here, we should ideally rollback the key, but for now we rely on TTL.

    if created_job_ids.is_empty() && !errors.is_empty() {
        // Rollback Idempotency if single job failed completely
        if let Some(key) = idempotency_key {
            if errors.len() == 1 { // Only if we tried 1 job and failed
                 let _ = state.queue.delete_idempotency_key(key).await;
            }
        }

        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": "Failed to dispatch any jobs",
            "details": errors
        })));
    }

    // Response Format
    if created_job_ids.len() == 1 {
        (StatusCode::ACCEPTED, Json(serde_json::json!({
            "status": "queued",
            "job_id": created_job_ids[0],
            "message": "Job added to processing queue"
        })))
    } else {
        (StatusCode::ACCEPTED, Json(serde_json::json!({
            "status": "queued",
            "job_ids": created_job_ids,
            "message": format!("{} jobs added to processing queue", created_job_ids.len())
        })))
    }
}

async fn handle_cancel_job(
    State(state): State<Arc<AppState>>,
    Auth(auth): Auth,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    // RBAC Check
    if auth.role != "admin" && !auth.permissions.contains(&"cancel".to_string()) && !auth.permissions.contains(&"*".to_string()) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Insufficient permissions"})));
    }

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
    let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT id, status, exit_code, image, created_at, user_id, artifact_url FROM jobs WHERE id = ");
    q_builder.push_bind(job_id);

    let row = q_builder.build_query_as::<JobStatus>()
        .fetch_optional(&state.db).await;

    match row {
        Ok(Some(job)) => {
            match serde_json::to_value(job) {
                Ok(val) => (StatusCode::OK, Json(val)),
                Err(e) => {
                    error!("Serialization error in get_job: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Internal serialization error"})))
                }
            }
        },
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
    let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT logs FROM jobs WHERE id = ");
    q_builder.push_bind(&job_id);
    
    let result = q_builder.build().fetch_optional(&state.db).await;

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
#[derive(Deserialize)]
struct StreamLogsQuery {
    last_id: Option<String>,
}

async fn handle_stream_logs(
    State(state): State<Arc<AppState>>, // Removed underscore to use state
    Path(job_id): Path<String>,
    Query(query): Query<StreamLogsQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    info!("Client connected to stream logs for job: {} (Resume: {:?})", job_id, query.last_id);

    // Redis Subscriber
    // Use the client from the queue
    let redis_client = state.queue.get_client();
    let publisher = RedisLogPublisher::new(redis_client);

    let stream = async_stream::stream! {
        match publisher.subscribe(&job_id, query.last_id).await {
            Ok(rx) => {
                 // Pin the stream
                 let mut pinned_rx = Box::pin(rx);
                 while let Some(res) = pinned_rx.next().await {
                     match res {
                         Ok((id, msg)) => {
                             yield Ok(Event::default()
                                .id(id)
                                .data(msg));
                         }
                         Err(e) => {
                             error!("Error in log stream for {}: {}", job_id, e);
                             break;
                         }
                     }
                 }
                 yield Ok(Event::default().data("[Stream Ended]"));
            },
            Err(e) => {
                error!("Failed to subscribe to redis log stream for {}: {}", job_id, e);
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

    let mut q_builder = sqlx::query_builder::QueryBuilder::<BuildDb>::new("SELECT id, status, exit_code, image, created_at, user_id, artifact_url FROM jobs");

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

async fn handle_workflow_dispatch(
    State(state): State<Arc<AppState>>,
    Auth(auth): Auth,
    body: String,
) -> (StatusCode, Json<serde_json::Value>) {
    // RBAC Check
    if auth.role != "admin" && !auth.permissions.contains(&"dispatch".to_string()) && !auth.permissions.contains(&"*".to_string()) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Insufficient permissions"})));
    }

    match workflow::parse_workflow(&body) {
        Ok(graph) => {
            let workflow_id = Uuid::new_v4().to_string();
            let mut job_uuid_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
            let mut queued_job_ids = Vec::new();
            let mut errors = Vec::new();

            // 1. Generate UUIDs for all jobs
            for (key, _) in &graph.jobs {
                job_uuid_map.insert(key.clone(), Uuid::new_v4().to_string());
            }

            // 2. Insert all jobs into DB (PENDING)
            for (key, job_req) in &graph.jobs {
                let job_id = job_uuid_map.get(key).unwrap().clone();
                let user_id = auth.user_id.clone().or(job_req.user.clone());

                let mut q_builder = sqlx::query_builder::QueryBuilder::new("INSERT INTO jobs (id, status, image, commands, env, services, on_success, callback_url, user_id, enable_network, run_at, workflow_id, dependencies_met) VALUES (");
                q_builder.push_bind(&job_id);
                q_builder.push(", 'PENDING', ");
                q_builder.push_bind(&job_req.image);
                q_builder.push(", ");
                q_builder.push_bind(serde_json::to_string(&job_req.commands).unwrap_or_default());
                q_builder.push(", ");
                q_builder.push_bind(serde_json::to_string(&job_req.env).unwrap_or_default());
                q_builder.push(", ");
                q_builder.push_bind(serde_json::to_string(&job_req.services).unwrap_or_default());
                q_builder.push(", ");
                q_builder.push_bind(serde_json::to_string(&job_req.on_success).unwrap_or_default());
                q_builder.push(", ");
                q_builder.push_bind(&job_req.callback_url);
                q_builder.push(", ");
                q_builder.push_bind(&user_id);
                q_builder.push(", ");
                q_builder.push_bind(job_req.enable_network);
                q_builder.push(", ");
                q_builder.push_bind(job_req.run_at);
                q_builder.push(", ");
                q_builder.push_bind(&workflow_id);
                q_builder.push(", 0)");

                if let Err(e) = q_builder.build().execute(&state.db).await {
                    error!("Database error during dispatch (job insert): {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                        "error": "Failed to persist jobs",
                        "details": e.to_string()
                    })));
                }
            }

            // 3. Insert Dependencies
            for (parent_key, children_keys) in &graph.dependencies {
                let parent_id = job_uuid_map.get(parent_key).unwrap();
                for child_key in children_keys {
                     let child_id = job_uuid_map.get(child_key).unwrap();
                     
                     let mut q_dep = sqlx::query_builder::QueryBuilder::new("INSERT INTO job_dependencies (parent_job_id, child_job_id) VALUES (");
                     q_dep.push_bind(parent_id);
                     q_dep.push(", ");
                     q_dep.push_bind(child_id);
                     q_dep.push(")");
                     
                     if let Err(e) = q_dep.build().execute(&state.db).await {
                         error!("Failed to insert dependency: {}", e);
                         return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Failed to map dependencies"})));
                     }
                }
            }

            // 4. Identify Roots (Jobs with 0 dependencies) and Enqueue them
            // We can check which jobs are NOT in the "values" of dependency map?
            // Or just check if they have incoming edges.
            
            // Build a set of jobs that are children
            let mut children_set = std::collections::HashSet::new();
            for children in graph.dependencies.values() {
                for child in children {
                    children_set.insert(child.clone());
                }
            }

            for (key, job_req) in &graph.jobs {
                if !children_set.contains(key) {
                    // This is a root job
                    let job_id = job_uuid_map.get(key).unwrap();
                    let user_id = auth.user_id.clone().or(job_req.user.clone());

                    // Update status to QUEUED
                    let _ = sqlx::query("UPDATE jobs SET status = 'QUEUED' WHERE id = ?")
                        .bind(job_id)
                        .execute(&state.db).await;

                    let context = JobContext {
                        id: job_id.clone(),
                        image: job_req.image.clone(),
                        commands: job_req.commands.clone(),
                        env: job_req.env.clone(),
                        limits: job_req.limits.clone().map(Arc::new),
                        callback_url: job_req.callback_url.clone(),
                        timeout_seconds: job_req.timeout_seconds,
                        artifacts_path: job_req.artifacts_path.clone(),
                        user: user_id,
                        cache_key: job_req.cache_key.clone(),
                        cache_paths: job_req.cache_paths.clone(),
                        services: job_req.services.clone(),
                        on_success: vec![], // Flattened, so no nested execution
                        debug: job_req.debug,
                        priority: job_req.priority.clone(),
                        retry_count: 0,
                        enable_network: job_req.enable_network,
                        run_at: job_req.run_at,
                        stream_id: None,
                        stream_name: None,
                    };

                    if let Err(e) = state.queue.enqueue(context).await {
                         error!("Redis error for job {}: {}", job_id, e);
                         errors.push(format!("Job {}: Queue failed", job_id));
                    } else {
                         metrics::inc_queued();
                         queued_job_ids.push(job_id.clone());
                    }
                }
            }

            (StatusCode::ACCEPTED, Json(serde_json::json!({
                "status": "processing",
                "workflow_id": workflow_id,
                "queued_roots": queued_job_ids,
                "errors": if errors.is_empty() { None } else { Some(errors) }
            })))
        },
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid workflow YAML", "details": e.to_string()})))
    }
}

pub fn create_router(state: Arc<AppState>) -> Router {
    // Rate Limiting: Allow 10 requests per second with a burst of 20
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(10)
            .burst_size(20)
            .finish()
            .unwrap(),
    );

    let rate_limit_layer = GovernorLayer::new(governor_conf);

    Router::new()
        .route("/", get(health_check))
        .route("/healthz", get(handle_healthz))
        .route("/metrics", get(handle_metrics))
        .route("/dispatch", post(handle_dispatch))
        .route("/workflow", post(handle_workflow_dispatch))
        .route("/job/:id", delete(handle_cancel_job).get(handle_get_job))
        .route("/job/:id/logs", get(handle_get_job_logs))
        .route("/job/:id/stream", get(handle_stream_logs))
        .route("/jobs", get(handle_list_jobs))
        .layer(rate_limit_layer)
        .with_state(state)
}

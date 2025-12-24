use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};
use validator::Validate;

// --- DATA STRUCTURES ---

pub type JobRegistry = Arc<RwLock<HashMap<String, String>>>;

#[derive(Debug, Deserialize, Clone, Serialize)] // ADDED Serialize here for API response
pub struct JobLimits {
    pub memory_mb: Option<i64>,
    pub cpu_cores: Option<f32>,
}

#[derive(Debug, Deserialize, Clone, Serialize, Validate)]
pub struct JobRequest {
    #[validate(length(min = 1, message = "Image name cannot be empty"))]
    pub image: String,
    #[validate(length(min = 1, max = 100, message = "Commands list must be between 1 and 100 items"))]
    pub commands: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<JobLimits>,
    pub callback_url: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub artifacts_path: Option<String>,
    pub user: Option<String>, // New field for configurable user
    pub cache_key: Option<String>,
    pub cache_paths: Option<Vec<String>>,
    #[serde(default)]
    pub on_success: Vec<JobRequest>,
    #[serde(default)]
    pub debug: bool,
    pub priority: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct JobStatus {
    pub id: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub image: String,
    pub created_at: DateTime<Utc>, // Changed to DateTime<Utc>
    pub artifact_url: Option<String>,
    pub user_id: Option<String>,
}

// FIX: Added Serialize and Deserialize so Redis can store/retrieve this struct
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct JobContext {
    pub id: String,
    pub image: String,
    pub commands: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<Arc<JobLimits>>,
    pub callback_url: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub artifacts_path: Option<String>,
    pub user: Option<String>, // New field for configurable user
    pub cache_key: Option<String>,
    pub cache_paths: Option<Vec<String>>,
    #[serde(default)]
    pub on_success: Vec<JobRequest>,
    #[serde(default)]
    pub debug: bool,
    pub priority: Option<String>,
    #[serde(default)]
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadTask {
    pub job_id: String,
    pub file_path: String,
    pub upload_type: String, // "log" or "artifact"
    pub s3_key: String,
    #[serde(default)]
    pub retry_count: u32,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct User {
    pub id: String,
    pub username: String,
    #[serde(skip)]
    pub password_hash: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ApiKey {
    pub id: String,
    pub user_id: String,
    #[serde(skip)]
    pub key_hash: String,
    pub label: Option<String>,
    pub permissions: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

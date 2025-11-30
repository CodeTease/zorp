use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// --- DATA STRUCTURES ---

pub type JobRegistry = Arc<RwLock<HashMap<String, String>>>;

#[derive(Debug, Deserialize, Clone, Serialize)] // ADDED Serialize here for API response
pub struct JobLimits {
    pub memory_mb: Option<i64>,
    pub cpu_cores: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub struct JobRequest {
    pub image: String,
    pub commands: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<JobLimits>,
    pub callback_url: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub artifacts_path: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct JobStatus {
    pub id: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub image: String,
    pub created_at: String,
    pub artifact_url: Option<String>,
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
}
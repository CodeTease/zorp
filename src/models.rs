use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// --- DATA STRUCTURES ---

#[derive(Debug, Deserialize, Clone)]
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
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct JobStatus {
    pub id: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub image: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct JobContext {
    pub id: String,
    pub image: String,
    pub commands: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<Arc<JobLimits>>,
    pub callback_url: Option<String>,
}
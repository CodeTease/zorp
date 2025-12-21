use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::{info, warn, error};
use crate::queue::JobQueue;
use serde::{Deserialize, Serialize};
use crate::security;

#[derive(Serialize, Deserialize, Debug)]
pub struct WebhookPayload {
    pub job_id: String,
    pub status: String,
    pub exit_code: i32,
    pub duration_seconds: f64,
    pub logs: String,
    pub callback_url: String,
    #[serde(default)]
    pub attempt: u32,
}

pub fn spawn(
    queue: Arc<dyn JobQueue>,
    http_client: reqwest::Client,
    secret_key: String,
    mut shutdown: Receiver<()>,
) {
    let queue_process = queue.clone();
    let queue_monitor = queue.clone();
    let client = http_client.clone();
    let key = secret_key.clone();
    
    // 1. Processor Task
    tokio::spawn(async move {
        info!("🔔 Webhook Worker started.");
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("🔔 Webhook Worker shutting down...");
                    break;
                }
                result = queue_process.dequeue_webhook() => {
                    match result {
                        Ok(Some(json_str)) => {
                            let mut payload: WebhookPayload = match serde_json::from_str(&json_str) {
                                Ok(p) => p,
                                Err(e) => {
                                    error!("Failed to parse webhook payload: {}", e);
                                    continue;
                                }
                            };

                            info!("[{}] Sending webhook (Attempt {})...", payload.job_id, payload.attempt + 1);

                            // Signature
                            // We need to reconstruct the payload sent to the user to sign it properly.
                            // The user expects: { job_id, status, exit_code, duration_seconds, logs }
                            let user_payload = serde_json::json!({
                                "job_id": payload.job_id,
                                "status": payload.status,
                                "exit_code": payload.exit_code,
                                "duration_seconds": payload.duration_seconds,
                                "logs": payload.logs
                            });
                            
                            let mut headers = reqwest::header::HeaderMap::new();
                            if let Ok(p_str) = serde_json::to_string(&user_payload) {
                                if let Some(sig) = security::sign_payload(&key, &p_str) {
                                    if let Ok(hv) = reqwest::header::HeaderValue::from_str(&sig) {
                                        headers.insert("X-Zorp-Signature", hv);
                                    }
                                }
                            }

                            let resp = client.post(&payload.callback_url)
                                .headers(headers)
                                .json(&user_payload)
                                .send()
                                .await;
                            
                            match resp {
                                Ok(res) => {
                                    if !res.status().is_success() {
                                        warn!("[{}] Webhook failed with status: {}", payload.job_id, res.status());
                                        handle_retry(&queue_process, payload).await;
                                    } else {
                                        info!("[{}] Webhook delivered successfully.", payload.job_id);
                                    }
                                }
                                Err(e) => {
                                    warn!("[{}] Webhook request error: {}", payload.job_id, e);
                                    handle_retry(&queue_process, payload).await;
                                }
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        Err(e) => {
                            error!("Webhook queue error: {}", e);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
    });

    // 2. Retry Monitor Task
    tokio::spawn(async move {
        info!("🔔 Webhook Retry Monitor started.");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            if let Err(e) = queue_monitor.monitor_webhook_retries().await {
                error!("Webhook monitor error: {}", e);
            }
        }
    });
}

async fn handle_retry(queue: &Arc<dyn JobQueue>, mut payload: WebhookPayload) {
    if payload.attempt >= 5 {
        error!("[{}] Webhook failed after 5 attempts. Giving up.", payload.job_id);
        return;
    }

    payload.attempt += 1;
    // Exponential backoff: 2^attempt * 30 seconds
    let delay = 30 * 2i64.pow(payload.attempt - 1); // 30s, 60s, 120s...
    
    if let Ok(json) = serde_json::to_string(&payload) {
        info!("[{}] Scheduling retry #{} in {}s", payload.job_id, payload.attempt, delay);
        if let Err(e) = queue.enqueue_webhook_retry(json, delay).await {
            error!("Failed to enqueue webhook retry: {}", e);
        }
    }
}

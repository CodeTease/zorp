use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn, error};
use crate::queue::{RedisQueue, JobQueue};
use crate::models::UploadTask;
use crate::db::DbPool;

// We need to pass the S3 client and other deps.
// To avoid complex structs, we can just pass them to the spawn function.

pub fn spawn(
    queue: Arc<RedisQueue>, 
    s3_client: aws_sdk_s3::Client, 
    s3_bucket: String, 
    _db_pool: DbPool, // Might be needed for status updates, currently unused in the block but passed in main
    mut shutdown_rx: broadcast::Receiver<()>
) -> tokio::task::JoinHandle<()> {
    
    tokio::spawn(async move {
        info!("☁️  Upload Worker thread started.");
        loop {
             tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("☁️  Upload Worker stopping...");
                    break;
                }
                task_res = queue.dequeue_upload() => {
                    match task_res {
                        Ok(Some(task)) => {
                            process_upload(&task, &s3_client, &s3_bucket, &queue).await;
                        }
                        Ok(None) => {
                             tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        Err(e) => {
                             error!("Upload Queue Error: {}", e);
                             tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
        info!("☁️  Upload Worker stopped.");
    })
}

async fn process_upload(
    task: &UploadTask, 
    s3_client: &aws_sdk_s3::Client, 
    s3_bucket: &str, 
    queue: &Arc<RedisQueue>
) {
    info!("☁️  Processing upload task for job {}: {} ({})", task.job_id, task.s3_key, task.upload_type);
    
    let path = std::path::Path::new(&task.file_path);
    if path.exists() {
        let body = match aws_sdk_s3::primitives::ByteStream::from_path(path).await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read file for upload {}: {}", task.file_path, e);
                return;
            }
        };

        match s3_client.put_object()
            .bucket(s3_bucket)
            .key(&task.s3_key)
            .body(body)
            .send()
            .await 
        {
            Ok(_) => {
                let s3_url = format!("s3://{}/{}", s3_bucket, task.s3_key);
                info!("✅ Upload successful: {}", s3_url);
                
                // Clean up file
                let _ = tokio::fs::remove_file(path).await;
            },
            Err(e) => {
                error!("❌ S3 Upload Failed: {}. (Attempt: {})", e, task.retry_count + 1);
                let mut retry_task = task.clone();
                retry_task.retry_count += 1;
                
                if retry_task.retry_count > 5 {
                     error!("❌ Upload exceeded max retries. Moving to DLQ.");
                     if let Err(dlq_err) = queue.enqueue_upload_dlq(retry_task).await {
                         error!("Failed to move upload task to DLQ: {}", dlq_err);
                     }
                } else {
                     // Non-blocking Exponential Backoff via Tokio Task
                     let delay = 2u64.pow(retry_task.retry_count);
                     warn!("Retrying upload in {}s...", delay);
                     
                     let q_clone = queue.clone();
                     tokio::spawn(async move {
                         tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                         if let Err(re_err) = q_clone.enqueue_upload(retry_task).await {
                              error!("Failed to re-enqueue upload task: {}", re_err);
                         }
                     });
                }
            }
        }
    } else {
        warn!("File not found for upload: {}", task.file_path);
    }
}

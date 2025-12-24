use bollard::Docker;
use bollard::container::{ListContainersOptions, RemoveContainerOptions};
use bollard::image::PruneImagesOptions;
use bollard::volume::PruneVolumesOptions;
use crate::models::JobRegistry;
use crate::db::DbPool;
use tracing::{info, warn, error};
use std::time::SystemTime;
use futures_util::StreamExt;
use bollard::system::EventsOptions;
use crate::queue::RedisQueue;
use std::sync::Arc;
use redis::AsyncCommands;
use uuid::Uuid;
use crate::models::JobContext;
use crate::queue::JobQueue;

pub fn spawn(docker: Docker, db: DbPool, registry: JobRegistry, queue: Arc<RedisQueue>, s3_client: aws_sdk_s3::Client, s3_bucket: String) -> tokio::task::JoinHandle<()> {
    // 1. Event Stream Listener (Real-time) - Keeps running independently?
    // The prompt says "Chia để trị Leader Election... Chỉ ông nào nắm giữ khóa Redis mới được quyền chạy task dọn dẹp hệ thống."
    // This usually implies the periodic reaper, not necessarily the event listener which is local to the instance's docker daemon.
    // However, if we have multiple dispatchers on the same machine (weird but possible) or different machines connecting to same remote docker?
    // Usually Zorp manages its *local* docker. So Event Listener should run for *this* instance.
    // The "Reaper" usually refers to the "Garbage Collector" and "Zombie Reaper" that might scan the DB and make global decisions.
    // But `reap_zombies` lists *local* containers.
    // `garbage_collection` lists *local* temp files and containers.
    // So actually, if we have multiple instances on different machines, they MUST run their own reaper for local cleanup.
    // BUT the prompt says "If you run 2-3 instances of Zorp dispatcher to load balance, Reapers will fight when cleaning up a container."
    // This implies they share the Docker Daemon (e.g. connecting to a remote Docker swarm or same socket).
    // If they share the same Docker Daemon, then yes, they fight.
    // If they have separate Docker Daemons, they don't fight.
    // Assuming Shared Docker Daemon or Shared DB where they might conflict on status updates.
    // The prompt says "Leader Election using Redis".

    let docker_events = docker.clone();
    let db_events = db.clone();
    
    tokio::spawn(async move {
        info!("👂 Docker Event Listener started.");
        let mut filters = std::collections::HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        filters.insert("event".to_string(), vec!["die".to_string()]);
        filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);

        let options = EventsOptions {
            filters,
            ..Default::default()
        };

        let mut stream = docker_events.events(Some(options));

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(event) => {
                    if let Some(actor) = event.actor {
                        if let Some(attributes) = actor.attributes {
                            if let Some(job_id) = attributes.get("job_id") {
                                info!("⚡ Event Listener: Detected death of job container {}", job_id);
                                
                                let mut q_select = sqlx::query_builder::QueryBuilder::new("SELECT status FROM jobs WHERE id = ");
                                q_select.push_bind(job_id);
                                
                                let row = q_select.build_query_as::< (String,) >()
                                    .fetch_optional(&db_events).await;

                                if let Ok(Some((status,))) = row {
                                    if status == "RUNNING" {
                                        warn!("⚡ Event Listener: Job {} died unexpectedly. Updating DB...", job_id);
                                        
                                        let mut q_update = sqlx::query_builder::QueryBuilder::new("UPDATE jobs SET status = 'FAILED', exit_code = -999 WHERE id = ");
                                        q_update.push_bind(job_id);
                                        
                                        let _ = q_update.build().execute(&db_events).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error in Docker event stream: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // 2. Fallback Poller (Every 60s) WITH LEADER ELECTION
    tokio::spawn(async move {
        info!("逐 Zombie Reaper & Garbage Collector task started.");
        
        let client = match redis::Client::open(std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to connect to Redis for Leader Election: {}", e);
                return;
            }
        };

        loop {
            // Try to acquire lock
            let lock_key = "zorp:leader_lock";
            let lock_ttl_ms = 65000; // slightly longer than the sleep
            let my_id = Uuid::new_v4().to_string();

            let mut con = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Redis connection error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    continue;
                }
            };

            // SET resource_name my_random_value NX PX max-lock-time
            let result: redis::RedisResult<String> = redis::cmd("SET")
                .arg(lock_key)
                .arg(&my_id)
                .arg("NX")
                .arg("PX")
                .arg(lock_ttl_ms)
                .query_async(&mut con)
                .await;

            match result {
                Ok(val) if val == "OK" => {
                    info!("👑 Acquired Leader Lock. Running maintenance tasks...");
                    
                    // Reap Zombies (every 60s)
                    reap_zombies(&docker, &db).await;
                    
                    // Garbage Collection (Temp files & Pruning)
                    garbage_collection(&docker, &registry).await;

                    // Retention Policy (New Feature)
                    cleanup_old_jobs(&db, &s3_client, &s3_bucket).await;

                    // Requeue Stuck Jobs (Distributed Transaction Recovery)
                    requeue_stuck_jobs(&db, &queue).await;
                }
                _ => {
                    // Not leader, just skip
                    // info!("Not leader, skipping maintenance.");
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    })
}

async fn requeue_stuck_jobs(db: &DbPool, queue: &Arc<RedisQueue>) {
    // Distributed Transaction Recovery:
    // If a job is persisted to DB as 'QUEUED' but fails to push to Redis (or Redis crashes),
    // it will remain 'QUEUED' forever (Orphan Job).
    // We scan for jobs that have been 'QUEUED' for > 5 minutes and are not running.

    let query = if cfg!(feature = "postgres") {
        "SELECT id, image, commands, callback_url, user_id FROM jobs WHERE status = 'QUEUED' AND created_at < NOW() - INTERVAL '5 minutes'"
    } else {
        "SELECT id, image, commands, callback_url, user_id FROM jobs WHERE status = 'QUEUED' AND created_at < date('now', '-5 minutes')"
    };

    // Using a struct that matches the query columns for sqlx::FromRow
    #[derive(sqlx::FromRow)]
    struct StuckJob {
        id: String,
        image: String,
        commands: String, // JSON string
        callback_url: Option<String>,
        user_id: Option<String>,
    }

    let rows: Result<Vec<StuckJob>, _> = sqlx::query_as(query).fetch_all(db).await;

    match rows {
        Ok(jobs) => {
            if jobs.is_empty() { return; }
            warn!("⚠️  Reaper found {} stuck/orphan jobs (QUEUED > 5m). Attempting to re-enqueue...", jobs.len());

            for job in jobs {
                let commands_vec: Vec<String> = serde_json::from_str(&job.commands).unwrap_or_default();
                
                let context = JobContext {
                    id: job.id.clone(),
                    image: job.image,
                    commands: commands_vec,
                    env: None, // Lost if not in DB
                    limits: None, // Lost if not in DB
                    callback_url: job.callback_url,
                    timeout_seconds: None, // Lost
                    artifacts_path: None, // Lost
                    user: job.user_id,
                    cache_key: None, // Lost
                    cache_paths: None, // Lost
                    on_success: vec![], // Lost
                    debug: false,
                    priority: None,
                    retry_count: 0,
                };

                info!("♻️  Re-enqueuing stuck job: {}", context.id);
                if let Err(e) = queue.enqueue(context).await {
                    error!("Failed to re-enqueue job {}: {}", job.id, e);
                }
            }
        },
        Err(e) => {
             error!("Failed to query stuck jobs: {}", e);
        }
    }
}

async fn cleanup_old_jobs(db: &DbPool, s3: &aws_sdk_s3::Client, bucket: &str) {
    info!("🧹 Running Artifact Retention Policy Cleanup...");
    
    let query = if cfg!(feature = "postgres") {
        "SELECT id, logs, artifact_url FROM jobs WHERE created_at < NOW() - INTERVAL '30 days'"
    } else {
        "SELECT id, logs, artifact_url FROM jobs WHERE created_at < date('now', '-30 days')"
    };

    let rows: Result<Vec<(String, Option<String>, Option<String>)>, _> = sqlx::query_as(query)
        .fetch_all(db).await;

    match rows {
        Ok(jobs) => {
            if jobs.is_empty() { return; }
            info!("Found {} old jobs to clean up.", jobs.len());

            for (id, logs, artifact_url) in jobs {
                if let Some(url) = artifact_url {
                    if let Some(key) = extract_s3_key(&url) {
                         info!("Deleting old artifact: {}", key);
                         let _ = s3.delete_object().bucket(bucket).key(&key).send().await;
                    }
                }
                
                if let Some(log_data) = logs {
                    if log_data.starts_with("s3://") {
                         if let Some(key) = extract_s3_key(&log_data) {
                             info!("Deleting old log: {}", key);
                             let _ = s3.delete_object().bucket(bucket).key(&key).send().await;
                         }
                    }
                }

                // Use QueryBuilder for DB compatibility ($1 vs ?)
                let mut q = sqlx::query_builder::QueryBuilder::new("UPDATE jobs SET logs = NULL, artifact_url = NULL WHERE id = ");
                q.push_bind(&id);
                let _ = q.build().execute(db).await;
            }
        },
        Err(e) => {
            error!("Failed to query old jobs: {}", e);
        }
    }
}

fn extract_s3_key(url: &str) -> Option<String> {
    // s3://bucket/key
    if url.starts_with("s3://") {
        let parts: Vec<&str> = url.splitn(4, '/').collect();
        if parts.len() >= 4 {
            return Some(parts[3].to_string());
        }
    }
    None
}

async fn reap_zombies(docker: &Docker, db: &DbPool) {
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);

    let options = ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    };

    if let Ok(containers) = docker.list_containers(Some(options)).await {
        for c in containers {
            let job_id = c.labels.as_ref()
                .and_then(|l| l.get("job_id"))
                .cloned();

            // Check if it's a DEBUG container
            let is_debug = c.labels.as_ref()
                .and_then(|l| l.get("zorp_debug"))
                .map(|v| v == "true")
                .unwrap_or(false);

            if let Some(jid) = job_id {
                 let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT status FROM jobs WHERE id = ");
                 q_builder.push_bind(&jid);
                 
                 let row = q_builder.build_query_as::< (String,) >()
                     .fetch_optional(db).await;

                 let should_kill = match row {
                     Ok(Some((status,))) => status != "RUNNING" && status != "QUEUED",
                     Ok(None) => true, // Job not in DB? Zombie.
                     Err(_) => false,
                 };

                 if should_kill {
                     if c.state.as_deref() == Some("running") {
                         warn!("逐 Reaper: Found active zombie {} (Job {}). Terminating...", c.id.as_deref().unwrap_or("?"), jid);
                         if let Some(id) = c.id.as_ref() {
                            let _ = docker.kill_container::<String>(id, None).await;
                         }
                     }
                     
                     if !is_debug {
                         if let Some(id) = c.id.as_ref() {
                            let _ = docker.remove_container(id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                         }
                     }
                 }
            }
        }
    }
}

async fn garbage_collection(docker: &Docker, registry: &JobRegistry) {
    info!("🧹 Running Garbage Collection & Pruning...");
    // 1. Clean up stale /tmp files (logs & artifacts) older than 1 hour
    let tmp_dir = std::path::Path::new("/tmp");
    if let Ok(mut entries) = tokio::fs::read_dir(tmp_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("zorp-") {
                    let job_id = if name.starts_with("zorp-artifacts-") {
                        name.trim_start_matches("zorp-artifacts-").trim_end_matches(".tar")
                    } else {
                        name.trim_start_matches("zorp-").trim_end_matches(".log")
                    };

                    if registry.read().await.contains_key(job_id) {
                         continue;
                    }

                    if let Ok(metadata) = entry.metadata().await {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(age) = SystemTime::now().duration_since(modified) {
                                if age > std::time::Duration::from_secs(3600) {
                                    info!("🧹 GC: Removing stale file {:?}", path);
                                    let _ = tokio::fs::remove_file(path).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Clean up Debug Containers (> 30 mins)
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec!["zorp_debug=true".to_string()]);
    
    let options = ListContainersOptions {
        all: true,
        filters: filters.clone(),
        ..Default::default()
    };

    if let Ok(containers) = docker.list_containers(Some(options)).await {
        for c in containers {
             if let Some(created) = c.created {
                 let created_time = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(created as u64);
                 if let Ok(age) = SystemTime::now().duration_since(created_time) {
                     if age > std::time::Duration::from_secs(1800) { 
                         if let Some(id) = c.id {
                             info!("🧹 GC: Removing expired debug container {}", id);
                             let _ = docker.remove_container(&id, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
                         }
                     }
                 }
             }
        }
    }

    // 3. Prune Docker Resources
    let _ = docker.prune_images(Some(PruneImagesOptions::<String> { filters: std::collections::HashMap::new() })).await; 
    let _ = docker.prune_volumes(Some(PruneVolumesOptions::<String> { filters: std::collections::HashMap::new() })).await;
}

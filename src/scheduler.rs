use std::sync::Arc;
use tokio_cron_scheduler::{JobScheduler, Job};
use tracing::{info, warn, error};
use crate::queue::JobQueue;
use crate::db::DbPool;
use crate::models::JobContext;
use uuid::Uuid;
use tokio::sync::broadcast::Receiver;

pub struct CronScheduler {
    scheduler: JobScheduler,
    db: DbPool,
    queue: Arc<dyn JobQueue>,
}

impl CronScheduler {
    pub async fn new(db: DbPool, queue: Arc<dyn JobQueue>) -> Result<Self, Box<dyn std::error::Error>> {
        let scheduler = JobScheduler::new().await?;
        Ok(Self { scheduler, db, queue })
    }

    pub async fn load_jobs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("⏳ Loading Cron Jobs from DB...");
        
        let rows = sqlx::query_as::<_, (String, String, String)>("SELECT id, schedule, payload FROM cron_jobs")
            .fetch_all(&self.db)
            .await?;

        for (id, schedule, payload_str) in rows {
            let queue = self.queue.clone();
            let db = self.db.clone();
            let job_id = id.clone();
            
            info!("   - Scheduled Job {}: {}", id, schedule);

            let job_closure = move |_uuid, _l| {
                let queue = queue.clone();
                let payload_str = payload_str.clone();
                let job_id = job_id.clone();
                let db = db.clone();

                Box::pin(async move {
                    info!("⏰ Triggering Cron Job: {}", job_id);
                    
                    match serde_json::from_str::<crate::models::JobRequest>(&payload_str) {
                        Ok(req) => {
                            let new_id = Uuid::new_v4().to_string();
                            let context = JobContext {
                                id: new_id.clone(),
                                image: req.image,
                                commands: req.commands,
                                env: req.env,
                                limits: req.limits.map(Arc::new),
                                callback_url: req.callback_url,
                                timeout_seconds: req.timeout_seconds,
                                artifacts_path: req.artifacts_path,
                                user: req.user,
                                cache_key: req.cache_key,
                                cache_paths: req.cache_paths,
                                services: req.services,
                                on_success: req.on_success,
                                debug: req.debug,
                                priority: req.priority,
                                retry_count: 0,
                            };
                            
                            // Log last run
                            let _ = sqlx::query("UPDATE cron_jobs SET last_run_at = CURRENT_TIMESTAMP WHERE id = $1")
                                .bind(&job_id)
                                .execute(&db).await;

                            // Insert into jobs table (required for foreign key constraints usually, or just consistency)
                            // We need to insert into 'jobs' before enqueueing so engine can update it.
                            // However, engine does UPDATE... so we need INSERT first.
                            
                            let cmds_json = match serde_json::to_string(&context.commands) {
                                Ok(j) => j,
                                Err(e) => {
                                    error!("Failed to serialize commands for job {}: {}", job_id, e);
                                    return;
                                }
                            };
                            let q_insert = sqlx::query("INSERT INTO jobs (id, status, image, commands, callback_url, user_id) VALUES ($1, 'QUEUED', $2, $3, $4, $5)")
                                .bind(&context.id)
                                .bind(&context.image)
                                .bind(&cmds_json)
                                .bind(&context.callback_url)
                                .bind(&context.user);

                            if let Err(e) = q_insert.execute(&db).await {
                                error!("Failed to persist cron job instance to DB: {}", e);
                                return;
                            }

                            if let Err(e) = queue.enqueue(context).await {
                                error!("Failed to enqueue cron job instance: {}", e);
                            } else {
                                info!("✅ Enqueued cron job instance: {}", new_id);
                            }
                        },
                        Err(e) => {
                            error!("Failed to parse cron job payload: {}", e);
                        }
                    }
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            };

            let job = Job::new_async(schedule.as_str(), job_closure)?;
            self.scheduler.add(job).await?;
        }

        self.scheduler.start().await?;
        Ok(())
    }
}

pub async fn spawn(db: DbPool, queue: Arc<dyn JobQueue>, mut shutdown: Receiver<()>) {
    let mut cron_scheduler = match CronScheduler::new(db, queue).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to initialize CronScheduler: {}", e);
            return;
        }
    };

    if let Err(e) = cron_scheduler.load_jobs().await {
        error!("Failed to load cron jobs: {}", e);
    }

    // Wait for shutdown signal
    let _ = shutdown.recv().await;
    info!("⏳ Stopping Cron Scheduler...");
    // There is no clean 'stop' method on the scheduler instance that we hold easily without Arc/Mutex, 
    // but dropping it or process exit handles it.
    // However, since we own it here, it will be dropped when this task ends.
}

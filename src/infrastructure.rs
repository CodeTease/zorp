use crate::db::{self, DbPool};
use crate::queue::{RedisQueue, JobQueue};
use crate::models::JobRegistry;
use bollard::Docker;
use aws_config::BehaviorVersion;
use std::env;
use std::sync::Arc;
use tracing::{error, info, warn};
use tokio::sync::RwLock;
use std::collections::HashMap;
use bollard::container::ListContainersOptions;

pub struct Infrastructure {
    pub db_pool: DbPool,
    pub queue: Arc<RedisQueue>,
    pub docker: Docker,
    pub s3_client: aws_sdk_s3::Client,
    pub s3_bucket: String,
    pub job_registry: JobRegistry,
}

pub async fn setup() -> Result<Infrastructure, Box<dyn std::error::Error>> {
    // 1. Initialize DB (with Retry)
    let mut db_retry_attempts = 0;
    let db_pool = loop {
        match db::init_pool().await {
            Ok(pool) => break pool,
            Err(e) => {
                db_retry_attempts += 1;
                if db_retry_attempts > 5 {
                    error!("❌ Failed to connect to DB after 5 attempts. Exiting.");
                    return Err(e);
                }
                warn!("⚠️  DB Connection failed: {}. Retrying in 5s... ({}/5)", e, db_retry_attempts);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    };
    info!("✅ Database connected successfully.");

    // 2. Initialize Redis Queue
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    if redis_url.starts_with("rediss://") {
        info!("🔐 Initializing Redis with TLS encryption...");
    } else {
        info!("🔌 Initializing Redis via standard connection...");
    }

    let queue = Arc::new(RedisQueue::new(&redis_url));
    info!("Connected to Redis Queue successfully.");

    // Restore stranded jobs
    info!("🔎 Checking for stranded jobs in processing queue...");
    match queue.restore_stranded().await {
        Ok(0) => info!("✅ No stranded jobs found."),
        Ok(count) => info!("♻️  Restored {} stranded jobs to the main queue.", count),
        Err(e) => error!("❌ Failed to restore stranded jobs: {}", e),
    }

    // Start Delayed Job Monitor
    let delay_queue = queue.clone();
    tokio::spawn(async move {
        info!("⏰ Delayed Job Monitor started.");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if let Err(e) = delay_queue.monitor_delayed_jobs().await {
                error!("Delayed job monitor error: {}", e);
            }
        }
    });

    // 3. Initialize Docker
    let docker = Docker::connect_with_local_defaults()?;

    // 3b. Initialize JobRegistry
    info!("🔄 Performing State Reconciliation...");
    let job_registry: JobRegistry = Arc::new(RwLock::new(HashMap::new()));
    
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec!["managed_by=zorp".to_string()]);
    
    let options = ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    };
    
    if let Ok(containers) = docker.list_containers(Some(options)).await {
        let mut reg = job_registry.write().await;
        for c in containers {
            // Only care about running containers to populate registry
            if c.state.as_deref() == Some("running") {
                if let (Some(id), Some(labels)) = (c.id, c.labels) {
                     if let Some(job_id) = labels.get("job_id") {
                         // Verify with DB
                         let mut q_builder = sqlx::query_builder::QueryBuilder::new("SELECT status FROM jobs WHERE id = ");
                         q_builder.push_bind(job_id);

                         if let Ok(Some((status,))) = q_builder.build_query_as::< (String,) >()
                            .fetch_optional(&db_pool).await 
                         {
                             if status == "RUNNING" {
                                 info!("   - Re-attached job: {} (Container: {})", job_id, id);
                                 reg.insert(job_id.clone(), id.clone());
                                 // Note: We cannot re-attach log stream easily for recovered jobs 
                                 // without implementing `docker attach` logic again, which is complex.
                                 // For now, recovered jobs won't support live streaming, only file logging.
                             } else {
                                 warn!("   - Zombie container found (Job {} is {}). Will be reaped.", job_id, status);
                             }
                         }
                     }
                }
            }
        }
    }
    info!("✅ State Reconciliation Complete. Active Jobs: {}", job_registry.read().await.len());

    // Mandatory S3 Configuration
    let s3_bucket = env::var("S3_BUCKET_NAME").map_err(|_| {
        error!("❌ S3_BUCKET_NAME is mandatory in Production Mode.");
        std::io::Error::new(std::io::ErrorKind::Other, "Missing S3_BUCKET_NAME")
    })?;

    info!("Configuring S3 client...");
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

    if let Ok(endpoint) = env::var("S3_ENDPOINT_URL") {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }
    if let Ok(force_path) = env::var("S3_FORCE_PATH_STYLE") {
            s3_config_builder = s3_config_builder.force_path_style(force_path.parse().unwrap_or(false));
    }
    
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    Ok(Infrastructure {
        db_pool,
        queue,
        docker,
        s3_client,
        s3_bucket,
        job_registry,
    })
}

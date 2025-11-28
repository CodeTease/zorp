// Zorp v0.1.0 - The Bulletproof Edition
// Copyright (c) 2025 CodeTease.

mod api;
mod db;
mod engine;
mod models;

use bollard::Docker;
use dotenvy::dotenv;
use std::env;

#[cfg(feature = "s3_logging")]
use aws_config::{self, BehaviorVersion};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

const PORT: u16 = 3000;
const MAX_CONCURRENT_JOBS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    let secret_key = env::var("ZORP_SECRET_KEY")
        .map_err(|_| {
            error!("ZORP_SECRET_KEY environment variable not set.");
            std::io::Error::new(std::io::ErrorKind::Other, "Missing ZORP_SECRET_KEY")
        })?;

    info!(":: Zorp v0.1.0 ::");
    
    // 1. Initialize DB
    let db_pool = db::init_pool().await?;

    // 2. Initialize Docker
    let docker = Docker::connect_with_local_defaults()?;

    // 3. Run Zombie Reaper (Clean up previous mess)
    engine::startup_reaper(&docker, &db_pool).await;

    // 4. Initialize Engine
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    
    // -- S3 CLIENT SETUP (CONDITIONAL COMPILE) --
    // Chỉ biên dịch đoạn này nếu có feature s3_logging
    #[cfg(feature = "s3_logging")]
    let (s3_client, s3_bucket) = {
        let mut client = None;
        let mut bucket = None;

        if let Ok(val) = env::var("ENABLE_S3_LOGGING") {
            if val.parse().unwrap_or(false) {
                info!("S3 logging is enabled. Configuring S3 client...");
                
                let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
                let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

                if let Ok(endpoint) = env::var("S3_ENDPOINT_URL") {
                    s3_config_builder = s3_config_builder.endpoint_url(endpoint);
                }

                if let Ok(force_path) = env::var("S3_FORCE_PATH_STYLE") {
                        s3_config_builder = s3_config_builder.force_path_style(force_path.parse().unwrap_or(false));
                }
                
                client = Some(aws_sdk_s3::Client::from_conf(s3_config_builder.build()));
                bucket = env::var("S3_BUCKET_NAME").ok();

                if bucket.is_none() {
                    error!("S3_BUCKET_NAME is not set. S3 logging will be disabled.");
                    client = None;
                } else {
                    info!("S3 client configured for bucket: {:?}", bucket.as_ref().unwrap());
                }
            } else {
                info!("S3 logging is disabled via configuration. Using database for logs.");
            }
        } else {
            info!("S3 logging is disabled by default. Using database for logs.");
        }
        (client, bucket)
    };
    
    let dispatcher = Arc::new(engine::Dispatcher::new(
        docker.clone(),
        db_pool.clone(),
        http_client,
        MAX_CONCURRENT_JOBS,
        // Chỉ truyền tham số này vào hàm new nếu feature bật
        #[cfg(feature = "s3_logging")]
        s3_client.clone(),
        #[cfg(feature = "s3_logging")]
        s3_bucket.clone(),
    ));

    // 5. Setup App State
    let state = Arc::new(api::AppState {
        db: db_pool,
        dispatcher,
        secret_key,
        // Chỉ khởi tạo field này nếu feature bật
        #[cfg(feature = "s3_logging")]
        s3_client,
        #[cfg(feature = "s3_logging")]
        s3_bucket,
    });

    // 6. Start Server
    let app = api::create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
// Zorp v0.1.0 - The Bulletproof Edition
// Copyright (c) 2025 CodeTease.

mod api;
mod db;
mod engine;
mod models;

use bollard::Docker;
use dotenvy::dotenv;
use std::env;
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
    
    let dispatcher = Arc::new(engine::Dispatcher::new(
        docker.clone(),
        db_pool.clone(),
        http_client,
        MAX_CONCURRENT_JOBS
    ));

    // 5. Setup App State
    let state = Arc::new(api::AppState {
        db: db_pool,
        dispatcher,
        secret_key,
    });

    // 6. Start Server
    let app = api::create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
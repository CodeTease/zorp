// Zorp
// The ephemeral CI/CD dispatcher.
// Copyright (c) 2025 CodeTease.

use axum::{
    extract::{State, Json},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use bollard::container::{
    Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use futures_util::TryStreamExt;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

// --- CONFIGURATION ---
const PORT: u16 = 3000;
const MAX_CONCURRENT_JOBS: usize = 20; 
const DOCKER_SOCKET: &str = "unix:///var/run/docker.sock";

// --- DATA STRUCTURES ---

#[derive(Debug, Deserialize)]
struct JobRequest {
    image: String,
    commands: Vec<String>,
}

#[derive(Debug, Clone)]
struct Job {
    id: String,
    image: String,
    commands: Vec<String>,
}

struct AppState {
    docker: Docker,
    limiter: Arc<Semaphore>,
}

// --- ENGINE LOGIC ---

impl AppState {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let docker = Docker::connect_with_socket(DOCKER_SOCKET, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self {
            docker,
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS)),
        })
    }

    async fn dispatch_job(&self, job: Job) {
        let permit = self.limiter.clone().acquire_owned().await.unwrap();
        let docker = self.docker.clone();

        tokio::spawn(async move {
            let _permit = permit; // Hold semaphore permit
            
            let start_time = Instant::now();
            let job_id = job.id.clone();
            let container_name = format!("zorp-{}", job_id);

            tracing::info!("[{}] Job accepted. Spawning runner...", job_id);

            // 1. Pull Image
            if let Err(e) = ensure_image(&docker, &job.image).await {
                tracing::error!("[{}] Image pull failed: {}", job_id, e);
                return;
            }

            // 2. Configure Container
            let config = Config {
                image: Some(job.image.clone()),
                cmd: Some(job.commands.clone()),
                host_config: Some(HostConfig {
                    auto_remove: Some(false), 
                    memory: Some(1024 * 1024 * 512), // 512MB limit
                    cpu_quota: Some(100000), // 1 CPU Core
                    ..Default::default()
                }),
                tty: Some(true),
                ..Default::default()
            };

            // 3. Run Container
            match docker.create_container(
                Some(CreateContainerOptions { name: container_name.as_str(), platform: None }), 
                config
            ).await {
                Ok(_) => {
                    if let Err(e) = docker.start_container(&container_name, None::<StartContainerOptions<String>>).await {
                        tracing::error!("[{}] Start failed: {}", job_id, e);
                    } else {
                        stream_logs(&docker, &container_name, &job_id).await;

                        let _ = docker.wait_container(&container_name, None::<WaitContainerOptions<String>>)
                            .try_collect::<Vec<_>>()
                            .await;
                    }
                }
                Err(e) => tracing::error!("[{}] Creation failed: {}", job_id, e),
            }

            // 4. Cleanup
            tracing::info!("[{}] Cleaning up...", job_id);
            let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;

            tracing::info!("[{}] Finished in {:.2?}s", job_id, start_time.elapsed());
        });
    }
}

async fn ensure_image(docker: &Docker, image: &str) -> Result<(), bollard::errors::Error> {
    let image = if !image.contains(':') { format!("{}:latest", image) } else { image.to_string() };
    
    if docker.inspect_image(&image).await.is_ok() { return Ok(()); }

    tracing::info!("Pulling image: {}", image);
    let mut stream = docker.create_image(Some(CreateImageOptions { from_image: image.clone(), ..Default::default() }), None, None);
    while let Some(_) = stream.try_next().await? {}
    Ok(())
}

async fn stream_logs(docker: &Docker, name: &str, job_id: &str) {
    let mut stream = docker.logs(name, Some(LogsOptions::<String> {
        stdout: true, stderr: true, follow: true, tail: "all".to_string(), ..Default::default()
    }));

    while let Ok(Some(log)) = stream.try_next().await {
        let msg = match log {
            LogOutput::StdOut { message } | LogOutput::Console { message } => String::from_utf8_lossy(&message).to_string(),
            LogOutput::StdErr { message } => String::from_utf8_lossy(&message).to_string(),
            _ => String::new(),
        };
        print!("[LOG|{}] {}", job_id, msg);
    }
}

// --- HTTP HANDLERS ---

async fn health_check() -> &'static str {
    "Zorp is running."
}

async fn handle_dispatch(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JobRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let job_id = Uuid::new_v4().to_string();
    
    let job = Job {
        id: job_id.clone(),
        image: payload.image,
        commands: payload.commands,
    };

    state.dispatch_job(job).await;

    (StatusCode::ACCEPTED, Json(serde_json::json!({
        "status": "queued",
        "job_id": job_id
    })))
}

// --- MAIN ENTRYPOINT ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // The clean, simple startup log
    tracing::info!(":: Zorp ::"); 
    
    let state = Arc::new(AppState::new()?);

    let app = Router::new()
        .route("/", get(health_check))
        .route("/dispatch", post(handle_dispatch))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], PORT));
    tracing::info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
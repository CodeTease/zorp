use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error};
use aws_sdk_s3::Client;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::Archive;

pub async fn restore_cache(
    s3_client: &Client,
    bucket: &str,
    cache_key: &str,
    target_path: &Path,
) -> Result<(), String> {
    let s3_key = format!("cache/{}.tar.gz", cache_key);
    info!("Checking for cache at s3://{}/{}", bucket, s3_key);

    // 1. Download
    let resp = s3_client
        .get_object()
        .bucket(bucket)
        .key(&s3_key)
        .send()
        .await
        .map_err(|e| format!("Failed to download cache: {}", e))?;

    let temp_tar_path = format!("/tmp/cache-{}.tar.gz", cache_key);
    let mut temp_file = File::create(&temp_tar_path).await
        .map_err(|e| format!("Failed to create temp file: {}", e))?;

    let mut stream = resp.body.into_async_read();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = stream.read(&mut buffer).await
            .map_err(|e| format!("Error reading S3 stream: {}", e))?;
        if bytes_read == 0 { break; }
        temp_file.write_all(&buffer[..bytes_read]).await
            .map_err(|e| format!("Error writing to temp file: {}", e))?;
    }
    
    // Ensure data is flushed
    temp_file.flush().await.map_err(|e| format!("Error flushing temp file: {}", e))?;

    // 2. Extract
    info!("Extracting cache to {:?}", target_path);
    if !target_path.exists() {
        fs::create_dir_all(target_path).await
            .map_err(|e| format!("Failed to create cache dir: {}", e))?;
    }

    // Blocking operation in async context? "tar" crate is synchronous.
    // We should use spawn_blocking for CPU/IO heavy sync operations.
    let temp_tar_path_clone = temp_tar_path.clone();
    let target_path_clone = target_path.to_path_buf();

    let extract_result = tokio::task::spawn_blocking(move || {
        let tar_gz = std::fs::File::open(temp_tar_path_clone)
            .map_err(|e| format!("Failed to open tar.gz: {}", e))?;
        let tar = GzDecoder::new(tar_gz);
        let mut archive = Archive::new(tar);
        archive.unpack(target_path_clone)
            .map_err(|e| format!("Failed to unpack archive: {}", e))?;
        Ok::<(), String>(())
    }).await;

    // Cleanup temp file
    let _ = fs::remove_file(&temp_tar_path).await;

    match extract_result {
        Ok(res) => res,
        Err(e) => Err(format!("Task join error: {}", e)),
    }
}

pub async fn save_cache(
    s3_client: &Client,
    bucket: &str,
    cache_key: &str,
    source_path: &Path,
) -> Result<(), String> {
    if !source_path.exists() {
        return Ok(()); // Nothing to save
    }

    let s3_key = format!("cache/{}.tar.gz", cache_key);
    let temp_tar_path = format!("/tmp/cache-{}-save.tar.gz", cache_key);
    
    info!("Compressing cache from {:?}", source_path);

    let temp_tar_path_clone = temp_tar_path.clone();
    let source_path_clone = source_path.to_path_buf();

    // Blocking compression
    let compress_result = tokio::task::spawn_blocking(move || {
        let tar_gz = std::fs::File::create(&temp_tar_path_clone)
            .map_err(|e| format!("Failed to create output file: {}", e))?;
        let enc = GzEncoder::new(tar_gz, Compression::default());
        let mut tar = tar::Builder::new(enc);
        tar.append_dir_all(".", source_path_clone)
            .map_err(|e| format!("Failed to append dir to tar: {}", e))?;
        tar.finish().map_err(|e| format!("Failed to finish tar: {}", e))?;
        Ok::<(), String>(())
    }).await;

    match compress_result {
        Ok(Ok(_)) => {},
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(format!("Compression task failed: {}", e)),
    }

    // Upload
    info!("Uploading cache to s3://{}/{}", bucket, s3_key);
    let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(&temp_tar_path)).await;
    
    match body {
        Ok(b) => {
            let _ = s3_client.put_object()
                .bucket(bucket)
                .key(&s3_key)
                .body(b)
                .send()
                .await
                .map_err(|e| format!("Failed to upload cache: {}", e))?;
        },
        Err(e) => return Err(format!("Failed to read compressed cache file: {}", e)),
    }

    let _ = fs::remove_file(&temp_tar_path).await;
    Ok(())
}

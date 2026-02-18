use std::path::{Path, PathBuf};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tracing::info;

use crate::error::TranscoderError;

// ---------------------------------------------------------------------------
// R2Client – thin wrapper around the S3-compatible SDK client
// ---------------------------------------------------------------------------

pub struct R2Client {
    client: Client,
    bucket: String,
    public_url: String,
}

impl R2Client {
    pub fn new(client: Client, bucket: String, public_url: String) -> Self {
        Self {
            client,
            bucket,
            public_url,
        }
    }

    // ── Upload a single file ─────────────────────────────────────────────

    pub async fn upload_file(&self, local_path: &Path, key: &str) -> Result<(), TranscoderError> {
        let body = ByteStream::from_path(local_path).await.map_err(|e| {
            TranscoderError::Upload(format!(
                "Failed to read {}: {}",
                local_path.display(),
                e
            ))
        })?;

        let content_type = guess_content_type(local_path);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| TranscoderError::Upload(format!("PutObject {key}: {e}")))?;

        Ok(())
    }

    // ── Upload an entire directory (iterative, not recursive-async) ──────

    pub async fn upload_directory(
        &self,
        local_dir: &Path,
        prefix: &str,
    ) -> Result<Vec<String>, TranscoderError> {
        let mut uploaded: Vec<String> = Vec::new();
        let mut stack: Vec<PathBuf> = vec![local_dir.to_path_buf()];

        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| TranscoderError::Upload(format!("read_dir: {e}")))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| TranscoderError::Upload(e.to_string()))?
            {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.is_file() {
                    let relative = path
                        .strip_prefix(local_dir)
                        .map_err(|e| TranscoderError::Upload(e.to_string()))?;
                    let key = format!("{}/{}", prefix, relative.display());
                    info!(key = %key, "Uploading HLS artefact");
                    self.upload_file(&path, &key).await?;
                    uploaded.push(key);
                }
            }
        }

        info!(count = uploaded.len(), prefix, "Directory upload complete");
        Ok(uploaded)
    }

    // ── Delete a single object ─────────────────────────────────────────

    pub async fn delete_object(&self, key: &str) -> Result<(), TranscoderError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| TranscoderError::Upload(format!("DeleteObject {key}: {e}")))?;
        Ok(())
    }

    // ── Public URL helper ────────────────────────────────────────────────

    pub fn public_url(&self, key: &str) -> String {
        format!(
            "{}/{}",
            self.public_url.trim_end_matches('/'),
            key.trim_start_matches('/')
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn guess_content_type(path: &Path) -> String {
    match path.extension().and_then(|e| e.to_str()) {
        Some("m3u8") => "application/vnd.apple.mpegurl".into(),
        Some("ts") => "video/mp2t".into(),
        Some("m4s") => "video/iso.segment".into(),
        Some("mp4") => "video/mp4".into(),
        Some("json") => "application/json".into(),
        _ => "application/octet-stream".into(),
    }
}

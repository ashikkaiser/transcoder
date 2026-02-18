use std::sync::Arc;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::db::repository::JobRepository;
use crate::queue::JobQueue;
use crate::settings::SettingsStore;

// ---------------------------------------------------------------------------
// Expected response format from the external polling endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PollResponse {
    #[serde(default)]
    data: Vec<PollVideo>,
}

#[derive(Debug, Deserialize)]
struct PollVideo {
    id: i64,
    s3_key: String,
    video_url: String,
    #[serde(default)]
    callback_url: String,
}

// ---------------------------------------------------------------------------
// Poller — periodically fetches pending videos from an external API
// ---------------------------------------------------------------------------

pub fn start(
    repo: Arc<JobRepository>,
    queue: JobQueue,
    settings: SettingsStore,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client for poller");

        info!("Poller task started");

        loop {
            // Read current config each iteration so changes take effect immediately
            let (enabled, endpoint, interval_secs, auth_header) = settings.poll_config().await;

            if !enabled || endpoint.is_empty() {
                // Polling disabled — sleep a bit then re-check
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                    _ = shutdown.cancelled() => {
                        info!("Poller shutting down");
                        return;
                    }
                }
                continue;
            }

            debug!(endpoint = %endpoint, interval = interval_secs, "Polling for pending videos");

            match poll_once(&http, &endpoint, &auth_header, &repo, &queue, &settings).await {
                Ok(count) => {
                    if count > 0 {
                        info!(count, endpoint = %endpoint, "Poller fetched new videos");
                    } else {
                        debug!("Poller: no new videos");
                    }
                }
                Err(e) => {
                    warn!(error = %e, endpoint = %endpoint, "Poller request failed");
                }
            }

            // Wait for the configured interval, or shutdown
            let sleep_dur = std::time::Duration::from_secs(interval_secs.max(5));
            tokio::select! {
                _ = tokio::time::sleep(sleep_dur) => {}
                _ = shutdown.cancelled() => {
                    info!("Poller shutting down");
                    return;
                }
            }
        }
    });
}

async fn poll_once(
    http: &reqwest::Client,
    endpoint: &str,
    auth_header: &Option<(String, String)>,
    repo: &JobRepository,
    queue: &JobQueue,
    settings: &SettingsStore,
) -> anyhow::Result<usize> {
    let mut req = http.get(endpoint);

    if let Some((name, value)) = auth_header {
        req = req.header(name.as_str(), value.as_str());
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("Poll endpoint returned HTTP {}", resp.status());
    }

    let body: PollResponse = resp.json().await?;

    if body.data.is_empty() {
        return Ok(0);
    }

    let max_attempts = settings.max_job_attempts().await;
    let mut created_count = 0usize;

    for v in &body.data {
        if v.id <= 0 {
            warn!(video_id = v.id, "Poller: skipping invalid video_id");
            continue;
        }
        if v.s3_key.is_empty() || v.video_url.is_empty() {
            warn!(video_id = v.id, "Poller: skipping video with empty s3_key/video_url");
            continue;
        }

        match repo
            .insert_or_get_active(v.id, &v.s3_key, &v.video_url, &v.callback_url, max_attempts)
            .await
        {
            Ok((_job, created)) => {
                if created {
                    created_count += 1;
                }
            }
            Err(e) => {
                error!(video_id = v.id, error = %e, "Poller: failed to insert job");
            }
        }
    }

    if created_count > 0 {
        queue.notify_new_job();
    }

    Ok(created_count)
}

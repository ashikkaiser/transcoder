use reqwest::Client;
use serde::Serialize;
use tracing::{info, warn};

use crate::error::TranscoderError;
use crate::retry;
use crate::settings::SettingsStore;

// ---------------------------------------------------------------------------
// Callback payload – sent to the origin server after job completes / fails
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CallbackPayload {
    pub video_id: i64,
    pub status: String,
    pub final_m3u8_url: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub attempt: i32,
    pub metadata: Option<TranscodeMetadata>,
}

#[derive(Debug, Serialize)]
pub struct TranscodeMetadata {
    pub duration_seconds: Option<f64>,
    pub renditions: Vec<String>,
}

// ---------------------------------------------------------------------------
// Callback attempt result — returned so the caller can log visibility events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct CallbackAttempt {
    pub number: u32,
    pub url: String,
    pub status_code: Option<u16>,
    pub error: Option<String>,
    pub success: bool,
}

#[derive(Debug)]
pub struct CallbackReport {
    #[allow(dead_code)]
    pub url: Option<String>,
    pub attempts: Vec<CallbackAttempt>,
    pub skipped: bool,
}

// ---------------------------------------------------------------------------
// send_callback – POST with retries, back-off, and optional auth header
// Returns a report of all attempts for webhook visibility logging.
// ---------------------------------------------------------------------------

pub async fn send_callback(
    http: &Client,
    settings: &SettingsStore,
    callback_url: &str,
    payload: &CallbackPayload,
    max_retries: u32,
) -> Result<CallbackReport, TranscoderError> {
    let base_url = settings.callback_base_url().await;
    let url = match resolve_url(&base_url, callback_url) {
        Some(u) => u,
        None => {
            info!(
                video_id = payload.video_id,
                "No callback URL configured, skipping callback"
            );
            return Ok(CallbackReport {
                url: None,
                attempts: vec![],
                skipped: true,
            });
        }
    };
    let auth_header = settings.callback_auth_header().await;

    info!(url = %url, video_id = payload.video_id, "Sending callback");

    let mut attempts = Vec::new();
    let mut last_error = String::new();

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let delay = retry::calculate_backoff(attempt as i32, 2, 60);
            info!(attempt, delay_secs = delay.as_secs(), "Retrying callback");
            tokio::time::sleep(delay).await;
        }

        let mut req = http
            .post(&url)
            .json(payload)
            .timeout(std::time::Duration::from_secs(30));

        if let Some((header_name, header_value)) = &auth_header {
            req = req.header(header_name, header_value);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    info!(video_id = payload.video_id, "Callback succeeded");
                    attempts.push(CallbackAttempt {
                        number: attempt + 1,
                        url: url.clone(),
                        status_code: Some(status.as_u16()),
                        error: None,
                        success: true,
                    });
                    return Ok(CallbackReport {
                        url: Some(url),
                        attempts,
                        skipped: false,
                    });
                }
                let is_non_retryable = status.is_client_error() && status.as_u16() != 429;
                let err_msg = format!("HTTP {status}");
                attempts.push(CallbackAttempt {
                    number: attempt + 1,
                    url: url.clone(),
                    status_code: Some(status.as_u16()),
                    error: Some(err_msg.clone()),
                    success: false,
                });
                if is_non_retryable {
                    return Err(TranscoderError::Callback(format!(
                        "Non-retryable HTTP {status}"
                    )));
                }
                last_error = err_msg;
                warn!(status = %status, attempt, "Callback failed, will retry");
            }
            Err(e) => {
                let err_msg = e.to_string();
                attempts.push(CallbackAttempt {
                    number: attempt + 1,
                    url: url.clone(),
                    status_code: None,
                    error: Some(err_msg.clone()),
                    success: false,
                });
                last_error = err_msg;
                warn!(error = %e, attempt, "Callback request error");
            }
        }
    }

    Err(TranscoderError::Callback(format!(
        "Failed after {} attempts: {last_error}",
        max_retries + 1
    )))
}

/// Resolve the final callback URL from the base URL and per-job callback_url.
///
/// Rules:
///   1. callback_url starts with http:// or https://  →  use as-is (ignore base)
///   2. callback_url is a relative path (e.g. `/api/callback`)  →  join with base
///   3. callback_url is empty  →  use base URL alone
///   4. Both empty  →  return None (skip callback)
///
/// Slash handling: double slashes between base and path are always normalised.
///   base = "https://api.example.com/"  +  url = "/api/done"  →  "https://api.example.com/api/done"
///   base = "https://api.example.com"   +  url = "api/done"   →  "https://api.example.com/api/done"
fn resolve_url(base: &str, url: &str) -> Option<String> {
    let url = url.trim();
    let base = base.trim();

    // 1) Absolute URL → use directly
    if url.starts_with("http://") || url.starts_with("https://") {
        return Some(url.to_string());
    }

    // 2) Both empty → no callback
    if base.is_empty() && url.is_empty() {
        return None;
    }

    // 3) callback_url is empty → use base URL alone
    if url.is_empty() {
        return if base.is_empty() {
            None
        } else {
            Some(base.to_string())
        };
    }

    // 4) base is empty but callback_url is a relative path → can't resolve
    if base.is_empty() {
        return None;
    }

    // 5) Join base + relative path, normalising slashes
    let base = base.trim_end_matches('/');
    let url = url.trim_start_matches('/');
    Some(format!("{base}/{url}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_url_ignores_base() {
        assert_eq!(
            resolve_url("https://api.example.com", "https://other.com/hook"),
            Some("https://other.com/hook".into())
        );
    }

    #[test]
    fn relative_path_joins_with_base() {
        assert_eq!(
            resolve_url("https://api.example.com", "/api/callback"),
            Some("https://api.example.com/api/callback".into())
        );
    }

    #[test]
    fn double_slash_normalised() {
        assert_eq!(
            resolve_url("https://api.example.com/", "/api/callback"),
            Some("https://api.example.com/api/callback".into())
        );
    }

    #[test]
    fn no_slashes_added_correctly() {
        assert_eq!(
            resolve_url("https://api.example.com", "api/callback"),
            Some("https://api.example.com/api/callback".into())
        );
    }

    #[test]
    fn empty_callback_uses_base() {
        assert_eq!(
            resolve_url("https://api.example.com/webhook", ""),
            Some("https://api.example.com/webhook".into())
        );
    }

    #[test]
    fn both_empty_returns_none() {
        assert_eq!(resolve_url("", ""), None);
    }

    #[test]
    fn empty_base_with_relative_returns_none() {
        assert_eq!(resolve_url("", "/api/callback"), None);
    }

    #[test]
    fn empty_base_with_absolute_works() {
        assert_eq!(
            resolve_url("", "https://other.com/hook"),
            Some("https://other.com/hook".into())
        );
    }
}

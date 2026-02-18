use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

// ---------------------------------------------------------------------------
// Internal transcoder error – classifies retryable vs non-retryable failures
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum TranscoderError {
    #[error("Download failed: {0}")]
    Download(String),

    #[error("Transcode failed: {0}")]
    Transcode(String),

    #[error("Upload failed: {0}")]
    Upload(String),

    #[error("Callback failed: {0}")]
    Callback(String),

    #[error("Video not found: {0}")]
    VideoNotFound(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Job canceled")]
    Canceled,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl TranscoderError {
    /// Returns `true` when the job should be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Download(_)
                | Self::Transcode(_)
                | Self::Upload(_)
                | Self::Callback(_)
                | Self::Internal(_)
        )
    }
}

// ---------------------------------------------------------------------------
// API error wrapper – used as the Err variant in axum handler return types
// ---------------------------------------------------------------------------

pub struct AppError {
    pub status: StatusCode,
    pub message: String,
}

impl AppError {
    pub fn internal(msg: impl ToString) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.to_string(),
        }
    }

    pub fn bad_request(msg: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn not_found(msg: impl ToString) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!(status = %self.status, "API error: {}", self.message);
        let body = json!({ "error": self.message });
        (self.status, Json(body)).into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        Self::internal(err)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self::internal(err)
    }
}

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TranscodeRequest {
    pub videos: Vec<VideoInput>,
}

#[derive(Debug, Deserialize)]
pub struct VideoInput {
    pub id: i64,
    pub s3_key: String,
    pub video_url: String,
    /// Optional — when empty, the callback base URL from settings is used.
    #[serde(default)]
    pub callback_url: String,
}

#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    pub video_id: i64,
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TranscodeResponse {
    pub accepted: Vec<AcceptedJob>,
}

#[derive(Debug, Serialize)]
pub struct AcceptedJob {
    pub video_id: i64,
    pub job_id: String,
    pub status: String,
    /// `true` if a new job was created; `false` if an existing active job was
    /// returned (idempotency).
    pub created: bool,
}

#[derive(Debug, Serialize)]
pub struct CancelResponse {
    pub video_id: i64,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub database: String,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub job: Option<crate::db::models::Job>,
    pub events: Vec<crate::db::models::JobEvent>,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub jobs_by_status: Vec<StatusCount>,
    pub worker_concurrency: usize,
    pub system: crate::system::SystemSnapshot,
}

#[derive(Debug, Serialize)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

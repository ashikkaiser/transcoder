use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// JobStatus enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Downloading,
    Transcoding,
    Uploading,
    CallbackPending,
    CallbackFailed,
    Completed,
    Failed,
    Canceled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Transcoding => "transcoding",
            Self::Uploading => "uploading",
            Self::CallbackPending => "callback_pending",
            Self::CallbackFailed => "callback_failed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "downloading" => Some(Self::Downloading),
            "transcoding" => Some(Self::Transcoding),
            "uploading" => Some(Self::Uploading),
            "callback_pending" => Some(Self::CallbackPending),
            "callback_failed" => Some(Self::CallbackFailed),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Canceled)
    }

    #[allow(dead_code)]
    pub fn is_active(&self) -> bool {
        !self.is_terminal() && *self != Self::Failed
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Job row – 1-to-1 map of the `jobs` table
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Job {
    pub id: String,
    pub video_id: i64,
    pub s3_key: String,
    pub video_url: String,
    pub callback_url: String,
    pub status: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub priority: i32,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub last_error: Option<String>,
    pub next_retry_at: Option<String>,
    pub canceled_at: Option<String>,
    pub cancel_reason: Option<String>,
    pub output_prefix: Option<String>,
    pub final_m3u8_url: Option<String>,
    pub worker_id: Option<String>,
    pub claimed_at: Option<String>,
    pub metadata: Option<String>,
    pub callback_attempt: i32,
}

impl Job {
    #[allow(dead_code)]
    pub fn status_enum(&self) -> JobStatus {
        JobStatus::from_str(&self.status).unwrap_or(JobStatus::Failed)
    }
}

// ---------------------------------------------------------------------------
// JobEvent row – audit / history log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct JobEvent {
    pub id: i64,
    pub job_id: String,
    pub event_type: String,
    pub old_status: Option<String>,
    pub new_status: Option<String>,
    pub details: Option<String>,
    pub created_at: String,
}

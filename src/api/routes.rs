use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use tracing::info;

use crate::cancellation::CancellationRegistry;
use crate::db::repository::JobRepository;
use crate::error::AppError;
use crate::queue::JobQueue;
use crate::settings::SettingsStore;
use crate::system::SystemMonitor;

use super::models::*;

// ---------------------------------------------------------------------------
// Shared state handed to every handler via axum's State extractor.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    pub repo: Arc<JobRepository>,
    pub queue: JobQueue,
    pub cancel_registry: Arc<CancellationRegistry>,
    pub start_time: std::time::Instant,
    pub sys_monitor: Arc<SystemMonitor>,
    pub settings: SettingsStore,
}

// ---------------------------------------------------------------------------
// POST /transcode
// ---------------------------------------------------------------------------

pub async fn handle_transcode(
    State(state): State<ApiState>,
    Json(payload): Json<TranscodeRequest>,
) -> Result<Json<TranscodeResponse>, AppError> {
    // ── Input validation ────────────────────────────────────────────
    if payload.videos.is_empty() {
        return Err(AppError::bad_request("videos array must not be empty"));
    }
    if payload.videos.len() > 100 {
        return Err(AppError::bad_request(
            "Batch size too large (max 100 videos per request)",
        ));
    }

    let mut accepted = Vec::with_capacity(payload.videos.len());

    for v in &payload.videos {
        if v.id <= 0 {
            return Err(AppError::bad_request(format!(
                "Invalid video_id {}: must be a positive integer",
                v.id
            )));
        }
        if v.s3_key.is_empty() || v.video_url.is_empty() {
            return Err(AppError::bad_request(format!(
                "Missing required fields (s3_key, video_url) for video_id {}",
                v.id
            )));
        }
        // Guard against path traversal in s3_key
        if v.s3_key.contains("..") || v.s3_key.starts_with('/') {
            return Err(AppError::bad_request(format!(
                "Invalid s3_key for video_id {}: path traversal not allowed",
                v.id
            )));
        }
        // Basic URL validation
        if !v.video_url.starts_with("http://") && !v.video_url.starts_with("https://") {
            return Err(AppError::bad_request(format!(
                "Invalid video_url for video_id {}: must be http(s)",
                v.id
            )));
        }

        let max_attempts = state.settings.max_job_attempts().await;
        let (job, created) = state
            .repo
            .insert_or_get_active(
                v.id,
                &v.s3_key,
                &v.video_url,
                &v.callback_url,
                max_attempts,
            )
            .await?;

        info!(video_id = v.id, job_id = %job.id, created, "Job accepted");

        accepted.push(AcceptedJob {
            video_id: v.id,
            job_id: job.id,
            status: job.status,
            created,
        });
    }

    // Wake workers
    state.queue.notify_new_job();

    Ok(Json(TranscodeResponse { accepted }))
}

// ---------------------------------------------------------------------------
// POST /transcode/cancel
// ---------------------------------------------------------------------------

pub async fn handle_cancel(
    State(state): State<ApiState>,
    Json(payload): Json<CancelRequest>,
) -> Result<Json<CancelResponse>, AppError> {
    let reason = payload.reason.as_deref();

    info!(video_id = payload.video_id, ?reason, "Cancel requested");

    // Signal the in-flight worker (if any)
    let was_active = state.cancel_registry.cancel(payload.video_id);

    match state.repo.cancel_job(payload.video_id, reason).await? {
        Some(job) => Ok(Json(CancelResponse {
            video_id: payload.video_id,
            status: job.status,
            message: format!("Job {} canceled", job.id),
        })),
        None => {
            if was_active {
                Ok(Json(CancelResponse {
                    video_id: payload.video_id,
                    status: "canceling".into(),
                    message: "Cancellation signal sent to active worker".into(),
                }))
            } else {
                Ok(Json(CancelResponse {
                    video_id: payload.video_id,
                    status: "not_found".into(),
                    message: "No active job found for this video".into(),
                }))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GET /transcode/status/:video_id
// ---------------------------------------------------------------------------

pub async fn handle_job_status(
    State(state): State<ApiState>,
    Path(video_id): Path<i64>,
) -> Result<Json<JobStatusResponse>, AppError> {
    let job = state.repo.get_job_by_video_id(video_id).await?;
    let events = match &job {
        Some(j) => state.repo.get_job_events(&j.id).await?,
        None => vec![],
    };
    Ok(Json(JobStatusResponse { job, events }))
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

pub async fn handle_health(
    State(state): State<ApiState>,
) -> Result<Json<HealthResponse>, AppError> {
    // Verify database connectivity
    let db_ok = state.repo.ping().await;

    let status = if db_ok { "ok" } else { "degraded" };

    Ok(Json(HealthResponse {
        status: status.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_seconds: state.start_time.elapsed().as_secs(),
        database: if db_ok { "connected" } else { "unreachable" }.into(),
    }))
}

// ---------------------------------------------------------------------------
// GET /metrics  (JSON)
// ---------------------------------------------------------------------------

pub async fn handle_metrics(
    State(state): State<ApiState>,
) -> Result<Json<MetricsResponse>, AppError> {
    let counts = state.repo.count_by_status().await?;
    let jobs_by_status = counts
        .into_iter()
        .map(|(status, count)| StatusCount { status, count })
        .collect();
    let system = state.sys_monitor.snapshot().await;
    let current_settings = state.settings.read().await;
    Ok(Json(MetricsResponse {
        jobs_by_status,
        worker_concurrency: current_settings.worker_concurrency,
        system,
    }))
}

// ---------------------------------------------------------------------------
// GET /metrics/prometheus  (Prometheus text exposition format)
// ---------------------------------------------------------------------------

pub async fn handle_metrics_prometheus(
    State(state): State<ApiState>,
) -> Result<axum::response::Response, AppError> {
    let counts = state.repo.count_by_status().await?;
    let sys = state.sys_monitor.snapshot().await;
    let current = state.settings.read().await;
    let uptime = state.start_time.elapsed().as_secs();

    let mut out = String::with_capacity(2048);

    // Jobs by status
    out.push_str("# HELP transcoder_jobs_total Number of jobs by status.\n");
    out.push_str("# TYPE transcoder_jobs_total gauge\n");
    for (status, count) in &counts {
        out.push_str(&format!(
            "transcoder_jobs_total{{status=\"{status}\"}} {count}\n"
        ));
    }

    // Workers
    out.push_str("# HELP transcoder_worker_concurrency Configured worker concurrency.\n");
    out.push_str("# TYPE transcoder_worker_concurrency gauge\n");
    out.push_str(&format!(
        "transcoder_worker_concurrency {}\n",
        current.worker_concurrency
    ));

    // Uptime
    out.push_str("# HELP transcoder_uptime_seconds Time since service started.\n");
    out.push_str("# TYPE transcoder_uptime_seconds counter\n");
    out.push_str(&format!("transcoder_uptime_seconds {uptime}\n"));

    // System CPU
    out.push_str("# HELP transcoder_system_cpu_usage_pct System CPU usage percentage.\n");
    out.push_str("# TYPE transcoder_system_cpu_usage_pct gauge\n");
    out.push_str(&format!(
        "transcoder_system_cpu_usage_pct {:.1}\n",
        sys.cpu_usage_pct
    ));

    // Process CPU
    out.push_str("# HELP transcoder_process_cpu_pct Process CPU usage percentage.\n");
    out.push_str("# TYPE transcoder_process_cpu_pct gauge\n");
    out.push_str(&format!(
        "transcoder_process_cpu_pct {:.1}\n",
        sys.proc_cpu_pct
    ));

    // Memory
    out.push_str("# HELP transcoder_system_memory_used_bytes System memory used.\n");
    out.push_str("# TYPE transcoder_system_memory_used_bytes gauge\n");
    out.push_str(&format!(
        "transcoder_system_memory_used_bytes {}\n",
        sys.mem_used_bytes
    ));
    out.push_str("# HELP transcoder_system_memory_total_bytes System memory total.\n");
    out.push_str("# TYPE transcoder_system_memory_total_bytes gauge\n");
    out.push_str(&format!(
        "transcoder_system_memory_total_bytes {}\n",
        sys.mem_total_bytes
    ));
    out.push_str("# HELP transcoder_process_memory_bytes Process RSS.\n");
    out.push_str("# TYPE transcoder_process_memory_bytes gauge\n");
    out.push_str(&format!(
        "transcoder_process_memory_bytes {}\n",
        sys.proc_mem_bytes
    ));

    // Disk
    out.push_str("# HELP transcoder_disk_used_bytes Disk usage.\n");
    out.push_str("# TYPE transcoder_disk_used_bytes gauge\n");
    out.push_str(&format!(
        "transcoder_disk_used_bytes {}\n",
        sys.disk_used_bytes
    ));
    out.push_str("# HELP transcoder_disk_total_bytes Disk total.\n");
    out.push_str("# TYPE transcoder_disk_total_bytes gauge\n");
    out.push_str(&format!(
        "transcoder_disk_total_bytes {}\n",
        sys.disk_total_bytes
    ));

    // Load average
    out.push_str("# HELP transcoder_load_average System load averages.\n");
    out.push_str("# TYPE transcoder_load_average gauge\n");
    out.push_str(&format!(
        "transcoder_load_average{{interval=\"1m\"}} {:.2}\n",
        sys.load_1
    ));
    out.push_str(&format!(
        "transcoder_load_average{{interval=\"5m\"}} {:.2}\n",
        sys.load_5
    ));
    out.push_str(&format!(
        "transcoder_load_average{{interval=\"15m\"}} {:.2}\n",
        sys.load_15
    ));

    Ok(axum::response::Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(axum::body::Body::from(out))
        .unwrap())
}

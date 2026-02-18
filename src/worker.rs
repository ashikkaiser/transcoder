use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};

use crate::callback::{self, CallbackPayload, TranscodeMetadata};
use crate::cancellation::CancellationRegistry;
use crate::config::Config;
use crate::db::models::JobStatus;
use crate::db::repository::JobRepository;
use crate::error::TranscoderError;
use crate::ffmpeg;
use crate::progress::ProgressTracker;
use crate::queue::JobQueue;
use crate::r2::R2Client;
use crate::retry;
use crate::settings::SettingsStore;
use crate::worker_tracker::WorkerTracker;

// ---------------------------------------------------------------------------
// WorkerPool – spawns N async worker tasks + background schedulers
// ---------------------------------------------------------------------------

pub struct WorkerPool {
    config: Arc<Config>,
    repo: Arc<JobRepository>,
    r2: Arc<R2Client>,
    http: reqwest::Client,
    queue: JobQueue,
    cancel_registry: Arc<CancellationRegistry>,
    shutdown: CancellationToken,
    settings: SettingsStore,
    tracker: Arc<WorkerTracker>,
    progress: ProgressTracker,
    /// Per-worker cancellation tokens (child of `shutdown`).
    worker_tokens: DashMap<String, CancellationToken>,
    /// Monotonic counter for unique worker IDs across scale events.
    next_worker_id: AtomicUsize,
}

impl WorkerPool {
    pub fn new(
        config: Arc<Config>,
        repo: Arc<JobRepository>,
        r2: Arc<R2Client>,
        http: reqwest::Client,
        queue: JobQueue,
        cancel_registry: Arc<CancellationRegistry>,
        shutdown: CancellationToken,
        settings: SettingsStore,
        tracker: Arc<WorkerTracker>,
        progress: ProgressTracker,
    ) -> Self {
        Self {
            config,
            repo,
            r2,
            http,
            queue,
            cancel_registry,
            shutdown,
            settings,
            tracker,
            progress,
            worker_tokens: DashMap::new(),
            next_worker_id: AtomicUsize::new(0),
        }
    }

    /// Current number of live workers.
    pub fn current_worker_count(&self) -> usize {
        self.worker_tokens.len()
    }

    /// Spawn worker tasks and background schedulers.
    pub fn spawn(pool: Arc<Self>) {
        let concurrency = pool.config.worker_concurrency;

        for _i in 0..concurrency {
            Self::spawn_one_worker(&pool);
        }

        // Retry scheduler
        let p = Arc::clone(&pool);
        tokio::spawn(
            async move { p.retry_scheduler().await }
                .instrument(tracing::info_span!("retry_scheduler")),
        );

        // Stale-job detector
        let p = Arc::clone(&pool);
        tokio::spawn(
            async move { p.stale_job_detector().await }
                .instrument(tracing::info_span!("stale_detector")),
        );

        info!(concurrency, "Worker pool started");
    }

    /// Spawn a single worker with a unique ID and its own cancellation token.
    fn spawn_one_worker(pool: &Arc<Self>) {
        let idx = pool.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let wid = format!("worker-{idx}");
        // Create a child token – cancelled if either the global shutdown fires
        // or if this specific worker is individually stopped via scale_to().
        let token = pool.shutdown.child_token();
        pool.worker_tokens.insert(wid.clone(), token.clone());
        pool.tracker.register(&wid);

        let p = Arc::clone(pool);
        let wid2 = wid.clone();
        tokio::spawn(
            async move {
                p.worker_loop(&wid2, &token).await;
                // Clean up after exit
                p.worker_tokens.remove(&wid2);
                p.tracker.unregister(&wid2);
                info!(worker_id = %wid2, "Worker exited");
            }
            .instrument(tracing::info_span!("worker", id = %wid)),
        );
    }

    /// Dynamically scale the worker pool to the desired concurrency.
    /// - If `desired > current`: new workers are spawned immediately.
    /// - If `desired < current`: excess **idle** workers are cancelled first,
    ///   then active ones (they finish their current job before exiting).
    pub fn scale_to(pool: &Arc<Self>, desired: usize) {
        let current = pool.worker_tokens.len();
        if desired == current {
            info!(current, "Worker pool already at desired concurrency");
            return;
        }

        if desired > current {
            let to_add = desired - current;
            info!(current, desired, to_add, "Scaling UP worker pool");
            for _ in 0..to_add {
                Self::spawn_one_worker(pool);
            }
        } else {
            let to_remove = current - desired;
            info!(current, desired, to_remove, "Scaling DOWN worker pool");

            // Collect worker IDs: prefer removing idle workers first.
            let mut idle_ids: Vec<String> = Vec::new();
            let mut active_ids: Vec<String> = Vec::new();
            for entry in pool.worker_tokens.iter() {
                let wid = entry.key().clone();
                if pool.tracker.is_idle(&wid) {
                    idle_ids.push(wid);
                } else {
                    active_ids.push(wid);
                }
            }

            // Sort for deterministic ordering (highest id first to remove newest)
            idle_ids.sort();
            idle_ids.reverse();
            active_ids.sort();
            active_ids.reverse();

            let mut removed = 0;
            // Cancel idle workers first
            for wid in idle_ids {
                if removed >= to_remove {
                    break;
                }
                if let Some((_, token)) = pool.worker_tokens.remove(&wid) {
                    token.cancel();
                    removed += 1;
                    info!(worker_id = %wid, "Cancelled idle worker");
                }
            }
            // If still need more, cancel active workers (they will finish current job)
            for wid in active_ids {
                if removed >= to_remove {
                    break;
                }
                if let Some((_, token)) = pool.worker_tokens.remove(&wid) {
                    token.cancel();
                    removed += 1;
                    info!(worker_id = %wid, "Cancelled active worker (will finish current job)");
                }
            }
        }
    }

    // ── Main worker loop ─────────────────────────────────────────────────

    async fn worker_loop(&self, worker_id: &str, my_token: &CancellationToken) {
        info!(worker_id, "Worker ready");

        loop {
            if my_token.is_cancelled() {
                info!(worker_id, "Shutting down");
                self.tracker.set_idle(worker_id);
                break;
            }

            match self.repo.claim_next_job(worker_id).await {
                Ok(Some(job)) => {
                    let job_id = job.id.clone();
                    let video_id = job.video_id;

                    info!(job_id = %job_id, video_id, attempt = job.attempt, "Claimed job");

                    // Mark worker as active
                    self.tracker
                        .set_active(worker_id, &job_id, video_id);

                    let cancel_token = self.cancel_registry.register(video_id);

                    match self.process_job(&job, &cancel_token).await {
                        Ok(()) => {
                            info!(job_id = %job_id, video_id, "Job completed");
                            self.tracker.record_completion(worker_id);
                        }
                        Err(TranscoderError::Canceled) => {
                            info!(job_id = %job_id, video_id, "Job canceled mid-flight");
                            self.progress.remove(video_id);
                            self.repo
                                .cancel_job(video_id, Some("canceled during processing"))
                                .await
                                .ok();
                            self.tracker.record_completion(worker_id);
                        }
                        Err(e) => {
                            let retryable = e.is_retryable();
                            let msg = e.to_string();
                            error!(job_id = %job_id, video_id, error = %msg, retryable, "Job failed");

                            let max_attempts = self.settings.max_job_attempts().await;
                            if retryable && job.attempt < max_attempts {
                                let nra = retry::next_retry_at(job.attempt);
                                self.repo
                                    .fail_job(&job_id, &msg, Some(nra.to_rfc3339()))
                                    .await
                                    .ok();
                            } else {
                                self.repo.fail_job(&job_id, &msg, None).await.ok();
                                self.send_failure_callback(&job, &msg).await;
                            }
                            self.progress.remove(video_id);
                            self.tracker.record_completion(worker_id);
                        }
                    }

                    self.cancel_registry.remove(video_id);
                    self.cleanup_workspace(&job).await;

                    // Back to idle
                    self.tracker.set_idle(worker_id);
                }
                Ok(None) => {
                    // Nothing to do – wait for a signal, timeout, or cancellation.
                    tokio::select! {
                        _ = self.queue.wait_for_job(std::time::Duration::from_secs(5)) => {}
                        _ = my_token.cancelled() => {}
                    }
                }
                Err(e) => {
                    error!(worker_id, error = %e, "claim_next_job failed");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = my_token.cancelled() => {}
                    }
                }
            }
        }
    }

    // ── Job processing pipeline ──────────────────────────────────────────

    async fn process_job(
        &self,
        job: &crate::db::models::Job,
        cancel_token: &CancellationToken,
    ) -> Result<(), TranscoderError> {
        let ws = self.workspace(job);
        tokio::fs::create_dir_all(&ws)
            .await
            .map_err(|e| TranscoderError::Internal(format!("mkdir workspace: {e}")))?;

        // ── Download ────────────────────────────────────────────────────
        self.progress.set_phase(job.video_id, &job.id, "downloading");
        let input_path = ws.join("input.mp4");
        self.download_video(job, &input_path, cancel_token).await?;
        check_cancel(cancel_token)?;

        // ── Transcode ───────────────────────────────────────────────────
        self.repo
            .update_status(&job.id, JobStatus::Transcoding)
            .await
            .map_err(|e| TranscoderError::Internal(e.to_string()))?;

        let output_dir = ws.join("output");
        tokio::fs::create_dir_all(&output_dir)
            .await
            .map_err(|e| TranscoderError::Internal(format!("mkdir output: {e}")))?;

        let (renditions, video_info) = ffmpeg::transcode_to_hls(
            &input_path,
            &output_dir,
            cancel_token,
            &self.settings,
            Some(&self.progress),
            job.video_id,
            &job.id,
        )
        .await?;
        check_cancel(cancel_token)?;

        // ── Upload ──────────────────────────────────────────────────────
        self.progress.set_phase(job.video_id, &job.id, "uploading");
        self.repo
            .update_status(&job.id, JobStatus::Uploading)
            .await
            .map_err(|e| TranscoderError::Internal(e.to_string()))?;

        let base_prefix = self.settings.output_path_prefix().await;
        let prefix = compute_output_prefix(&base_prefix, &job.s3_key);
        self.r2.upload_directory(&output_dir, &prefix).await?;
        check_cancel(cancel_token)?;

        // ── Callback ────────────────────────────────────────────────────
        self.progress.set_phase(job.video_id, &job.id, "callback");
        self.repo
            .update_status(&job.id, JobStatus::CallbackPending)
            .await
            .map_err(|e| TranscoderError::Internal(e.to_string()))?;

        let m3u8_key = format!("{prefix}/master.m3u8");
        let m3u8_url = self.r2.public_url(&m3u8_key);

        let payload = CallbackPayload {
            video_id: job.video_id,
            status: "completed".into(),
            final_m3u8_url: Some(m3u8_url.clone()),
            error_message: None,
            started_at: job.started_at.clone(),
            finished_at: Some(Utc::now().to_rfc3339()),
            attempt: job.attempt,
            metadata: Some(TranscodeMetadata {
                duration_seconds: Some(video_info.duration_seconds),
                renditions: renditions.iter().map(|r| r.name.clone()).collect(),
            }),
        };

        let report = callback::send_callback(
            &self.http, &self.settings, &job.callback_url, &payload, 3,
        )
        .await?;

        // Log callback attempts as job events for webhook visibility
        self.log_callback_events(&job.id, &report).await;

        // ── Complete ────────────────────────────────────────────────────
        self.progress.remove(job.video_id);

        let meta_json = serde_json::to_string(&payload.metadata).ok();
        self.repo
            .complete_job(&job.id, &m3u8_url, &prefix, meta_json.as_deref())
            .await
            .map_err(|e| TranscoderError::Internal(e.to_string()))?;

        // ── Delete source from R2 if enabled ───────────────────────────
        if self.settings.delete_source_after_transcode().await {
            info!(video_id = job.video_id, s3_key = %job.s3_key, "Deleting source file from R2");
            if let Err(e) = self.r2.delete_object(&job.s3_key).await {
                warn!(video_id = job.video_id, error = %e, "Failed to delete source file (non-fatal)");
            }
        }

        Ok(())
    }

    // ── Download with streaming ──────────────────────────────────────────

    async fn download_video(
        &self,
        job: &crate::db::models::Job,
        output: &PathBuf,
        cancel_token: &CancellationToken,
    ) -> Result<(), TranscoderError> {
        info!(video_id = job.video_id, url = %job.video_url, "Downloading video");

        let resp = self
            .http
            .get(&job.video_url)
            .timeout(std::time::Duration::from_secs(600))
            .send()
            .await
            .map_err(|e| TranscoderError::Download(e.to_string()))?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(TranscoderError::VideoNotFound(job.video_url.clone()));
        }
        if !status.is_success() {
            return Err(TranscoderError::Download(format!("HTTP {status}")));
        }

        let mut file = tokio::fs::File::create(output)
            .await
            .map_err(|e| TranscoderError::Download(format!("create file: {e}")))?;

        let mut stream = resp.bytes_stream();
        let mut total: u64 = 0;

        while let Some(chunk) = stream.next().await {
            if cancel_token.is_cancelled() {
                return Err(TranscoderError::Canceled);
            }
            let bytes = chunk.map_err(|e| TranscoderError::Download(format!("stream: {e}")))?;
            file.write_all(&bytes)
                .await
                .map_err(|e| TranscoderError::Download(format!("write: {e}")))?;
            total += bytes.len() as u64;
        }

        file.flush()
            .await
            .map_err(|e| TranscoderError::Download(format!("flush: {e}")))?;

        info!(video_id = job.video_id, bytes = total, "Download complete");
        Ok(())
    }

    // ── Failure callback ─────────────────────────────────────────────────

    async fn send_failure_callback(&self, job: &crate::db::models::Job, error_msg: &str) {
        let payload = CallbackPayload {
            video_id: job.video_id,
            status: "failed".into(),
            final_m3u8_url: None,
            error_message: Some(error_msg.to_string()),
            started_at: job.started_at.clone(),
            finished_at: Some(Utc::now().to_rfc3339()),
            attempt: job.attempt,
            metadata: None,
        };

        match callback::send_callback(&self.http, &self.settings, &job.callback_url, &payload, 2).await {
            Ok(report) => {
                self.log_callback_events(&job.id, &report).await;
            }
            Err(e) => {
                error!(video_id = job.video_id, error = %e, "Failure callback also failed");
            }
        }
    }

    // ── Log callback attempts as job events ──────────────────────────────

    async fn log_callback_events(&self, job_id: &str, report: &callback::CallbackReport) {
        if report.skipped {
            self.repo
                .log_event_public(job_id, "callback_skipped", None, None, Some("No callback URL configured"))
                .await
                .ok();
            return;
        }
        for attempt in &report.attempts {
            let detail = if attempt.success {
                format!(
                    "Callback #{} to {} → HTTP {} ✓",
                    attempt.number,
                    attempt.url,
                    attempt.status_code.unwrap_or(0)
                )
            } else {
                format!(
                    "Callback #{} to {} → {} ✗",
                    attempt.number,
                    attempt.url,
                    attempt.error.as_deref().unwrap_or("unknown")
                )
            };
            let event_type = if attempt.success {
                "callback_success"
            } else {
                "callback_attempt_failed"
            };
            self.repo
                .log_event_public(job_id, event_type, None, None, Some(&detail))
                .await
                .ok();
        }
    }

    // ── Background: retry scheduler ──────────────────────────────────────

    async fn retry_scheduler(&self) {
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }

            match self.repo.get_retryable_jobs().await {
                Ok(jobs) => {
                    for job in jobs {
                        info!(job_id = %job.id, video_id = job.video_id, attempt = job.attempt,
                              "Re-queuing job for retry");
                        if let Err(e) = self.repo.requeue_for_retry(&job.id).await {
                            error!(job_id = %job.id, error = %e, "requeue failed");
                        } else {
                            self.queue.notify_new_job();
                        }
                    }
                }
                Err(e) => error!(error = %e, "get_retryable_jobs failed"),
            }

            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
                _ = self.shutdown.cancelled() => break,
            }
        }
    }

    // ── Background: stale job detector ───────────────────────────────────

    async fn stale_job_detector(&self) {
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }

            let threshold =
                (chrono::Utc::now() - chrono::Duration::seconds(1800)).to_rfc3339();

            match self.repo.find_stale_jobs(&threshold).await {
                Ok(jobs) => {
                    for job in jobs {
                        warn!(job_id = %job.id, video_id = job.video_id, "Resetting stale job");
                        if let Err(e) = self.repo.reset_stale_job(&job.id).await {
                            error!(job_id = %job.id, error = %e, "reset stale failed");
                        } else {
                            self.queue.notify_new_job();
                        }
                    }
                }
                Err(e) => error!(error = %e, "find_stale_jobs failed"),
            }

            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                _ = self.shutdown.cancelled() => break,
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    fn workspace(&self, job: &crate::db::models::Job) -> PathBuf {
        PathBuf::from(&self.config.temp_dir).join(&job.id)
    }

    async fn cleanup_workspace(&self, job: &crate::db::models::Job) {
        let ws = self.workspace(job);
        if ws.exists() {
            if let Err(e) = tokio::fs::remove_dir_all(&ws).await {
                warn!(job_id = %job.id, error = %e, "Workspace cleanup failed");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn check_cancel(token: &CancellationToken) -> Result<(), TranscoderError> {
    if token.is_cancelled() {
        Err(TranscoderError::Canceled)
    } else {
        Ok(())
    }
}

/// Turn `products/82650/videos/xyz.mp4` → `<base>/products/82650/videos/xyz`
///
/// The `base` prefix is configurable via settings (e.g. `"hls"`, `"transcoded/output"`).
/// The rest of the path follows the s3_key segments with the file extension stripped.
fn compute_output_prefix(base: &str, s3_key: &str) -> String {
    let without_ext = std::path::Path::new(s3_key)
        .with_extension("")
        .to_string_lossy()
        .to_string();
    let base = base.trim_matches('/');
    if base.is_empty() {
        without_ext
    } else {
        format!("{base}/{without_ext}")
    }
}

You are a senior Rust backend engineer. Design and implement a production-grade “Transcoder Service” in Rust.

Goal:
A transcoder receives HTTP requests containing a list of videos to transcode (hosted on Cloudflare R2). The transcoder must maintain its own local database, manage a reliable job queue, download input videos, transcode them into HLS (m3u8 + segments), upload results back to R2, and send a callback to the provided callback_url with status + final m3u8 URL. It must be fail-safe with retries, cancellation, and idempotency.

Incoming request example:
POST /transcode
Content-Type: application/json
{
  "videos": [
    {
      "id": 5,
      "s3_key": "products/82650/videos/1770718759_698b0627197f1.mp4",
      "video_url": "https://pub-a8e6f7cd6f704d0ca864c418eab62d94.r2.dev/products/82650/videos/1770718759_698b0627197f1.mp4",
      "callback_url": "api/product/video-callback"
    },
    {
      "id": 6,
      "s3_key": "products/82650/videos/1770718762_698b062a6c915.mp4",
      "video_url": "https://pub-a8e6f7cd6f704d0ca864c418eab62d94.r2.dev/products/82650/videos/1770718762_698b062a6c915.mp4",
      "callback_url": "api/product/video-callback"
    }
  ]
}

Cancellation request:
POST /transcode/cancel
Content-Type: application/json
{
  "video_id": 6,
  "reason": "deleted"
}

Expected behavior:
1) On /transcode:
- Validate payload.
- Insert/update jobs in local DB (idempotent: same video id should not create duplicate active jobs).
- Enqueue jobs for processing.
- Respond immediately with accepted IDs and current status.

2) Job processing workflow (for each video):
- Mark job as queued → downloading → transcoding → uploading → callback_pending → completed (or failed/canceled).
- Download source video from video_url (R2 public URL). Support large files with streaming download, not loading into memory.
- Transcode to HLS using ffmpeg:
  - Output: master.m3u8 + variant playlists + segments (.ts or .m4s).
  - Create at least 3 renditions (e.g., 360p, 720p, 1080p) with reasonable bitrates.
  - Store output temporarily on disk in a per-job workspace directory.
- Upload all HLS outputs to R2 under a deterministic prefix:
  e.g. hls/{s3_key_without_ext}/ or products/.../hls/{id}/
- Determine the final public URL to the master playlist (m3u8).
- Send callback (HTTP POST) to callback_url:
  - Must include: video_id, status, final_m3u8_url, error_message (nullable), started_at, finished_at, attempt, and optional metadata (duration, renditions).
- Mark job complete only after callback succeeds OR after a retry policy is exhausted.

3) Failure safety / retry mechanics:
- Use exponential backoff with jitter for retries (both job execution retries and callback retries).
- Persist attempt count, last_error, next_retry_at in DB.
- Have a background scheduler that picks jobs ready to retry.
- Differentiate retryable vs non-retryable errors:
  - Retryable: network failures, temporary R2 errors, ffmpeg transient failure, callback 5xx/timeouts.
  - Non-retryable: input 404 (video missing), invalid format, callback 4xx (except 429), explicit cancel.
- Ensure system can recover after crash/restart: DB is source of truth; unfinished jobs resume or retry.

4) Cancellation / deletion:
- If /transcode/cancel arrives:
  - If job is queued: remove from queue and set status=canceled.
  - If job is downloading/transcoding/uploading: signal cancellation, stop ffmpeg process, clean up temp files, set status=canceled.
  - If job already completed: ignore or record cancel_requested but no action.
- Cancellation must be best-effort and safe.

5) Database and queue:
- Use SQLite (preferred) for local state, but code should be structured so it could swap to Postgres later.
- Tables:
  - jobs (video_id, s3_key, video_url, callback_url, status, attempt, priority, created_at, updated_at, started_at, finished_at, last_error, next_retry_at, canceled_at, cancel_reason, output_prefix, final_m3u8_url, worker_id/lock)
  - job_events or logs table (optional) for auditing
- Use a work queue:
  - Either in-memory + DB scheduler (recommended) or a lightweight persistent queue pattern.
  - Implement job locking so multiple workers won’t pick same job (row lock via “claimed_at/worker_id” + TTL).
- Concurrency:
  - Configurable parallel transcoding workers (e.g., 2–4).
  - Limit CPU usage and number of simultaneous ffmpeg processes.
  - Use Tokio for async orchestration.

6) Security / config:
- Read configuration from env:
  - R2 credentials (S3-compatible endpoint, access key, secret)
  - R2 bucket
  - Public base URL for final HLS paths
  - Callback base URL (if callback_url can be relative)
  - Worker concurrency
  - Temp workspace path
- Support signed R2 access if video_url is not public (optional; design should allow it).

7) Observability:
- Structured logging (tracing).
- Basic metrics endpoints (/health, /metrics optional Prometheus).
- Include job ids in logs.

8) Deliverables:
- Provide a full Rust project structure:
  - Cargo.toml
  - src/main.rs
  - modules: api, db, queue, worker, ffmpeg, r2, callback, retry, cancellation
- Use common crates:
  - axum (API), tokio, sqlx (sqlite), reqwest, tracing, uuid, serde
- Include example SQL migrations.
- Include example ffmpeg command builders.
- Include sample callback payload JSON schema.
- Include tests for:
  - idempotent job insert
  - retry backoff calculation
  - cancel logic state transitions

Important:
- Write code that compiles.
- Keep the design clean and production-ready.
- Show how to run locally and with Docker (optional).
- Ensure no busy-looping; use timers and DB next_retry_at scheduling.

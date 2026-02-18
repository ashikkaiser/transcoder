# Video Transcoder Service

A production-grade HTTP transcoding service built in Rust. Receives transcode
requests, downloads source videos from Cloudflare R2, transcodes them into
multi-bitrate HLS (master.m3u8 + variant playlists + `.ts` segments), uploads
results back to R2, and sends a callback to a configurable endpoint.

## Architecture

```
                 ┌──────────────┐
  POST /transcode│   axum API   │  POST /transcode/cancel
  ──────────────►│              │◄──────────────────────
                 └───────┬──────┘
                         │ insert/cancel in SQLite
                         ▼
                 ┌──────────────┐
                 │   SQLite DB  │  (jobs + job_events)
                 └───────┬──────┘
                         │ Notify
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │ Worker 0 │   │ Worker 1 │   │ Worker N │  (configurable)
    └────┬─────┘   └────┬─────┘   └────┬─────┘
         │              │              │
         ▼              ▼              ▼
   Download ─► ffmpeg HLS ─► R2 Upload ─► HTTP Callback
```

### Module Map

| Module          | Responsibility                                       |
|-----------------|------------------------------------------------------|
| `api/`          | axum routes & request/response models                |
| `db/`           | SQLite models + repository (job CRUD, claim, cancel) |
| `worker`        | Tokio worker pool – download → transcode → upload → callback |
| `ffmpeg`        | ffprobe + per-rendition ffmpeg HLS transcoding       |
| `r2`            | S3-compatible upload via `aws-sdk-s3`                |
| `callback`      | HTTP POST with exponential-backoff retries           |
| `queue`         | `Notify`-based in-memory signal (DB is the real queue) |
| `retry`         | Equal-jitter exponential backoff calculator           |
| `cancellation`  | `DashMap<video_id, CancellationToken>` registry      |
| `config`        | Env-based configuration                              |
| `error`         | `TranscoderError` (retryable classification) + `AppError` for axum |

## Quick Start

### Prerequisites

- **Rust** ≥ 1.88 (for latest AWS SDK)
- **ffmpeg** & **ffprobe** on `$PATH`
- A **Cloudflare R2** bucket with API credentials

### Run locally

```bash
cp .env.example .env
# Edit .env with your R2 credentials

cargo run
# Server starts on http://0.0.0.0:3000
```

### Run with Docker

```bash
docker build -t transcoder .
docker run --env-file .env -p 3000:3000 transcoder
```

### Run tests

```bash
cargo test
```

## API Reference

### `POST /transcode`

Submit one or more videos for transcoding.

```json
{
  "videos": [
    {
      "id": 5,
      "s3_key": "products/82650/videos/1770718759_698b0627197f1.mp4",
      "video_url": "https://pub-a8e6f7cd6f704d0ca864c418eab62d94.r2.dev/products/82650/videos/1770718759_698b0627197f1.mp4",
      "callback_url": "api/product/video-callback"
    }
  ]
}
```

**Response** `200 OK`:

```json
{
  "accepted": [
    {
      "video_id": 5,
      "job_id": "a1b2c3d4-...",
      "status": "queued",
      "created": true
    }
  ]
}
```

Idempotent: submitting the same `video_id` while a job is still active
returns the existing job with `"created": false`.

### `POST /transcode/cancel`

```json
{
  "video_id": 6,
  "reason": "deleted"
}
```

**Response** `200 OK`:

```json
{
  "video_id": 6,
  "status": "canceled",
  "message": "Job a1b2c3d4-... canceled"
}
```

If the job is mid-processing, the worker receives a cancellation signal,
kills ffmpeg, cleans up temp files, and marks the job as canceled.

### `GET /transcode/status/:video_id`

Returns the job and its full event log.

### `GET /health`

```json
{ "status": "ok", "version": "0.1.0", "uptime_seconds": 42 }
```

### `GET /metrics`

```json
{
  "jobs_by_status": [
    { "status": "completed", "count": 10 },
    { "status": "queued", "count": 2 }
  ],
  "worker_concurrency": 2
}
```

## Callback Payload

When a job completes (or fails permanently), the service POSTs to the
`callback_url`:

```json
{
  "video_id": 5,
  "status": "completed",
  "final_m3u8_url": "https://pub-xxx.r2.dev/hls/products/82650/videos/1770718759_698b0627197f1/master.m3u8",
  "error_message": null,
  "started_at": "2025-01-15T10:30:00+00:00",
  "finished_at": "2025-01-15T10:35:12+00:00",
  "attempt": 1,
  "metadata": {
    "duration_seconds": 120.5,
    "renditions": ["360p", "720p", "1080p"]
  }
}
```

On failure the `status` is `"failed"` and `error_message` is populated.

## Job State Machine

```
           ┌──────────────────────────────────────────┐
           │              (any state)                  │
           │                  │ cancel                 │
           │                  ▼                        │
  queued ──► downloading ──► transcoding ──► uploading │
    ▲                                           │      │
    │                                           ▼      │
    │ retry          callback_pending ──► completed     │
    │                      │                           │
    └──── failed ◄─────────┘                  canceled ◄┘
```

**Retryable errors**: network failures, R2 transient errors, ffmpeg
transient failures, callback 5xx/timeout.

**Non-retryable errors**: video 404, invalid format, callback 4xx
(except 429), explicit cancellation.

## Configuration

| Variable            | Default            | Description                              |
|---------------------|--------------------|------------------------------------------|
| `HOST`              | `0.0.0.0`          | Bind address                             |
| `PORT`              | `3000`             | Bind port                                |
| `R2_ACCOUNT_ID`     | —                  | Cloudflare account ID                    |
| `R2_ACCESS_KEY_ID`  | —                  | R2 API access key                        |
| `R2_SECRET_ACCESS_KEY` | —               | R2 API secret key                        |
| `R2_BUCKET_NAME`    | —                  | Target R2 bucket                         |
| `R2_PUBLIC_URL`     | —                  | Public base URL for the bucket           |
| `CALLBACK_BASE_URL` | `""`               | Prepended to relative `callback_url`s    |
| `WORKER_CONCURRENCY`| `2`                | Parallel transcoding workers             |
| `MAX_JOB_ATTEMPTS`  | `5`                | Max attempts before permanent failure    |
| `TEMP_DIR`          | `/tmp/transcoder`  | Workspace for downloads & ffmpeg output  |
| `DATABASE_URL`      | `sqlite:transcoder.db` | SQLite database path                 |
| `RUST_LOG`          | `video_transcoder=info` | tracing env filter                  |

## Database Schema

See [`migrations/001_init.sql`](migrations/001_init.sql).

Two tables:

- **`jobs`** – full job state including status, attempt count, retry
  scheduling, worker lock, and output URLs.
- **`job_events`** – append-only audit log of every state transition.

The schema uses `CREATE TABLE IF NOT EXISTS` so it's safe to re-run on
startup.

## HLS Output

For each video the service probes the source resolution and creates up to
three renditions (360p, 720p, 1080p – never upscaling beyond the source).
Output is uploaded under:

```
hls/{s3_key_without_extension}/
  master.m3u8
  360p/playlist.m3u8
  360p/segment_000.ts
  ...
  720p/playlist.m3u8
  ...
```

## Crash Recovery

The SQLite database is the source of truth:

1. **Unfinished jobs** (status ∈ downloading/transcoding/uploading) whose
   `claimed_at` is older than 30 minutes are detected by the stale-job
   detector and re-queued.
2. **Failed jobs** with `next_retry_at ≤ now` are picked up by the retry
   scheduler and moved back to `queued`.
3. On restart, both schedulers resume from the DB – no work is lost.

## License

Private / proprietary.

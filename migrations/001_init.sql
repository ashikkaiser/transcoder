CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT    PRIMARY KEY NOT NULL,
    video_id        INTEGER NOT NULL,
    s3_key          TEXT    NOT NULL,
    video_url       TEXT    NOT NULL,
    callback_url    TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'queued',
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    last_error      TEXT,
    next_retry_at   TEXT,
    canceled_at     TEXT,
    cancel_reason   TEXT,
    output_prefix   TEXT,
    final_m3u8_url  TEXT,
    worker_id       TEXT,
    claimed_at      TEXT,
    metadata        TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_video_id       ON jobs(video_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status         ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_next_retry_at  ON jobs(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_at     ON jobs(claimed_at);

CREATE TABLE IF NOT EXISTS job_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    old_status  TEXT,
    new_status  TEXT,
    details     TEXT,
    created_at  TEXT    NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);

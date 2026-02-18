use chrono::Utc;
use sqlx::SqlitePool;
use uuid::Uuid;

use super::models::{Job, JobEvent, JobStatus};

// ---------------------------------------------------------------------------
// JobRepository – all database operations for the transcoder service
// ---------------------------------------------------------------------------

pub struct JobRepository {
    pool: SqlitePool,
}

impl JobRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // ── Health check ──────────────────────────────────────────────────────

    /// Quick connectivity check — returns `true` when the database is reachable.
    pub async fn ping(&self) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }

    // ── Schema bootstrap ─────────────────────────────────────────────────

    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let migrations = [
            include_str!("../../migrations/001_init.sql"),
            include_str!("../../migrations/002_settings.sql"),
        ];
        for sql in migrations {
            for statement in sql.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() {
                    sqlx::query(trimmed).execute(&self.pool).await?;
                }
            }
        }
        Ok(())
    }

    // ── Settings persistence ─────────────────────────────────────────────

    pub async fn load_all_settings(&self) -> Result<std::collections::HashMap<String, String>, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct Row { key: String, value: String }
        let rows: Vec<Row> = sqlx::query_as("SELECT key, value FROM settings")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| (r.key, r.value)).collect())
    }

    #[allow(dead_code)]
    pub async fn save_setting(&self, key: &str, value: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn save_all_settings(&self, entries: &[(&str, String)]) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for (key, value) in entries {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            )
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    // ── Idempotent insert ────────────────────────────────────────────────

    /// Returns `(job, created)`.  If an active (non-terminal, non-exhausted)
    /// job already exists for the given `video_id` we return it untouched and
    /// `created = false`.
    pub async fn insert_or_get_active(
        &self,
        video_id: i64,
        s3_key: &str,
        video_url: &str,
        callback_url: &str,
        max_attempts: i32,
    ) -> Result<(Job, bool), sqlx::Error> {
        // Check for an existing active job
        let existing = sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs \
             WHERE video_id = ?1 \
               AND status NOT IN ('completed', 'canceled') \
               AND NOT (status = 'failed' AND attempt >= max_attempts) \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(video_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(job) = existing {
            return Ok((job, false));
        }

        let now = Utc::now().to_rfc3339();
        let id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO jobs \
                (id, video_id, s3_key, video_url, callback_url, status, \
                 attempt, max_attempts, priority, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'queued', 0, ?6, 0, ?7, ?7)",
        )
        .bind(&id)
        .bind(video_id)
        .bind(s3_key)
        .bind(video_url)
        .bind(callback_url)
        .bind(max_attempts)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = ?1")
            .bind(&id)
            .fetch_one(&self.pool)
            .await?;

        self.log_event(&id, "created", None, Some("queued"), None)
            .await
            .ok();
        Ok((job, true))
    }

    // ── Claim / lock ─────────────────────────────────────────────────────

    /// Atomically claim the next queued job for the given worker.
    pub async fn claim_next_job(&self, worker_id: &str) -> Result<Option<Job>, sqlx::Error> {
        let now = Utc::now().to_rfc3339();

        let mut tx = self.pool.begin().await?;

        let maybe_job = sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs \
             WHERE status = 'queued' \
               AND (next_retry_at IS NULL OR next_retry_at <= ?1) \
             ORDER BY priority DESC, created_at ASC \
             LIMIT 1",
        )
        .bind(&now)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(mut job) = maybe_job else {
            tx.commit().await?;
            return Ok(None);
        };

        sqlx::query(
            "UPDATE jobs \
             SET status      = 'downloading', \
                 worker_id   = ?1, \
                 claimed_at  = ?2, \
                 updated_at  = ?2, \
                 attempt     = attempt + 1, \
                 started_at  = CASE WHEN started_at IS NULL THEN ?2 ELSE started_at END \
             WHERE id = ?3",
        )
        .bind(worker_id)
        .bind(&now)
        .bind(&job.id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // Reflect the changes in the returned struct
        let old_status = std::mem::replace(&mut job.status, "downloading".to_string());
        job.worker_id = Some(worker_id.to_string());
        job.claimed_at = Some(now.clone());
        job.attempt += 1;
        if job.started_at.is_none() {
            job.started_at = Some(now.clone());
        }
        job.updated_at = now;

        self.log_event(
            &job.id,
            "claimed",
            Some(&old_status),
            Some("downloading"),
            Some(&format!("worker: {worker_id}")),
        )
        .await
        .ok();

        Ok(Some(job))
    }

    // ── Status transitions ───────────────────────────────────────────────

    pub async fn update_status(
        &self,
        job_id: &str,
        status: JobStatus,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let status_str = status.as_str();

        let current = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = ?1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;

        sqlx::query("UPDATE jobs SET status = ?1, updated_at = ?2 WHERE id = ?3")
            .bind(status_str)
            .bind(&now)
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        if let Some(cur) = current {
            self.log_event(job_id, "status_change", Some(&cur.status), Some(status_str), None)
                .await
                .ok();
        }
        Ok(())
    }

    pub async fn complete_job(
        &self,
        job_id: &str,
        m3u8_url: &str,
        output_prefix: &str,
        metadata: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE jobs \
             SET status        = 'completed', \
                 finished_at   = ?1, \
                 updated_at    = ?1, \
                 final_m3u8_url = ?2, \
                 output_prefix = ?3, \
                 metadata      = ?4 \
             WHERE id = ?5",
        )
        .bind(&now)
        .bind(m3u8_url)
        .bind(output_prefix)
        .bind(metadata)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        self.log_event(job_id, "completed", Some("callback_pending"), Some("completed"), None)
            .await
            .ok();
        Ok(())
    }

    pub async fn fail_job(
        &self,
        job_id: &str,
        error: &str,
        next_retry_at: Option<String>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE jobs \
             SET status       = 'failed', \
                 last_error   = ?1, \
                 next_retry_at = ?2, \
                 updated_at   = ?3, \
                 worker_id    = NULL, \
                 claimed_at   = NULL \
             WHERE id = ?4",
        )
        .bind(error)
        .bind(&next_retry_at)
        .bind(&now)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        self.log_event(job_id, "failed", None, Some("failed"), Some(error))
            .await
            .ok();
        Ok(())
    }

    // ── Cancellation ─────────────────────────────────────────────────────

    pub async fn cancel_job(
        &self,
        video_id: i64,
        reason: Option<&str>,
    ) -> Result<Option<Job>, sqlx::Error> {
        let now = Utc::now().to_rfc3339();

        let job = sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs \
             WHERE video_id = ?1 \
               AND status NOT IN ('completed', 'canceled') \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(video_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(job) = job else {
            return Ok(None);
        };

        let old_status = job.status.clone();

        sqlx::query(
            "UPDATE jobs \
             SET status       = 'canceled', \
                 canceled_at  = ?1, \
                 cancel_reason = ?2, \
                 updated_at   = ?1, \
                 worker_id    = NULL, \
                 claimed_at   = NULL \
             WHERE id = ?3",
        )
        .bind(&now)
        .bind(reason)
        .bind(&job.id)
        .execute(&self.pool)
        .await?;

        self.log_event(&job.id, "canceled", Some(&old_status), Some("canceled"), reason)
            .await
            .ok();

        let updated = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = ?1")
            .bind(&job.id)
            .fetch_one(&self.pool)
            .await?;

        Ok(Some(updated))
    }

    // ── Retry helpers ────────────────────────────────────────────────────

    pub async fn get_retryable_jobs(&self) -> Result<Vec<Job>, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs \
             WHERE status = 'failed' \
               AND attempt < max_attempts \
               AND next_retry_at IS NOT NULL \
               AND next_retry_at <= ?1 \
             ORDER BY next_retry_at ASC",
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn requeue_for_retry(&self, job_id: &str) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE jobs \
             SET status = 'queued', next_retry_at = NULL, updated_at = ?1 \
             WHERE id = ?2",
        )
        .bind(&now)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        self.log_event(job_id, "requeued_for_retry", Some("failed"), Some("queued"), None)
            .await
            .ok();
        Ok(())
    }

    // ── Stale job detection ──────────────────────────────────────────────

    pub async fn find_stale_jobs(&self, stale_threshold: &str) -> Result<Vec<Job>, sqlx::Error> {
        sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs \
             WHERE status IN ('downloading', 'transcoding', 'uploading', 'callback_pending') \
               AND claimed_at IS NOT NULL \
               AND claimed_at < ?1",
        )
        .bind(stale_threshold)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn reset_stale_job(&self, job_id: &str) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE jobs \
             SET status = 'queued', worker_id = NULL, claimed_at = NULL, updated_at = ?1 \
             WHERE id = ?2",
        )
        .bind(&now)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        self.log_event(job_id, "reset_stale", None, Some("queued"), None)
            .await
            .ok();
        Ok(())
    }

    // ── Admin: Retry / Clean / Purge ─────────────────────────────────

    /// Force-retry a single failed job: reset to queued regardless of attempt count.
    pub async fn admin_retry_job(&self, job_id: &str) -> Result<bool, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "UPDATE jobs \
             SET status = 'queued', next_retry_at = NULL, last_error = NULL, \
                 worker_id = NULL, claimed_at = NULL, updated_at = ?1 \
             WHERE id = ?2 AND status = 'failed'",
        )
        .bind(&now)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        if res.rows_affected() > 0 {
            self.log_event(job_id, "admin_retry", Some("failed"), Some("queued"), None)
                .await
                .ok();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Retry ALL failed jobs – resets them to queued.
    /// Returns the number of jobs re-queued.
    pub async fn admin_retry_all_failed(&self) -> Result<u64, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "UPDATE jobs \
             SET status = 'queued', next_retry_at = NULL, last_error = NULL, \
                 worker_id = NULL, claimed_at = NULL, updated_at = ?1 \
             WHERE status = 'failed'",
        )
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected())
    }

    /// Delete a single job (and its events).
    pub async fn admin_delete_job(&self, job_id: &str) -> Result<bool, sqlx::Error> {
        // Delete events first (FK-free but keeps integrity)
        sqlx::query("DELETE FROM job_events WHERE job_id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        let res = sqlx::query("DELETE FROM jobs WHERE id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        Ok(res.rows_affected() > 0)
    }

    /// Purge (delete) all jobs with a given status. Returns count deleted.
    pub async fn admin_purge_by_status(&self, status: &str) -> Result<u64, sqlx::Error> {
        // Delete matching events
        sqlx::query(
            "DELETE FROM job_events WHERE job_id IN \
             (SELECT id FROM jobs WHERE status = ?1)",
        )
        .bind(status)
        .execute(&self.pool)
        .await?;

        let res = sqlx::query("DELETE FROM jobs WHERE status = ?1")
            .bind(status)
            .execute(&self.pool)
            .await?;

        Ok(res.rows_affected())
    }

    /// Purge all terminal jobs (completed, failed, canceled).
    /// Returns count deleted.
    pub async fn admin_purge_terminal(&self) -> Result<u64, sqlx::Error> {
        sqlx::query(
            "DELETE FROM job_events WHERE job_id IN \
             (SELECT id FROM jobs WHERE status IN ('completed', 'failed', 'canceled'))",
        )
        .execute(&self.pool)
        .await?;

        let res = sqlx::query(
            "DELETE FROM jobs WHERE status IN ('completed', 'failed', 'canceled')",
        )
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected())
    }

    /// Purge terminal jobs older than a threshold datetime string (RFC 3339).
    /// Returns count deleted.
    pub async fn admin_purge_older_than(&self, before: &str) -> Result<u64, sqlx::Error> {
        sqlx::query(
            "DELETE FROM job_events WHERE job_id IN \
             (SELECT id FROM jobs WHERE status IN ('completed', 'failed', 'canceled') \
              AND updated_at < ?1)",
        )
        .bind(before)
        .execute(&self.pool)
        .await?;

        let res = sqlx::query(
            "DELETE FROM jobs \
             WHERE status IN ('completed', 'failed', 'canceled') \
               AND updated_at < ?1",
        )
        .bind(before)
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected())
    }

    /// Count terminal jobs grouped by status, for the cleanup panel UI.
    pub async fn count_terminal_jobs(&self) -> Result<(i64, i64, i64), sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct Row {
            status: String,
            count: i64,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT status, COUNT(*) as count FROM jobs \
             WHERE status IN ('completed', 'failed', 'canceled') \
             GROUP BY status",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut completed = 0i64;
        let mut failed = 0i64;
        let mut canceled = 0i64;
        for r in rows {
            match r.status.as_str() {
                "completed" => completed = r.count,
                "failed" => failed = r.count,
                "canceled" => canceled = r.count,
                _ => {}
            }
        }
        Ok((completed, failed, canceled))
    }

    // ── Read helpers ─────────────────────────────────────────────────────

    #[allow(dead_code)]
    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = ?1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn get_job_by_video_id(&self, video_id: i64) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs WHERE video_id = ?1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(video_id)
        .fetch_optional(&self.pool)
        .await
    }

    #[allow(dead_code)]
    pub async fn list_jobs(&self, limit: i64, offset: i64) -> Result<Vec<Job>, sqlx::Error> {
        sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?1 OFFSET ?2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn list_jobs_filtered(
        &self,
        status: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Job>, sqlx::Error> {
        self.search_jobs(None, status, "created_at", "desc", limit, offset)
            .await
    }

    /// Search/filter jobs by optional video_id and/or status, with sorting.
    pub async fn search_jobs(
        &self,
        video_id: Option<i64>,
        status: Option<&str>,
        sort_by: &str,
        sort_dir: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Job>, sqlx::Error> {
        // Whitelist sort column to prevent injection
        let col = match sort_by {
            "video_id" => "video_id",
            "status" => "status",
            "attempt" => "attempt",
            "updated_at" => "updated_at",
            _ => "created_at",
        };
        let dir = if sort_dir == "asc" { "ASC" } else { "DESC" };

        let sql = format!(
            "SELECT * FROM jobs \
             WHERE (?1 IS NULL OR video_id = ?1) \
               AND (?2 IS NULL OR status = ?2) \
             ORDER BY {col} {dir} LIMIT ?3 OFFSET ?4"
        );

        sqlx::query_as::<_, Job>(&sql)
            .bind(video_id)
            .bind(status)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
    }

    #[allow(dead_code)]
    pub async fn count_jobs(&self, status: Option<&str>) -> Result<i64, sqlx::Error> {
        self.count_jobs_search(None, status).await
    }

    /// Count jobs matching optional video_id and/or status.
    pub async fn count_jobs_search(
        &self,
        video_id: Option<i64>,
        status: Option<&str>,
    ) -> Result<i64, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct CountRow {
            count: i64,
        }
        let row: CountRow = sqlx::query_as(
            "SELECT COUNT(*) as count FROM jobs \
             WHERE (?1 IS NULL OR video_id = ?1) \
               AND (?2 IS NULL OR status = ?2)",
        )
        .bind(video_id)
        .bind(status)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.count)
    }

    #[allow(dead_code)]
    pub async fn get_all_jobs_for_video(
        &self,
        video_id: i64,
    ) -> Result<Vec<Job>, sqlx::Error> {
        sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs WHERE video_id = ?1 ORDER BY created_at DESC",
        )
        .bind(video_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn count_by_status(&self) -> Result<Vec<(String, i64)>, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct Row {
            status: String,
            count: i64,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT status, COUNT(*) as count FROM jobs GROUP BY status",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| (r.status, r.count)).collect())
    }

    pub async fn get_job_events(&self, job_id: &str) -> Result<Vec<JobEvent>, sqlx::Error> {
        sqlx::query_as::<_, JobEvent>(
            "SELECT * FROM job_events WHERE job_id = ?1 ORDER BY created_at ASC",
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await
    }

    // ── Audit log helper ─────────────────────────────────────────────────

    /// Public entry point for logging arbitrary job events (e.g. callback attempts).
    pub async fn log_event_public(
        &self,
        job_id: &str,
        event_type: &str,
        old_status: Option<&str>,
        new_status: Option<&str>,
        details: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        self.log_event(job_id, event_type, old_status, new_status, details)
            .await
    }

    async fn log_event(
        &self,
        job_id: &str,
        event_type: &str,
        old_status: Option<&str>,
        new_status: Option<&str>,
        details: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO job_events \
                (job_id, event_type, old_status, new_status, details, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(job_id)
        .bind(event_type)
        .bind(old_status)
        .bind(new_status)
        .bind(details)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_repo() -> JobRepository {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let repo = JobRepository::new(pool);
        repo.migrate().await.unwrap();
        repo
    }

    #[tokio::test]
    async fn idempotent_insert_returns_same_job() {
        let repo = test_repo().await;

        let (j1, c1) = repo
            .insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();
        assert!(c1, "first insert should report created=true");

        let (j2, c2) = repo
            .insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();
        assert!(!c2, "second insert should be idempotent");
        assert_eq!(j1.id, j2.id);
    }

    #[tokio::test]
    async fn cancel_state_transitions() {
        let repo = test_repo().await;

        // Create
        let (job, _) = repo
            .insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();
        assert_eq!(job.status, "queued");

        // Cancel
        let canceled = repo.cancel_job(1, Some("deleted")).await.unwrap();
        assert!(canceled.is_some());
        assert_eq!(canceled.unwrap().status, "canceled");

        // Cancel again → None
        let again = repo.cancel_job(1, Some("again")).await.unwrap();
        assert!(again.is_none());

        // New job for the same video should be allowed
        let (new_job, created) = repo
            .insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();
        assert!(created);
        assert_ne!(job.id, new_job.id);
    }

    #[tokio::test]
    async fn claim_and_no_double_claim() {
        let repo = test_repo().await;

        repo.insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();

        let claimed = repo.claim_next_job("w-0").await.unwrap();
        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.status, "downloading");
        assert_eq!(claimed.attempt, 1);

        // Second worker should find nothing
        let none = repo.claim_next_job("w-1").await.unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn fail_and_retry_cycle() {
        let repo = test_repo().await;

        let (job, _) = repo
            .insert_or_get_active(1, "a/b.mp4", "http://x/a.mp4", "http://cb/1", 5)
            .await
            .unwrap();

        // Claim
        let claimed = repo.claim_next_job("w-0").await.unwrap().unwrap();
        assert_eq!(claimed.id, job.id);

        // Fail with a retry time in the past so it's immediately retryable
        let past = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        repo.fail_job(&job.id, "network timeout", Some(past))
            .await
            .unwrap();

        // Should appear in retryable list
        let retryable = repo.get_retryable_jobs().await.unwrap();
        assert_eq!(retryable.len(), 1);

        // Re-queue
        repo.requeue_for_retry(&job.id).await.unwrap();

        // Now it should be claimable again
        let reclaimed = repo.claim_next_job("w-0").await.unwrap();
        assert!(reclaimed.is_some());
        assert_eq!(reclaimed.unwrap().attempt, 2);
    }
}

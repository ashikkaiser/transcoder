use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Per-worker snapshot (immutable, for display)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct WorkerSnapshot {
    pub id: String,
    pub status: WorkerStatus,
    /// Current job ID if active
    pub current_job_id: Option<String>,
    /// Current video ID if active
    pub current_video_id: Option<i64>,
    /// Seconds the current job has been running (0 if idle)
    pub current_duration_secs: u64,
    /// Total number of jobs this worker has completed since boot
    pub jobs_completed: u64,
    /// Cumulative seconds spent processing across all jobs
    pub total_processing_secs: u64,
    /// Average seconds per job (0 if no jobs completed)
    pub avg_processing_secs: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Idle,
    Active,
}

// ---------------------------------------------------------------------------
// Display-ready version for Askama templates
// ---------------------------------------------------------------------------

pub struct WorkerDisplay {
    pub id: String,
    pub status: String,
    pub status_class: &'static str,
    pub status_dot: &'static str,
    pub current_job_id: String,
    pub current_video_id: String,
    pub current_duration: String,
    pub jobs_completed: u64,
    pub avg_processing: String,
}

impl WorkerDisplay {
    pub fn from_snapshot(s: &WorkerSnapshot) -> Self {
        let (status, status_class, status_dot) = match s.status {
            WorkerStatus::Active => (
                "Active".to_string(),
                "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
                "bg-emerald-400 status-pulse",
            ),
            WorkerStatus::Idle => (
                "Idle".to_string(),
                "bg-gray-500/10 text-gray-400 border-gray-500/20",
                "bg-gray-500",
            ),
        };

        Self {
            id: s.id.clone(),
            status,
            status_class,
            status_dot,
            current_job_id: s
                .current_job_id
                .as_deref()
                .map(|id| truncate_uuid(id))
                .unwrap_or_else(|| "—".to_string()),
            current_video_id: s
                .current_video_id
                .map(|v| v.to_string())
                .unwrap_or_else(|| "—".to_string()),
            current_duration: if s.current_duration_secs > 0 {
                fmt_duration(s.current_duration_secs)
            } else {
                "—".to_string()
            },
            jobs_completed: s.jobs_completed,
            avg_processing: if s.avg_processing_secs > 0 {
                fmt_duration(s.avg_processing_secs)
            } else {
                "—".to_string()
            },
        }
    }
}

/// Summary stats across all workers
pub struct WorkersSummary {
    pub total: usize,
    pub active: usize,
    pub idle: usize,
    pub total_jobs_completed: u64,
    pub overall_avg_secs: String,
}

// ---------------------------------------------------------------------------
// Internal mutable state per worker
// ---------------------------------------------------------------------------

struct WorkerState {
    status: WorkerStatus,
    current_job_id: Option<String>,
    current_video_id: Option<i64>,
    job_started_at: Option<Instant>,
    jobs_completed: u64,
    total_processing_secs: u64,
}

// ---------------------------------------------------------------------------
// WorkerTracker — thread-safe tracker using DashMap
// ---------------------------------------------------------------------------

pub struct WorkerTracker {
    workers: DashMap<String, WorkerState>,
}

impl WorkerTracker {
    pub fn new() -> Self {
        Self {
            workers: DashMap::new(),
        }
    }

    /// Register a new worker as idle.
    pub fn register(&self, worker_id: &str) {
        self.workers.insert(
            worker_id.to_string(),
            WorkerState {
                status: WorkerStatus::Idle,
                current_job_id: None,
                current_video_id: None,
                job_started_at: None,
                jobs_completed: 0,
                total_processing_secs: 0,
            },
        );
    }

    /// Mark a worker as actively processing a job.
    pub fn set_active(&self, worker_id: &str, job_id: &str, video_id: i64) {
        if let Some(mut w) = self.workers.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
            w.current_job_id = Some(job_id.to_string());
            w.current_video_id = Some(video_id);
            w.job_started_at = Some(Instant::now());
        }
    }

    /// Remove a worker from the tracker entirely (used when scaling down).
    pub fn unregister(&self, worker_id: &str) {
        self.workers.remove(worker_id);
    }

    /// Check if a worker is currently idle.
    pub fn is_idle(&self, worker_id: &str) -> bool {
        self.workers
            .get(worker_id)
            .map(|w| w.status == WorkerStatus::Idle)
            .unwrap_or(true)
    }

    /// Mark a worker as idle after job completion/failure.
    pub fn set_idle(&self, worker_id: &str) {
        if let Some(mut w) = self.workers.get_mut(worker_id) {
            w.status = WorkerStatus::Idle;
            w.current_job_id = None;
            w.current_video_id = None;
            w.job_started_at = None;
        }
    }

    /// Record that the current job finished (to update stats), then go idle.
    pub fn record_completion(&self, worker_id: &str) {
        if let Some(mut w) = self.workers.get_mut(worker_id) {
            if let Some(started) = w.job_started_at {
                let elapsed = started.elapsed().as_secs();
                w.total_processing_secs += elapsed;
                w.jobs_completed += 1;
            }
        }
    }

    /// Number of currently active (non-idle) workers.
    pub fn active_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|e| e.value().status == WorkerStatus::Active)
            .count()
    }

    /// Get a point-in-time snapshot of all workers.
    pub fn snapshot_all(&self) -> Vec<WorkerSnapshot> {
        let mut out: Vec<WorkerSnapshot> = self
            .workers
            .iter()
            .map(|entry| {
                let w = entry.value();
                let current_duration_secs = match (w.status.clone(), w.job_started_at) {
                    (WorkerStatus::Active, Some(started)) => started.elapsed().as_secs(),
                    _ => 0,
                };
                let avg_processing_secs = if w.jobs_completed > 0 {
                    w.total_processing_secs / w.jobs_completed
                } else {
                    0
                };
                WorkerSnapshot {
                    id: entry.key().clone(),
                    status: w.status.clone(),
                    current_job_id: w.current_job_id.clone(),
                    current_video_id: w.current_video_id,
                    current_duration_secs,
                    jobs_completed: w.jobs_completed,
                    total_processing_secs: w.total_processing_secs,
                    avg_processing_secs,
                }
            })
            .collect();

        // Sort by worker id for consistent ordering
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Get display-ready snapshots for templates.
    pub fn display_all(&self) -> (Vec<WorkerDisplay>, WorkersSummary) {
        let snaps = self.snapshot_all();

        let total = snaps.len();
        let active = snaps.iter().filter(|s| s.status == WorkerStatus::Active).count();
        let idle = total - active;
        let total_jobs_completed: u64 = snaps.iter().map(|s| s.jobs_completed).sum();
        let total_proc_secs: u64 = snaps.iter().map(|s| s.total_processing_secs).sum();
        let overall_avg_secs = if total_jobs_completed > 0 {
            fmt_duration(total_proc_secs / total_jobs_completed)
        } else {
            "—".to_string()
        };

        let displays = snaps.iter().map(WorkerDisplay::from_snapshot).collect();

        let summary = WorkersSummary {
            total,
            active,
            idle,
            total_jobs_completed,
            overall_avg_secs,
        };

        (displays, summary)
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn fmt_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn truncate_uuid(id: &str) -> String {
    if id.len() > 8 {
        format!("{}…", &id[..8])
    } else {
        id.to_string()
    }
}

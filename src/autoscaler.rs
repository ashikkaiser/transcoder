use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};

use crate::db::repository::JobRepository;
use crate::settings::SettingsStore;
use crate::system::SystemMonitor;
use crate::worker::WorkerPool;
use crate::worker_tracker::WorkerTracker;

/// Interval between auto-scaler evaluations.
const CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Cooldown after a scale event to prevent thrashing.
const COOLDOWN: Duration = Duration::from_secs(30);

/// Start the auto-scaler background loop.
/// Runs forever until the process shuts down.
pub fn start(
    pool: Arc<WorkerPool>,
    settings: SettingsStore,
    sys_monitor: Arc<SystemMonitor>,
    repo: Arc<JobRepository>,
    tracker: Arc<WorkerTracker>,
) {
    tokio::spawn(async move {
        let mut last_scale = tokio::time::Instant::now().checked_sub(COOLDOWN).unwrap_or_else(tokio::time::Instant::now);

        loop {
            tokio::time::sleep(CHECK_INTERVAL).await;

            let (enabled, cpu_limit, mem_limit, min_w, max_w) =
                settings.auto_scale_config().await;

            if !enabled {
                continue;
            }

            // Respect cooldown
            if last_scale.elapsed() < COOLDOWN {
                continue;
            }

            let snap = sys_monitor.snapshot().await;
            let current_workers = pool.current_worker_count();

            // Count queued/pending jobs
            let pending = repo
                .count_jobs_search(None, Some("queued"))
                .await
                .unwrap_or(0) as usize;

            let active_workers = tracker.active_count();
            let idle_workers = current_workers.saturating_sub(active_workers);

            let cpu_ok = snap.cpu_usage_pct < cpu_limit;
            let mem_ok = {
                let mem_pct = if snap.mem_total_bytes > 0 {
                    (snap.mem_used_bytes as f32 / snap.mem_total_bytes as f32) * 100.0
                } else {
                    0.0
                };
                mem_pct < mem_limit
            };

            let desired = compute_desired(
                current_workers,
                pending,
                active_workers,
                idle_workers,
                cpu_ok,
                mem_ok,
                min_w,
                max_w,
            );

            if desired != current_workers {
                info!(
                    from = current_workers,
                    to = desired,
                    pending,
                    active = active_workers,
                    idle = idle_workers,
                    cpu = format!("{:.1}%", snap.cpu_usage_pct),
                    mem_ok,
                    "Auto-scaler adjusting workers"
                );
                WorkerPool::scale_to(&pool, desired);
                // Update the stored concurrency so the dashboard reflects reality
                settings.update(|s| s.worker_concurrency = desired).await;
                last_scale = tokio::time::Instant::now();
            } else {
                debug!(
                    current = current_workers,
                    pending,
                    active = active_workers,
                    "Auto-scaler: no change needed"
                );
            }
        }
    });
}

/// Decide how many workers we want.
fn compute_desired(
    current: usize,
    pending: usize,
    _active: usize,
    idle: usize,
    cpu_ok: bool,
    mem_ok: bool,
    min_w: usize,
    max_w: usize,
) -> usize {
    let mut desired = current;

    // ── Scale UP ────────────────────────────────────────────────
    // If there are queued jobs AND we have headroom, add workers.
    if pending > 0 && cpu_ok && mem_ok {
        // Add one worker per evaluation cycle (conservative ramp-up).
        // If there are way more pending jobs than workers, step up faster.
        let step = if pending > current * 3 { 2 } else { 1 };
        desired = current + step;
    }

    // ── Scale DOWN ──────────────────────────────────────────────
    // If there's nothing to do and some workers are idle, shed one.
    if pending == 0 && idle > 1 {
        desired = current.saturating_sub(1);
    }

    // If resources are tight, shed a worker even if there's work.
    if !cpu_ok || !mem_ok {
        if current > min_w {
            desired = current.saturating_sub(1);
        }
    }

    // Clamp to [min, max]
    desired.clamp(min_w, max_w)
}

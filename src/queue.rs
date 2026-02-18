use std::sync::Arc;
use tokio::sync::Notify;

/// Lightweight in-memory notifier.
///
/// The **database** is the authoritative queue; this struct merely wakes
/// sleeping workers so they can call `claim_next_job` without polling.
#[derive(Clone)]
pub struct JobQueue {
    notify: Arc<Notify>,
}

impl JobQueue {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self { notify }
    }

    /// Wake all waiting workers so they can try to claim new work.
    pub fn notify_new_job(&self) {
        self.notify.notify_waiters();
    }

    /// Block until either a notification arrives or the timeout elapses.
    pub async fn wait_for_job(&self, timeout: std::time::Duration) {
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = tokio::time::sleep(timeout) => {}
        }
    }
}

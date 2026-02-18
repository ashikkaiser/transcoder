use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

/// Thread-safe registry that maps `video_id` → `CancellationToken`.
///
/// Workers register a token when they start processing a job; the cancel
/// endpoint looks up and triggers the token so the worker can abort
/// gracefully (stop ffmpeg, delete temp files, etc.).
pub struct CancellationRegistry {
    tokens: DashMap<i64, CancellationToken>,
}

impl CancellationRegistry {
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
        }
    }

    /// Register a fresh token for `video_id` and return a clone the worker
    /// can poll / select on.
    pub fn register(&self, video_id: i64) -> CancellationToken {
        let token = CancellationToken::new();
        self.tokens.insert(video_id, token.clone());
        token
    }

    /// Signal cancellation for `video_id`.
    /// Returns `true` if a token was found (and cancelled).
    pub fn cancel(&self, video_id: i64) -> bool {
        if let Some((_, token)) = self.tokens.remove(&video_id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Remove the token without cancelling – used after a job finishes
    /// normally.
    pub fn remove(&self, video_id: i64) {
        self.tokens.remove(&video_id);
    }
}

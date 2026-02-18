use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Progress info for a single in-flight transcode job
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct ProgressInfo {
    pub video_id: i64,
    pub job_id: String,
    pub current_rendition: String,
    pub rendition_index: usize,
    pub total_renditions: usize,
    /// Percentage of the *current* rendition (0–100).
    pub rendition_pct: f32,
    /// Percentage of the *overall* transcode (0–100).
    pub overall_pct: f32,
    /// FFmpeg encoding speed (e.g. "2.1x").
    pub speed: String,
    /// Phase: "downloading", "transcoding", "uploading"
    pub phase: String,
}

// ---------------------------------------------------------------------------
// Context passed into transcode functions so they can report progress
// ---------------------------------------------------------------------------

pub struct ProgressContext {
    pub tracker: ProgressTracker,
    pub video_id: i64,
    pub job_id: String,
    pub duration_secs: f64,
    pub total_renditions: usize,
    pub rendition_index: usize,
    pub rendition_name: String,
}

impl ProgressContext {
    /// Called from the ffmpeg progress parser for each update.
    pub fn report(&self, time_secs: f64, speed: &str) {
        let rendition_pct = if self.duration_secs > 0.0 {
            ((time_secs / self.duration_secs) * 100.0).clamp(0.0, 100.0) as f32
        } else {
            0.0
        };

        // Overall % = (completed_renditions + current_fraction) / total_renditions * 100
        let overall_pct = if self.total_renditions > 0 {
            ((self.rendition_index as f64 + (rendition_pct as f64 / 100.0))
                / self.total_renditions as f64
                * 100.0) as f32
        } else {
            0.0
        };

        self.tracker.update(
            self.video_id,
            ProgressInfo {
                video_id: self.video_id,
                job_id: self.job_id.clone(),
                current_rendition: self.rendition_name.clone(),
                rendition_index: self.rendition_index,
                total_renditions: self.total_renditions,
                rendition_pct,
                overall_pct,
                speed: speed.to_string(),
                phase: "transcoding".into(),
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Global progress tracker (Arc<DashMap>)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ProgressTracker {
    inner: Arc<DashMap<i64, ProgressInfo>>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn update(&self, video_id: i64, info: ProgressInfo) {
        self.inner.insert(video_id, info);
    }

    /// Set a simple phase-only progress (e.g. "downloading", "uploading")
    pub fn set_phase(&self, video_id: i64, job_id: &str, phase: &str) {
        self.inner.insert(
            video_id,
            ProgressInfo {
                video_id,
                job_id: job_id.to_string(),
                current_rendition: String::new(),
                rendition_index: 0,
                total_renditions: 0,
                rendition_pct: 0.0,
                overall_pct: 0.0,
                speed: String::new(),
                phase: phase.to_string(),
            },
        );
    }

    pub fn get(&self, video_id: i64) -> Option<ProgressInfo> {
        self.inner.get(&video_id).map(|v| v.clone())
    }

    pub fn remove(&self, video_id: i64) {
        self.inner.remove(&video_id);
    }

    #[allow(dead_code)]
    pub fn all_active(&self) -> Vec<ProgressInfo> {
        self.inner.iter().map(|e| e.value().clone()).collect()
    }
}

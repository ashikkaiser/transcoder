use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::db::repository::JobRepository;

// ---------------------------------------------------------------------------
// Callback auth: how the service authenticates when hitting callback URLs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CallbackAuthType {
    None,
    BearerToken,
    ApiKey,
}

impl CallbackAuthType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BearerToken => "bearer_token",
            Self::ApiKey => "api_key",
        }
    }

    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "bearer_token" | "bearer" => Self::BearerToken,
            "api_key" | "apikey" => Self::ApiKey,
            _ => Self::None,
        }
    }
}

// ---------------------------------------------------------------------------
// Hardware acceleration mode for video encoding
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HwAccelMode {
    /// Software-only (libx264). Always works.
    Cpu,
    /// Automatic — try GPU first, fall back to CPU.
    Auto,
    /// Apple VideoToolbox (macOS / Apple Silicon).
    Videotoolbox,
    /// NVIDIA NVENC.
    Nvenc,
    /// Intel Quick Sync Video.
    Qsv,
    /// VA-API (Linux AMD / Intel).
    Vaapi,
}

impl HwAccelMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Auto => "auto",
            Self::Videotoolbox => "videotoolbox",
            Self::Nvenc => "nvenc",
            Self::Qsv => "qsv",
            Self::Vaapi => "vaapi",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Cpu => "CPU Only (libx264)",
            Self::Auto => "Auto (prefer GPU, fallback CPU)",
            Self::Videotoolbox => "Apple VideoToolbox (macOS)",
            Self::Nvenc => "NVIDIA NVENC",
            Self::Qsv => "Intel Quick Sync (QSV)",
            Self::Vaapi => "VA-API (Linux)",
        }
    }

    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "auto" => Self::Auto,
            "videotoolbox" | "vt" | "apple" => Self::Videotoolbox,
            "nvenc" | "nvidia" => Self::Nvenc,
            "qsv" | "intel" => Self::Qsv,
            "vaapi" | "va-api" => Self::Vaapi,
            _ => Self::Cpu,
        }
    }

    /// All known modes for the settings UI.
    pub fn all() -> Vec<Self> {
        vec![
            Self::Auto,
            Self::Cpu,
            Self::Videotoolbox,
            Self::Nvenc,
            Self::Qsv,
            Self::Vaapi,
        ]
    }
}

// ---------------------------------------------------------------------------
// Rendition preset configuration
// ---------------------------------------------------------------------------

/// All known rendition presets. Each can be enabled or disabled by the admin.
#[derive(Clone, Debug, Serialize)]
pub struct RenditionPreset {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub video_bitrate: String,
    pub audio_bitrate: String,
    pub bandwidth: u64,
    pub enabled: bool,
}

impl RenditionPreset {
    /// The full catalog of available presets (all enabled by default except 240p and 1440p).
    pub fn all_defaults() -> Vec<Self> {
        vec![
            Self {
                name: "240p".into(),
                width: 426,
                height: 240,
                video_bitrate: "400k".into(),
                audio_bitrate: "64k".into(),
                bandwidth: 464_000,
                enabled: false,
            },
            Self {
                name: "360p".into(),
                width: 640,
                height: 360,
                video_bitrate: "800k".into(),
                audio_bitrate: "96k".into(),
                bandwidth: 896_000,
                enabled: true,
            },
            Self {
                name: "480p".into(),
                width: 854,
                height: 480,
                video_bitrate: "1400k".into(),
                audio_bitrate: "128k".into(),
                bandwidth: 1_528_000,
                enabled: false,
            },
            Self {
                name: "720p".into(),
                width: 1280,
                height: 720,
                video_bitrate: "2800k".into(),
                audio_bitrate: "128k".into(),
                bandwidth: 2_928_000,
                enabled: true,
            },
            Self {
                name: "1080p".into(),
                width: 1920,
                height: 1080,
                video_bitrate: "5000k".into(),
                audio_bitrate: "192k".into(),
                bandwidth: 5_192_000,
                enabled: true,
            },
            Self {
                name: "1440p".into(),
                width: 2560,
                height: 1440,
                video_bitrate: "8000k".into(),
                audio_bitrate: "192k".into(),
                bandwidth: 8_192_000,
                enabled: false,
            },
        ]
    }
}

// ---------------------------------------------------------------------------
// RuntimeSettings — the mutable bag of settings the admin can change via UI
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeSettings {
    pub callback_base_url: String,
    pub callback_auth_type: CallbackAuthType,
    pub callback_secret: String,
    pub max_job_attempts: i32,
    pub worker_concurrency: usize,
    pub admin_username: String,
    pub admin_password: String,
    pub renditions: Vec<RenditionPreset>,
    /// API key for machine-to-machine API authentication.
    /// When empty, API routes are open (dev mode).
    pub api_key: String,
    /// Base prefix for HLS output paths in R2/S3.
    /// e.g. `"hls"` → uploads to `hls/<s3_key_without_ext>/master.m3u8`
    pub output_path_prefix: String,
    /// Hardware acceleration mode for FFmpeg video encoding.
    pub hw_accel: HwAccelMode,
    /// When true, delete the source MP4 from R2/S3 after successful transcode + callback.
    pub delete_source_after_transcode: bool,

    // ── Poll / Pull settings ──────────────────────────────────────
    /// When true, the service periodically polls an external API for pending videos.
    pub poll_enabled: bool,
    /// The endpoint URL to poll for pending videos.
    /// Expected to return JSON: `{ "data": [{ "id": ..., "s3_key": ..., "video_url": ..., "callback_url": ... }] }`
    pub poll_endpoint: String,
    /// How often to poll, in seconds.
    pub poll_interval_secs: u64,
    /// Auth type for the poll endpoint (same options as callback auth).
    pub poll_auth_type: CallbackAuthType,
    /// Secret/token for authenticating poll requests.
    pub poll_auth_secret: String,

    // ── Auto-scale settings ─────────────────────────────────────
    /// When true, the system automatically adjusts worker count based on
    /// CPU / memory usage and queue depth. `worker_concurrency` becomes
    /// a manual-override that is only used when auto_scale is off.
    pub auto_scale: bool,
    /// CPU usage threshold (0–100). System won't scale up if CPU is above this.
    pub auto_scale_cpu_threshold: f32,
    /// Memory usage threshold (0–100). System won't scale up if memory is above this.
    pub auto_scale_mem_threshold: f32,
    /// Optional lower bound on worker count (defaults to 1).
    pub auto_scale_min: usize,
    /// Optional upper bound (defaults to CPU core count).
    pub auto_scale_max: usize,
}

// ---------------------------------------------------------------------------
// SettingsStore — Arc<RwLock<RuntimeSettings>> with convenience methods
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SettingsStore {
    inner: Arc<RwLock<RuntimeSettings>>,
}

impl SettingsStore {
    /// Seed from the initial Config + env vars (used as defaults before DB overlay).
    pub fn from_config(config: &Config) -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let env = |key: &str| std::env::var(key).unwrap_or_default();
        let env_or = |key: &str, def: &str| std::env::var(key).unwrap_or_else(|_| def.to_string());
        let env_bool = |key: &str| {
            std::env::var(key)
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
                .unwrap_or(false)
        };

        let mut renditions = RenditionPreset::all_defaults();
        if let Ok(env_val) = std::env::var("ENABLED_RENDITIONS") {
            let enabled: Vec<String> = env_val.split(',').map(|s| s.trim().to_lowercase()).collect();
            for r in &mut renditions {
                r.enabled = enabled.contains(&r.name.to_lowercase());
            }
        }

        Self {
            inner: Arc::new(RwLock::new(RuntimeSettings {
                callback_base_url: config.callback_base_url.clone(),
                callback_auth_type: CallbackAuthType::from_str_loose(&env("CALLBACK_AUTH_TYPE")),
                callback_secret: env("CALLBACK_SECRET"),
                max_job_attempts: config.max_job_attempts,
                worker_concurrency: config.worker_concurrency,
                admin_username: config.admin_username.clone(),
                admin_password: config.admin_password.clone(),
                renditions,
                api_key: env("API_KEY"),
                output_path_prefix: env_or("OUTPUT_PATH_PREFIX", "hls"),
                hw_accel: HwAccelMode::from_str_loose(&env_or("HW_ACCEL", "auto")),
                delete_source_after_transcode: env_bool("DELETE_SOURCE_AFTER_TRANSCODE"),
                poll_enabled: env_bool("POLL_ENABLED"),
                poll_endpoint: env("POLL_ENDPOINT"),
                poll_interval_secs: env_or("POLL_INTERVAL_SECS", "30").parse().unwrap_or(30),
                poll_auth_type: CallbackAuthType::from_str_loose(&env("POLL_AUTH_TYPE")),
                poll_auth_secret: env("POLL_AUTH_SECRET"),
                auto_scale: env_bool("AUTO_SCALE"),
                auto_scale_cpu_threshold: env_or("AUTO_SCALE_CPU_THRESHOLD", "80").parse().unwrap_or(80.0),
                auto_scale_mem_threshold: env_or("AUTO_SCALE_MEM_THRESHOLD", "85").parse().unwrap_or(85.0),
                auto_scale_min: env_or("AUTO_SCALE_MIN", "1").parse().unwrap_or(1),
                auto_scale_max: env_or("AUTO_SCALE_MAX", &cpu_count.to_string()).parse().unwrap_or(cpu_count),
            })),
        }
    }

    /// Overlay saved DB settings on top of env-var defaults.
    /// Call once at startup after `from_config`.
    pub async fn load_from_db(&self, repo: &JobRepository) {
        let db_map = match repo.load_all_settings().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Could not load settings from DB, using env defaults");
                return;
            }
        };
        if db_map.is_empty() {
            info!("No saved settings in DB, using env defaults");
            return;
        }

        info!(count = db_map.len(), "Loaded settings from DB");

        let mut guard = self.inner.write().await;
        apply_db_settings(&mut guard, &db_map);
    }

    pub async fn read(&self) -> RuntimeSettings {
        self.inner.read().await.clone()
    }

    pub async fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut RuntimeSettings),
    {
        let mut guard = self.inner.write().await;
        f(&mut guard);
    }

    /// Persist current settings to the database.
    pub async fn persist(&self, repo: &JobRepository) {
        let s = self.inner.read().await.clone();
        let enabled_renditions: String = s.renditions.iter()
            .filter(|r| r.enabled)
            .map(|r| r.name.clone())
            .collect::<Vec<_>>()
            .join(",");

        let entries: Vec<(&str, String)> = vec![
            ("callback_base_url", s.callback_base_url),
            ("callback_auth_type", s.callback_auth_type.as_str().to_string()),
            ("callback_secret", s.callback_secret),
            ("max_job_attempts", s.max_job_attempts.to_string()),
            ("worker_concurrency", s.worker_concurrency.to_string()),
            ("admin_username", s.admin_username),
            ("admin_password", s.admin_password),
            ("api_key", s.api_key),
            ("output_path_prefix", s.output_path_prefix),
            ("hw_accel", s.hw_accel.as_str().to_string()),
            ("enabled_renditions", enabled_renditions),
            ("delete_source_after_transcode", s.delete_source_after_transcode.to_string()),
            ("poll_enabled", s.poll_enabled.to_string()),
            ("poll_endpoint", s.poll_endpoint),
            ("poll_interval_secs", s.poll_interval_secs.to_string()),
            ("poll_auth_type", s.poll_auth_type.as_str().to_string()),
            ("poll_auth_secret", s.poll_auth_secret),
            ("auto_scale", s.auto_scale.to_string()),
            ("auto_scale_cpu_threshold", s.auto_scale_cpu_threshold.to_string()),
            ("auto_scale_mem_threshold", s.auto_scale_mem_threshold.to_string()),
            ("auto_scale_min", s.auto_scale_min.to_string()),
            ("auto_scale_max", s.auto_scale_max.to_string()),
        ];

        if let Err(e) = repo.save_all_settings(&entries).await {
            tracing::error!(error = %e, "Failed to persist settings to DB");
        }
    }

    /// Helper: get the callback auth header (if any) for use by the HTTP client.
    pub async fn callback_auth_header(&self) -> Option<(String, String)> {
        let s = self.inner.read().await;
        if s.callback_secret.is_empty() {
            return None;
        }
        match s.callback_auth_type {
            CallbackAuthType::None => None,
            CallbackAuthType::BearerToken => Some((
                "Authorization".to_string(),
                format!("Bearer {}", s.callback_secret),
            )),
            CallbackAuthType::ApiKey => {
                Some(("X-API-Key".to_string(), s.callback_secret.clone()))
            }
        }
    }

    /// Helper: get the current callback base URL.
    pub async fn callback_base_url(&self) -> String {
        self.inner.read().await.callback_base_url.clone()
    }

    /// Helper: get the current max job attempts.
    pub async fn max_job_attempts(&self) -> i32 {
        self.inner.read().await.max_job_attempts
    }

    /// Helper: get the HLS output path prefix.
    pub async fn output_path_prefix(&self) -> String {
        self.inner.read().await.output_path_prefix.clone()
    }

    /// Helper: get the hardware acceleration mode.
    pub async fn hw_accel(&self) -> HwAccelMode {
        self.inner.read().await.hw_accel.clone()
    }

    /// Helper: get the poll configuration snapshot.
    pub async fn poll_config(&self) -> (bool, String, u64, Option<(String, String)>) {
        let s = self.inner.read().await;
        let auth = if s.poll_auth_secret.is_empty() {
            None
        } else {
            match s.poll_auth_type {
                CallbackAuthType::None => None,
                CallbackAuthType::BearerToken => Some((
                    "Authorization".to_string(),
                    format!("Bearer {}", s.poll_auth_secret),
                )),
                CallbackAuthType::ApiKey => {
                    Some(("X-API-Key".to_string(), s.poll_auth_secret.clone()))
                }
            }
        };
        (s.poll_enabled, s.poll_endpoint.clone(), s.poll_interval_secs, auth)
    }

    /// Helper: check if auto-scale is enabled.
    pub async fn auto_scale_enabled(&self) -> bool {
        self.inner.read().await.auto_scale
    }

    /// Helper: read auto-scale config snapshot.
    pub async fn auto_scale_config(&self) -> (bool, f32, f32, usize, usize) {
        let s = self.inner.read().await;
        (
            s.auto_scale,
            s.auto_scale_cpu_threshold,
            s.auto_scale_mem_threshold,
            s.auto_scale_min,
            s.auto_scale_max,
        )
    }

    /// Helper: get only the enabled rendition names.
    pub async fn enabled_rendition_names(&self) -> Vec<String> {
        self.inner
            .read()
            .await
            .renditions
            .iter()
            .filter(|r| r.enabled)
            .map(|r| r.name.clone())
            .collect()
    }

    /// Helper: check if source file should be deleted after successful transcode.
    pub async fn delete_source_after_transcode(&self) -> bool {
        self.inner.read().await.delete_source_after_transcode
    }
}

// ---------------------------------------------------------------------------
// Apply DB key-value map onto RuntimeSettings
// ---------------------------------------------------------------------------

fn parse_bool(v: &str) -> bool {
    v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")
}

fn apply_db_settings(s: &mut RuntimeSettings, db: &HashMap<String, String>) {
    if let Some(v) = db.get("callback_base_url") { s.callback_base_url = v.clone(); }
    if let Some(v) = db.get("callback_auth_type") { s.callback_auth_type = CallbackAuthType::from_str_loose(v); }
    if let Some(v) = db.get("callback_secret") { s.callback_secret = v.clone(); }
    if let Some(v) = db.get("max_job_attempts") { if let Ok(n) = v.parse() { s.max_job_attempts = n; } }
    if let Some(v) = db.get("worker_concurrency") { if let Ok(n) = v.parse() { s.worker_concurrency = n; } }
    if let Some(v) = db.get("admin_username") { if !v.is_empty() { s.admin_username = v.clone(); } }
    if let Some(v) = db.get("admin_password") { if !v.is_empty() { s.admin_password = v.clone(); } }
    if let Some(v) = db.get("api_key") { s.api_key = v.clone(); }
    if let Some(v) = db.get("output_path_prefix") { s.output_path_prefix = v.clone(); }
    if let Some(v) = db.get("hw_accel") { s.hw_accel = HwAccelMode::from_str_loose(v); }
    if let Some(v) = db.get("delete_source_after_transcode") { s.delete_source_after_transcode = parse_bool(v); }
    if let Some(v) = db.get("poll_enabled") { s.poll_enabled = parse_bool(v); }
    if let Some(v) = db.get("poll_endpoint") { s.poll_endpoint = v.clone(); }
    if let Some(v) = db.get("poll_interval_secs") { if let Ok(n) = v.parse() { s.poll_interval_secs = n; } }
    if let Some(v) = db.get("poll_auth_type") { s.poll_auth_type = CallbackAuthType::from_str_loose(v); }
    if let Some(v) = db.get("poll_auth_secret") { s.poll_auth_secret = v.clone(); }
    if let Some(v) = db.get("auto_scale") { s.auto_scale = parse_bool(v); }
    if let Some(v) = db.get("auto_scale_cpu_threshold") { if let Ok(n) = v.parse() { s.auto_scale_cpu_threshold = n; } }
    if let Some(v) = db.get("auto_scale_mem_threshold") { if let Ok(n) = v.parse() { s.auto_scale_mem_threshold = n; } }
    if let Some(v) = db.get("auto_scale_min") { if let Ok(n) = v.parse() { s.auto_scale_min = n; } }
    if let Some(v) = db.get("auto_scale_max") { if let Ok(n) = v.parse() { s.auto_scale_max = n; } }

    if let Some(v) = db.get("enabled_renditions") {
        let enabled: Vec<String> = v.split(',').map(|s| s.trim().to_lowercase()).collect();
        if !enabled.is_empty() && !enabled.iter().all(|e| e.is_empty()) {
            for r in &mut s.renditions {
                r.enabled = enabled.contains(&r.name.to_lowercase());
            }
        }
    }
}

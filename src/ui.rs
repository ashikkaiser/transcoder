use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Form;
use axum_extra::extract::cookie::{Cookie, SameSite};
use axum_extra::extract::CookieJar;
use serde::Deserialize;

use crate::auth::{SessionStore, COOKIE_NAME};
use crate::cancellation::CancellationRegistry;
use crate::db::models::Job;
use crate::db::repository::JobRepository;
use crate::ffmpeg::FfmpegInfo;
use crate::progress::{ProgressInfo, ProgressTracker};
use crate::queue::JobQueue;
use crate::settings::{CallbackAuthType, HwAccelMode, RenditionPreset, RuntimeSettings, SettingsStore};
use crate::system::{SystemDisplay, SystemMonitor};
use crate::worker::WorkerPool;
use crate::worker_tracker::{WorkerDisplay, WorkerTracker, WorkersSummary};

// ---------------------------------------------------------------------------
// Shared state for all UI + auth handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct UiState {
    pub repo: Arc<JobRepository>,
    pub queue: JobQueue,
    pub cancel_registry: Arc<CancellationRegistry>,
    pub sessions: Arc<SessionStore>,
    pub sys_monitor: Arc<SystemMonitor>,
    pub settings: SettingsStore,
    pub tracker: Arc<WorkerTracker>,
    pub pool: Arc<WorkerPool>,
    pub progress: ProgressTracker,
}

// ---------------------------------------------------------------------------
// Dashboard stats helper
// ---------------------------------------------------------------------------

pub struct DashboardStats {
    pub total: i64,
    pub queued: i64,
    pub active: i64,
    pub completed: i64,
    pub failed: i64,
    pub canceled: i64,
}

impl DashboardStats {
    fn from_counts(counts: &[(String, i64)]) -> Self {
        let mut stats = Self {
            total: 0,
            queued: 0,
            active: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        };
        for (status, count) in counts {
            stats.total += count;
            match status.as_str() {
                "queued" => stats.queued += count,
                "downloading" | "transcoding" | "uploading" | "callback_pending" => {
                    stats.active += count
                }
                "completed" => stats.completed += count,
                "failed" => stats.failed += count,
                "canceled" => stats.canceled += count,
                _ => {}
            }
        }
        stats
    }
}

// ---------------------------------------------------------------------------
// Askama template structs
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {
    version: String,
    error: bool,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    stats: DashboardStats,
    worker_concurrency: usize,
    jobs: Vec<Job>,
    sys: SystemDisplay,
    setup_warnings: Vec<SetupWarning>,
    ffmpeg: FfmpegInfo,
    active_encoder_label: String,
    active_encoder_is_hw: bool,
    auto_scale: bool,
    auto_scale_min: usize,
    auto_scale_max: usize,
}

/// A single setup warning shown on the dashboard.
pub struct SetupWarning {
    pub icon: &'static str, // SVG path d-attribute
    pub title: String,
    pub detail: String,
}

#[derive(Template)]
#[template(path = "jobs.html")]
struct JobsTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    jobs: Vec<Job>,
    total_jobs: i64,
    current_page: i64,
    total_pages: i64,
    current_status: String,
    status_param: String,
    search_query: String,
    search_param: String,
    sort_by: String,
    sort_dir: String,
    sort_param: String,
    all_statuses: Vec<StatusTab>,
    // Cleanup panel counts
    terminal_completed: i64,
    terminal_failed: i64,
    terminal_canceled: i64,
}

pub struct StatusTab {
    pub name: String,
    pub active: bool,
}

/// Wrapper around JobEvent with precomputed CSS classes for the timeline.
pub struct UiEvent {
    pub event_type: String,
    pub new_status: Option<String>,
    pub created_at: String,
    pub details: Option<String>,
    pub dot_border: &'static str,
    pub dot_fill: &'static str,
}

impl UiEvent {
    fn from_db(e: crate::db::models::JobEvent) -> Self {
        let (dot_border, dot_fill) = match e.new_status.as_deref() {
            Some("completed") => (
                "border-emerald-500 bg-emerald-500/20",
                "bg-emerald-400",
            ),
            Some("failed") => ("border-red-500 bg-red-500/20", "bg-red-400"),
            Some("canceled") => ("border-gray-500 bg-gray-500/20", "bg-gray-400"),
            _ => ("border-accent bg-accent/20", "bg-accent-light"),
        };
        Self {
            event_type: e.event_type,
            new_status: e.new_status,
            created_at: e.created_at,
            details: e.details,
            dot_border,
            dot_fill,
        }
    }
}

#[derive(Template)]
#[template(path = "job_detail.html")]
struct JobDetailTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    job: Job,
    events: Vec<UiEvent>,
    progress: Option<ProgressInfo>,
}

#[derive(Template)]
#[template(path = "submit.html")]
struct SubmitTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
}

#[derive(Template)]
#[template(path = "settings.html")]
struct SettingsTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    settings: RuntimeSettings,
    renditions: Vec<RenditionPreset>,
    hw_accel_modes: Vec<HwAccelModeOption>,
    ffmpeg: crate::ffmpeg::FfmpegInfo,
}

/// View model for HW accel radio buttons.
pub struct HwAccelModeOption {
    pub value: String,
    pub label: String,
    pub selected: bool,
    /// Is the required encoder actually available on this machine?
    pub available: bool,
}

#[derive(Template)]
#[template(path = "workers.html")]
struct WorkersTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    workers: Vec<WorkerDisplay>,
    summary: WorkersSummary,
}

#[derive(Template)]
#[template(path = "_stats.html")]
struct StatsPartialTemplate {
    stats: DashboardStats,
}

#[derive(Template)]
#[template(path = "_jobs_table.html")]
struct JobsTablePartialTemplate {
    jobs: Vec<Job>,
}

#[derive(Template)]
#[template(path = "_system.html")]
struct SystemPartialTemplate {
    sys: SystemDisplay,
}

#[derive(Template)]
#[template(path = "_workers.html")]
struct WorkersPartialTemplate {
    workers: Vec<WorkerDisplay>,
    summary: WorkersSummary,
}

// ---------------------------------------------------------------------------
// Query parameter structs
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct FlashQuery {
    pub flash: Option<String>,
    pub flash_type: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct JobsQuery {
    pub status: Option<String>,
    pub search: Option<String>,
    pub page: Option<i64>,
    pub sort: Option<String>,
    pub dir: Option<String>,
    pub flash: Option<String>,
    pub flash_type: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct LoginQuery {
    pub error: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SubmitForm {
    #[serde(rename = "video_id[]")]
    pub video_id: Vec<String>,
    #[serde(rename = "s3_key[]")]
    pub s3_key: Vec<String>,
    #[serde(rename = "video_url[]")]
    pub video_url: Vec<String>,
    #[serde(rename = "callback_url[]")]
    pub callback_url: Vec<String>,
}

#[derive(Deserialize, Default)]
pub struct PartialJobsQuery {
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct SettingsForm {
    pub callback_base_url: Option<String>,
    pub callback_auth_type: Option<String>,
    pub callback_secret: Option<String>,
    pub max_job_attempts: Option<i32>,
    pub worker_concurrency: Option<usize>,
    pub admin_username: Option<String>,
    pub admin_password: Option<String>,
    pub api_key: Option<String>,
    pub output_path_prefix: Option<String>,
    pub hw_accel: Option<String>,
    /// "on" when checked, absent when unchecked
    pub delete_source_after_transcode: Option<String>,
    /// Comma-separated rendition names collected by JS from checkboxes.
    #[serde(default)]
    pub enabled_renditions: Option<String>,
    // ── Poll / Pull fields ─────────────────────────
    /// "on" when the checkbox is checked, absent when unchecked
    pub poll_enabled: Option<String>,
    pub poll_endpoint: Option<String>,
    pub poll_interval_secs: Option<u64>,
    pub poll_auth_type: Option<String>,
    pub poll_auth_secret: Option<String>,
    // ── Auto-scale fields ──────────────────────────
    /// "on" when the checkbox is checked, absent when unchecked
    pub auto_scale: Option<String>,
    pub auto_scale_cpu_threshold: Option<f32>,
    pub auto_scale_mem_threshold: Option<f32>,
    pub auto_scale_min: Option<usize>,
    pub auto_scale_max: Option<usize>,
}

// ---------------------------------------------------------------------------
// Auth handlers (login/logout)
// ---------------------------------------------------------------------------

pub async fn page_login(Query(q): Query<LoginQuery>) -> impl IntoResponse {
    LoginTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        error: q.error.is_some(),
    }
}

pub async fn handle_login(
    State(state): State<UiState>,
    jar: CookieJar,
    Form(form): Form<LoginForm>,
) -> Response {
    // Check against runtime settings (which the admin can change)
    let current = state.settings.read().await;
    if form.username == current.admin_username && form.password == current.admin_password {
        let token = state.sessions.create();
        let cookie = Cookie::build((COOKIE_NAME, token))
            .path("/")
            .http_only(true)
            .same_site(SameSite::Strict)
            .build();
        (jar.add(cookie), Redirect::to("/ui")).into_response()
    } else {
        Redirect::to("/auth/login?error=1").into_response()
    }
}

pub async fn handle_logout(State(state): State<UiState>, jar: CookieJar) -> Response {
    if let Some(cookie) = jar.get(COOKIE_NAME) {
        state.sessions.remove(cookie.value());
    }
    let removal = Cookie::build((COOKIE_NAME, ""))
        .path("/")
        .build();
    (jar.remove(removal), Redirect::to("/auth/login")).into_response()
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

pub async fn page_dashboard(
    State(state): State<UiState>,
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    let counts = state.repo.count_by_status().await.unwrap_or_default();
    let stats = DashboardStats::from_counts(&counts);
    let jobs = state
        .repo
        .list_jobs_filtered(None, 10, 0)
        .await
        .unwrap_or_default();

    let sys = state.sys_monitor.snapshot().await.to_display();
    let current = state.settings.read().await;

    // Detect FFmpeg installation
    let ffmpeg = crate::ffmpeg::detect().await;

    // Resolve which encoder will actually be used
    let resolved = crate::ffmpeg::resolve_encoder(&current.hw_accel, &ffmpeg);

    // Build setup warnings (now also checks FFmpeg)
    let setup_warnings = build_setup_warnings(&current, &ffmpeg);

    DashboardTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "dashboard".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        stats,
        worker_concurrency: current.worker_concurrency,
        jobs,
        sys,
        setup_warnings,
        active_encoder_label: resolved.label.clone(),
        active_encoder_is_hw: resolved.is_hw,
        ffmpeg,
        auto_scale: current.auto_scale,
        auto_scale_min: current.auto_scale_min,
        auto_scale_max: current.auto_scale_max,
    }
}

/// Check runtime settings and FFmpeg installation to produce warnings.
fn build_setup_warnings(s: &RuntimeSettings, ff: &FfmpegInfo) -> Vec<SetupWarning> {
    // Shield icon path
    const SHIELD: &str = "M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z";
    // Link icon path
    const LINK: &str = "M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71";
    // Key icon path
    const KEY: &str = "M21 2l-2 2m-7.61 7.61a5.5 5.5 0 11-7.778 7.778 5.5 5.5 0 017.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4";
    // Folder icon path
    const FOLDER: &str = "M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z";
    // Alert icon path
    const ALERT: &str = "M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z";
    // Terminal / tool icon path
    const TERMINAL: &str = "M4 17l6-6-6-6m8 14h8";

    let mut w = Vec::new();

    // FFmpeg not found (critical)
    if !ff.ffmpeg_found {
        w.push(SetupWarning {
            icon: TERMINAL,
            title: "FFmpeg is not installed".into(),
            detail: "This machine does not have ffmpeg. Install FFmpeg to enable transcoding. (brew install ffmpeg / apt install ffmpeg)".into(),
        });
    }

    // ffprobe not found (critical)
    if !ff.ffprobe_found {
        w.push(SetupWarning {
            icon: TERMINAL,
            title: "ffprobe is not installed".into(),
            detail: "ffprobe is required for video analysis. It is usually bundled with FFmpeg.".into(),
        });
    }

    // libx264 encoder missing
    if ff.ffmpeg_found && !ff.encoders.iter().any(|e| e == "libx264") {
        w.push(SetupWarning {
            icon: ALERT,
            title: "libx264 encoder not available".into(),
            detail: "FFmpeg was found but lacks the libx264 encoder. Reinstall with --enable-libx264 or use a full build.".into(),
        });
    }

    // API key
    if s.api_key.is_empty() {
        w.push(SetupWarning {
            icon: SHIELD,
            title: "API routes are unprotected".into(),
            detail: "Set an API Key in Settings to require authentication on /transcode, /cancel, and /metrics.".into(),
        });
    }

    // Callback URL
    if s.callback_base_url.is_empty() {
        w.push(SetupWarning {
            icon: LINK,
            title: "No callback base URL configured".into(),
            detail: "Jobs with relative callback paths will be skipped. Set a Callback Base URL in Settings.".into(),
        });
    }

    // Weak admin password
    if s.admin_password == "admin" || s.admin_password == "password" || s.admin_password.len() < 6 {
        w.push(SetupWarning {
            icon: KEY,
            title: "Weak admin password".into(),
            detail: "Change the default admin password in Settings to something stronger.".into(),
        });
    }

    // No renditions enabled
    if !s.renditions.iter().any(|r| r.enabled) {
        w.push(SetupWarning {
            icon: FOLDER,
            title: "No renditions enabled".into(),
            detail: "Enable at least one FFmpeg rendition preset in Settings.".into(),
        });
    }

    // Callback auth not configured when base URL exists
    if !s.callback_base_url.is_empty()
        && s.callback_auth_type == crate::settings::CallbackAuthType::None
    {
        w.push(SetupWarning {
            icon: ALERT,
            title: "Callback requests have no authentication".into(),
            detail: "Consider setting a Bearer Token or API Key for callback authentication in Settings.".into(),
        });
    }

    w
}

// ---------------------------------------------------------------------------
// Jobs list
// ---------------------------------------------------------------------------

const PAGE_SIZE: i64 = 20;

pub async fn page_jobs(
    State(state): State<UiState>,
    Query(q): Query<JobsQuery>,
) -> impl IntoResponse {
    let page = q.page.unwrap_or(1).max(1);
    let offset = (page - 1) * PAGE_SIZE;
    let status_filter = q.status.as_deref();

    // Parse search query as video_id (integer)
    let search_query = q.search.clone().unwrap_or_default().trim().to_string();
    let video_id_filter: Option<i64> = if search_query.is_empty() {
        None
    } else {
        search_query.parse::<i64>().ok()
    };

    // Sorting — default: latest created first
    let sort_by = q.sort.clone().unwrap_or_else(|| "created_at".to_string());
    let sort_dir = q.dir.clone().unwrap_or_else(|| "desc".to_string());

    let total_jobs = state
        .repo
        .count_jobs_search(video_id_filter, status_filter)
        .await
        .unwrap_or(0);
    let total_pages = ((total_jobs + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let jobs = state
        .repo
        .search_jobs(
            video_id_filter,
            status_filter,
            &sort_by,
            &sort_dir,
            PAGE_SIZE,
            offset,
        )
        .await
        .unwrap_or_default();

    let current_status = q.status.clone().unwrap_or_default();
    let status_param = if current_status.is_empty() {
        String::new()
    } else {
        format!("&status={}", current_status)
    };
    let search_param = if search_query.is_empty() {
        String::new()
    } else {
        format!("&search={}", search_query)
    };
    let sort_param = format!("&sort={}&dir={}", sort_by, sort_dir);

    let status_names = [
        "queued",
        "downloading",
        "transcoding",
        "uploading",
        "completed",
        "failed",
        "canceled",
    ];
    let all_statuses = status_names
        .iter()
        .map(|s| StatusTab {
            name: s.to_string(),
            active: current_status == *s,
        })
        .collect();

    let (terminal_completed, terminal_failed, terminal_canceled) =
        state.repo.count_terminal_jobs().await.unwrap_or((0, 0, 0));

    JobsTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "jobs".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        jobs,
        total_jobs,
        current_page: page,
        total_pages,
        current_status,
        status_param,
        search_query,
        search_param,
        sort_by,
        sort_dir,
        sort_param,
        all_statuses,
        terminal_completed,
        terminal_failed,
        terminal_canceled,
    }
}

// ---------------------------------------------------------------------------
// Job detail
// ---------------------------------------------------------------------------

pub async fn page_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
    Query(q): Query<FlashQuery>,
) -> Response {
    let job = match state.repo.get_job(&job_id).await {
        Ok(Some(j)) => j,
        _ => return Redirect::to("/ui/jobs?flash=Job+not+found&flash_type=error").into_response(),
    };
    let raw_events = state.repo.get_job_events(&job.id).await.unwrap_or_default();
    let events: Vec<UiEvent> = raw_events.into_iter().map(UiEvent::from_db).collect();
    let progress = state.progress.get(job.video_id);

    JobDetailTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "jobs".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        job,
        events,
        progress,
    }
    .into_response()
}

// ---------------------------------------------------------------------------
// Submit form
// ---------------------------------------------------------------------------

pub async fn page_submit(
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    SubmitTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "submit".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
    }
}

pub async fn handle_submit(
    State(state): State<UiState>,
    Form(form): Form<SubmitForm>,
) -> Response {
    let count = form.video_id.len();
    if count == 0 {
        return Redirect::to("/ui/submit?flash=No+videos+provided&flash_type=error").into_response();
    }

    let max_attempts = state.settings.max_job_attempts().await;

    let mut created_count = 0u32;
    for i in 0..count {
        let video_id: i64 = match form.video_id[i].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let s3_key = form.s3_key.get(i).map(|s| s.as_str()).unwrap_or("");
        let video_url = form.video_url.get(i).map(|s| s.as_str()).unwrap_or("");
        // callback_url is optional — empty means use the base URL from settings
        let callback_url = form.callback_url.get(i).map(|s| s.as_str()).unwrap_or("");

        if s3_key.is_empty() || video_url.is_empty() {
            continue;
        }

        match state
            .repo
            .insert_or_get_active(video_id, s3_key, video_url, callback_url, max_attempts)
            .await
        {
            Ok(_) => {
                created_count += 1;
            }
            Err(e) => {
                tracing::error!("Failed to create job: {}", e);
            }
        }
    }

    // Wake workers
    state.queue.notify_new_job();

    let msg = format!("{}+job(s)+submitted+successfully", created_count);
    Redirect::to(&format!("/ui?flash={}&flash_type=success", msg)).into_response()
}

// ---------------------------------------------------------------------------
// Cancel from UI
// ---------------------------------------------------------------------------

pub async fn handle_ui_cancel(
    State(state): State<UiState>,
    Path(video_id): Path<i64>,
) -> Response {
    // Signal in-flight worker
    state.cancel_registry.cancel(video_id);

    match state.repo.cancel_job(video_id, Some("Canceled via UI")).await {
        Ok(Some(_)) => {
            Redirect::to("/ui/jobs?flash=Job+canceled&flash_type=success").into_response()
        }
        Ok(None) => {
            Redirect::to("/ui/jobs?flash=No+active+job+found&flash_type=error").into_response()
        }
        Err(e) => {
            tracing::error!("Cancel failed: {}", e);
            Redirect::to("/ui/jobs?flash=Cancel+failed&flash_type=error").into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Admin: Retry / Delete / Purge actions
// ---------------------------------------------------------------------------

/// Retry a single failed job (by job_id).
pub async fn handle_retry_job(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Response {
    match state.repo.admin_retry_job(&job_id).await {
        Ok(true) => {
            state.queue.notify_new_job();
            Redirect::to(&format!(
                "/ui/jobs/{}?flash=Job+re-queued+for+retry&flash_type=success",
                job_id
            ))
            .into_response()
        }
        Ok(false) => Redirect::to(&format!(
            "/ui/jobs/{}?flash=Job+is+not+in+failed+state&flash_type=error",
            job_id
        ))
        .into_response(),
        Err(e) => {
            tracing::error!("Retry failed: {}", e);
            Redirect::to(&format!(
                "/ui/jobs/{}?flash=Retry+failed&flash_type=error",
                job_id
            ))
            .into_response()
        }
    }
}

/// Delete a single job (by job_id).
pub async fn handle_delete_job(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Response {
    match state.repo.admin_delete_job(&job_id).await {
        Ok(true) => {
            Redirect::to("/ui/jobs?flash=Job+deleted&flash_type=success").into_response()
        }
        Ok(false) => {
            Redirect::to("/ui/jobs?flash=Job+not+found&flash_type=error").into_response()
        }
        Err(e) => {
            tracing::error!("Delete job failed: {}", e);
            Redirect::to("/ui/jobs?flash=Delete+failed&flash_type=error").into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct PurgeForm {
    pub action: String,
    /// How many days old the jobs must be (for "purge_older" action).
    pub older_than_days: Option<i64>,
}

/// Bulk purge / cleanup actions.
pub async fn handle_purge(
    State(state): State<UiState>,
    Form(form): Form<PurgeForm>,
) -> Response {
    let result = match form.action.as_str() {
        "purge_completed" => state.repo.admin_purge_by_status("completed").await,
        "purge_failed" => state.repo.admin_purge_by_status("failed").await,
        "purge_canceled" => state.repo.admin_purge_by_status("canceled").await,
        "purge_all_terminal" => state.repo.admin_purge_terminal().await,
        "retry_all_failed" => {
            let res = state.repo.admin_retry_all_failed().await;
            if let Ok(c) = &res {
                if *c > 0 {
                    state.queue.notify_new_job();
                }
            }
            res
        }
        "purge_older" => {
            let days = form.older_than_days.unwrap_or(7).max(1);
            let cutoff = (chrono::Utc::now() - chrono::Duration::days(days)).to_rfc3339();
            state.repo.admin_purge_older_than(&cutoff).await
        }
        _ => {
            return Redirect::to("/ui/jobs?flash=Unknown+action&flash_type=error").into_response();
        }
    };

    match result {
        Ok(count) => {
            let label = form.action.replace('_', "+");
            let msg = format!("{}+job(s)+affected+by+{}", count, label);
            Redirect::to(&format!("/ui/jobs?flash={}&flash_type=success", msg)).into_response()
        }
        Err(e) => {
            tracing::error!("Purge action '{}' failed: {}", form.action, e);
            Redirect::to("/ui/jobs?flash=Action+failed&flash_type=error").into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Settings page
// ---------------------------------------------------------------------------

pub async fn page_settings(
    State(state): State<UiState>,
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    let current = state.settings.read().await;
    let renditions = current.renditions.clone();
    let ffmpeg = crate::ffmpeg::detect().await;

    // Build HW accel options with availability status
    let hw_accel_modes: Vec<HwAccelModeOption> = HwAccelMode::all()
        .into_iter()
        .map(|m| {
            let available = match &m {
                HwAccelMode::Auto | HwAccelMode::Cpu => true,
                HwAccelMode::Videotoolbox => ffmpeg.encoders.contains(&"h264_videotoolbox".to_string()),
                HwAccelMode::Nvenc => ffmpeg.encoders.contains(&"h264_nvenc".to_string()),
                HwAccelMode::Qsv => ffmpeg.encoders.contains(&"h264_qsv".to_string()),
                HwAccelMode::Vaapi => ffmpeg.encoders.contains(&"h264_vaapi".to_string()),
            };
            HwAccelModeOption {
                selected: m == current.hw_accel,
                value: m.as_str().to_string(),
                label: m.label().to_string(),
                available,
            }
        })
        .collect();

    SettingsTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "settings".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        settings: current,
        renditions,
        hw_accel_modes,
        ffmpeg,
    }
}

pub async fn handle_settings(
    State(state): State<UiState>,
    Form(form): Form<SettingsForm>,
) -> Response {
    // Parse comma-separated renditions from hidden field
    let selected_renditions: Vec<String> = form
        .enabled_renditions
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Track the desired worker concurrency so we can scale after settings update.
    let desired_concurrency = form.worker_concurrency.filter(|&wc| wc >= 1 && wc <= 32);

    state
        .settings
        .update(|s| {
            if let Some(url) = form.callback_base_url {
                s.callback_base_url = url;
            }
            if let Some(auth) = form.callback_auth_type {
                s.callback_auth_type = CallbackAuthType::from_str_loose(&auth);
            }
            if let Some(secret) = form.callback_secret {
                s.callback_secret = secret;
            }
            if let Some(max) = form.max_job_attempts {
                if max >= 1 {
                    s.max_job_attempts = max;
                }
            }
            if let Some(wc) = desired_concurrency {
                s.worker_concurrency = wc;
            }
            if let Some(user) = form.admin_username {
                if !user.is_empty() {
                    s.admin_username = user;
                }
            }
            if let Some(pass) = form.admin_password {
                if !pass.is_empty() {
                    s.admin_password = pass;
                }
            }
            // API key — allow setting to empty (disables API auth)
            if let Some(key) = form.api_key {
                s.api_key = key;
            }
            // Output path prefix
            if let Some(prefix) = form.output_path_prefix {
                s.output_path_prefix = prefix.trim_matches('/').to_string();
            }
            // Hardware acceleration mode
            if let Some(hw) = form.hw_accel {
                s.hw_accel = HwAccelMode::from_str_loose(&hw);
            }

            // Delete source after transcode
            s.delete_source_after_transcode = form.delete_source_after_transcode.as_deref() == Some("on");

            // Poll / Pull settings
            s.poll_enabled = form.poll_enabled.as_deref() == Some("on");
            if let Some(ep) = form.poll_endpoint {
                s.poll_endpoint = ep.trim().to_string();
            }
            if let Some(iv) = form.poll_interval_secs {
                s.poll_interval_secs = iv.max(5);
            }
            if let Some(auth) = form.poll_auth_type {
                s.poll_auth_type = CallbackAuthType::from_str_loose(&auth);
            }
            if let Some(secret) = form.poll_auth_secret {
                s.poll_auth_secret = secret;
            }

            // Auto-scale settings
            s.auto_scale = form.auto_scale.as_deref() == Some("on");
            if let Some(cpu) = form.auto_scale_cpu_threshold {
                s.auto_scale_cpu_threshold = cpu.clamp(10.0, 100.0);
            }
            if let Some(mem) = form.auto_scale_mem_threshold {
                s.auto_scale_mem_threshold = mem.clamp(10.0, 100.0);
            }
            if let Some(min) = form.auto_scale_min {
                s.auto_scale_min = min.max(1);
            }
            if let Some(max) = form.auto_scale_max {
                s.auto_scale_max = max.max(1);
            }

            // Update rendition enabled states
            if !selected_renditions.is_empty() {
                for r in &mut s.renditions {
                    r.enabled = selected_renditions.contains(&r.name);
                }
            }
            // Ensure at least one rendition is enabled
            if !s.renditions.iter().any(|r| r.enabled) {
                if let Some(r) = s.renditions.iter_mut().find(|r| r.name == "360p") {
                    r.enabled = true;
                } else if let Some(r) = s.renditions.first_mut() {
                    r.enabled = true;
                }
            }
        })
        .await;

    // Persist settings to DB so they survive restarts
    state.settings.persist(&state.repo).await;

    // Dynamically scale the worker pool if concurrency was changed AND auto-scale is off.
    // When auto-scale is on, the autoscaler task manages concurrency.
    let auto_on = state.settings.auto_scale_enabled().await;
    if !auto_on {
        if let Some(wc) = desired_concurrency {
            WorkerPool::scale_to(&state.pool, wc);
        }
    }

    Redirect::to("/ui/settings?flash=Settings+saved+successfully&flash_type=success")
        .into_response()
}

// ---------------------------------------------------------------------------
// Workers page
// ---------------------------------------------------------------------------

pub async fn page_workers(
    State(state): State<UiState>,
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    let (workers, summary) = state.tracker.display_all();

    WorkersTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "workers".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        workers,
        summary,
    }
}

// ---------------------------------------------------------------------------
// API Docs page
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "api_docs.html")]
struct ApiDocsTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    port: u16,
    api_key: String,
}

pub async fn page_api_docs(
    State(state): State<UiState>,
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    let current = state.settings.read().await;

    ApiDocsTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "api_docs".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        port: 3005,
        api_key: current.api_key.clone(),
    }
}

// ---------------------------------------------------------------------------
// Callback Tester page
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "callback_tester.html")]
struct CallbackTesterTemplate {
    version: String,
    active_page: String,
    flash: Option<String>,
    flash_type: String,
    callback_base_url: String,
    callback_auth_type: String,
    callback_secret: String,
    callback_secret_masked: String,
    output_path_prefix: String,
}

pub async fn page_callback_tester(
    State(state): State<UiState>,
    Query(q): Query<FlashQuery>,
) -> impl IntoResponse {
    let current = state.settings.read().await;

    let secret = current.callback_secret.clone();
    let masked = if secret.is_empty() {
        String::new()
    } else if secret.len() <= 4 {
        "****".to_string()
    } else {
        format!("{}****{}", &secret[..2], &secret[secret.len() - 2..])
    };

    CallbackTesterTemplate {
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_page: "callback_tester".to_string(),
        flash: q.flash,
        flash_type: q.flash_type.unwrap_or_default(),
        callback_base_url: current.callback_base_url.clone(),
        callback_auth_type: format!("{:?}", current.callback_auth_type),
        callback_secret: secret,
        callback_secret_masked: masked,
        output_path_prefix: current.output_path_prefix.clone(),
    }
}

/// Proxy endpoint to send a test callback (avoids CORS issues).
pub async fn handle_callback_test(
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or("");
    let payload = body.get("payload").cloned().unwrap_or(serde_json::Value::Null);

    if url.is_empty() {
        return axum::Json(serde_json::json!({
            "success": false,
            "error": "No URL provided"
        }));
    }

    let client = reqwest::Client::new();
    let start = std::time::Instant::now();

    match client
        .post(url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let elapsed = start.elapsed().as_millis();
            let body_text = resp.text().await.unwrap_or_default();

            axum::Json(serde_json::json!({
                "success": status >= 200 && status < 300,
                "status_code": status,
                "elapsed_ms": elapsed,
                "body": body_text,
            }))
        }
        Err(e) => axum::Json(serde_json::json!({
            "success": false,
            "error": e.to_string(),
            "elapsed_ms": start.elapsed().as_millis(),
        })),
    }
}

// ---------------------------------------------------------------------------
// HTMX partials
// ---------------------------------------------------------------------------

pub async fn partial_stats(State(state): State<UiState>) -> impl IntoResponse {
    let counts = state.repo.count_by_status().await.unwrap_or_default();
    let stats = DashboardStats::from_counts(&counts);
    StatsPartialTemplate { stats }
}

pub async fn partial_jobs(
    State(state): State<UiState>,
    Query(q): Query<PartialJobsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(10);
    let jobs = state
        .repo
        .list_jobs_filtered(None, limit, 0)
        .await
        .unwrap_or_default();
    JobsTablePartialTemplate { jobs }
}

pub async fn partial_system(State(state): State<UiState>) -> impl IntoResponse {
    let sys = state.sys_monitor.snapshot().await.to_display();
    SystemPartialTemplate { sys }
}

pub async fn partial_workers(State(state): State<UiState>) -> impl IntoResponse {
    let (workers, summary) = state.tracker.display_all();
    WorkersPartialTemplate { workers, summary }
}

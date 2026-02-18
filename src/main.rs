mod api;
mod auth;
mod autoscaler;
mod callback;
mod cancellation;
mod config;
mod db;
mod error;
mod ffmpeg;
mod middleware;
mod poller;
mod progress;
mod queue;
mod r2;
mod retry;
mod settings;
mod system;
mod ui;
mod worker;
mod worker_tracker;

use std::sync::Arc;

use axum::middleware as axum_mw;
use axum::routing::{get, post};
use axum::Router;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::api::routes::{self, ApiState};
use crate::auth::SessionStore;
use crate::cancellation::CancellationRegistry;
use crate::config::Config;
use crate::db::repository::JobRepository;
use crate::middleware::ApiAuthState;
use crate::queue::JobQueue;
use crate::r2::R2Client;
use crate::settings::SettingsStore;
use crate::system::SystemMonitor;
use crate::ui::UiState;
use crate::worker::WorkerPool;
use crate::worker_tracker::WorkerTracker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Environment & logging ────────────────────────────────────────────
    dotenvy::dotenv().ok();

    // Use stderr with line-by-line flushing so logs are visible immediately
    // even when output is piped or captured.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "video_transcoder=info,tower_http=info".into()),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_ansi(true)
        .with_writer(|| std::io::LineWriter::new(std::io::stderr()))
        .init();

    let config = Arc::new(Config::from_env()?);

    println!(
        "\n  Video Transcoder Service v{}\n  Listening on {}:{} | workers: {}\n",
        env!("CARGO_PKG_VERSION"),
        config.host,
        config.port,
        config.worker_concurrency,
    );

    info!(
        version = env!("CARGO_PKG_VERSION"),
        host = %config.host,
        port = config.port,
        workers = config.worker_concurrency,
        "Starting Video Transcoder Service"
    );

    // ── SQLite ───────────────────────────────────────────────────────────
    let db_opts = SqliteConnectOptions::from_str(&config.database_url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(30));

    let db_pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(db_opts)
        .await?;

    let repo = Arc::new(JobRepository::new(db_pool));
    repo.migrate().await?;
    info!("Database initialised");

    // ── R2 (S3-compatible) client ────────────────────────────────────────
    let r2_creds = aws_credential_types::Credentials::new(
        &config.r2_access_key_id,
        &config.r2_secret_access_key,
        None,
        None,
        "r2-static",
    );

    let s3_config = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(config.r2_endpoint())
        .credentials_provider(aws_sdk_s3::config::SharedCredentialsProvider::new(r2_creds))
        .build();

    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    let r2 = Arc::new(R2Client::new(
        s3_client,
        config.r2_bucket_name.clone(),
        config.r2_public_url.clone(),
    ));

    // ── Shared primitives ────────────────────────────────────────────────
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;
    let job_notify = Arc::new(Notify::new());
    let cancel_registry = Arc::new(CancellationRegistry::new());
    let shutdown = CancellationToken::new();
    let job_queue = JobQueue::new(Arc::clone(&job_notify));

    // ── Runtime settings ─────────────────────────────────────────────────
    let settings = SettingsStore::from_config(&config);
    settings.load_from_db(&repo).await;
    info!("Settings loaded (DB → env fallback)");

    // ── Worker tracker ───────────────────────────────────────────────────
    let tracker = Arc::new(WorkerTracker::new());

    // ── Progress tracker ─────────────────────────────────────────────────
    let progress_tracker = progress::ProgressTracker::new();

    // Ensure temp dir exists
    tokio::fs::create_dir_all(&config.temp_dir).await?;

    // ── Worker pool ──────────────────────────────────────────────────────
    let pool = Arc::new(WorkerPool::new(
        Arc::clone(&config),
        Arc::clone(&repo),
        Arc::clone(&r2),
        http_client,
        job_queue.clone(),
        Arc::clone(&cancel_registry),
        shutdown.clone(),
        settings.clone(),
        Arc::clone(&tracker),
        progress_tracker.clone(),
    ));
    WorkerPool::spawn(Arc::clone(&pool));

    // ── System monitor ─────────────────────────────────────────────────
    let sys_monitor = Arc::new(SystemMonitor::new());
    sys_monitor.start();

    // ── Auto-scaler ────────────────────────────────────────────────────
    autoscaler::start(
        Arc::clone(&pool),
        settings.clone(),
        Arc::clone(&sys_monitor),
        Arc::clone(&repo),
        Arc::clone(&tracker),
    );

    // ── Poller (pull mode) ──────────────────────────────────────────────
    poller::start(
        Arc::clone(&repo),
        job_queue.clone(),
        settings.clone(),
        shutdown.clone(),
    );

    // ── Session store ────────────────────────────────────────────────────
    let sessions = Arc::new(SessionStore::new());

    // ── HTTP server ──────────────────────────────────────────────────────
    let api_state = ApiState {
        repo: Arc::clone(&repo),
        queue: job_queue.clone(),
        cancel_registry: Arc::clone(&cancel_registry),
        start_time: std::time::Instant::now(),
        sys_monitor: Arc::clone(&sys_monitor),
        settings: settings.clone(),
    };

    let api_auth = ApiAuthState {
        settings: settings.clone(),
    };

    let ui_state = UiState {
        repo,
        queue: job_queue,
        cancel_registry,
        sessions: Arc::clone(&sessions),
        sys_monitor,
        settings,
        tracker,
        pool,
        progress: progress_tracker,
    };

    // -- CORS configuration -----------------------------------------------
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any()) // Tighten in production with specific origins
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderName::from_static("x-api-key"),
            axum::http::HeaderName::from_static("x-request-id"),
        ]);

    // JSON API routes — protected by API key when configured
    let api_routes = Router::new()
        .route("/transcode", post(routes::handle_transcode))
        .route("/transcode/cancel", post(routes::handle_cancel))
        .route(
            "/transcode/status/:video_id",
            get(routes::handle_job_status),
        )
        .route("/metrics", get(routes::handle_metrics))
        .route_layer(axum_mw::from_fn_with_state(
            api_auth,
            middleware::require_api_key,
        ))
        .with_state(api_state.clone());

    // Health & Prometheus — always open (for load balancers / k8s probes / scrapers)
    let health_routes = Router::new()
        .route("/health", get(routes::handle_health))
        .route(
            "/metrics/prometheus",
            get(routes::handle_metrics_prometheus),
        )
        .with_state(api_state);

    // Auth routes (login/logout – no auth middleware)
    let auth_routes = Router::new()
        .route("/auth/login", get(ui::page_login).post(ui::handle_login))
        .route("/auth/logout", post(ui::handle_logout))
        .with_state(ui_state.clone());

    // UI routes (protected by session auth middleware)
    let ui_routes = Router::new()
        .route("/", get(ui::page_dashboard))
        .route("/jobs", get(ui::page_jobs))
        .route("/jobs/purge", post(ui::handle_purge))
        .route("/jobs/:job_id", get(ui::page_job_detail))
        .route("/jobs/:job_id/retry", post(ui::handle_retry_job))
        .route("/jobs/:job_id/delete", post(ui::handle_delete_job))
        .route("/jobs/:video_id/cancel", post(ui::handle_ui_cancel))
        .route("/submit", get(ui::page_submit).post(ui::handle_submit))
        .route(
            "/settings",
            get(ui::page_settings).post(ui::handle_settings),
        )
        .route("/workers", get(ui::page_workers))
        .route("/api-docs", get(ui::page_api_docs))
        .route("/callback-tester", get(ui::page_callback_tester))
        .route(
            "/callback-tester/send",
            post(ui::handle_callback_test),
        )
        .route("/partials/stats", get(ui::partial_stats))
        .route("/partials/jobs", get(ui::partial_jobs))
        .route("/partials/system", get(ui::partial_system))
        .route("/partials/workers", get(ui::partial_workers))
        .route_layer(axum_mw::from_fn_with_state(
            sessions,
            auth::require_auth,
        ))
        .with_state(ui_state);

    let app = Router::new()
        .merge(api_routes)
        .merge(health_routes)
        .merge(auth_routes)
        .nest("/ui", ui_routes)
        // ── Global middleware (outermost → innermost) ─────────────────
        .layer(CompressionLayer::new())               // gzip response compression
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024)) // 10 MB max body
        .layer(cors)
        .layer(axum_mw::from_fn(middleware::security_headers))
        .layer(axum_mw::from_fn(middleware::request_id))
        .layer(TraceLayer::new_for_http());

    let addr = format!("{}:{}", config.host, config.port);
    info!(addr = %addr, "Listening");

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await?;

    info!("Shut down cleanly");
    Ok(())
}

// ---------------------------------------------------------------------------
// Graceful-shutdown signal handler
// ---------------------------------------------------------------------------

async fn shutdown_signal(shutdown: CancellationToken) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Ctrl-C handler failed");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler failed")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl-C"),
        _ = terminate => info!("Received SIGTERM"),
    }

    shutdown.cancel();
}

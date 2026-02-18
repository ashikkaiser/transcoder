use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use uuid::Uuid;

use crate::settings::SettingsStore;

// ---------------------------------------------------------------------------
// Request ID — adds X-Request-Id to every request & response for tracing
// ---------------------------------------------------------------------------

pub async fn request_id(mut req: Request<Body>, next: Next) -> Response {
    let id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    req.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&id).unwrap_or_else(|_| HeaderValue::from_static("unknown")),
    );

    let mut resp = next.run(req).await;

    if let Ok(val) = HeaderValue::from_str(&id) {
        resp.headers_mut().insert("x-request-id", val);
    }

    resp
}

// ---------------------------------------------------------------------------
// Security headers — defence-in-depth HTTP headers
// ---------------------------------------------------------------------------

pub async fn security_headers(req: Request<Body>, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();

    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::X_FRAME_OPTIONS,
        HeaderValue::from_static("DENY"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    h.insert(
        "x-xss-protection",
        HeaderValue::from_static("1; mode=block"),
    );
    h.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    // CSP: allow self + CDN assets (tailwind, htmx) + inline styles for tailwind
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://unpkg.com; \
             style-src 'self' 'unsafe-inline'; \
             img-src 'self' data:; \
             connect-src 'self'; \
             font-src 'self'; \
             frame-ancestors 'none'",
        ),
    );

    resp
}

// ---------------------------------------------------------------------------
// API key authentication — protects machine-to-machine API routes
// ---------------------------------------------------------------------------

/// State for the API auth middleware.
#[derive(Clone)]
pub struct ApiAuthState {
    pub settings: SettingsStore,
}

/// Middleware that checks for a valid API key in the `X-API-Key` header
/// or `Authorization: Bearer <key>`.
///
/// When no `api_key` is configured in settings, the middleware passes through
/// (open access — for development / migration convenience).
pub async fn require_api_key(
    State(auth): State<ApiAuthState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let settings = auth.settings.read().await;
    let configured_key = &settings.api_key;

    // If no API key is configured, allow all requests (dev mode).
    if configured_key.is_empty() {
        drop(settings);
        return next.run(req).await;
    }

    // Check X-API-Key header first
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Fallback: Authorization: Bearer <key>
    let provided = provided.or_else(|| {
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer ").map(String::from))
    });

    match provided {
        Some(key) if key == *configured_key => {
            drop(settings);
            next.run(req).await
        }
        _ => {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid or missing API key" })),
            )
                .into_response()
        }
    }
}

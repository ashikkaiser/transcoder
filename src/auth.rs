use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum_extra::extract::CookieJar;
use chrono::{Duration, Utc};
use dashmap::DashMap;
use uuid::Uuid;

pub const COOKIE_NAME: &str = "transcoder_session";
const SESSION_TTL_HOURS: i64 = 24;

// ---------------------------------------------------------------------------
// In-memory session store backed by DashMap
// ---------------------------------------------------------------------------

pub struct SessionStore {
    tokens: DashMap<String, chrono::DateTime<chrono::Utc>>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
        }
    }

    /// Create a new session token and return it.
    pub fn create(&self) -> String {
        let token = Uuid::new_v4().to_string();
        let expires = Utc::now() + Duration::hours(SESSION_TTL_HOURS);
        self.tokens.insert(token.clone(), expires);
        token
    }

    /// Returns `true` when the token is valid and not expired.
    pub fn validate(&self, token: &str) -> bool {
        if let Some(entry) = self.tokens.get(token) {
            if *entry > Utc::now() {
                return true;
            }
            // Expired – remove lazily
            drop(entry);
            self.tokens.remove(token);
        }
        false
    }

    /// Invalidate a session token.
    pub fn remove(&self, token: &str) {
        self.tokens.remove(token);
    }
}

// ---------------------------------------------------------------------------
// Axum middleware – redirects to /auth/login when session is missing/invalid
// ---------------------------------------------------------------------------

pub async fn require_auth(
    State(sessions): State<Arc<SessionStore>>,
    jar: CookieJar,
    request: Request<Body>,
    next: Next,
) -> Response {
    if let Some(cookie) = jar.get(COOKIE_NAME) {
        if sessions.validate(cookie.value()) {
            return next.run(request).await;
        }
    }
    Redirect::to("/auth/login").into_response()
}

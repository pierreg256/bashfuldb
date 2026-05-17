//! Authentication endpoints: login, refresh, logout.

use axum::{Json, extract::State, http::HeaderMap, http::StatusCode, response::IntoResponse};
use bashfuldb_auth::Credentials;
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState};

// ─── Request / Response types ─────────────────────────────────────────────────

/// Request body for `POST /v1/auth/login`.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// Username.
    pub username: String,
    /// Password.
    pub password: String,
}

/// Request body for `POST /v1/auth/refresh`.
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    /// The refresh token.
    pub refresh_token: String,
}

/// Response body for successful authentication.
#[derive(Debug, Serialize)]
pub struct AuthResponse {
    /// Short-lived access token.
    pub access_token: String,
    /// Long-lived refresh token.
    pub refresh_token: String,
    /// Access token TTL in seconds.
    pub access_ttl_secs: u64,
    /// Refresh token TTL in seconds.
    pub refresh_ttl_secs: u64,
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Extracts the Bearer token from the request's `Authorization` header.
fn extract_bearer_from_headers(headers: &HeaderMap) -> Result<String, ApiError> {
    let hdr = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| ApiError::Unauthorized("missing Authorization header".into()))?;
    let val = hdr
        .to_str()
        .map_err(|_| ApiError::Unauthorized("malformed Authorization header".into()))?;
    val.strip_prefix("Bearer ")
        .map(|t| t.to_string())
        .ok_or_else(|| ApiError::Unauthorized("expected Bearer token".into()))
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

/// `POST /v1/auth/login`
///
/// Validates credentials and returns a token pair.
pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let credentials = Credentials {
        username: body.username,
        password: body.password,
    };
    let tokens = state.authenticator.login(&credentials).await?;
    let resp = AuthResponse {
        access_token: tokens.access_token.clone(),
        refresh_token: tokens.refresh_token.clone(),
        access_ttl_secs: tokens.access_ttl.as_secs(),
        refresh_ttl_secs: tokens.refresh_ttl.as_secs(),
    };
    Ok((StatusCode::OK, Json(resp)))
}

/// `POST /v1/auth/refresh`
///
/// Exchanges a valid refresh token for a new token pair.
pub async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tokens = state
        .authenticator
        .refresh(&body.refresh_token)
        .await
        .map_err(ApiError::from)?;
    let resp = AuthResponse {
        access_token: tokens.access_token.clone(),
        refresh_token: tokens.refresh_token.clone(),
        access_ttl_secs: tokens.access_ttl.as_secs(),
        refresh_ttl_secs: tokens.refresh_ttl.as_secs(),
    };
    Ok((StatusCode::OK, Json(resp)))
}

/// `POST /v1/auth/logout`
///
/// Revokes the provided Bearer token.
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let token = extract_bearer_from_headers(&headers)?;
    state
        .authenticator
        .logout(&token)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

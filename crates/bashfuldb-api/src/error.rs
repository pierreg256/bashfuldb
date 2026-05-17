//! Structured API error types with axum `IntoResponse` implementation.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

/// Structured error body returned in all error responses.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Machine-readable error code (e.g. `"NOT_FOUND"`).
    pub error: String,
    /// Human-readable message.
    pub message: String,
    /// HTTP status code.
    pub status: u16,
}

/// All errors that the API layer can produce.
#[derive(Debug, Error)]
pub enum ApiError {
    /// 400 — request payload or query parameter is invalid.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// 401 — missing or invalid Bearer token.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// 403 — valid token but insufficient permissions.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// 404 — object or resource not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// 409 — version conflict or idempotency-key reuse with different payload.
    #[error("conflict: {0}")]
    Conflict(String),

    /// 412 — `If-Match` precondition failed.
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    /// 429 — rate limit exceeded.
    #[error("too many requests: {0}")]
    TooManyRequests(String),

    /// 504 — quorum timeout / insufficient replicas.
    #[error("gateway timeout: {0}")]
    GatewayTimeout(String),

    /// 500 — unexpected internal error.
    #[error("internal server error: {0}")]
    Internal(String),
}

impl ApiError {
    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            ApiError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED"),
            ApiError::Forbidden(_) => (StatusCode::FORBIDDEN, "FORBIDDEN"),
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
            ApiError::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
            ApiError::PreconditionFailed(_) => {
                (StatusCode::PRECONDITION_FAILED, "PRECONDITION_FAILED")
            }
            ApiError::TooManyRequests(_) => (StatusCode::TOO_MANY_REQUESTS, "TOO_MANY_REQUESTS"),
            ApiError::GatewayTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "GATEWAY_TIMEOUT"),
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();
        let message = self.to_string();
        let body = ErrorBody {
            error: code.to_string(),
            message,
            status: status.as_u16(),
        };
        (status, Json(body)).into_response()
    }
}

impl From<bashfuldb_auth::AuthError> for ApiError {
    fn from(e: bashfuldb_auth::AuthError) -> Self {
        use bashfuldb_auth::AuthError;
        match e {
            AuthError::InvalidCredentials => ApiError::Unauthorized("invalid credentials".into()),
            AuthError::TokenExpired => ApiError::Unauthorized("token expired".into()),
            AuthError::InvalidToken { reason } => {
                ApiError::Unauthorized(format!("invalid token: {reason}"))
            }
            AuthError::TokenRevoked => ApiError::Unauthorized("token revoked".into()),
            AuthError::PermissionDenied { action, resource } => {
                ApiError::Forbidden(format!("permission denied: {action:?} on {resource}"))
            }
            AuthError::UserNotFound { username } => {
                ApiError::NotFound(format!("user not found: {username}"))
            }
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl From<bashfuldb_replication::ReplicationError> for ApiError {
    fn from(e: bashfuldb_replication::ReplicationError) -> Self {
        use bashfuldb_replication::ReplicationError;
        match e {
            ReplicationError::InsufficientReplicas { .. } => {
                ApiError::GatewayTimeout(e.to_string())
            }
            ReplicationError::IdempotencyConflict => {
                ApiError::Conflict("idempotency key reused with different payload".into())
            }
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl From<bashfuldb_document::DocumentError> for ApiError {
    fn from(e: bashfuldb_document::DocumentError) -> Self {
        ApiError::BadRequest(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn error_codes_map_to_correct_status() {
        let cases: &[(ApiError, StatusCode)] = &[
            (ApiError::BadRequest("x".into()), StatusCode::BAD_REQUEST),
            (ApiError::Unauthorized("x".into()), StatusCode::UNAUTHORIZED),
            (ApiError::Forbidden("x".into()), StatusCode::FORBIDDEN),
            (ApiError::NotFound("x".into()), StatusCode::NOT_FOUND),
            (ApiError::Conflict("x".into()), StatusCode::CONFLICT),
            (
                ApiError::PreconditionFailed("x".into()),
                StatusCode::PRECONDITION_FAILED,
            ),
            (
                ApiError::TooManyRequests("x".into()),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                ApiError::GatewayTimeout("x".into()),
                StatusCode::GATEWAY_TIMEOUT,
            ),
            (
                ApiError::Internal("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (err, expected_status) in cases {
            let (status, _) = err.status_and_code();
            assert_eq!(status, *expected_status, "wrong status for {err:?}");
        }
    }
}

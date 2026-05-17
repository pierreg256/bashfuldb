//! Authentication middleware.
//!
//! Extracts the `Authorization: Bearer <token>` header, calls
//! `Authenticator::verify()`, and injects the resulting [`Claims`] into the
//! request extensions so that handlers can retrieve them.

use crate::{ApiError, AppState};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use bashfuldb_auth::Claims;

/// Extractor key for the verified JWT claims stored in request extensions.
#[derive(Clone)]
pub struct AuthenticatedClaims(pub Claims);

/// Bearer-token extraction + verification middleware.
///
/// On success, inserts [`AuthenticatedClaims`] into `req.extensions()`.
/// On failure, returns a 401 Unauthorized response immediately.
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let token = extract_bearer(&req)?;
    let claims = state
        .authenticator
        .verify(&token)
        .await
        .map_err(ApiError::from)?;

    req.extensions_mut().insert(AuthenticatedClaims(claims));
    Ok(next.run(req).await)
}

/// Extracts the Bearer token from the `Authorization` header.
fn extract_bearer(req: &Request) -> Result<String, ApiError> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| ApiError::Unauthorized("missing Authorization header".into()))?;

    let value = header
        .to_str()
        .map_err(|_| ApiError::Unauthorized("malformed Authorization header".into()))?;

    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::Unauthorized("expected Bearer token".into()))?;

    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Request};

    fn req_with_auth(value: &str) -> Request<axum::body::Body> {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(value).unwrap(),
        );
        let mut req = Request::new(axum::body::Body::empty());
        *req.headers_mut() = headers;
        req
    }

    #[test]
    fn missing_header_returns_unauthorized() {
        let req = Request::new(axum::body::Body::empty());
        assert!(matches!(
            extract_bearer(&req),
            Err(ApiError::Unauthorized(_))
        ));
    }

    #[test]
    fn non_bearer_scheme_returns_unauthorized() {
        let req = req_with_auth("Basic dXNlcjpwYXNz");
        assert!(matches!(
            extract_bearer(&req),
            Err(ApiError::Unauthorized(_))
        ));
    }

    #[test]
    fn valid_bearer_extracts_token() {
        let req = req_with_auth("Bearer mytoken123");
        assert_eq!(extract_bearer(&req).unwrap(), "mytoken123");
    }
}

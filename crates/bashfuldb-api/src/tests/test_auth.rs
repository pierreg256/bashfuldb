//! Tests for authentication endpoints.

use super::mocks::test_app;
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn login_success_returns_200_with_tokens() {
    let app = test_app();
    let resp = app
        .post("/v1/auth/login")
        .json(&json!({"username": "user", "password": "password"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert!(body["access_token"].is_string());
    assert!(body["refresh_token"].is_string());
    assert!(body["access_ttl_secs"].is_number());
}

#[tokio::test]
async fn login_bad_credentials_returns_401() {
    let app = test_app();
    let resp = app
        .post("/v1/auth/login")
        .json(&json!({"username": "user", "password": "wrong"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "UNAUTHORIZED");
    assert_eq!(body["status"], 401);
}

#[tokio::test]
async fn login_missing_body_returns_4xx() {
    let app = test_app();
    let resp = app.post("/v1/auth/login").text("not json").await;
    // axum may return 400, 415, or 422 depending on the extractor and content-type
    let status = resp.status_code().as_u16();
    assert!((400..500).contains(&status), "expected 4xx, got {status}");
}

#[tokio::test]
async fn refresh_valid_token_returns_200() {
    let app = test_app();
    let resp = app
        .post("/v1/auth/refresh")
        .json(&json!({"refresh_token": "refresh_user"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert!(body["access_token"].is_string());
}

#[tokio::test]
async fn refresh_invalid_token_returns_401() {
    let app = test_app();
    let resp = app
        .post("/v1/auth/refresh")
        .json(&json!({"refresh_token": "bad_refresh"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logout_with_valid_token_returns_204() {
    let app = test_app();
    let resp = app
        .post("/v1/auth/logout")
        .add_header(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer valid_test_token"),
        )
        .await;
    assert_eq!(resp.status_code(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn logout_without_token_returns_401() {
    let app = test_app();
    let resp = app.post("/v1/auth/logout").await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "UNAUTHORIZED");
}

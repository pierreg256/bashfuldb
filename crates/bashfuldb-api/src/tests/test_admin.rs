//! Tests for admin endpoints.

use super::mocks::test_app;
use axum::http::StatusCode;

fn admin_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer admin_test_token"),
    )
}

fn user_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer valid_test_token"),
    )
}

#[tokio::test]
async fn list_nodes_as_admin_returns_200() {
    let app = test_app();
    let resp = app
        .get("/v1/admin/nodes")
        .add_header(admin_header().0, admin_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert!(body["nodes"].is_array());
}

#[tokio::test]
async fn list_nodes_as_user_returns_403() {
    let app = test_app();
    let resp = app
        .get("/v1/admin/nodes")
        .add_header(user_header().0, user_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "FORBIDDEN");
}

#[tokio::test]
async fn list_nodes_without_auth_returns_401() {
    let app = test_app();
    let resp = app.get("/v1/admin/nodes").await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ring_info_as_admin_returns_200() {
    let app = test_app();
    let resp = app
        .get("/v1/admin/ring")
        .add_header(admin_header().0, admin_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert!(body["vnode_count"].is_number());
    assert!(body["node_count"].is_number());
}

#[tokio::test]
async fn ring_info_as_user_returns_403() {
    let app = test_app();
    let resp = app
        .get("/v1/admin/ring")
        .add_header(user_header().0, user_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
}

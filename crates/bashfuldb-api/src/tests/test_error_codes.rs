//! Tests for all HTTP error codes and query guardrails.

use super::mocks::{MockAuthenticator, MockAuthorizer, MockCoordinator, MockSchemaManager};
use axum::http::StatusCode;
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

const BASE: &str = "/v1/tenants/acme/databases/main/collections/users/objects";

fn auth_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer valid_test_token"),
    )
}

// ─── 400 Bad Request ──────────────────────────────────────────────────────────

#[tokio::test]
async fn invalid_name_in_tenant_returns_400() {
    use super::mocks::test_app;
    let app = test_app();
    // Uppercase tenant name should be rejected
    let resp = app
        .get("/v1/tenants/ACME/databases/main/collections/users/objects")
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "BAD_REQUEST");
    assert_eq!(body["status"], 400);
}

#[tokio::test]
async fn invalid_name_with_dash_returns_400() {
    use super::mocks::test_app;
    let app = test_app();
    let resp = app
        .get("/v1/tenants/acme/databases/my-database/collections/users/objects")
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn non_indexed_filter_field_returns_400() {
    use super::mocks::test_app;
    let app = test_app();
    // "age" is not indexed in the mock schema manager
    let resp = app
        .get(&format!("{BASE}?filter_field=age"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "BAD_REQUEST");
    assert!(body["message"].as_str().unwrap().contains("not indexed"));
}

#[tokio::test]
async fn indexed_filter_field_returns_200() {
    // Build state with an indexed field
    let schema = MockSchemaManager::new().with_index("acme/main/users", "email");
    let state = AppState::new(
        Arc::new(MockAuthenticator),
        Arc::new(MockAuthorizer),
        Arc::new(MockCoordinator::new()),
        Arc::new(schema),
    );
    let app = axum_test::TestServer::new(crate::build_router(state));

    let resp = app
        .get(&format!("{BASE}?filter_field=email"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
}

// ─── 401 Unauthorized ─────────────────────────────────────────────────────────

#[tokio::test]
async fn missing_auth_header_returns_401() {
    use super::mocks::test_app;
    let app = test_app();
    let resp = app.get(BASE).await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "UNAUTHORIZED");
    assert_eq!(body["status"], 401);
}

#[tokio::test]
async fn invalid_token_returns_401() {
    use super::mocks::test_app;
    let app = test_app();
    let resp = app
        .get(BASE)
        .add_header(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer bad_token"),
        )
        .await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
}

// ─── 403 Forbidden ────────────────────────────────────────────────────────────

#[tokio::test]
async fn non_admin_cannot_access_admin_endpoints() {
    use super::mocks::test_app;
    let app = test_app();
    let resp = app
        .get("/v1/admin/nodes")
        .add_header(auth_header().0, auth_header().1) // valid_test_token = ReadWrite
        .await;
    assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "FORBIDDEN");
    assert_eq!(body["status"], 403);
}

// ─── 404 Not Found ────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_nonexistent_object_returns_404() {
    use super::mocks::test_app;
    use bashfuldb_document::ObjectId;
    let app = test_app();
    let id = ObjectId::new();
    let resp = app
        .get(&format!("{BASE}/{id}"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "NOT_FOUND");
    assert_eq!(body["status"], 404);
}

// ─── 409 Conflict ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn idempotency_conflict_returns_409() {
    use super::mocks::test_app;
    let app = test_app();
    let idem_key = "conflict-test-key";

    app.post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&json!({"payload": "first"}))
        .await;

    let resp = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&json!({"payload": "different"}))
        .await;

    assert_eq!(resp.status_code(), StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "CONFLICT");
    assert_eq!(body["status"], 409);
}

// ─── 412 Precondition Failed ──────────────────────────────────────────────────

#[tokio::test]
async fn wrong_etag_on_put_returns_412() {
    use super::mocks::{MockCoordinator, test_app_with_coordinator};
    use bashfuldb_document::{Document, ObjectId, Value};
    use bashfuldb_replication::ObjectKey;
    use std::collections::BTreeMap;

    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    let mut map = BTreeMap::new();
    map.insert("x".into(), Value::String("y".into()));
    let doc = Document::new(id, Value::Object(map)).unwrap();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), doc);
    let app = test_app_with_coordinator(coordinator);

    let resp = app
        .put(&format!("{BASE}/{id}"))
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::IF_MATCH,
            axum::http::HeaderValue::from_static("\"completely_wrong\""),
        )
        .json(&json!({"name": "Updated"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::PRECONDITION_FAILED);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "PRECONDITION_FAILED");
    assert_eq!(body["status"], 412);
}

// ─── Error response structure ─────────────────────────────────────────────────

#[tokio::test]
async fn error_responses_have_required_fields() {
    use super::mocks::test_app;
    let app = test_app();
    let resp = app.get(BASE).await; // no auth → 401
    let body: serde_json::Value = resp.json();
    assert!(body.get("error").is_some(), "missing 'error' field");
    assert!(body.get("message").is_some(), "missing 'message' field");
    assert!(body.get("status").is_some(), "missing 'status' field");
}

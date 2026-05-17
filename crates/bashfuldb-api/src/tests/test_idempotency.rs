//! Tests for idempotency key handling.

use super::mocks::{MockCoordinator, test_app, test_app_with_coordinator};
use axum::http::StatusCode;
use serde_json::json;

const BASE: &str = "/v1/tenants/acme/databases/main/collections/users/objects";

fn auth_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer valid_test_token"),
    )
}

#[tokio::test]
async fn same_idempotency_key_same_payload_returns_cached_response() {
    let coordinator = MockCoordinator::new();
    let app = test_app_with_coordinator(coordinator);
    let payload = json!({"name": "Idempotent Alice"});
    let idem_key = "test-key-001";

    // First request
    let resp1 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&payload)
        .await;
    assert_eq!(resp1.status_code(), StatusCode::CREATED);
    let body1: serde_json::Value = resp1.json();
    let id1 = body1["_id"].as_str().unwrap().to_string();

    // Second request — same key + same payload → cached 201
    let resp2 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&payload)
        .await;
    assert_eq!(resp2.status_code(), StatusCode::CREATED);
    let body2: serde_json::Value = resp2.json();
    // The _id must be the same as the first response
    assert_eq!(body2["_id"].as_str().unwrap(), id1);
}

#[tokio::test]
async fn same_idempotency_key_different_payload_returns_409() {
    let coordinator = MockCoordinator::new();
    let app = test_app_with_coordinator(coordinator);
    let idem_key = "test-key-conflict";

    // First request
    let resp1 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&json!({"name": "First payload"}))
        .await;
    assert_eq!(resp1.status_code(), StatusCode::CREATED);

    // Second request — same key, DIFFERENT payload → 409 Conflict
    let resp2 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&json!({"name": "Different payload!"}))
        .await;
    assert_eq!(resp2.status_code(), StatusCode::CONFLICT);
    let body: serde_json::Value = resp2.json();
    assert_eq!(body["error"], "CONFLICT");
    assert_eq!(body["status"], 409);
}

#[tokio::test]
async fn requests_without_idempotency_key_are_not_cached() {
    let app = test_app();

    // Two requests without idempotency key should both create new objects
    let resp1 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .json(&json!({"name": "Object One"}))
        .await;
    assert_eq!(resp1.status_code(), StatusCode::CREATED);
    let id1 = resp1.json::<serde_json::Value>()["_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp2 = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .json(&json!({"name": "Object Two"}))
        .await;
    assert_eq!(resp2.status_code(), StatusCode::CREATED);
    let id2 = resp2.json::<serde_json::Value>()["_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Different objects should have different IDs
    assert_ne!(id1, id2);
}

#[tokio::test]
async fn put_idempotency_same_key_returns_cached() {
    use bashfuldb_document::ObjectId;
    let app = test_app();
    let id = ObjectId::new();
    let url = format!("{BASE}/{id}");
    let idem_key = "put-key-001";
    let payload = json!({"name": "Idempotent Bob"});

    // First PUT
    let resp1 = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&payload)
        .await;
    assert_eq!(resp1.status_code(), StatusCode::CREATED);

    // Second PUT — same key + same payload → cached
    let resp2 = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::HeaderName::from_static("idempotency-key"),
            axum::http::HeaderValue::from_static(idem_key),
        )
        .json(&payload)
        .await;
    assert_eq!(resp2.status_code(), StatusCode::CREATED);
}

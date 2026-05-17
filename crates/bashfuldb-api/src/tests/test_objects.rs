//! Tests for document CRUD endpoints.

use super::mocks::{MockCoordinator, test_app, test_app_with_coordinator};
use axum::http::StatusCode;
use bashfuldb_document::{Document, ObjectId, Value};
use bashfuldb_replication::ObjectKey;
use serde_json::json;
use std::collections::BTreeMap;

const BASE: &str = "/v1/tenants/acme/databases/main/collections/users/objects";

fn auth_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer valid_test_token"),
    )
}

fn make_doc(id: ObjectId) -> Document {
    let mut map = BTreeMap::new();
    map.insert("name".into(), Value::String("Alice".into()));
    Document::new(id, Value::Object(map)).unwrap()
}

// ─── List ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_objects_returns_200_empty() {
    let app = test_app();
    let resp = app
        .get(BASE)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert!(body["items"].is_array());
    assert!(body["pagination"].is_object());
}

#[tokio::test]
async fn list_objects_requires_auth() {
    let app = test_app();
    let resp = app.get(BASE).await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn list_objects_invalid_name_returns_400() {
    let app = test_app();
    let url = "/v1/tenants/INVALID/databases/main/collections/users/objects";
    let resp = app
        .get(url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

// ─── Create ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_object_returns_201_with_location() {
    let app = test_app();
    let resp = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .json(&json!({"name": "Alice", "age": 30}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json();
    assert!(body["_id"].is_string());
    // Location header should be present
    assert!(resp.headers().get("location").is_some());
}

#[tokio::test]
async fn create_object_requires_auth() {
    let app = test_app();
    let resp = app.post(BASE).json(&json!({"name": "Alice"})).await;
    assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn create_object_invalid_json_returns_400() {
    let app = test_app();
    let resp = app
        .post(BASE)
        .add_header(auth_header().0, auth_header().1)
        .text("not json {{{")
        .add_header(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        )
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

// ─── Get ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_existing_object_returns_200() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    let doc = make_doc(id);
    coordinator.seed(&ObjectKey::new("acme/main/users", id), doc);
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .get(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["_id"], id.to_string());
}

#[tokio::test]
async fn get_missing_object_returns_404() {
    let app = test_app();
    let id = ObjectId::new();
    let url = format!("{BASE}/{id}");
    let resp = app
        .get(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "NOT_FOUND");
    assert_eq!(body["status"], 404);
}

#[tokio::test]
async fn get_object_invalid_id_returns_400() {
    let app = test_app();
    let url = format!("{BASE}/not-a-uuid");
    let resp = app
        .get(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_object_returns_etag_header() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .get(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    assert!(resp.headers().get("etag").is_some());
}

// ─── Upsert ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn upsert_new_object_returns_201() {
    let app = test_app();
    let id = ObjectId::new();
    let url = format!("{BASE}/{id}");
    let resp = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .json(&json!({"name": "Bob"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::CREATED);
}

#[tokio::test]
async fn upsert_existing_object_returns_200() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .json(&json!({"name": "Updated"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn upsert_with_matching_if_match_succeeds() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");

    // First: get the ETag
    let get_resp = app
        .get(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    let etag = get_resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Then PUT with the correct ETag
    let resp = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::IF_MATCH,
            axum::http::HeaderValue::from_str(&etag).unwrap(),
        )
        .json(&json!({"name": "Updated with ETag"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn upsert_with_wrong_if_match_returns_412() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .put(&url)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::IF_MATCH,
            axum::http::HeaderValue::from_static("\"wrong_etag_value\""),
        )
        .json(&json!({"name": "Will fail"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::PRECONDITION_FAILED);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "PRECONDITION_FAILED");
    assert_eq!(body["status"], 412);
}

// ─── Delete ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn delete_existing_object_returns_204() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .delete(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn delete_missing_object_returns_404() {
    let app = test_app();
    let id = ObjectId::new();
    let url = format!("{BASE}/{id}");
    let resp = app
        .delete(&url)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_with_wrong_if_match_returns_412() {
    let coordinator = MockCoordinator::new();
    let id = ObjectId::new();
    coordinator.seed(&ObjectKey::new("acme/main/users", id), make_doc(id));
    let app = test_app_with_coordinator(coordinator);
    let url = format!("{BASE}/{id}");
    let resp = app
        .delete(&url)
        .add_header(auth_header().0, auth_header().1)
        .add_header(
            axum::http::header::IF_MATCH,
            axum::http::HeaderValue::from_static("\"wrong_etag\""),
        )
        .await;
    assert_eq!(resp.status_code(), StatusCode::PRECONDITION_FAILED);
}

// ─── RBAC ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn read_only_user_cannot_create() {
    // Create state with ReadOnly claims
    use crate::AppState;
    use crate::tests::mocks::{MockAuthorizer, MockCoordinator, MockSchemaManager};
    use bashfuldb_auth::{Claims, Role};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct ReadOnlyAuthenticator;
    #[async_trait::async_trait]
    impl bashfuldb_auth::Authenticator for ReadOnlyAuthenticator {
        async fn login(
            &self,
            _: &bashfuldb_auth::Credentials,
        ) -> bashfuldb_auth::Result<bashfuldb_auth::TokenPair> {
            Ok(bashfuldb_auth::TokenPair {
                access_token: "readonly_token".into(),
                refresh_token: "readonly_refresh".into(),
                access_ttl: Duration::from_secs(3600),
                refresh_ttl: Duration::from_secs(172800),
            })
        }
        async fn refresh(&self, _: &str) -> bashfuldb_auth::Result<bashfuldb_auth::TokenPair> {
            Err(bashfuldb_auth::AuthError::InvalidCredentials)
        }
        async fn verify(&self, _: &str) -> bashfuldb_auth::Result<Claims> {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            Ok(Claims::new(
                "readonly1".into(),
                Some("acme".into()),
                vec![Role::ReadOnly],
                now,
                now + 3600,
            ))
        }
        async fn logout(&self, _: &str) -> bashfuldb_auth::Result<()> {
            Ok(())
        }
    }

    let state = AppState::new(
        Arc::new(ReadOnlyAuthenticator),
        Arc::new(MockAuthorizer),
        Arc::new(MockCoordinator::new()),
        Arc::new(MockSchemaManager::new()),
    );
    let app = axum_test::TestServer::new(crate::build_router(state));

    let resp = app
        .post(BASE)
        .add_header(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer readonly_token"),
        )
        .json(&json!({"name": "Alice"}))
        .await;
    assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json();
    assert_eq!(body["error"], "FORBIDDEN");
}

//! Tests for cursor-based pagination.

use super::mocks::test_app;
use axum::http::StatusCode;
use bashfuldb_document::ObjectId;

const BASE: &str = "/v1/tenants/acme/databases/main/collections/users/objects";

fn auth_header() -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_static("Bearer valid_test_token"),
    )
}

#[tokio::test]
async fn list_returns_pagination_object() {
    let app = test_app();
    let resp = app
        .get(BASE)
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: serde_json::Value = resp.json();
    let pagination = &body["pagination"];
    assert!(pagination.is_object());
    assert!(pagination["count"].is_number());
    // next_cursor is null when no items
    assert!(pagination["next_cursor"].is_null());
}

#[tokio::test]
async fn list_with_invalid_cursor_returns_400() {
    let app = test_app();
    let resp = app
        .get(&format!("{BASE}?cursor=!!!invalid!!!"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_with_valid_cursor_accepted() {
    use crate::pagination::encode_cursor;
    let app = test_app();
    let id = ObjectId::new();
    let cursor = encode_cursor(id);
    let resp = app
        .get(&format!("{BASE}?cursor={cursor}"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    // Cursor is valid (200 even if no results)
    assert_eq!(resp.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn list_with_limit_parameter_accepted() {
    let app = test_app();
    let resp = app
        .get(&format!("{BASE}?limit=10"))
        .add_header(auth_header().0, auth_header().1)
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn pagination_unit_empty_list() {
    use crate::pagination::paginate_ids;
    let (page, next) = paginate_ids(&[], None, 10);
    assert!(page.is_empty());
    assert!(next.is_none());
}

#[tokio::test]
async fn pagination_unit_single_page() {
    use crate::pagination::paginate_ids;
    let ids: Vec<ObjectId> = (0..5).map(|_| ObjectId::new()).collect();
    let (page, next) = paginate_ids(&ids, None, 10);
    assert_eq!(page.len(), 5);
    assert!(next.is_none());
}

#[tokio::test]
async fn pagination_unit_multi_page() {
    use crate::pagination::paginate_ids;
    let ids: Vec<ObjectId> = (0..10).map(|_| ObjectId::new()).collect();
    // First page
    let (page1, cursor1) = paginate_ids(&ids, None, 3);
    assert_eq!(page1.len(), 3);
    assert!(cursor1.is_some());

    // Decode and fetch next page
    use crate::pagination::decode_cursor;
    let after = decode_cursor(&cursor1.unwrap()).unwrap();
    let (page2, cursor2) = paginate_ids(&ids, Some(after), 3);
    assert_eq!(page2.len(), 3);
    assert!(cursor2.is_some());
    // Pages should not overlap
    for id in &page1 {
        assert!(!page2.contains(id));
    }
}

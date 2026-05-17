//! Axum router factory.
//!
//! Wires up all routes and applies the authentication middleware to the
//! protected route groups.

use axum::{
    Router, middleware,
    routing::{delete, get, post, put},
};

use crate::{
    AppState,
    handlers::{admin, auth, objects},
    middleware::auth_middleware,
};

/// Builds the complete axum [`Router`] for the BashfulDB HTTP API.
///
/// # Route groups
///
/// ## Auth (unauthenticated)
/// - `POST /v1/auth/login`
/// - `POST /v1/auth/refresh`
/// - `POST /v1/auth/logout`
///
/// ## Objects (requires Bearer token)
/// - `GET    /v1/tenants/{t}/databases/{d}/collections/{c}/objects`
/// - `POST   /v1/tenants/{t}/databases/{d}/collections/{c}/objects`
/// - `GET    /v1/tenants/{t}/databases/{d}/collections/{c}/objects/{id}`
/// - `PUT    /v1/tenants/{t}/databases/{d}/collections/{c}/objects/{id}`
/// - `DELETE /v1/tenants/{t}/databases/{d}/collections/{c}/objects/{id}`
///
/// ## Admin (requires Bearer token + ManageCluster permission)
/// - `GET /v1/admin/nodes`
/// - `GET /v1/admin/ring`
pub fn build_router(state: AppState) -> Router {
    // ── Auth routes (no middleware required) ──────────────────────────────────
    let auth_routes = Router::new()
        .route("/v1/auth/login", post(auth::login))
        .route("/v1/auth/refresh", post(auth::refresh))
        .route("/v1/auth/logout", post(auth::logout));

    // ── Object routes (protected by auth middleware) ──────────────────────────
    let object_base = "/v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects";
    let object_id = "/v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects/{id}";

    let object_routes = Router::new()
        .route(object_base, get(objects::list_objects))
        .route(object_base, post(objects::create_object))
        .route(object_id, get(objects::get_object))
        .route(object_id, put(objects::upsert_object))
        .route(object_id, delete(objects::delete_object))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // ── Admin routes (protected by auth middleware) ───────────────────────────
    let admin_routes = Router::new()
        .route("/v1/admin/nodes", get(admin::list_nodes))
        .route("/v1/admin/ring", get(admin::ring_info))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // ── Merge all route groups ────────────────────────────────────────────────
    Router::new()
        .merge(auth_routes)
        .merge(object_routes)
        .merge(admin_routes)
        .with_state(state)
}

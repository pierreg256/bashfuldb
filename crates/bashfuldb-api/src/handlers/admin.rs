//! Admin endpoints: node listing and ring topology.
//!
//! These endpoints require `ManageCluster` permission (ServerAdmin role).

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use bashfuldb_auth::{Action, Resource};
use serde::Serialize;

use crate::{ApiError, AppState, middleware::AuthenticatedClaims};

/// Response body for `GET /v1/admin/nodes`.
#[derive(Debug, Serialize)]
pub struct NodesResponse {
    /// List of known cluster nodes.
    pub nodes: Vec<NodeInfo>,
}

/// Compact node information returned by the admin API.
#[derive(Debug, Serialize)]
pub struct NodeInfo {
    /// Node ID (UUID).
    pub id: String,
    /// Number of virtual nodes owned.
    pub vnode_count: usize,
    /// Lifecycle state string.
    pub state: String,
}

/// Response body for `GET /v1/admin/ring`.
#[derive(Debug, Serialize)]
pub struct RingResponse {
    /// Total number of virtual nodes on the ring.
    pub vnode_count: usize,
    /// Number of physical nodes.
    pub node_count: usize,
}

/// `GET /v1/admin/nodes`
pub async fn list_nodes(
    State(state): State<AppState>,
    claims_ext: axum::Extension<AuthenticatedClaims>,
) -> Result<impl IntoResponse, ApiError> {
    // Only ServerAdmin may query cluster state.
    state
        .authorizer
        .check(&claims_ext.0.0, Action::ManageCluster, &Resource::cluster())
        .await
        .map_err(ApiError::from)?;

    // In a full deployment the AppState would hold a Membership handle.
    // For now we return an empty list so the endpoint is exercisable and
    // testable.  Deployers wire in real membership via a richer AppState.
    let resp = NodesResponse { nodes: vec![] };
    Ok((StatusCode::OK, Json(resp)))
}

/// `GET /v1/admin/ring`
pub async fn ring_info(
    State(state): State<AppState>,
    claims_ext: axum::Extension<AuthenticatedClaims>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .authorizer
        .check(&claims_ext.0.0, Action::ManageCluster, &Resource::cluster())
        .await
        .map_err(ApiError::from)?;

    let resp = RingResponse {
        vnode_count: 0,
        node_count: 0,
    };
    Ok((StatusCode::OK, Json(resp)))
}

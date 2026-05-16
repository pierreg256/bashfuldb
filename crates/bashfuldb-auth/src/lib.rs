//! Authentication (JWT, Argon2id) and Role-Based Access Control.
//!
//! This crate handles user authentication, token lifecycle, and
//! authorization checks. It is agnostic to the underlying storage —
//! users/roles are persisted via the [`StorageEngine`](bashfuldb_storage::StorageEngine) trait.
//!
//! # Key traits
//!
//! - [`Authenticator`] — Login, token refresh, verification, logout.
//! - [`Authorizer`] — RBAC permission checks.
//!
//! # Key types
//!
//! - [`Role`], [`Action`], [`Resource`] — RBAC model.
//! - [`Claims`] — JWT payload.
//! - [`TokenPair`] — Access + refresh tokens.

mod authn;
mod authz;
mod error;
mod rbac;
mod traits;
mod types;

pub use authn::{AuthConfig, AuthService, JwtMode, UserCreateRequest};
pub use authz::RbacAuthorizer;
pub use error::AuthError;
pub use rbac::{Action, Resource, Role};
pub use traits::{Authenticator, Authorizer};
pub use types::{
    Claims, Credentials, TokenPair, DEFAULT_ACCESS_TTL, DEFAULT_REFRESH_TTL, BOOTSTRAP_ADMIN_USER,
};

/// Result type for auth operations.
pub type Result<T> = std::result::Result<T, AuthError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_are_usable() {
        let _ = DEFAULT_ACCESS_TTL;
        let _ = DEFAULT_REFRESH_TTL;
        let _ = BOOTSTRAP_ADMIN_USER;
        let _ = Resource::cluster();
    }
}

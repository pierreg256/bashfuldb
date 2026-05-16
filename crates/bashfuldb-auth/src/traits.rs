use async_trait::async_trait;
use crate::{Action, Claims, Credentials, Resource, Result, TokenPair};

/// Authentication: login, token lifecycle, verification.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates a user and returns a token pair.
    async fn login(&self, credentials: &Credentials) -> Result<TokenPair>;

    /// Refreshes an access token using a valid refresh token.
    async fn refresh(&self, refresh_token: &str) -> Result<TokenPair>;

    /// Verifies an access token and returns its claims.
    async fn verify(&self, access_token: &str) -> Result<Claims>;

    /// Revokes a token (logout).
    async fn logout(&self, access_token: &str) -> Result<()>;
}

/// Authorization: RBAC permission checks.
///
/// There is **no automatic scope inheritance** — a `TenantAdmin` on
/// tenant A has zero access to tenant B.
#[async_trait]
pub trait Authorizer: Send + Sync + 'static {
    /// Checks if the claims permit the action on the resource.
    ///
    /// Returns `Ok(())` if permitted, or
    /// [`AuthError::PermissionDenied`](crate::AuthError::PermissionDenied).
    async fn check(&self, claims: &Claims, action: Action, resource: &Resource) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn trait_object_bounds_compile() {
        assert_send_sync_static::<Box<dyn Authenticator>>();
        assert_send_sync_static::<Box<dyn Authorizer>>();
    }
}

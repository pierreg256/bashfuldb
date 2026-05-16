use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Login credentials.
///
/// The `Debug` implementation redacts the password field.
#[derive(Clone)]
pub struct Credentials {
    /// Username.
    pub username: String,
    /// Password (plaintext — will be hashed internally).
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// A pair of access and refresh tokens returned on login/refresh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPair {
    /// The short-lived access token (default TTL: 1 hour).
    pub access_token: String,
    /// The longer-lived refresh token (default TTL: 48 hours).
    pub refresh_token: String,
    /// Access token time-to-live.
    pub access_ttl: Duration,
    /// Refresh token time-to-live.
    pub refresh_ttl: Duration,
}

/// JWT claims payload.
///
/// Fields are private to prevent mutation after verification. Use the
/// accessor methods to read claim values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    sub: String,
    tenant: Option<String>,
    roles: Vec<crate::Role>,
    iat: u64,
    exp: u64,
}

impl Claims {
    /// Creates a new Claims value.
    pub fn new(
        sub: String,
        tenant: Option<String>,
        roles: Vec<crate::Role>,
        iat: u64,
        exp: u64,
    ) -> Self {
        Self { sub, tenant, roles, iat, exp }
    }

    /// Returns the subject (user ID).
    pub fn sub(&self) -> &str {
        &self.sub
    }

    /// Returns the tenant, or `None` for cluster-wide users.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Returns the assigned roles.
    pub fn roles(&self) -> &[crate::Role] {
        &self.roles
    }

    /// Returns the issued-at timestamp (seconds since UNIX epoch).
    pub fn iat(&self) -> u64 {
        self.iat
    }

    /// Returns the expiration timestamp (seconds since UNIX epoch).
    pub fn exp(&self) -> u64 {
        self.exp
    }

    /// Returns true if the token has expired given the current timestamp.
    pub fn is_expired(&self, now_secs: u64) -> bool {
        now_secs >= self.exp
    }

    /// Returns the highest-privilege role in this claim set.
    pub fn highest_role(&self) -> Option<&crate::Role> {
        self.roles.iter().min_by_key(|r| match r {
            crate::Role::ServerAdmin => 0,
            crate::Role::TenantAdmin => 1,
            crate::Role::DatabaseAdmin => 2,
            crate::Role::ReadWrite => 3,
            crate::Role::ReadOnly => 4,
        })
    }
}

/// Default access token TTL: 1 hour.
pub const DEFAULT_ACCESS_TTL: Duration = Duration::from_secs(3_600);

/// Default refresh token TTL: 48 hours.
pub const DEFAULT_REFRESH_TTL: Duration = Duration::from_secs(172_800);

/// The bootstrap admin username.
pub const BOOTSTRAP_ADMIN_USER: &str = "_system__admin";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Role;

    #[test]
    fn claims_expiration() {
        let claims = Claims::new(
            "user1".into(),
            Some("acme".into()),
            vec![Role::ReadWrite],
            1000,
            2000,
        );
        assert!(!claims.is_expired(1500));
        assert!(claims.is_expired(2000));
        assert!(claims.is_expired(3000));
    }

    #[test]
    fn highest_role() {
        let claims = Claims::new(
            "user1".into(),
            None,
            vec![Role::ReadOnly, Role::ServerAdmin, Role::ReadWrite],
            0,
            0,
        );
        assert_eq!(claims.highest_role(), Some(&Role::ServerAdmin));
    }

    #[test]
    fn claims_accessors() {
        let claims = Claims::new(
            "alice".into(),
            Some("acme".into()),
            vec![Role::ReadWrite],
            100,
            200,
        );
        assert_eq!(claims.sub(), "alice");
        assert_eq!(claims.tenant(), Some("acme"));
        assert_eq!(claims.roles(), &[Role::ReadWrite]);
        assert_eq!(claims.iat(), 100);
        assert_eq!(claims.exp(), 200);
    }

    #[test]
    fn credentials_debug_redacts_password() {
        let creds = Credentials {
            username: "alice".into(),
            password: "secret123".into(),
        };
        let debug = format!("{:?}", creds);
        assert!(debug.contains("alice"));
        assert!(!debug.contains("secret123"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn default_ttls() {
        assert_eq!(DEFAULT_ACCESS_TTL.as_secs(), 3_600);
        assert_eq!(DEFAULT_REFRESH_TTL.as_secs(), 172_800);
    }
}

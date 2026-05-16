use thiserror::Error;

/// Errors that can occur in authentication and authorization.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Invalid credentials (wrong username or password).
    #[error("invalid credentials")]
    InvalidCredentials,

    /// The token has expired.
    #[error("token expired")]
    TokenExpired,

    /// The token is malformed or has an invalid signature.
    #[error("invalid token: {reason}")]
    InvalidToken { reason: String },

    /// The token has been revoked (logged out).
    #[error("token revoked")]
    TokenRevoked,

    /// The user does not have permission to perform this action.
    #[error("permission denied: {action:?} on {resource}")]
    PermissionDenied {
        action: crate::Action,
        resource: String,
    },

    /// The user was not found.
    #[error("user not found: {username:?}")]
    UserNotFound { username: String },

    /// The user already exists.
    #[error("user already exists: {username:?}")]
    UserAlreadyExists { username: String },

    /// Password does not meet requirements.
    #[error("password too weak: {reason}")]
    WeakPassword { reason: String },

    /// An internal error in the auth subsystem.
    #[error("auth internal error: {message}")]
    Internal { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_stable() {
        assert_eq!(AuthError::InvalidCredentials.to_string(), "invalid credentials");
        assert_eq!(AuthError::TokenExpired.to_string(), "token expired");
    }
}

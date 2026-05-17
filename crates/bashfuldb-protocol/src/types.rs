//! Core protocol types: [`Command`], [`Response`], [`ConnectionContext`],
//! [`ServerConfig`], and [`TlsConfig`].

use std::net::SocketAddr;

use bytes::Bytes;

/// Protocol version string sent in the server banner.
pub const PROTOCOL_VERSION: &str = "4.0.0";

/// Server banner sent immediately after a new TCP connection is accepted.
pub const SERVER_BANNER: &str = "+BASHFULDB 4.0.0\r\n";

/// Default TCP port the server binds to.
pub const DEFAULT_PORT: u16 = 6380;

/// Maximum number of simultaneous connections (default).
pub const DEFAULT_MAX_CONNECTIONS: usize = 1_024;

/// Maximum line length in bytes (1 MiB).
pub const DEFAULT_MAX_LINE_LENGTH: usize = 1_048_576;

/// Maximum bulk-string length in bytes (16 MiB).
pub const DEFAULT_MAX_BULK_LENGTH: usize = 16_777_216;

/// Maximum number of consecutive protocol errors before the server drops a
/// connection.
pub const MAX_CONSECUTIVE_ERRORS: u32 = 10;

/// Maximum RESP array nesting depth.
pub const MAX_NESTING_DEPTH: usize = 8;

// ─── Command ─────────────────────────────────────────────────────────────────

/// A parsed client command.
///
/// Commands arrive as RESP arrays where the first element is the command
/// name (upper-cased by the parser) and the remaining elements are the
/// arguments.
#[derive(Debug, Clone)]
pub struct Command {
    /// Command name in upper-case ASCII (e.g. `"GET"`, `"AUTH"`).
    pub name: String,
    /// Raw argument bytes.
    pub args: Vec<Bytes>,
}

// ─── Response ────────────────────────────────────────────────────────────────

/// A value the server sends back to the client.
///
/// Serialised via [`crate::serializer::serialize_response`].
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// `+<message>\r\n`
    Simple(String),
    /// `-<message>\r\n`
    Error(String),
    /// `:<n>\r\n`
    Integer(i64),
    /// `$<len>\r\n<data>\r\n` or `$-1\r\n` for null.
    Bulk(Option<Bytes>),
    /// `*<count>\r\n` followed by each element.
    Array(Vec<Response>),
    /// `+_warning <message>\r\n` — non-fatal advisory sent inline.
    Warning(String),
}

// ─── ConnectionContext ────────────────────────────────────────────────────────

/// Per-connection state passed to every [`crate::handler::CommandHandler`] call.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// Remote peer address.
    pub peer_addr: SocketAddr,
    /// Monotonically increasing identifier assigned by the server.
    pub conn_id: u64,
    /// JWT claims, set after a successful AUTH handshake.
    pub claims: Option<bashfuldb_auth::Claims>,
}

// ─── TLS configuration ───────────────────────────────────────────────────────

/// TLS configuration for the TCP server.
///
/// Requires the `tls` feature to have any effect.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded certificate chain.  Ignored when `dev_mode` is `true`.
    pub cert_pem: String,
    /// PEM-encoded private key.  Ignored when `dev_mode` is `true`.
    pub key_pem: String,
    /// When `true`, a self-signed certificate is generated automatically.
    /// Suitable for development only.
    pub dev_mode: bool,
}

impl TlsConfig {
    /// Convenience constructor for development mode (auto-generated self-signed
    /// certificate).
    pub fn dev() -> Self {
        Self {
            cert_pem: String::new(),
            key_pem: String::new(),
            dev_mode: true,
        }
    }
}

// ─── ServerConfig ─────────────────────────────────────────────────────────────

/// Configuration for the BashfulDB TCP/TLS server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address the server listens on.  Default: `0.0.0.0:6380`.
    pub bind_addr: SocketAddr,
    /// Maximum number of simultaneous connections.  Default: `1_024`.
    pub max_connections: usize,
    /// Optional TLS configuration.  When `None` the server accepts plain TCP.
    pub tls: Option<TlsConfig>,
    /// Maximum byte length of a single RESP line (excluding CRLF).  Default: 1 MiB.
    pub max_line_length: usize,
    /// Maximum byte length of a bulk string.  Default: 16 MiB.
    pub max_bulk_length: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_PORT)),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            tls: None,
            max_line_length: DEFAULT_MAX_LINE_LENGTH,
            max_bulk_length: DEFAULT_MAX_BULK_LENGTH,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.bind_addr.port(), DEFAULT_PORT);
        assert_eq!(cfg.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(cfg.max_line_length, DEFAULT_MAX_LINE_LENGTH);
        assert_eq!(cfg.max_bulk_length, DEFAULT_MAX_BULK_LENGTH);
        assert!(cfg.tls.is_none());
    }

    #[test]
    fn tls_config_dev() {
        let tls = TlsConfig::dev();
        assert!(tls.dev_mode);
    }

    #[test]
    fn constants_match_spec() {
        assert_eq!(PROTOCOL_VERSION, "4.0.0");
        assert_eq!(DEFAULT_PORT, 6380);
        assert_eq!(DEFAULT_MAX_CONNECTIONS, 1_024);
        assert_eq!(DEFAULT_MAX_LINE_LENGTH, 1_048_576);
        assert_eq!(DEFAULT_MAX_BULK_LENGTH, 16_777_216);
        assert_eq!(MAX_CONSECUTIVE_ERRORS, 10);
    }

    #[test]
    fn response_equality() {
        assert_eq!(
            Response::Simple("OK".to_string()),
            Response::Simple("OK".to_string())
        );
        assert_eq!(Response::Integer(42), Response::Integer(42));
        assert_ne!(Response::Integer(1), Response::Integer(2));
    }

    #[test]
    fn command_fields() {
        let cmd = Command {
            name: "PING".to_string(),
            args: vec![Bytes::from("hello")],
        };
        assert_eq!(cmd.name, "PING");
        assert_eq!(cmd.args.len(), 1);
    }
}

//! Protocol-level errors for BashfulDB.

use thiserror::Error;

/// All errors that can arise while parsing or serving the RESP-like protocol.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// A single line exceeded the maximum allowed length.
    #[error("line too long: {len} bytes exceeds max {max}")]
    LineTooLong {
        /// Number of bytes read so far.
        len: usize,
        /// Configured maximum.
        max: usize,
    },

    /// A bulk-string length field declared more bytes than the configured limit.
    #[error("bulk string too large: {len} bytes exceeds max {max}")]
    BulkTooLarge {
        /// Declared bulk length.
        len: usize,
        /// Configured maximum.
        max: usize,
    },

    /// The input does not conform to the wire format.
    #[error("malformed protocol input: {0}")]
    Malformed(String),

    /// The peer closed the connection in the middle of a frame.
    #[error("unexpected end of stream")]
    UnexpectedEof,

    /// Recursive RESP arrays exceeded the maximum nesting depth.
    #[error("array nesting too deep (max {0} levels)")]
    NestingTooDeep(usize),

    /// An unrecognised RESP type byte was received.
    #[error("unknown RESP type byte: {0:#04x}")]
    UnknownType(u8),

    /// The server has reached its maximum number of simultaneous connections.
    #[error("server at maximum connection capacity")]
    TooManyConnections,

    /// The client sent a command before completing the AUTH handshake.
    #[error("authentication required")]
    AuthRequired,

    /// Authentication failed (wrong credentials or invalid token).
    #[error("authentication failed: {0}")]
    AuthFailed(String),

    /// Error while building or negotiating TLS.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formats_are_human_readable() {
        let e = ProtocolError::LineTooLong {
            len: 2_000_000,
            max: 1_048_576,
        };
        let s = e.to_string();
        assert!(s.contains("2000000"), "should include actual len: {s}");
        assert!(s.contains("1048576"), "should include max: {s}");

        let e2 = ProtocolError::BulkTooLarge {
            len: 32_000_000,
            max: 16_777_216,
        };
        let s2 = e2.to_string();
        assert!(s2.contains("32000000"), "{s2}");

        let e3 = ProtocolError::UnknownType(0xAB);
        let s3 = e3.to_string();
        assert!(
            s3.contains("0xab") || s3.contains("AB") || s3.contains("171"),
            "{s3}"
        );

        let e4 = ProtocolError::AuthFailed("bad token".to_string());
        assert!(e4.to_string().contains("bad token"));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let proto_err = ProtocolError::from(io_err);
        assert!(proto_err.to_string().contains("pipe broke"));
    }
}

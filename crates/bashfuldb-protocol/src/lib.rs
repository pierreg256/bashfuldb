//! BashfulDB wire protocol: RESP-like parser, serialiser, and TCP/TLS server.
//!
//! # Overview
//!
//! This crate provides:
//!
//! - **[`parser`]** — Async RESP parser: line reader, bulk reader, array
//!   recursion with configurable limits.
//! - **[`serializer`]** — Serialise [`Response`] values to wire bytes.
//! - **[`handler`]** — The [`CommandHandler`] trait for plugging in application
//!   logic.
//! - **[`server`]** — The async TCP/TLS server: accept loop, AUTH handshake,
//!   error counting, graceful shutdown.
//!
//! # Wire format (RESP-like)
//!
//! | Type              | Wire encoding                                |
//! |-------------------|----------------------------------------------|
//! | Simple string     | `+OK\r\n`                                    |
//! | Error             | `-ERR message\r\n`                           |
//! | Integer           | `:42\r\n`                                    |
//! | Bulk string       | `$6\r\nfoobar\r\n`                           |
//! | Null bulk         | `$-1\r\n`                                    |
//! | Array             | `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`          |
//! | Warning sentinel  | `+_warning message\r\n`                      |
//!
//! # Limits
//!
//! | Parameter        | Default                       |
//! |------------------|-------------------------------|
//! | Max line length  | 1 MiB (`1_048_576` bytes)     |
//! | Max bulk length  | 16 MiB (`16_777_216` bytes)   |
//! | Max connections  | `1_024`                       |
//! | Default port     | `6380`                        |

pub mod error;
pub mod handler;
pub mod parser;
pub mod serializer;
pub mod server;
pub mod types;

#[cfg(feature = "tls")]
pub mod tls;

// Convenient top-level re-exports
pub use error::ProtocolError;
pub use handler::CommandHandler;
pub use parser::{RespFrame, parse_command, parse_frame};
pub use serializer::{bulk, serialize_response, write_response};
pub use server::{Server, default_shutdown_signal};
pub use types::{
    Command, ConnectionContext, DEFAULT_MAX_BULK_LENGTH, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_LINE_LENGTH, DEFAULT_PORT, MAX_CONSECUTIVE_ERRORS, PROTOCOL_VERSION, Response,
    SERVER_BANNER, ServerConfig, TlsConfig,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_exports_are_usable() {
        let _ = PROTOCOL_VERSION;
        let _ = SERVER_BANNER;
        let _ = DEFAULT_PORT;
        let _ = DEFAULT_MAX_CONNECTIONS;
        let _ = DEFAULT_MAX_LINE_LENGTH;
        let _ = DEFAULT_MAX_BULK_LENGTH;
        let _ = MAX_CONSECUTIVE_ERRORS;

        let cfg = ServerConfig::default();
        assert_eq!(cfg.bind_addr.port(), DEFAULT_PORT);

        let resp = Response::Simple("OK".to_string());
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"+OK\r\n");
    }

    #[tokio::test]
    async fn round_trip_command_parse() {
        use tokio::io::BufReader;
        let input = b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n";
        let mut reader = BufReader::new(&input[..]);
        let cmd = parse_command(
            &mut reader,
            DEFAULT_MAX_LINE_LENGTH,
            DEFAULT_MAX_BULK_LENGTH,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(cmd.name, "PING");
        assert_eq!(cmd.args[0], b"hello".as_slice());
    }

    #[test]
    fn response_serialize_round_trip() {
        let cases = vec![
            Response::Simple("OK".into()),
            Response::Error("ERR something".into()),
            Response::Integer(99),
            Response::Bulk(None),
            Response::Bulk(Some(bytes::Bytes::from("data"))),
            Response::Warning("lag".into()),
            Response::Array(vec![Response::Integer(1), Response::Integer(2)]),
        ];
        for r in &cases {
            let bytes = serialize_response(r);
            assert!(!bytes.is_empty(), "empty serialisation for {r:?}");
        }
    }
}

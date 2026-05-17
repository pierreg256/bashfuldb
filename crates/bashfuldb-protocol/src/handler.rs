//! The [`CommandHandler`] trait that plugs application logic into the server.

use async_trait::async_trait;

use crate::types::{Command, ConnectionContext, Response};

/// Application-level handler for client commands.
///
/// Implement this trait and pass it to [`crate::server::Server`] to handle
/// all commands received after the AUTH handshake.
///
/// # Thread-safety
///
/// Implementations must be `Send + Sync` because the server may dispatch
/// multiple connections concurrently.
///
/// # Example
///
/// ```rust,ignore
/// use async_trait::async_trait;
/// use bashfuldb_protocol::{Command, CommandHandler, ConnectionContext, Response};
///
/// struct PingHandler;
///
/// #[async_trait]
/// impl CommandHandler for PingHandler {
///     async fn handle(&self, cmd: Command, _ctx: &ConnectionContext) -> Response {
///         if cmd.name == "PING" {
///             Response::Simple("PONG".into())
///         } else {
///             Response::Error(format!("ERR unknown command '{}'", cmd.name))
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait CommandHandler: Send + Sync {
    /// Handle one parsed command and return the response to send back.
    ///
    /// This method is called inside a tokio task; blocking for a long time
    /// will stall that connection.
    async fn handle(&self, cmd: Command, ctx: &ConnectionContext) -> Response;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct EchoHandler;

    #[async_trait]
    impl CommandHandler for EchoHandler {
        async fn handle(&self, cmd: Command, _ctx: &ConnectionContext) -> Response {
            if cmd.name == "PING" {
                Response::Simple("PONG".to_string())
            } else if cmd.name == "ECHO" {
                match cmd.args.first() {
                    Some(b) => Response::Bulk(Some(b.clone())),
                    None => Response::Error("ERR wrong number of arguments".to_string()),
                }
            } else {
                Response::Error(format!("ERR unknown command '{}'", cmd.name))
            }
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn trait_object_bounds_compile() {
        assert_send_sync::<Box<dyn CommandHandler>>();
        assert_send_sync::<Arc<dyn CommandHandler>>();
    }

    #[tokio::test]
    async fn echo_handler_ping() {
        use std::net::SocketAddr;
        let h = EchoHandler;
        let ctx = ConnectionContext {
            peer_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            conn_id: 1,
            claims: None,
        };
        let cmd = Command {
            name: "PING".to_string(),
            args: vec![],
        };
        let resp = h.handle(cmd, &ctx).await;
        assert_eq!(resp, Response::Simple("PONG".to_string()));
    }

    #[tokio::test]
    async fn echo_handler_echo() {
        use bytes::Bytes;
        use std::net::SocketAddr;
        let h = EchoHandler;
        let ctx = ConnectionContext {
            peer_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            conn_id: 2,
            claims: None,
        };
        let cmd = Command {
            name: "ECHO".to_string(),
            args: vec![Bytes::from("hello")],
        };
        let resp = h.handle(cmd, &ctx).await;
        assert_eq!(resp, Response::Bulk(Some(Bytes::from("hello"))));
    }
}

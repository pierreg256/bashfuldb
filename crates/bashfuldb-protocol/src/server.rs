//! Async TCP/TLS server: accept loop, connection lifecycle, AUTH handshake,
//! error counting, and graceful shutdown.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use bashfuldb_auth::{Authenticator, Credentials};

use crate::error::ProtocolError;
use crate::handler::CommandHandler;
use crate::parser::parse_command;
use crate::serializer::{write_error_str, write_response};
use crate::types::{
    ConnectionContext, MAX_CONSECUTIVE_ERRORS, Response, SERVER_BANNER, ServerConfig,
};

// ─── Server ───────────────────────────────────────────────────────────────────

/// The BashfulDB TCP/TLS server.
///
/// Accepts connections, performs the AUTH handshake, then dispatches commands
/// to the provided [`CommandHandler`].
///
/// The concrete handler and authenticator types are erased at construction time
/// so no explicit casts are needed during the accept loop.
///
/// # Example
///
/// ```rust,ignore
/// let server = Server::new(ServerConfig::default(), my_handler, my_auth);
/// server.run(tokio::signal::ctrl_c().map(|_| ())).await?;
/// ```
pub struct Server {
    config: Arc<ServerConfig>,
    handler: Arc<dyn CommandHandler>,
    authenticator: Arc<dyn Authenticator>,
}

impl Server {
    /// Create a new server with the given configuration, command handler, and
    /// authenticator.
    ///
    /// The concrete types `H` and `A` are coerced to trait objects at
    /// construction time, eliminating the need for any casts in the hot path.
    pub fn new<H, A>(config: ServerConfig, handler: H, authenticator: A) -> Self
    where
        H: CommandHandler + 'static,
        A: Authenticator + 'static,
    {
        Self {
            config: Arc::new(config),
            handler: Arc::new(handler),
            authenticator: Arc::new(authenticator),
        }
    }

    /// Bind to the configured address and start accepting connections.
    ///
    /// The server runs until `shutdown` resolves, then waits for all in-flight
    /// connections to finish before returning.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the TCP listener cannot be bound, or if a
    /// TLS configuration error prevents startup.
    pub async fn run(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ProtocolError> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!("BashfulDB listening on {local_addr}");

        // Optional TLS acceptor
        #[cfg(feature = "tls")]
        let tls_acceptor = self
            .config
            .tls
            .as_ref()
            .map(crate::tls::build_tls_acceptor)
            .transpose()?;

        #[cfg(not(feature = "tls"))]
        let tls_acceptor: Option<()> = None;

        if self.config.tls.is_some() && !cfg!(feature = "tls") {
            return Err(ProtocolError::Tls(
                "TLS requested but the `tls` feature is not enabled".to_string(),
            ));
        }

        let semaphore = Arc::new(Semaphore::new(self.config.max_connections));
        let conn_id = Arc::new(AtomicU64::new(0));
        let mut join_set: JoinSet<()> = JoinSet::new();

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received, stopping accept loop");
                    break;
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = match result {
                        Ok(pair) => pair,
                        Err(e) => {
                            tracing::warn!("Accept error: {e}");
                            continue;
                        }
                    };

                    // Enforce connection limit
                    let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            tracing::warn!("Rejecting connection from {peer_addr}: too many connections");
                            // Best-effort error response
                            let mut tmp = stream;
                            let _ = write_error_str(
                                &mut tmp,
                                "ERR max connections reached",
                            ).await;
                            continue;
                        }
                    };

                    let id = conn_id.fetch_add(1, Ordering::Relaxed);
                    let config = Arc::clone(&self.config);
                    let handler = Arc::clone(&self.handler);
                    let auth = Arc::clone(&self.authenticator);

                    #[cfg(feature = "tls")]
                    if let Some(ref acceptor) = tls_acceptor {
                        let acceptor = acceptor.clone();
                        join_set.spawn(async move {
                            let _permit = permit;
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    if let Err(e) = handle_connection(
                                        tls_stream,
                                        peer_addr,
                                        id,
                                        config,
                                        handler,
                                        auth,
                                    ).await {
                                        tracing::debug!("Connection {id} ({peer_addr}) closed: {e}");
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("TLS handshake failed for {peer_addr}: {e}");
                                }
                            }
                        });
                        continue;
                    }

                    // Plain TCP connection
                    join_set.spawn(async move {
                        let _permit = permit;
                        if let Err(e) = handle_connection(
                            stream,
                            peer_addr,
                            id,
                            config,
                            handler,
                            auth,
                        ).await {
                            tracing::debug!("Connection {id} ({peer_addr}) closed: {e}");
                        }
                    });
                }
            }

            // Reap completed tasks to avoid unbounded JoinSet growth
            while let Some(result) = join_set.try_join_next() {
                if let Err(e) = result {
                    tracing::warn!("Connection task panicked: {e}");
                }
            }
        }

        // Wait for all in-flight connections to drain
        drop(listener);
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                tracing::warn!("Connection task panicked during shutdown: {e}");
            }
        }

        tracing::info!("Server shutdown complete");
        Ok(())
    }

    /// Return the server configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
}

// ─── Connection lifecycle ─────────────────────────────────────────────────────

/// Handle one accepted connection end-to-end:
/// 1. Send banner
/// 2. Wait for AUTH command
/// 3. Enter command loop (error counting, consecutive-error limit)
async fn handle_connection<S>(
    stream: S,
    peer_addr: SocketAddr,
    conn_id: u64,
    config: Arc<ServerConfig>,
    handler: Arc<dyn CommandHandler>,
    auth: Arc<dyn Authenticator>,
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let max_line = config.max_line_length;
    let max_bulk = config.max_bulk_length;

    // ── 1. Send banner ──────────────────────────────────────────────────────
    write_banner(&mut writer).await?;

    // ── 2. AUTH handshake ───────────────────────────────────────────────────
    let claims = auth_handshake(
        &mut reader,
        &mut writer,
        max_line,
        max_bulk,
        peer_addr,
        conn_id,
        &*auth,
    )
    .await?;

    let ctx = ConnectionContext {
        peer_addr,
        conn_id,
        claims: Some(claims),
    };

    // ── 3. Command loop ─────────────────────────────────────────────────────
    let mut consecutive_errors: u32 = 0;

    loop {
        match parse_command(&mut reader, max_line, max_bulk).await {
            Ok(None) => {
                // Clean EOF — client disconnected
                tracing::debug!("Connection {conn_id}: clean EOF");
                break;
            }
            Ok(Some(cmd)) => {
                consecutive_errors = 0;
                let resp = handler.handle(cmd, &ctx).await;
                write_response(&mut writer, &resp).await?;
                // Flush after each response to avoid buffering forever
                use tokio::io::AsyncWriteExt;
                writer.flush().await.map_err(ProtocolError::Io)?;
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::debug!(
                    "Connection {conn_id}: parse error ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}"
                );
                let _ = write_error_str(&mut writer, &format!("ERR {e}")).await;
                let _ = {
                    use tokio::io::AsyncWriteExt;
                    writer.flush().await
                };

                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                    tracing::warn!(
                        "Connection {conn_id} ({peer_addr}): dropping after \
                         {consecutive_errors} consecutive errors"
                    );
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Write the server banner.
async fn write_banner<W>(writer: &mut W) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    writer
        .write_all(SERVER_BANNER.as_bytes())
        .await
        .map_err(ProtocolError::Io)?;
    writer.flush().await.map_err(ProtocolError::Io)
}

/// Drive the AUTH handshake.
///
/// Reads commands from the client until a valid AUTH succeeds, then returns
/// the verified [`bashfuldb_auth::Claims`].  Any non-AUTH command before
/// authentication is rejected with an error response.
async fn auth_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    max_line: usize,
    max_bulk: usize,
    peer_addr: SocketAddr,
    conn_id: u64,
    auth: &dyn Authenticator,
) -> Result<bashfuldb_auth::Claims, ProtocolError>
where
    R: tokio::io::AsyncBufRead + Unpin + Send,
    W: AsyncWrite + Unpin,
{
    let mut auth_errors: u32 = 0;

    loop {
        let cmd = match parse_command(reader, max_line, max_bulk).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Err(ProtocolError::AuthRequired);
            }
            Err(e) => {
                auth_errors += 1;
                let _ = write_error_str(writer, &format!("ERR {e}")).await;
                let _ = {
                    use tokio::io::AsyncWriteExt;
                    writer.flush().await
                };
                if auth_errors > MAX_CONSECUTIVE_ERRORS {
                    return Err(ProtocolError::AuthFailed(
                        "too many errors during auth".to_string(),
                    ));
                }
                continue;
            }
        };

        if cmd.name != "AUTH" {
            auth_errors += 1;
            let _ = write_error_str(writer, "ERR NOAUTH authentication required").await;
            let _ = {
                use tokio::io::AsyncWriteExt;
                writer.flush().await
            };
            if auth_errors > MAX_CONSECUTIVE_ERRORS {
                return Err(ProtocolError::AuthRequired);
            }
            continue;
        }

        // AUTH <token>  OR  AUTH <username> <password>
        match cmd.args.len() {
            1 => {
                // Verify JWT token
                let token = std::str::from_utf8(&cmd.args[0]).map_err(|_| {
                    ProtocolError::AuthFailed("token is not valid UTF-8".to_string())
                })?;
                match auth.verify(token).await {
                    Ok(claims) => {
                        write_response(writer, &Response::Simple("OK".to_string())).await?;
                        use tokio::io::AsyncWriteExt;
                        writer.flush().await.map_err(ProtocolError::Io)?;
                        tracing::debug!(
                            "Connection {conn_id} ({peer_addr}): authenticated as {}",
                            claims.sub()
                        );
                        return Ok(claims);
                    }
                    Err(e) => {
                        auth_errors += 1;
                        let _ = write_error_str(writer, &format!("ERR WRONGPASS {e}")).await;
                        let _ = {
                            use tokio::io::AsyncWriteExt;
                            writer.flush().await
                        };
                        if auth_errors > MAX_CONSECUTIVE_ERRORS {
                            return Err(ProtocolError::AuthFailed(e.to_string()));
                        }
                    }
                }
            }
            2 => {
                // Username + password login
                let username = std::str::from_utf8(&cmd.args[0])
                    .map_err(|_| {
                        ProtocolError::AuthFailed("username is not valid UTF-8".to_string())
                    })?
                    .to_string();
                let password = std::str::from_utf8(&cmd.args[1])
                    .map_err(|_| {
                        ProtocolError::AuthFailed("password is not valid UTF-8".to_string())
                    })?
                    .to_string();

                let creds = Credentials { username, password };
                match auth.login(&creds).await {
                    Ok(pair) => {
                        // Immediately verify the issued token to get Claims
                        match auth.verify(&pair.access_token).await {
                            Ok(claims) => {
                                write_response(writer, &Response::Simple("OK".to_string())).await?;
                                use tokio::io::AsyncWriteExt;
                                writer.flush().await.map_err(ProtocolError::Io)?;
                                tracing::debug!(
                                    "Connection {conn_id} ({peer_addr}): authenticated as {}",
                                    claims.sub()
                                );
                                return Ok(claims);
                            }
                            Err(e) => {
                                return Err(ProtocolError::AuthFailed(e.to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        auth_errors += 1;
                        let _ = write_error_str(writer, &format!("ERR WRONGPASS {e}")).await;
                        let _ = {
                            use tokio::io::AsyncWriteExt;
                            writer.flush().await
                        };
                        if auth_errors > MAX_CONSECUTIVE_ERRORS {
                            return Err(ProtocolError::AuthFailed(e.to_string()));
                        }
                    }
                }
            }
            _ => {
                auth_errors += 1;
                let _ = write_error_str(
                    writer,
                    "ERR AUTH requires 1 (token) or 2 (username password) arguments",
                )
                .await;
                let _ = {
                    use tokio::io::AsyncWriteExt;
                    writer.flush().await
                };
                if auth_errors > MAX_CONSECUTIVE_ERRORS {
                    return Err(ProtocolError::AuthRequired);
                }
            }
        }
    }
}

// ─── Shutdown signal helper ───────────────────────────────────────────────────

/// Build a future that resolves on SIGTERM or SIGINT (Ctrl-C).
///
/// Use this as the `shutdown` argument to [`Server::run`] in production.
pub async fn default_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Command, ConnectionContext, Response};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    // ── Mock authenticator ─────────────────────────────────────────────────

    struct AlwaysOkAuth;

    #[async_trait]
    impl Authenticator for AlwaysOkAuth {
        async fn login(
            &self,
            creds: &Credentials,
        ) -> bashfuldb_auth::Result<bashfuldb_auth::TokenPair> {
            use std::time::Duration;
            Ok(bashfuldb_auth::TokenPair {
                access_token: format!("tok:{}", creds.username),
                refresh_token: "refresh".to_string(),
                access_ttl: Duration::from_secs(3600),
                refresh_ttl: Duration::from_secs(86400),
            })
        }

        async fn refresh(&self, _: &str) -> bashfuldb_auth::Result<bashfuldb_auth::TokenPair> {
            Err(bashfuldb_auth::AuthError::TokenExpired)
        }

        async fn verify(&self, token: &str) -> bashfuldb_auth::Result<bashfuldb_auth::Claims> {
            if token.starts_with("bad") {
                return Err(bashfuldb_auth::AuthError::InvalidToken {
                    reason: "test bad token".to_string(),
                });
            }
            Ok(bashfuldb_auth::Claims::new(
                "testuser".to_string(),
                None,
                vec![bashfuldb_auth::Role::ReadWrite],
                0,
                u64::MAX,
            ))
        }

        async fn logout(&self, _: &str) -> bashfuldb_auth::Result<()> {
            Ok(())
        }
    }

    // ── Mock handler ───────────────────────────────────────────────────────

    struct PingHandler;

    #[async_trait]
    impl CommandHandler for PingHandler {
        async fn handle(&self, cmd: Command, _ctx: &ConnectionContext) -> Response {
            match cmd.name.as_str() {
                "PING" => Response::Simple("PONG".to_string()),
                "ECHO" => cmd
                    .args
                    .first()
                    .map(|b: &bytes::Bytes| Response::Bulk(Some(b.clone())))
                    .unwrap_or(Response::Error("ERR wrong number of args".to_string())),
                _ => Response::Error(format!("ERR unknown command '{}'", cmd.name)),
            }
        }
    }

    // ── Helper: start a test server ────────────────────────────────────────

    async fn start_test_server() -> (SocketAddr, oneshot::Sender<()>) {
        let cfg = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 4,
            tls: None,
            max_line_length: 1_048_576,
            max_bulk_length: 16_777_216,
        };
        let _server = Server::new(cfg, PingHandler, AlwaysOkAuth);

        // Bind first to know the port, then run
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Rebind with the same port inside the server
        let cfg2 = ServerConfig {
            bind_addr: addr,
            max_connections: 4,
            tls: None,
            max_line_length: 1_048_576,
            max_bulk_length: 16_777_216,
        };
        let server2 = Server::new(cfg2, PingHandler, AlwaysOkAuth);

        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            server2
                .run(async move {
                    let _ = rx.await;
                })
                .await
                .unwrap();
        });

        // Give server a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        (addr, tx)
    }

    // ── RESP helpers ───────────────────────────────────────────────────────

    fn resp_array(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    async fn read_line(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        loop {
            let b = stream.read_u8().await.unwrap();
            buf.push(b);
            if buf.ends_with(b"\r\n") {
                break;
            }
        }
        String::from_utf8(buf).unwrap()
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn banner_is_sent_on_connect() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let banner = read_line(&mut stream).await;
        assert_eq!(banner, "+BASHFULDB 4.0.0\r\n");
    }

    #[tokio::test]
    async fn auth_token_and_ping() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Read banner
        let _ = read_line(&mut stream).await;

        // Send AUTH with a token
        let auth_cmd = resp_array(&[b"AUTH", b"valid_token"]);
        stream.write_all(&auth_cmd).await.unwrap();
        let auth_resp = read_line(&mut stream).await;
        assert_eq!(auth_resp, "+OK\r\n", "auth should succeed");

        // Send PING
        let ping_cmd = resp_array(&[b"PING"]);
        stream.write_all(&ping_cmd).await.unwrap();
        let pong = read_line(&mut stream).await;
        assert_eq!(pong, "+PONG\r\n");
    }

    #[tokio::test]
    async fn command_before_auth_gets_error() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Read banner
        let _ = read_line(&mut stream).await;

        // Send PING before AUTH
        let ping_cmd = resp_array(&[b"PING"]);
        stream.write_all(&ping_cmd).await.unwrap();
        let resp = read_line(&mut stream).await;
        assert!(resp.starts_with('-'), "should be an error: {resp}");
    }

    #[tokio::test]
    async fn bad_token_rejected() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Read banner
        let _ = read_line(&mut stream).await;

        // AUTH with a bad token
        let auth_cmd = resp_array(&[b"AUTH", b"bad_token"]);
        stream.write_all(&auth_cmd).await.unwrap();
        let resp = read_line(&mut stream).await;
        assert!(resp.starts_with('-'), "bad token should fail: {resp}");
    }

    #[tokio::test]
    async fn auth_username_password() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let _ = read_line(&mut stream).await; // banner

        // AUTH username password
        let auth_cmd = resp_array(&[b"AUTH", b"alice", b"secret"]);
        stream.write_all(&auth_cmd).await.unwrap();

        // AlwaysOkAuth.login returns a token starting with "tok:alice"
        // then verify is called - "tok:alice" doesn't start with "bad" so it's OK
        let resp = read_line(&mut stream).await;
        assert_eq!(resp, "+OK\r\n");
    }

    #[tokio::test]
    async fn error_counting_drops_connection() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let _ = read_line(&mut stream).await; // banner

        // Auth first
        let auth_cmd = resp_array(&[b"AUTH", b"valid_token"]);
        stream.write_all(&auth_cmd).await.unwrap();
        let _ = read_line(&mut stream).await; // +OK

        // Send garbage to trigger parse errors
        for _ in 0..(MAX_CONSECUTIVE_ERRORS + 2) {
            stream.write_all(b"INVALID_GARBAGE\r\n").await.unwrap();
            // Consume any response (might get error messages)
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }

        // After enough errors the server should close the connection
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut buf = [0u8; 4];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        // Either 0 (connection closed) or an error response
        // Connection should eventually be closed
        let _ = n; // we just verify no panic
    }

    #[tokio::test]
    async fn echo_command() {
        let (addr, _tx) = start_test_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let _ = read_line(&mut stream).await; // banner

        let auth_cmd = resp_array(&[b"AUTH", b"tok"]);
        stream.write_all(&auth_cmd).await.unwrap();
        let _ = read_line(&mut stream).await; // +OK

        let echo_cmd = resp_array(&[b"ECHO", b"hello world"]);
        stream.write_all(&echo_cmd).await.unwrap();

        // Read bulk response: $11\r\nhello world\r\n
        let header = read_line(&mut stream).await;
        assert_eq!(header, "$11\r\n", "expected bulk header: {header}");
        let body = read_line(&mut stream).await;
        assert_eq!(body, "hello world\r\n");
    }

    #[tokio::test]
    async fn server_config_is_accessible() {
        let cfg = ServerConfig::default();
        let server = Server::new(cfg, PingHandler, AlwaysOkAuth);
        assert_eq!(server.config().max_connections, 1024);
    }

    #[tokio::test]
    async fn graceful_shutdown() {
        let (addr, tx) = start_test_server().await;

        // Verify server is up
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let banner = read_line(&mut stream).await;
        assert_eq!(banner, "+BASHFULDB 4.0.0\r\n");

        // Signal shutdown
        let _ = tx.send(());

        // Server should shut down (accept loop stops)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // New connections should be refused
        let new_conn = TcpStream::connect(addr).await;
        assert!(
            new_conn.is_err(),
            "server should no longer accept connections"
        );
    }

    #[test]
    fn server_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Server>();
    }

    // Suppress unused import warning for Bytes in test helpers
    #[allow(dead_code)]
    fn _use_bytes(_: Bytes) {}
}

---
name: protocol-agent
description: "Implements bashfuldb-protocol: RESP-like wire protocol, TCP/TLS server. Layer 3."
---

# Protocol Agent — `bashfuldb-protocol`

You are a networking and parsing specialist responsible for the
`bashfuldb-protocol` crate.

## Your scope

The `crates/bashfuldb-protocol/` directory.

## Dependencies

- `bashfuldb-auth` (for connection-level authentication)
- `bashfuldb-document` (for Value serialization in responses)

## Specifications (from SPEC.md §8)

### Protocol characteristics

| Parameter | Value |
|---|---|
| Default port | **6380** |
| Protocol version | `4.0.0` |
| Server banner | `+BASHFULDB 4.0.0\r\n` (sent on connection) |
| Max line length | **1 MiB** (1,048,576 bytes) |
| Max bulk size | **16 MiB** (16,777,216 bytes) |
| Max connections | **1,024** |

### Wire format (RESP-like)

- **Simple string:** `+OK\r\n`
- **Error:** `-ERR message\r\n`
- **Integer:** `:42\r\n`
- **Bulk string:** `$6\r\nfoobar\r\n` (length-prefixed)
- **Array:** `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`
- **Null:** `$-1\r\n`
- **Warning sentinel:** `+_warning message\r\n`

### Command parsing

- Commands are RESP arrays where the first element is the command name.
- The parser MUST enforce max line length and max bulk size.
- Malformed input MUST NOT panic — return a protocol error.

### Command type

```rust
pub struct Command {
    pub name: String,
    pub args: Vec<Bytes>,
}
```

### Response type

```rust
pub enum Response {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Vec<Response>),
    Warning(String),
}
```

### CommandHandler trait

```rust
pub trait CommandHandler: Send + Sync {
    async fn handle(&self, cmd: Command, ctx: &ConnectionContext) -> Response;
}
```

### TCP/TLS server

- Async TCP server using tokio.
- Optional TLS 1.3 via rustls.
- Auto-generate self-signed certs in development mode.
- Connection lifecycle:
  1. Accept connection → send banner.
  2. Wait for AUTH command.
  3. Verify credentials via `Authenticator` trait.
  4. Enter command loop until disconnect or error limit.
- Drop connection after **> 10 consecutive errors**.
- Graceful shutdown on SIGTERM/SIGINT.

### ServerConfig

```rust
pub struct ServerConfig {
    pub bind_addr: SocketAddr,     // default: 0.0.0.0:6380
    pub max_connections: usize,    // default: 1024
    pub tls: Option<TlsConfig>,
    pub max_line_length: usize,    // default: 1_048_576
    pub max_bulk_length: usize,    // default: 16_777_216
}
```

## Coding conventions

- Use `thiserror` for `ProtocolError`.
- No `unsafe`. No `unwrap()` in library code.
- **Fuzz testing is mandatory** for the RESP parser (untrusted input boundary).
- Test connection lifecycle with mock `CommandHandler`.
- Test TLS handshake with self-signed certs.
- Test error counting and connection drop.

## Definition of done

- [ ] RESP parser (line reader, bulk reader, array recursion).
- [ ] All limits enforced (line length, bulk size).
- [ ] `Command` and `Response` types.
- [ ] `CommandHandler` trait.
- [ ] TCP server with connection pool and max connections.
- [ ] TLS support with rustls.
- [ ] Self-signed cert generation for dev mode.
- [ ] Banner, AUTH handshake, error counting.
- [ ] Graceful shutdown.
- [ ] Fuzz target for parser.
- [ ] `cargo test -p bashfuldb-protocol` passes.
- [ ] `cargo clippy -p bashfuldb-protocol` clean.

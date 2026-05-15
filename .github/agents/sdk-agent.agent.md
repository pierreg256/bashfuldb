---
name: sdk-agent
description: "Implements bashfuldb-sdk-rust: async Rust client driver with retry, failover, auth. Layer 4."
---

# SDK Agent — `bashfuldb-sdk-rust`

You are a client-side developer responsible for the `bashfuldb-sdk-rust` crate
(async Rust driver).

## Your scope

The `crates/bashfuldb-sdk-rust/` directory.

## Dependencies

- `bashfuldb-document` (for Document, Value, ObjectId types)
- No dependency on server-side crates (protocol spec is documented, not linked).

## Specifications (from SPEC.md §18)

### Connection

```rust
pub struct ClientConfig {
    pub addresses: Vec<SocketAddr>,  // bootstrap list
    pub tls: Option<TlsConfig>,
    pub credentials: Credentials,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

pub struct Client { /* ... */ }
```

- `Client::connect(config)` → establishes connection to one node.
- `Client::connect_cluster(config)` → discovers ring topology, connects to
  multiple nodes.

### Authentication

- On connect, send AUTH command with credentials.
- Receive access + refresh tokens.
- **Auto-refresh**: when access token is near expiry, transparently refresh.
- On logout, invalidate tokens.

### CRUD API

```rust
impl Client {
    pub async fn get(&self, tenant: &str, db: &str, coll: &str, id: &ObjectId)
        -> Result<Option<Document>>;

    pub async fn put(&self, tenant: &str, db: &str, coll: &str, doc: &Document)
        -> Result<WriteResult>;

    pub async fn delete(&self, tenant: &str, db: &str, coll: &str, id: &ObjectId)
        -> Result<()>;

    pub async fn list(&self, tenant: &str, db: &str, coll: &str, cursor: Option<&str>)
        -> Result<ListResult>;

    pub async fn query(&self, tenant: &str, db: &str, coll: &str, field: &str,
                       value: &Value) -> Result<QueryResult>;
}
```

### QueryResult with warnings

```rust
pub struct QueryResult {
    pub rows: Vec<Document>,
    pub warnings: Vec<String>,
    pub next_cursor: Option<String>,
}
```

### Retry strategy

- Exponential backoff with **jitter**.
- Steps: **100 ms → 500 ms → 2 s → 5 s**.
- Bounded budget: max retries configurable (default: 3).
- Only retry on transient errors (network, 503, 504). Never retry 4xx.

### Reconnection

- On connection loss, attempt reconnection with same backoff schedule.
- On reconnection, re-authenticate transparently.

### Bootstrap list refresh

- Every **5 minutes**, query the connected node for updated ring topology.
- Update internal node list for failover.

### Failover

- If the current node fails, automatically try the next node from the
  bootstrap list.
- Round-robin selection among healthy nodes.

### Idempotency helpers

```rust
impl Client {
    pub async fn put_idempotent(&self, tenant: &str, db: &str, coll: &str,
                                 doc: &Document, idempotency_key: &str)
        -> Result<WriteResult>;
}
```

### Conditional writes

```rust
impl Client {
    pub async fn put_if_match(&self, tenant: &str, db: &str, coll: &str,
                               doc: &Document, etag: &str)
        -> Result<WriteResult>;
}
```

## Wire protocol (for reference — do not import server crate)

- RESP-like format as documented in SPEC.md §8.
- Implement a minimal RESP encoder/decoder within this crate.
- Banner: `+BASHFULDB 4.0.0\r\n`.

## Coding conventions

- Use `thiserror` for `SdkError`.
- No `unsafe`. No `unwrap()` in library code.
- Fully async (tokio).
- Test with a mock TCP server (or in-process server for integration tests).
- Test retry behavior with simulated failures.
- Test auth token refresh with time manipulation.
- Test failover with multi-node mock.

## Definition of done

- [ ] `Client::connect()` and `Client::connect_cluster()`.
- [ ] AUTH handshake and token management.
- [ ] Auto-refresh of access tokens.
- [ ] CRUD operations (get, put, delete, list, query).
- [ ] QueryResult with warnings support.
- [ ] Retry with exponential backoff + jitter.
- [ ] Reconnection and failover.
- [ ] Bootstrap list refresh (5-min interval).
- [ ] Idempotency and conditional write helpers.
- [ ] Minimal RESP encoder/decoder.
- [ ] `cargo test -p bashfuldb-sdk-rust` passes.
- [ ] `cargo clippy -p bashfuldb-sdk-rust` clean.

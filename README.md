# BashfulDB

A distributed document database written in Rust.

## Overview

BashfulDB combines a RocksDB-backed storage engine with a coordinator-less
distributed ring topology, quorum-based replication (N=3, R=2, W=2), and a
hardened HTTP API with JWT/RBAC authentication.

**Status:** Early development — trait stabilization phase.

## Architecture

The project is organized as a Cargo workspace with 11 independent crates:

| Layer | Crate | Purpose |
|-------|-------|---------|
| 0 | `bashfuldb-clock` | HLC, vector clocks, causal ordering |
| 0 | `bashfuldb-document` | Value types, binary codec, JSON conversion |
| 1 | `bashfuldb-storage` | StorageEngine trait + RocksDB/in-memory impls |
| 1 | `bashfuldb-cluster` | Ring topology, vnodes, gossip, failure detection |
| 1 | `bashfuldb-auth` | JWT (RS256), Argon2id, RBAC |
| 2 | `bashfuldb-replication` | Quorum R/W, hinted handoff, read repair |
| 2 | `bashfuldb-schema` | DDL, secondary indexes, schema gossip |
| 3 | `bashfuldb-protocol` | RESP-like wire protocol, TCP/TLS server |
| 3 | `bashfuldb-api` | Public HTTP API (axum) |
| 3 | `bashfuldb-ops` | Health, metrics, backup/restore, SLOs |
| 4 | `bashfuldb-sdk-rust` | Async Rust client driver |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full dependency graph and
team mapping.

## Documentation

- **[SPEC.md](SPEC.md)** — Consolidated specifications (normative reference)
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — Module decomposition
- **[.github/agents/](.github/agents/)** — Custom agent definitions per crate

## Building

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace
```

## License

MIT

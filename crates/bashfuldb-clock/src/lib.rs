//! Hybrid Logical Clocks (HLC) and vector clocks for causal ordering.
//!
//! This crate provides the time primitives used throughout BashfulDB for
//! mutation stamping, conflict detection, and causal ordering across nodes.
//!
//! # Key types
//!
//! - [`Hlc`] — A 64-bit hybrid logical clock value.
//! - [`VectorClock`] — A map of `NodeId → Hlc` for concurrency detection.
//! - [`CausalOrder`] — The result of comparing two vector clocks.
//!
//! # Key traits
//!
//! - [`Clock`] — Abstraction over HLC tick/update operations.
#![deny(missing_docs)]

mod error;
mod hlc;
mod node_id;
mod traits;
mod vector_clock;

pub use error::ClockError;
pub use hlc::Hlc;
pub use node_id::NodeId;
pub use traits::{Clock, ManualClock, WallClock};
pub use vector_clock::{CausalOrder, VectorClock};

/// Result type for clock operations.
pub type Result<T> = std::result::Result<T, ClockError>;

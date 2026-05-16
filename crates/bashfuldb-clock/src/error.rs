use thiserror::Error;

/// Errors that can occur in clock operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ClockError {
    /// The vector clock has reached its maximum capacity (4,096 entries).
    #[error("vector clock capacity exceeded (max 4096 entries)")]
    VectorClockCapacityExceeded,

    /// The physical clock moved backward (clock skew detected).
    #[error("physical clock moved backward: got {got_ms}, expected >= {expected_ms}")]
    ClockSkew {
        /// Minimum expected physical timestamp in milliseconds.
        expected_ms: u64,
        /// Observed physical timestamp in milliseconds.
        got_ms: u64,
    },

    /// The HLC logical counter overflowed (65,535 ticks in the same ms).
    #[error("logical counter overflow at physical_ms={physical_ms}")]
    LogicalOverflow {
        /// Physical timestamp in milliseconds where overflow occurred.
        physical_ms: u64,
    },

    /// The physical timestamp exceeds 48 bits.
    #[error("physical_ms {physical_ms} exceeds 48-bit limit")]
    PhysicalOverflow {
        /// Physical timestamp in milliseconds that exceeded 48-bit range.
        physical_ms: u64,
    },
}

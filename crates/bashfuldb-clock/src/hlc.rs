use serde::{Deserialize, Serialize};
use std::fmt;

/// A Hybrid Logical Clock value packed into 64 bits.
///
/// # Layout
///
/// ```text
/// Bits 63..16  (48 bits) — physical timestamp in milliseconds (UTC)
/// Bits 15..0   (16 bits) — logical counter (max 65,535 per ms)
/// ```
///
/// HLC values are totally ordered and monotonically increasing on a single
/// node. They combine wall-clock proximity with logical consistency.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Hlc(u64);

impl Hlc {
    /// Bit shift for the physical component.
    const PHYSICAL_SHIFT: u32 = 16;

    /// Mask for the logical component (lower 16 bits).
    const LOGICAL_MASK: u64 = 0xFFFF;

    /// Maximum value for the logical counter.
    pub const MAX_LOGICAL: u16 = 65_535;

    /// Creates a zero HLC (epoch).
    pub const fn zero() -> Self {
        Self(0)
    }

    /// Creates an HLC from physical (ms) and logical components.
    ///
    /// Returns an error if `physical_ms` exceeds 48 bits.
    pub fn try_new(physical_ms: u64, logical: u16) -> crate::Result<Self> {
        if physical_ms >= (1u64 << 48) {
            return Err(crate::ClockError::PhysicalOverflow { physical_ms });
        }
        Ok(Self((physical_ms << Self::PHYSICAL_SHIFT) | (logical as u64)))
    }

    /// Creates an HLC from physical (ms) and logical components.
    ///
    /// # Panics
    ///
    /// Panics if `physical_ms` exceeds 48 bits.
    pub fn new(physical_ms: u64, logical: u16) -> Self {
        Self::try_new(physical_ms, logical).expect("physical_ms exceeds 48 bits")
    }

    /// Returns the physical timestamp in milliseconds.
    pub fn physical_ms(&self) -> u64 {
        self.0 >> Self::PHYSICAL_SHIFT
    }

    /// Returns the logical counter.
    pub fn logical(&self) -> u16 {
        (self.0 & Self::LOGICAL_MASK) as u16
    }

    /// Returns the raw packed u64 representation.
    pub fn to_u64(self) -> u64 {
        self.0
    }

    /// Creates an HLC from a raw packed u64.
    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Serializes to big-endian bytes.
    pub fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Deserializes from big-endian bytes.
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Hlc(phys={}, log={})",
            self.physical_ms(),
            self.logical()
        )
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.physical_ms(), self.logical())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_accessors() {
        let hlc = Hlc::new(1_000_000, 42);
        assert_eq!(hlc.physical_ms(), 1_000_000);
        assert_eq!(hlc.logical(), 42);
    }

    #[test]
    fn zero_is_minimum() {
        assert_eq!(Hlc::zero().to_u64(), 0);
        assert!(Hlc::zero() < Hlc::new(1, 0));
    }

    #[test]
    fn ordering_physical_first() {
        let a = Hlc::new(100, 50);
        let b = Hlc::new(101, 0);
        assert!(a < b);
    }

    #[test]
    fn ordering_logical_tiebreak() {
        let a = Hlc::new(100, 0);
        let b = Hlc::new(100, 1);
        assert!(a < b);
    }

    #[test]
    fn u64_roundtrip() {
        let hlc = Hlc::new(123_456_789, 1000);
        assert_eq!(Hlc::from_u64(hlc.to_u64()), hlc);
    }

    #[test]
    fn bytes_roundtrip() {
        let hlc = Hlc::new(999_999_999, 65_535);
        assert_eq!(Hlc::from_bytes(hlc.to_bytes()), hlc);
    }

    #[test]
    fn max_logical() {
        let hlc = Hlc::new(1, Hlc::MAX_LOGICAL);
        assert_eq!(hlc.logical(), 65_535);
    }
}

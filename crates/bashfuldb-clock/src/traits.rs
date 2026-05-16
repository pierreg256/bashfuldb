use crate::Hlc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Abstraction over HLC tick and update operations.
///
/// Implementations must guarantee that successive calls to [`tick()`](Clock::tick)
/// return strictly increasing [`Hlc`] values.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current HLC without advancing it.
    fn now(&self) -> Hlc;

    /// Advances the local HLC and returns the new value.
    ///
    /// The returned value is guaranteed to be strictly greater than any
    /// previous value returned by `tick()` or `update()` on this instance.
    ///
    /// Returns an error if the logical counter overflows (65,535 ticks in
    /// the same millisecond without physical clock advancement).
    fn tick(&self) -> crate::Result<Hlc>;

    /// Merges a received HLC with the local clock and returns the new value.
    ///
    /// The returned value is guaranteed to be strictly greater than both
    /// the previous local value and the received value.
    fn update(&self, received: Hlc) -> crate::Result<Hlc>;
}

/// Production clock backed by the system wall clock.
pub struct WallClock {
    state: Mutex<Hlc>,
}

impl WallClock {
    /// Creates a new wall clock initialized to the current system time.
    pub fn new() -> Self {
        let now_ms = system_time_ms();
        Self {
            state: Mutex::new(Hlc::new(now_ms, 0)),
        }
    }

    /// Creates a wall clock restored from a persisted HLC value.
    ///
    /// This ensures monotonicity across restarts: the clock will start
    /// at least one tick ahead of the persisted value.
    pub fn from_persisted(persisted: Hlc) -> Self {
        let now_ms = system_time_ms();
        let physical = std::cmp::max(now_ms, persisted.physical_ms());
        let logical = if physical == persisted.physical_ms() {
            persisted.logical().saturating_add(1)
        } else {
            0
        };
        Self {
            state: Mutex::new(Hlc::new(physical, logical)),
        }
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    fn now(&self) -> Hlc {
        *self.state.lock().expect("clock mutex poisoned")
    }

    fn tick(&self) -> crate::Result<Hlc> {
        let mut state = self.state.lock().expect("clock mutex poisoned");
        let now_ms = system_time_ms();
        let new = if now_ms > state.physical_ms() {
            Hlc::new(now_ms, 0)
        } else {
            let next_logical = state.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: state.physical_ms(),
                },
            )?;
            Hlc::new(state.physical_ms(), next_logical)
        };
        *state = new;
        Ok(new)
    }

    fn update(&self, received: Hlc) -> crate::Result<Hlc> {
        let mut state = self.state.lock().expect("clock mutex poisoned");
        let now_ms = system_time_ms();

        let max_physical = now_ms
            .max(state.physical_ms())
            .max(received.physical_ms());

        let logical = if max_physical == state.physical_ms()
            && max_physical == received.physical_ms()
        {
            state
                .logical()
                .max(received.logical())
                .checked_add(1)
                .ok_or(crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                })?
        } else if max_physical == state.physical_ms() {
            state.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                },
            )?
        } else if max_physical == received.physical_ms() {
            received.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                },
            )?
        } else {
            0
        };

        let new = Hlc::new(max_physical, logical);
        *state = new;
        Ok(new)
    }
}

/// A manually-controlled clock for deterministic testing.
///
/// The physical time only advances when explicitly set via [`set_time()`](ManualClock::set_time).
pub struct ManualClock {
    state: Mutex<Hlc>,
    physical_ms: Mutex<u64>,
}

impl ManualClock {
    /// Creates a manual clock starting at the given millisecond timestamp.
    pub fn new(initial_ms: u64) -> Self {
        Self {
            state: Mutex::new(Hlc::new(initial_ms, 0)),
            physical_ms: Mutex::new(initial_ms),
        }
    }

    /// Advances the physical time to the given millisecond timestamp.
    ///
    /// # Panics
    ///
    /// Panics if `ms` is less than the current physical time.
    pub fn set_time(&self, ms: u64) {
        let mut physical = self.physical_ms.lock().expect("mutex poisoned");
        assert!(ms >= *physical, "ManualClock: cannot move time backward");
        *physical = ms;
    }

    /// Returns the current physical time setting.
    pub fn current_time_ms(&self) -> u64 {
        *self.physical_ms.lock().expect("mutex poisoned")
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Hlc {
        *self.state.lock().expect("mutex poisoned")
    }

    fn tick(&self) -> crate::Result<Hlc> {
        let mut state = self.state.lock().expect("mutex poisoned");
        let physical = *self.physical_ms.lock().expect("mutex poisoned");

        let new = if physical > state.physical_ms() {
            Hlc::new(physical, 0)
        } else {
            let next_logical = state.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: state.physical_ms(),
                },
            )?;
            Hlc::new(state.physical_ms(), next_logical)
        };
        *state = new;
        Ok(new)
    }

    fn update(&self, received: Hlc) -> crate::Result<Hlc> {
        let mut state = self.state.lock().expect("mutex poisoned");
        let physical = *self.physical_ms.lock().expect("mutex poisoned");

        let max_physical = physical
            .max(state.physical_ms())
            .max(received.physical_ms());

        let logical = if max_physical == state.physical_ms()
            && max_physical == received.physical_ms()
        {
            state
                .logical()
                .max(received.logical())
                .checked_add(1)
                .ok_or(crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                })?
        } else if max_physical == state.physical_ms() {
            state.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                },
            )?
        } else if max_physical == received.physical_ms() {
            received.logical().checked_add(1).ok_or(
                crate::ClockError::LogicalOverflow {
                    physical_ms: max_physical,
                },
            )?
        } else {
            0
        };

        let new = Hlc::new(max_physical, logical);
        *state = new;
        Ok(new)
    }
}

fn system_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn wall_clock_tick_is_monotonic() {
        let clock = WallClock::new();
        let a = clock.tick().unwrap();
        let b = clock.tick().unwrap();
        let c = clock.tick().unwrap();
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn wall_clock_update_advances_past_received() {
        let clock = WallClock::new();
        let far_future = Hlc::new(clock.now().physical_ms() + 100_000, 0);
        let result = clock.update(far_future).unwrap();
        assert!(result > far_future);
    }

    #[test]
    fn wall_clock_from_persisted_is_monotonic() {
        let persisted = Hlc::new(99_999_999_999, 100);
        let clock = WallClock::from_persisted(persisted);
        let first = clock.tick().unwrap();
        assert!(first > persisted);
    }

    #[test]
    fn manual_clock_deterministic() {
        let clock = ManualClock::new(1000);

        let a = clock.tick().unwrap();
        assert_eq!(a.physical_ms(), 1000);

        let b = clock.tick().unwrap();
        assert_eq!(b.physical_ms(), 1000);
        assert!(b > a);

        clock.set_time(2000);
        let c = clock.tick().unwrap();
        assert_eq!(c.physical_ms(), 2000);
        assert_eq!(c.logical(), 0);
        assert!(c > b);
    }

    #[test]
    fn manual_clock_update_merges() {
        let clock = ManualClock::new(1000);
        clock.tick().unwrap();

        let remote = Hlc::new(3000, 5);
        let result = clock.update(remote).unwrap();
        assert!(result > remote);
        assert_eq!(result.physical_ms(), 3000);
        assert_eq!(result.logical(), 6);
    }

    #[test]
    #[should_panic(expected = "cannot move time backward")]
    fn manual_clock_rejects_backward_time() {
        let clock = ManualClock::new(1000);
        clock.set_time(500);
    }

    proptest! {
        #[test]
        fn manual_tick_is_strictly_monotonic(
            initial_ms in 0u64..1_000_000u64,
            deltas in proptest::collection::vec(0u16..8u16, 1..512),
        ) {
            let clock = ManualClock::new(initial_ms);
            let mut current_ms = initial_ms;
            let mut previous = clock.now();

            for delta in deltas {
                current_ms += u64::from(delta);
                clock.set_time(current_ms);
                let next = clock.tick().expect("bounded property test should not overflow");
                prop_assert!(next > previous);
                previous = next;
            }
        }
    }
}

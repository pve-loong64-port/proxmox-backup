use std::time::{Duration, Instant};

/// Rate-limits recurring log output, such as progress lines, with an exponential backoff.
///
/// [`should_log`](Self::should_log) returns `true` at most once per the current interval. Starting
/// from an initial interval, the delay doubles after each hit up to a configured maximum, giving
/// frequent feedback early on while avoiding log spam during long-running operations.
pub struct LogThrottle {
    last: Instant,
    interval: Duration,
    max_interval: Duration,
}

impl LogThrottle {
    /// Creates a throttle with the given initial interval, doubling up to `max_interval` after each
    /// hit. The initial interval is capped at `max_interval`.
    pub fn new(initial_interval: Duration, max_interval: Duration) -> Self {
        Self {
            last: Instant::now(),
            interval: initial_interval.min(max_interval),
            max_interval,
        }
    }

    /// Returns `true` if the current interval has elapsed since the last hit, advancing the
    /// throttle and doubling the interval (capped at the maximum); returns `false` otherwise.
    pub fn should_log(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last) < self.interval {
            return false;
        }
        self.last = now;
        self.interval = (self.interval * 2).min(self.max_interval);
        true
    }
}

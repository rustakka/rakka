//! Backoff supervisor — restart child with exponential backoff.
//! akka.net: `Pattern/BackoffSupervisor.cs`.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BackoffOptions {
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub random_factor: f64,
    pub max_restarts: Option<u32>,
}

impl Default for BackoffOptions {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(30),
            random_factor: 0.2,
            max_restarts: Some(10),
        }
    }
}

impl BackoffOptions {
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let base = self.min_backoff.as_secs_f64() * 2f64.powi(attempt as i32);
        let capped = base.min(self.max_backoff.as_secs_f64());
        let jitter = 1.0 + (pseudo_random_01(attempt) * self.random_factor);
        Duration::from_secs_f64(capped * jitter)
    }
}

fn pseudo_random_01(seed: u32) -> f64 {
    // Deterministic stand-in — tests don't depend on true randomness.

    ((seed.wrapping_mul(2654435761)) % 10_000) as f64 / 10_000.0
}

/// Wrapper around the supervisor logic — held by tests and demonstrations.
pub struct BackoffSupervisor {
    pub options: BackoffOptions,
}

impl BackoffSupervisor {
    pub fn new(options: BackoffOptions) -> Self {
        Self { options }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_but_capped() {
        let o = BackoffOptions::default();
        let d0 = o.next_delay(0);
        let d1 = o.next_delay(1);
        let huge = o.next_delay(30);
        assert!(d0 < d1);
        assert!(huge <= o.max_backoff.mul_f64(1.0 + o.random_factor));
    }
}

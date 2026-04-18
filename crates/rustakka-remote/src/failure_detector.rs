//! Failure detector trait. akka.net: `Remote/FailureDetector.cs`.

use std::time::Duration;

pub trait FailureDetector: Send + Sync {
    fn is_available(&self) -> bool;
    fn is_monitoring(&self) -> bool;
    fn heartbeat(&self);
    fn reset(&self);
    fn since_last_heartbeat(&self) -> Option<Duration>;
}

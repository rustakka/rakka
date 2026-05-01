//! rakka-testkit. akka.net: `src/core/Akka.TestKit/`.
//!
//! Provides:
//! * [`TestKit`] — an actor system preconfigured for tests.
//! * [`TestProbe`] — a typed receiver that assertions run against.
//! * [`EventFilter`] — observes events on the system's event stream.

mod event_filter;
mod multinode;
mod probe;
mod test_kit;
mod test_scheduler;

pub use event_filter::EventFilter;
pub use multinode::{MultiNodeError, MultiNodeSpec};
pub use probe::{TestProbe, TestProbeError};
pub use test_kit::TestKit;
pub use test_scheduler::{ScheduledToken, TestScheduler};

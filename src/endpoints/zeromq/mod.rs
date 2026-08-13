//! ZeroMQ endpoints. Two interchangeable backends share the framing/format
//! `codec`:
//!
//! * [`zmq`] — the `zeromq` crate (zmq.rs), behind the `zeromq` feature.
//! * [`omq`] — `omq-tokio` (omq.rs), behind the `zeromq-omq` feature (MSRV 1.93 /
//!   edition 2024, so it is in `full` but not `portable`). Faster on the
//!   per-message `raw`/`raw_framed` path.
//!
//! `backend: try_omq` (the default) picks omq when it is compiled in and falls
//! back to zmq otherwise; naming a backend explicitly makes it a requirement.
//!
//! The backend types are re-exported here so callers keep using
//! `endpoints::zeromq::{ZeroMqConsumer, ZeroMqPublisher, ...}`.

#[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
pub(crate) mod codec;
#[cfg(feature = "zeromq-omq")]
pub mod omq;
#[cfg(feature = "zeromq")]
pub mod zmq;

#[cfg(feature = "zeromq-omq")]
pub use omq::{ZeroMqOmqConsumer, ZeroMqOmqPublisher};
#[cfg(feature = "zeromq")]
pub use zmq::{ZeroMqConsumer, ZeroMqPublisher};

use crate::models::{ZeroMqBackend, ZeroMqConfig};
use crate::traits::{MessageConsumer, MessagePublisher};
use anyhow::Result;

/// A backend actually to build with — `try_omq` is already resolved away.
#[derive(Debug, PartialEq, Eq)]
enum Resolved {
    Zmq,
    Omq,
}

/// Resolve the configured backend to a concrete one.
///
/// `try_omq` (the default) prefers omq and falls back to zmq when omq isn't
/// compiled in, so the same config runs on a build without `zeromq-omq`. An
/// explicitly named backend is a hard requirement and is kept as-is, so a
/// missing feature stays a startup error instead of silently changing backend.
fn resolve_backend(backend: &ZeroMqBackend) -> Resolved {
    match backend {
        ZeroMqBackend::Zmq => Resolved::Zmq,
        ZeroMqBackend::Omq => Resolved::Omq,
        ZeroMqBackend::TryOmq => {
            if cfg!(feature = "zeromq-omq") {
                Resolved::Omq
            } else {
                Resolved::Zmq
            }
        }
    }
}

/// Build a ZeroMQ consumer for the configured backend (`zmq` or `omq`). Each
/// backend is behind its own build feature; explicitly requesting one that
/// wasn't compiled in is a clear config error rather than a silent fallback.
pub(crate) async fn create_consumer(cfg: &ZeroMqConfig) -> Result<Box<dyn MessageConsumer>> {
    match resolve_backend(&cfg.backend) {
        Resolved::Zmq => {
            #[cfg(feature = "zeromq")]
            return Ok(Box::new(zmq::ZeroMqConsumer::new(cfg).await?) as Box<dyn MessageConsumer>);
            #[cfg(not(feature = "zeromq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'zmq' requires the `zeromq` build feature"
            ));
        }
        Resolved::Omq => {
            #[cfg(feature = "zeromq-omq")]
            return Ok(
                Box::new(omq::ZeroMqOmqConsumer::new(cfg).await?) as Box<dyn MessageConsumer>
            );
            #[cfg(not(feature = "zeromq-omq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'omq' requires the `zeromq-omq` build feature"
            ));
        }
    }
}

/// Publisher counterpart to [`create_consumer`].
pub(crate) async fn create_publisher(cfg: &ZeroMqConfig) -> Result<Box<dyn MessagePublisher>> {
    match resolve_backend(&cfg.backend) {
        Resolved::Zmq => {
            #[cfg(feature = "zeromq")]
            return Ok(Box::new(zmq::ZeroMqPublisher::new(cfg).await?) as Box<dyn MessagePublisher>);
            #[cfg(not(feature = "zeromq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'zmq' requires the `zeromq` build feature"
            ));
        }
        Resolved::Omq => {
            #[cfg(feature = "zeromq-omq")]
            return Ok(
                Box::new(omq::ZeroMqOmqPublisher::new(cfg).await?) as Box<dyn MessagePublisher>
            );
            #[cfg(not(feature = "zeromq-omq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'omq' requires the `zeromq-omq` build feature"
            ));
        }
    }
}

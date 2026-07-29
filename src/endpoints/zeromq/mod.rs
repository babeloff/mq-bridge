//! ZeroMQ endpoints. Two interchangeable backends share the framing/format
//! [`codec`]:
//!
//! * [`zmq`] — the default backend on the `zeromq` crate (zmq.rs). Enabled by
//!   the `zeromq` feature.
//! * [`omq`] — a PoC backend on `omq-tokio` (omq.rs), PUSH/PULL + PUB/SUB only.
//!   Enabled by the opt-in `zeromq-omq` feature (MSRV 1.93 / edition 2024, so it
//!   is kept out of `full`/`portable`).
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

/// Build a ZeroMQ consumer for the configured backend (`zmq` or `omq`). Each
/// backend is behind its own build feature; requesting one that wasn't compiled
/// in is a clear config error rather than a silent fallback.
pub(crate) async fn create_consumer(cfg: &ZeroMqConfig) -> Result<Box<dyn MessageConsumer>> {
    match cfg.backend {
        ZeroMqBackend::Zmq => {
            #[cfg(feature = "zeromq")]
            return Ok(Box::new(zmq::ZeroMqConsumer::new(cfg).await?) as Box<dyn MessageConsumer>);
            #[cfg(not(feature = "zeromq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'zmq' requires the `zeromq` build feature"
            ));
        }
        ZeroMqBackend::Omq => {
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
    match cfg.backend {
        ZeroMqBackend::Zmq => {
            #[cfg(feature = "zeromq")]
            return Ok(Box::new(zmq::ZeroMqPublisher::new(cfg).await?) as Box<dyn MessagePublisher>);
            #[cfg(not(feature = "zeromq"))]
            return Err(anyhow::anyhow!(
                "ZeroMQ backend 'zmq' requires the `zeromq` build feature"
            ));
        }
        ZeroMqBackend::Omq => {
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

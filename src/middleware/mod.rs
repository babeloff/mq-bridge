//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::extensions::get_middleware_factory;
use crate::models::{Endpoint, Middleware};
use crate::traits::{MessageConsumer, MessagePublisher};
use anyhow::Result;
use std::sync::Arc;

mod buffer;
mod cookie_jar;
#[cfg(feature = "dedup")]
mod deduplication;
mod delay;
mod dlq;
mod limiter;
#[cfg(feature = "metrics")]
mod metrics;
mod random_panic;
mod retry;
mod weak_join;

use buffer::{BufferConsumer, BufferPublisher};
use cookie_jar::{CookieJarConsumer, CookieJarPublisher};
#[cfg(feature = "dedup")]
use deduplication::DeduplicationConsumer;
use delay::{DelayConsumer, DelayPublisher};
use dlq::DlqPublisher;
use limiter::{LimiterConsumer, LimiterPublisher};
#[cfg(feature = "metrics")]
use metrics::{MetricsConsumer, MetricsPublisher};
use random_panic::{RandomPanicConsumer, RandomPanicPublisher};
use retry::RetryPublisher;
use weak_join::WeakJoinConsumer;

/// Wraps a `MessageConsumer` with the middlewares specified in the endpoint configuration.
///
/// Middlewares are applied in reverse order of the configuration list.
/// This means the first middleware in the config is the outermost layer, executed first.
pub async fn apply_middlewares_to_consumer(
    mut consumer: Box<dyn MessageConsumer>,
    endpoint: &Endpoint,
    route_name: &str,
) -> Result<Box<dyn MessageConsumer>> {
    for middleware in endpoint.middlewares.iter().rev() {
        consumer = match middleware {
            #[cfg(feature = "dedup")]
            Middleware::Deduplication(cfg) => {
                Box::new(DeduplicationConsumer::new(consumer, cfg, route_name)?)
            }
            #[cfg(feature = "metrics")]
            Middleware::Metrics(cfg) => {
                Box::new(MetricsConsumer::new(consumer, cfg, route_name, "input"))
            }
            Middleware::Dlq(_) => {
                tracing::warn!("Dlq middleware is ignored on consumers (input endpoints). It is currently publisher-only.");
                consumer
            }
            Middleware::Retry(_) => {
                tracing::warn!("Retry middleware is ignored on consumers (input endpoints). It is currently publisher-only.");
                consumer
            }
            Middleware::Delay(cfg) => Box::new(DelayConsumer::new(consumer, cfg)),
            Middleware::RandomPanic(cfg) => Box::new(RandomPanicConsumer::new(consumer, cfg)),
            Middleware::WeakJoin(cfg) => Box::new(WeakJoinConsumer::new(consumer, cfg)),
            Middleware::Limiter(cfg) => Box::new(LimiterConsumer::new(consumer, cfg)?),
            Middleware::Buffer(cfg) => Box::new(BufferConsumer::new(consumer, cfg)?),
            Middleware::CookieJar(cfg) => Box::new(CookieJarConsumer::new(consumer, cfg)),
            Middleware::Custom { name, config } => {
                let factory = get_middleware_factory(name).ok_or_else(|| {
                    anyhow::anyhow!("Custom middleware factory '{}' not found", name)
                })?;
                factory.apply_consumer(consumer, route_name, config).await?
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(anyhow::anyhow!(
                    "[middleware:{}] Unsupported consumer middleware",
                    route_name
                ))
            }
        };
    }
    Ok(consumer)
}

/// Wraps a `MessagePublisher` with the middlewares specified in the endpoint configuration.
///
/// Middlewares are applied in the order of the configuration list.
/// This means the first middleware in the config is the outermost layer, executed first.
pub async fn apply_middlewares_to_publisher(
    mut publisher: Box<dyn MessagePublisher>,
    endpoint: &Endpoint,
    route_name: &str,
) -> Result<Arc<dyn MessagePublisher>> {
    for middleware in &endpoint.middlewares {
        publisher = match middleware {
            Middleware::Dlq(cfg) => Box::new(DlqPublisher::new(publisher, cfg, route_name).await?),
            #[cfg(feature = "metrics")]
            Middleware::Metrics(cfg) => {
                Box::new(MetricsPublisher::new(publisher, cfg, route_name, "output"))
            }
            // This middleware is consumer-only
            #[cfg(feature = "dedup")]
            Middleware::Deduplication(_) => {
                tracing::warn!("Deduplication middleware is ignored on publishers (output endpoints). It should be configured on the input endpoint.");
                publisher
            }
            Middleware::Retry(cfg) => Box::new(RetryPublisher::new(publisher, cfg.clone())),
            Middleware::Delay(cfg) => Box::new(DelayPublisher::new(publisher, cfg)),
            Middleware::RandomPanic(cfg) => Box::new(RandomPanicPublisher::new(publisher, cfg)),
            Middleware::Limiter(cfg) => Box::new(LimiterPublisher::new(publisher, cfg)?),
            Middleware::Buffer(cfg) => Box::new(BufferPublisher::new(publisher, cfg)?),
            Middleware::CookieJar(cfg) => Box::new(CookieJarPublisher::new(publisher, cfg)),
            Middleware::Custom { name, config } => {
                let factory = get_middleware_factory(name).ok_or_else(|| {
                    anyhow::anyhow!("Custom middleware factory '{}' not found", name)
                })?;
                factory
                    .apply_publisher(publisher, route_name, config)
                    .await?
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(anyhow::anyhow!(
                    "[middleware:{}] Unsupported publisher middleware",
                    route_name
                ))
            }
        };
    }
    Ok(publisher.into())
}

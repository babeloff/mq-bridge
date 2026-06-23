//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Process-wide cache of shared transport clients.
//!
//! Many publishers (and consumers) target the same broker/server. Their underlying
//! transport clients — an rdkafka `FutureProducer`, an `async_nats::Client`, a SQLx
//! pool — are designed to be shared across topics/subjects and threads. Without a
//! cache, each route builds its own, multiplying TCP connections, background threads,
//! and buffers.
//!
//! This module keeps one shared client per *connection identity* (URL + auth + TLS +
//! client-level options — never topic/subject/collection/queue). Entries are held as
//! `Weak` references, so a shared client (and any side-effecting `Drop`, e.g. a Kafka
//! flush) is released once the last holder is dropped.
//!
//! Callers opt out with `shared = false`, which always builds a fresh, un-cached client
//! — this is also how a latency-sensitive Kafka publisher gets its own producer queue.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock, RwLock, Weak};

type Key = (&'static str, u64);

static REGISTRY: OnceLock<RwLock<HashMap<Key, Weak<dyn Any + Send + Sync>>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<Key, Weak<dyn Any + Send + Sync>>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Hash a connection identity (any `Hash`able value) into the `u64` used in the key.
///
/// Pass only fields that determine which underlying client is appropriate, e.g.
/// `(url, username, &tls)`. Two callers with the same tag and identity share a client.
pub fn identity_hash<H: Hash>(identity: H) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    identity.hash(&mut hasher);
    hasher.finish()
}

/// Return the cached shared client of type `T` for `(tag, identity)`, or build one with
/// `build` and cache it.
///
/// When `shared` is false, always builds a fresh client and does not touch the cache.
/// The build runs without holding the registry lock across `.await`; a concurrent build
/// racing on the same key resolves by preferring whichever entry is already live.
pub async fn get_or_create<T, F, Fut>(
    tag: &'static str,
    identity: u64,
    shared: bool,
    build: F,
) -> anyhow::Result<Arc<T>>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    if !shared {
        return Ok(Arc::new(build().await?));
    }

    let key = (tag, identity);

    if let Some(existing) = lookup::<T>(&key) {
        return Ok(existing);
    }

    // Build outside the lock — never await while holding it.
    let built: Arc<T> = Arc::new(build().await?);

    let mut map = registry()
        .write()
        .expect("connection registry lock poisoned");
    // Another task may have inserted a live client while we were building.
    if let Some(weak) = map.get(&key) {
        if let Some(arc) = weak.upgrade() {
            if let Ok(typed) = arc.downcast::<T>() {
                return Ok(typed);
            }
        }
    }
    map.insert(
        key,
        Arc::downgrade(&(built.clone() as Arc<dyn Any + Send + Sync>)),
    );
    Ok(built)
}

fn lookup<T: Send + Sync + 'static>(key: &Key) -> Option<Arc<T>> {
    let map = registry()
        .read()
        .expect("connection registry lock poisoned");
    map.get(key)
        .and_then(Weak::upgrade)
        .and_then(|arc| arc.downcast::<T>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn build_client(value: u32) -> anyhow::Result<u32> {
        Ok(value)
    }

    #[tokio::test]
    async fn shared_key_returns_same_arc_and_builds_once() {
        let builds = Arc::new(AtomicUsize::new(0));
        let id = identity_hash(("broker:9092", "alice"));
        let make = |value: u32| {
            let builds = builds.clone();
            move || {
                let builds = builds.clone();
                async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                }
            }
        };

        let a = get_or_create("test-share", id, true, make(1))
            .await
            .unwrap();
        let b = get_or_create("test-share", id, true, make(2))
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*a, 1); // second build is discarded; first client wins
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn not_shared_always_builds_fresh() {
        let id = identity_hash(("broker:9092", "bob"));
        let a = get_or_create("test-dedicated", id, false, || build_client(1))
            .await
            .unwrap();
        let b = get_or_create("test-dedicated", id, false, || build_client(1))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn entry_released_after_last_arc_drops() {
        let id = identity_hash(("broker:9092", "carol"));
        let a = get_or_create("test-release", id, true, || build_client(7))
            .await
            .unwrap();
        let key = ("test-release", id);
        assert!(lookup::<u32>(&key).is_some());
        drop(a);
        // Weak no longer upgrades once the last strong reference is gone.
        assert!(lookup::<u32>(&key).is_none());
    }

    #[tokio::test]
    async fn differing_identity_yields_distinct_clients() {
        let a = get_or_create("test-id", identity_hash("server-a"), true, || {
            build_client(1)
        })
        .await
        .unwrap();
        let b = get_or_create("test-id", identity_hash("server-b"), true, || {
            build_client(2)
        })
        .await
        .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }
}

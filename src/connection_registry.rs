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
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

/// `(tag, identity)` where `identity` is the full structural connection identity
/// (see [`connection_identity`]). Using the complete identity rather than a hash
/// means two distinct connections can never collide onto the same cache entry.
type Key = (&'static str, String);

static REGISTRY: OnceLock<RwLock<HashMap<Key, Weak<dyn Any + Send + Sync>>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<Key, Weak<dyn Any + Send + Sync>>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Per-key async locks that serialize first-time construction so concurrent cold
/// callers don't each run `build` and open duplicate connections.
static INFLIGHT: OnceLock<Mutex<HashMap<Key, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();

fn inflight() -> &'static Mutex<HashMap<Key, Arc<tokio::sync::Mutex<()>>>> {
    INFLIGHT.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build the canonical connection identity used in the cache key from any
/// `Debug`able value. The `Debug` representation is a complete, lossless encoding
/// of the identity, so — unlike a hash — distinct connections never collide.
///
/// Pass only fields that determine which underlying client is appropriate, e.g.
/// `(url, username, &tls)`. Two callers with the same tag and identity share a client.
pub fn connection_identity<H: Debug>(identity: H) -> String {
    format!("{identity:?}")
}

/// Return the cached shared client of type `T` for `(tag, identity)`, or build one with
/// `build` and cache it.
///
/// When `shared` is false, always builds a fresh client and does not touch the cache.
/// The build runs without holding the registry lock across `.await`; a concurrent build
/// racing on the same key resolves by preferring whichever entry is already live.
pub async fn get_or_create<T, F, Fut>(
    tag: &'static str,
    identity: String,
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

    // Serialize construction per key: only one builder runs at a time, so racing
    // cold callers wait here and then find the cached client instead of each
    // opening its own connection.
    let build_lock = {
        let mut inflight = inflight()
            .lock()
            .expect("connection registry inflight lock poisoned");
        Arc::clone(inflight.entry(key.clone()).or_default())
    };
    let _build_guard = build_lock.lock().await;

    // A builder that won the race may have cached a live client while we waited.
    if let Some(existing) = lookup::<T>(&key) {
        return Ok(existing);
    }

    // Build outside the registry lock — never await while holding it.
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
        let id = connection_identity(("broker:9092", "alice"));
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

        let a = get_or_create("test-share", id.clone(), true, make(1))
            .await
            .unwrap();
        let b = get_or_create("test-share", id, true, make(2))
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*a, 1); // second build is discarded; first client wins
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_callers_build_once() {
        let builds = Arc::new(AtomicUsize::new(0));
        let id = connection_identity(("broker:9092", "concurrent"));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let builds = builds.clone();
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                get_or_create("test-concurrent", id, true, || {
                    let builds = builds.clone();
                    async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        // Hold the build open so racing callers pile up behind it.
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(42u32)
                    }
                })
                .await
                .unwrap()
            }));
        }

        let first = handles.pop().unwrap().await.unwrap();
        for h in handles {
            assert!(Arc::ptr_eq(&first, &h.await.unwrap()));
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn not_shared_always_builds_fresh() {
        let id = connection_identity(("broker:9092", "bob"));
        let a = get_or_create("test-dedicated", id.clone(), false, || build_client(1))
            .await
            .unwrap();
        let b = get_or_create("test-dedicated", id, false, || build_client(1))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn entry_released_after_last_arc_drops() {
        let id = connection_identity(("broker:9092", "carol"));
        let a = get_or_create("test-release", id.clone(), true, || build_client(7))
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
        let a = get_or_create("test-id", connection_identity("server-a"), true, || {
            build_client(1)
        })
        .await
        .unwrap();
        let b = get_or_create("test-id", connection_identity("server-b"), true, || {
            build_client(2)
        })
        .await
        .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }
}

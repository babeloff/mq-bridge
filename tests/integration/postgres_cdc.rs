//! Postgres logical-replication CDC integration + restart-safety tests.
//!
//! Requires Docker (a Postgres started with `wal_level=logical`, see
//! `docker-compose/postgres_cdc.yml`). Run with:
//!   `cargo test --test integration_test --features "test-utils postgres-cdc" -- --include-ignored postgres_cdc`
#![cfg(all(feature = "postgres-cdc", feature = "test-utils"))]
#![allow(dead_code)]

use mq_bridge::endpoints::postgres::PostgresCdcConsumer;
use mq_bridge::models::PostgresCdcConfig;
use mq_bridge::sqlx::{Connection, PgConnection};
use mq_bridge::test_utils::{run_test_with_docker, run_test_with_docker_controller, setup_logging};
use mq_bridge::traits::{MessageConsumer, MessageDisposition};
use std::collections::BTreeSet;
use std::time::Duration;

const COMPOSE: &str = "tests/integration/docker-compose/postgres_cdc.yml";
const URL: &str = "postgres://testuser:testpass@localhost:5432/testdb";
const PUBLICATION: &str = "mqb_cdc_pub";

fn cfg(slot: &str) -> PostgresCdcConfig {
    PostgresCdcConfig {
        url: URL.to_string(),
        publication: PUBLICATION.to_string(),
        slot_name: slot.to_string(),
        create_slot: true,
        temporary_slot: false,
        cursor_id: None,
        checkpoint_store: None,
        status_interval_ms: 500,
        tls: Default::default(),
    }
}

/// Connect a plain SQL connection, retrying briefly (used after a restart).
async fn connect_retry() -> PgConnection {
    for _ in 0..30 {
        if let Ok(conn) = PgConnection::connect(URL).await {
            return conn;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    PgConnection::connect(URL)
        .await
        .expect("connect to postgres")
}

/// Drop any leftover slot/publication/table and recreate a clean schema.
/// sqlx 0.9's `&mut PgConnection` executor requires `'static` query text, so we
/// use literal statements (the table/publication names are compile-time
/// constants matching `PUBLICATION`) and bind only the dynamic slot name.
async fn reset_schema(slot: &str) {
    let mut conn = connect_retry().await;
    // A slot may linger from a previous run; drop it if present (ignore errors).
    let _ = sqlx::query(
        "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .execute(&mut conn)
    .await;
    let _ = sqlx::query("DROP PUBLICATION IF EXISTS mqb_cdc_pub")
        .execute(&mut conn)
        .await;
    sqlx::query("DROP TABLE IF EXISTS cdc_users")
        .execute(&mut conn)
        .await
        .expect("drop table");
    sqlx::query("CREATE TABLE cdc_users (id INT PRIMARY KEY, name TEXT)")
        .execute(&mut conn)
        .await
        .expect("create table");
    sqlx::query("CREATE PUBLICATION mqb_cdc_pub FOR TABLE cdc_users")
        .execute(&mut conn)
        .await
        .expect("create publication");
}

async fn insert_rows(range: std::ops::RangeInclusive<i32>) {
    let mut conn = connect_retry().await;
    for id in range {
        sqlx::query("INSERT INTO cdc_users (id, name) VALUES ($1, $2)")
            .bind(id)
            .bind(format!("name-{id}"))
            .execute(&mut conn)
            .await
            .expect("insert row");
    }
}

/// Drain change events until `want` rows have been collected, acking each batch.
/// Returns (operation, id) pairs. Fails the test if it stalls.
async fn drain_ids(consumer: &mut PostgresCdcConsumer, want: usize) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    while out.len() < want {
        let batch = tokio::time::timeout(Duration::from_secs(20), consumer.receive_batch(1024))
            .await
            .expect("timed out waiting for CDC events")
            .expect("receive_batch failed");
        let n = batch.messages.len();
        for msg in &batch.messages {
            let op = msg
                .metadata
                .get("postgres.operation")
                .cloned()
                .unwrap_or_default();
            let body: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap_or_default();
            let id = body.get("id").and_then(|v| v.as_i64()).unwrap_or(-1);
            out.push((op, id));
        }
        (batch.commit)(vec![MessageDisposition::Ack; n])
            .await
            .expect("commit (ack) failed");
    }
    out
}

/// Basic pipeline: inserts are captured as `insert` change events with flat rows.
pub async fn test_postgres_cdc_pipeline() {
    setup_logging();
    run_test_with_docker(COMPOSE, || async {
        let slot = "mqb_cdc_basic_slot";
        reset_schema(slot).await;
        // Create the consumer (and slot) BEFORE inserting so the changes are captured.
        let mut consumer = PostgresCdcConsumer::new(&cfg(slot))
            .await
            .expect("create CDC consumer");

        insert_rows(1..=100).await;
        let seen = drain_ids(&mut consumer, 100).await;

        assert!(seen.iter().all(|(op, _)| op == "insert"), "all inserts");
        let ids: BTreeSet<i64> = seen.iter().map(|(_, id)| *id).collect();
        for id in 1..=100 {
            assert!(ids.contains(&(id as i64)), "row {id} must be captured");
        }
    })
    .await;
}

/// Restart-safety: an in-flight, un-acked batch survives a Postgres restart.
/// After a clean restart, a fresh consumer resumes from the slot's confirmed
/// LSN and redelivers the un-acked changes — no data loss, no gap into
/// already-acknowledged changes.
pub async fn test_postgres_cdc_restart_safety() {
    setup_logging();
    run_test_with_docker_controller(COMPOSE, |controller| async move {
        let slot = "mqb_cdc_restart_slot";
        reset_schema(slot).await;
        let mut consumer = PostgresCdcConsumer::new(&cfg(slot))
            .await
            .expect("create CDC consumer");

        // Batch 1: consume and ACK — advances the slot's confirmed LSN.
        insert_rows(1..=50).await;
        let seen1 = drain_ids(&mut consumer, 50).await;
        assert_eq!(seen1.len(), 50, "batch 1 fully consumed");

        // Batch 2: read (in-flight) but DO NOT ack, then simulate a crash by
        // dropping the consumer before commit — the confirmed LSN stays put.
        insert_rows(51..=100).await;
        let _inflight = tokio::time::timeout(Duration::from_secs(20), consumer.receive_batch(1024))
            .await
            .expect("timed out reading in-flight batch")
            .expect("receive_batch failed");
        // Intentionally no commit() call.
        drop(consumer);

        // Clean restart of the database (permanent slot + confirmed LSN persist).
        controller.stop_service("postgres");
        controller.start_service("postgres");

        // Fresh consumer resumes from the slot's confirmed LSN (past batch 1).
        let mut consumer2 = {
            let mut last_err = None;
            let mut built = None;
            for _ in 0..30 {
                match PostgresCdcConsumer::new(&cfg(slot)).await {
                    Ok(c) => {
                        built = Some(c);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            built.unwrap_or_else(|| panic!("reconnect CDC consumer: {last_err:?}"))
        };

        let seen2 = drain_ids(&mut consumer2, 50).await;
        let ids2: BTreeSet<i64> = seen2.iter().map(|(_, id)| *id).collect();

        // No loss: every un-acked change (51..=100) is redelivered.
        for id in 51..=100 {
            assert!(
                ids2.contains(&(id as i64)),
                "row {id} must be redelivered after restart (no data loss)"
            );
        }
        // No gap into already-acknowledged changes: batch 1 (1..=50) is not
        // re-read, since its LSN was confirmed before the restart.
        assert!(
            ids2.iter().all(|id| *id >= 51),
            "already-acked rows (<=50) must not be redelivered; got {ids2:?}"
        );
    })
    .await;
}

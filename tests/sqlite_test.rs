// Runs the sqlite integration helpers (file-backed, no Docker)
#![allow(unused_imports, dead_code)]
#![cfg(feature = "sqlx")]

#[path = "integration/sqlite.rs"]
mod sqlite;

#[tokio::test]
async fn sqlite_pipeline_integration() {
    sqlite::test_sqlite_pipeline().await;
}

#[tokio::test]
#[ignore = "Test takes too long"]
async fn sqlite_performance_direct() {
    // File-backed, no Docker. Locally measured on 2026-06-21 at ~42s.
    // Run explicitly with:
    // `cargo test --test sqlite_test --features sqlx,test-utils sqlite_performance_direct -- --ignored --nocapture`.
    sqlite::test_sqlite_performance_direct().await;
}

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
    // Ignored by default; run explicitly with `cargo test sqlite_performance_direct -- --ignored`.
    sqlite::test_sqlite_performance_direct().await;
}

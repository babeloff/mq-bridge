// Runs the sqlite integration helpers (file-backed, no Docker)
#![allow(unused_imports, dead_code)]

#[path = "integration/sqlite.rs"]
mod sqlite;

#[tokio::test]
async fn sqlite_pipeline_integration() {
    sqlite::test_sqlite_pipeline().await;
}

#[tokio::test]
async fn sqlite_performance_direct() {
    // Keep this small by default; the underlying helper records performance.
    sqlite::test_sqlite_performance_direct().await;
}

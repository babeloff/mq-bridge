// Runs the mariadb integration helpers (requires Docker)
#![allow(unused_imports, dead_code)]
#![cfg(feature = "sqlx")]

#[path = "integration/mariadb.rs"]
mod mariadb;

#[tokio::test]
async fn mariadb_pipeline_integration() {
    mariadb::test_mariadb_pipeline().await;
}

#[tokio::test]
async fn mariadb_performance_direct() {
    mariadb::test_mariadb_performance_direct().await;
}

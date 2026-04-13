// Runs the mysql integration helpers (requires Docker)
#![allow(unused_imports, dead_code)]

#[path = "integration/mysql.rs"]
mod mysql;

#[tokio::test]
async fn mysql_pipeline_integration() {
    mysql::test_mysql_pipeline().await;
}

#[tokio::test]
async fn mysql_performance_direct() {
    mysql::test_mysql_performance_direct().await;
}

use super::*;
use crate::traits::{MessageConsumer, MessagePublisher};
use tempfile::tempdir;

/// Build a SQLite connection URL from a filesystem path, portably. On Windows a file URL
/// needs the three-slash form and forward slashes; elsewhere the two-slash form is fine.
fn sqlite_url(path: &std::path::Path) -> String {
    #[cfg(windows)]
    {
        format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        format!("sqlite://{}", path.to_str().unwrap())
    }
}

#[test]
fn copy_escape_text_escapes_control_chars() {
    assert_eq!(copy_escape_text("plain"), "plain");
    assert_eq!(copy_escape_text("a\tb\nc\r\\d"), "a\\tb\\nc\\r\\\\d");
}

#[test]
fn extract_copy_columns_accepts_token_only_tuple() {
    let cols = extract_copy_columns(
            "INSERT INTO orders (sku, qty, cust) VALUES (${payload:sku}, ${payload:qty}, ${metadata:cust})",
            3,
        )
        .unwrap();
    assert_eq!(cols, vec!["sku", "qty", "cust"]);
}

#[test]
fn extract_copy_columns_rejects_on_conflict_and_literals() {
    // ON CONFLICT is not expressible via COPY.
    assert!(extract_copy_columns(
        "INSERT INTO t (a) VALUES (${payload:a}) ON CONFLICT DO NOTHING",
        1,
    )
    .is_err());
    // A non-token literal in the VALUES tuple breaks positional mapping. token_count matches the
    // two columns so the count check passes and the literal-residue check is what rejects it.
    assert!(extract_copy_columns("INSERT INTO t (a, b) VALUES (${payload:a}, now())", 2,).is_err());
    // Column count must match the token count.
    assert!(extract_copy_columns("INSERT INTO t (a, b) VALUES (${payload:a})", 1).is_err());
}

async fn setup_db_file() -> (tempfile::TempDir, String) {
    use sqlx::Connection;
    sqlx::any::install_default_drivers();
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let url = sqlite_url(&path);

    // Explicitly create the file first and drop the handle to avoid locking issues on Windows.
    // The `connect` call will create the file if it doesn't exist, but this can be racy in tests.
    drop(tokio::fs::File::create(&path).await.unwrap());

    let mut conn = sqlx::AnyConnection::connect(&url).await.unwrap();
    sqlx::query(
        "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB NOT NULL,
                locked_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    (dir, url)
}

/// Creates an arbitrary (non mq-bridge) table `orders(id, sku, qty)` seeded with `n` rows.
async fn setup_arbitrary_table(n: i64) -> (tempfile::TempDir, String, AnyPool) {
    sqlx::any::install_default_drivers();
    let dir = tempdir().unwrap();
    let path = dir.path().join("arb.db");
    let url = sqlite_url(&path);
    drop(tokio::fs::File::create(&path).await.unwrap());
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    for i in 1..=n {
        sqlx::query("INSERT INTO orders (id, sku, qty) VALUES (?, ?, ?)")
            .bind(i)
            .bind(format!("sku{}", i))
            .bind(i * 10)
            .execute(&pool)
            .await
            .unwrap();
    }
    (dir, url, pool)
}

#[test]
fn test_sql_cursor_encode_decode_roundtrip() {
    for c in [SqlCursor::Int(42), SqlCursor::Text("abc:def".into())] {
        assert_eq!(SqlCursor::decode(&c.encode()), Some(c));
    }
    assert_eq!(SqlCursor::decode("garbage"), None);
}

// Regression: a non-unique cursor_column must not lose rows that share the value at a
// page boundary. `ts` has a duplicate (20) straddling a batch of 2; all 5 rows must be
// emitted exactly once. The naive `col > last` + LIMIT would drop the second ts=20.
#[tokio::test]
async fn test_sqlx_cursor_reader_non_unique_column_no_loss() {
    sqlx::any::install_default_drivers();
    let dir = tempdir().unwrap();
    let path = dir.path().join("dup.db");
    let url = sqlite_url(&path);
    drop(tokio::fs::File::create(&path).await.unwrap());
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, ts) in [(1, 10), (2, 20), (3, 20), (4, 30), (5, 40)] {
        sqlx::query("INSERT INTO events (id, ts) VALUES (?, ?)")
            .bind(id)
            .bind(ts)
            .execute(&pool)
            .await
            .unwrap();
    }

    let config = SqlxConfig {
        url: url.clone(),
        table: "events".to_string(),
        cursor_column: Some("ts".to_string()),
        ..Default::default()
    };
    let mut reader = SqlxCursorReader::new(&config).await.unwrap();

    let mut ids = Vec::new();
    loop {
        let b = reader.receive_batch(2).await.unwrap();
        if b.messages.is_empty() {
            break;
        }
        for m in &b.messages {
            let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
            ids.push(v["id"].as_i64().unwrap());
        }
        let n = b.messages.len();
        (b.commit)(vec![MessageDisposition::Ack; n]).await.unwrap();
    }
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5],
        "no row lost at the duplicate boundary"
    );
}

// Regression: a mid-batch Nack must make the nacked (and following) rows re-read by the
// same running reader, not skipped until a restart. Rows 1..4 are read; row 3 is nacked,
// so the next receive_batch on the same reader resumes at row 3.
#[tokio::test]
async fn test_sqlx_cursor_reader_nack_redelivers_in_process() {
    let (_dir, url, _pool) = setup_arbitrary_table(5).await;
    let config = SqlxConfig {
        url: url.clone(),
        table: "orders".to_string(),
        cursor_column: Some("id".to_string()),
        ..Default::default()
    };
    let mut reader = SqlxCursorReader::new(&config).await.unwrap();

    let b = reader.receive_batch(4).await.unwrap();
    assert_eq!(b.messages.len(), 4);
    (b.commit)(vec![
        MessageDisposition::Ack,
        MessageDisposition::Ack,
        MessageDisposition::Nack,
        MessageDisposition::Ack,
    ])
    .await
    .unwrap();

    // Same reader (no restart): must re-read from row 3 (the first nacked row).
    let b2 = reader.receive_batch(4).await.unwrap();
    let ids: Vec<i64> = b2
        .messages
        .iter()
        .map(|m| {
            serde_json::from_slice::<serde_json::Value>(&m.payload).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ids,
        vec![3, 4, 5],
        "nacked rows must be redelivered in-process"
    );
}

#[tokio::test]
async fn test_sqlx_cursor_reader_resumes_and_is_nondestructive() {
    let (_dir, url, pool) = setup_arbitrary_table(5).await;
    let config = SqlxConfig {
        url: url.clone(),
        table: "orders".to_string(),
        cursor_column: Some("id".to_string()),
        cursor_id: Some("copy-1".to_string()),
        ..Default::default()
    };

    let mut reader = SqlxCursorReader::new(&config).await.unwrap();
    let b1 = reader.receive_batch(3).await.unwrap();
    assert_eq!(b1.messages.len(), 3);
    // Payload is the full row serialized to JSON.
    let v: serde_json::Value = serde_json::from_slice(&b1.messages[0].payload).unwrap();
    assert_eq!(v["id"], 1);
    assert_eq!(v["sku"], "sku1");
    assert_eq!(v["qty"], 10);
    (b1.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

    let b2 = reader.receive_batch(3).await.unwrap();
    assert_eq!(b2.messages.len(), 2);
    (b2.commit)(vec![MessageDisposition::Ack; 2]).await.unwrap();

    // Drained -> empty batch, independent of how the route handles empty batches.
    let b3 = reader.receive_batch(3).await.unwrap();
    assert!(b3.messages.is_empty());

    // Source table is untouched.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 5);

    // Checkpoint persisted in the auto-unique default meta table, keyed by <source>:<id>.
    let last: String = sqlx::query_scalar(
        "SELECT last_value FROM mqb_cursors_orders WHERE cursor_id = 'orders:copy-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(last, "int:5");

    // Restart from a fresh reader: resumes past the checkpoint -> nothing re-emitted.
    let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
    let again = reader2.receive_batch(10).await.unwrap();
    assert!(again.messages.is_empty());
}

#[tokio::test]
async fn test_sqlx_cursor_reader_partial_ack_resumes_at_boundary() {
    let (_dir, url, _pool) = setup_arbitrary_table(5).await;
    let config = SqlxConfig {
        url: url.clone(),
        table: "orders".to_string(),
        cursor_column: Some("id".to_string()),
        cursor_id: Some("copy-1".to_string()),
        ..Default::default()
    };

    let mut reader = SqlxCursorReader::new(&config).await.unwrap();
    let b = reader.receive_batch(4).await.unwrap();
    assert_eq!(b.messages.len(), 4);
    // Ack the first two, nack the rest: checkpoint must stop at the contiguous boundary.
    (b.commit)(vec![
        MessageDisposition::Ack,
        MessageDisposition::Ack,
        MessageDisposition::Nack,
        MessageDisposition::Nack,
    ])
    .await
    .unwrap();

    // A restart resumes at row 3 (ids 3,4,5), never skipping the nacked rows.
    let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
    let b2 = reader2.receive_batch(10).await.unwrap();
    assert_eq!(b2.messages.len(), 3);
    let first: serde_json::Value = serde_json::from_slice(&b2.messages[0].payload).unwrap();
    assert_eq!(first["id"], 3);
}

#[tokio::test]
async fn test_sqlx_cursor_reader_text_column_with_file_checkpoint() {
    sqlx::any::install_default_drivers();
    let dir = tempdir().unwrap();
    let path = dir.path().join("ev.db");
    let url = sqlite_url(&path);
    drop(tokio::fs::File::create(&path).await.unwrap());
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE events (k TEXT PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for k in ["a", "b", "c"] {
        sqlx::query("INSERT INTO events (k, data) VALUES (?, ?)")
            .bind(k)
            .bind(format!("data-{}", k))
            .execute(&pool)
            .await
            .unwrap();
    }

    let ckpt = dir.path().join("cursors.json");
    // Absolute tempdir path -> `file:///abs/path` (three-slash form), portable across OSes.
    let ckpt_url = url::Url::from_file_path(&ckpt).unwrap().to_string();
    let config = SqlxConfig {
        url: url.clone(),
        table: "events".to_string(),
        cursor_column: Some("k".to_string()),
        cursor_id: Some("c1".to_string()),
        checkpoint_store: Some(ckpt_url),
        ..Default::default()
    };

    let mut reader = SqlxCursorReader::new(&config).await.unwrap();
    let b = reader.receive_batch(10).await.unwrap();
    assert_eq!(b.messages.len(), 3);
    (b.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

    // File checkpoint written; source DB has NO meta table (read-only-source path).
    assert!(ckpt.exists());
    let meta_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'mqb_cursors%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(meta_tables, 0);

    // Restart resumes from the file checkpoint -> nothing re-emitted.
    let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
    let again = reader2.receive_batch(10).await.unwrap();
    assert!(again.messages.is_empty());
}

// checkpoint_store pointing at a *different* database persists the cursor there, leaves the
// source DB untouched, and still resumes across a restart.
#[tokio::test]
async fn test_sqlx_cursor_reader_external_db_checkpoint() {
    let (_dir_a, url_a, pool_a) = setup_arbitrary_table(3).await;

    // A separate SQLite database used only for checkpoints.
    let dir_b = tempdir().unwrap();
    let path_b = dir_b.path().join("ckpt.db");
    let url_b = sqlite_url(&path_b);
    drop(tokio::fs::File::create(&path_b).await.unwrap());
    let pool_b = AnyPool::connect(&url_b).await.unwrap();

    let config = SqlxConfig {
        url: url_a.clone(),
        table: "orders".to_string(),
        cursor_column: Some("id".to_string()),
        cursor_id: Some("copy-1".to_string()),
        checkpoint_store: Some(url_b.clone()),
        ..Default::default()
    };

    let mut reader = SqlxCursorReader::new(&config).await.unwrap();
    let b = reader.receive_batch(10).await.unwrap();
    assert_eq!(b.messages.len(), 3);
    (b.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

    // Cursor landed in the external DB, in the auto-unique table keyed by <source>:<id>.
    let last: String = sqlx::query_scalar(
        "SELECT last_value FROM mqb_cursors_orders WHERE cursor_id = 'orders:copy-1'",
    )
    .fetch_one(&pool_b)
    .await
    .unwrap();
    assert_eq!(last, "int:3");

    // The source DB was never written to (no meta table).
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'mqb_cursors%'",
    )
    .fetch_one(&pool_a)
    .await
    .unwrap();
    assert_eq!(n, 0);

    // Restart resumes from the external checkpoint -> nothing re-emitted.
    let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
    assert!(reader2.receive_batch(10).await.unwrap().messages.is_empty());
}

#[tokio::test]
async fn test_sqlx_roundtrip_delete() {
    let (_dir, url) = setup_db_file().await;

    let config = SqlxConfig {
        url: url.clone(),
        table: "messages".to_string(),
        delete_after_read: true,
        ..Default::default()
    };

    let publisher = SqlxPublisher::new(&config).await.unwrap();
    let msg_payload = b"hello sqlx".to_vec();
    let msg = CanonicalMessage::new(msg_payload.clone(), None);
    publisher.send(msg).await.unwrap();

    let mut consumer = SqlxConsumer::new(&config).await.unwrap();
    let received_batch = consumer.receive_batch(1).await.unwrap();
    assert_eq!(received_batch.messages.len(), 1);
    assert_eq!(received_batch.messages[0].payload.as_ref(), &msg_payload);

    (received_batch.commit)(vec![MessageDisposition::Ack])
        .await
        .unwrap();

    let pool = AnyPool::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_sqlx_roundtrip_no_delete() {
    let (_dir, url) = setup_db_file().await;

    let config = SqlxConfig {
        url: url.clone(),
        table: "messages".to_string(),
        delete_after_read: false,
        ..Default::default()
    };

    let publisher = SqlxPublisher::new(&config).await.unwrap();
    let msg_payload = b"hello sqlx no delete".to_vec();
    let msg = CanonicalMessage::new(msg_payload.clone(), None);
    publisher.send(msg).await.unwrap();

    let mut consumer = SqlxConsumer::new(&config).await.unwrap();
    let received_batch = consumer.receive_batch(1).await.unwrap();
    assert_eq!(received_batch.messages.len(), 1);

    (received_batch.commit)(vec![MessageDisposition::Ack])
        .await
        .unwrap();

    let pool = AnyPool::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_parse_insert_template_no_tokens() {
    let (q, sources) =
        parse_insert_template("INSERT INTO t (payload) VALUES (?)", "SQLite").unwrap();
    assert_eq!(q, "INSERT INTO t (payload) VALUES (?)");
    assert!(sources.is_empty());
}

#[test]
fn test_parse_insert_template_single_metadata() {
    let (q, sources) =
        parse_insert_template("INSERT INTO t (a) VALUES (${metadata:x})", "PostgreSQL").unwrap();
    assert_eq!(q, "INSERT INTO t (a) VALUES ($1)");
    assert_eq!(sources, vec![ColumnSource::Metadata("x".to_string())]);
}

#[test]
fn test_parse_insert_template_mixed_dialects() {
    let tpl = "INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})";
    let expected = vec![
        ColumnSource::Metadata("a".to_string()),
        ColumnSource::Payload("b".to_string()),
    ];

    let (q, s) = parse_insert_template(tpl, "PostgreSQL").unwrap();
    assert_eq!(q, "INSERT INTO t (a, b) VALUES ($1, $2)");
    assert_eq!(s, expected);

    let (q, s) = parse_insert_template(tpl, "Microsoft SQL Server").unwrap();
    assert_eq!(q, "INSERT INTO t (a, b) VALUES (@p1, @p2)");
    assert_eq!(s, expected);

    let (q, s) = parse_insert_template(tpl, "MySQL").unwrap();
    assert_eq!(q, "INSERT INTO t (a, b) VALUES (?, ?)");
    assert_eq!(s, expected);
}

#[test]
fn test_parse_insert_template_malformed() {
    assert!(parse_insert_template("VALUES (${metadata:x)", "SQLite").is_err()); // unclosed
    assert!(parse_insert_template("VALUES (${bogus:x})", "SQLite").is_err()); // bad prefix
    assert!(parse_insert_template("VALUES (${metadata})", "SQLite").is_err()); // no ':'
    assert!(parse_insert_template("VALUES (${payload:})", "SQLite").is_err());
    // empty field
}

#[test]
fn test_resolve_source_metadata() {
    let mut msg = CanonicalMessage::new(b"{}".to_vec(), None);
    msg.metadata.insert("k".to_string(), "v".to_string());
    let json = serde_json::from_slice(&msg.payload).ok();
    assert_eq!(
        resolve_source(&msg, &ColumnSource::Metadata("k".to_string()), &json),
        BindValue::Text("v".to_string())
    );
    assert_eq!(
        resolve_source(&msg, &ColumnSource::Metadata("nope".to_string()), &json),
        BindValue::Null
    );
}

#[test]
fn test_resolve_source_payload_types() {
    let msg = CanonicalMessage::new(
        br#"{"s":"x","i":5,"f":1.5,"b":true,"arr":[1],"n":null}"#.to_vec(),
        None,
    );
    let json = serde_json::from_slice(&msg.payload).ok();
    let p = |f: &str| resolve_source(&msg, &ColumnSource::Payload(f.to_string()), &json);
    assert_eq!(p("s"), BindValue::Text("x".to_string()));
    assert_eq!(p("i"), BindValue::Int(5));
    assert_eq!(p("f"), BindValue::Float(1.5));
    assert_eq!(p("b"), BindValue::Bool(true));
    assert_eq!(p("arr"), BindValue::Null);
    assert_eq!(p("n"), BindValue::Null);
    assert_eq!(p("missing"), BindValue::Null);
}

#[cfg(feature = "float-roundtrip")]
#[test]
fn test_resolve_source_f64_bit_exact() {
    // A JSON payload number survives parse+bind bit-for-bit *only* with the
    // opt-in `float-roundtrip` feature. Default serde_json parsing loses ~1 ULP
    // on ~19% of 17-significant-digit doubles, so this test is gated on the feature.
    for g in 1..20_000i64 {
        let truth = (g as f64) / 7.0;
        // Written the way ryu-shortest (serde_json output, Postgres text) does.
        let payload = format!(r#"{{"ratio":{truth}}}"#);
        let msg = CanonicalMessage::new(payload.into_bytes(), None);
        let json = serde_json::from_slice(&msg.payload).ok();
        let bound = resolve_source(&msg, &ColumnSource::Payload("ratio".to_string()), &json);
        // g divisible by 7 renders as an integer literal ("1", not "1.0") and binds
        // as Int; every other value must survive parse+bind bit-for-bit as a Float.
        match bound {
            BindValue::Int(i) => assert_eq!(i as f64, truth, "int mismatch g={g}"),
            BindValue::Float(f) => assert_eq!(
                f.to_bits(),
                truth.to_bits(),
                "ULP loss binding g={g}: got {f}, want {truth}"
            ),
            other => panic!("expected numeric for g={g}, got {other:?}"),
        }
    }
}

#[test]
fn test_deterministic_sqlstate_classification() {
    // Deterministic (dead-letter, never retry): syntax/type, data-exception, integrity.
    for code in [
        "42804", "42601", "42703", "42P01", "22P02", "22003", "23505",
    ] {
        assert!(
            is_deterministic_sqlstate(code),
            "{code} should be permanent"
        );
    }
    // Transient (retry): connection, deadlock/serialization, resources, operator intervention.
    for code in [
        "08006", "08003", "40001", "40P01", "53300", "57P03", "55006",
    ] {
        assert!(
            !is_deterministic_sqlstate(code),
            "{code} should be transient"
        );
    }
    // Too-short / unknown codes must not be misclassified as permanent.
    assert!(!is_deterministic_sqlstate(""));
    assert!(!is_deterministic_sqlstate("4"));
}

// Issue 3 (regression): a SQLite schema error (missing column/table) reports no
// Postgres SQLSTATE, so it must be classified permanent via its message — otherwise
// the consumer wraps it as a retryable Connection error and reconnect-loops forever.
#[tokio::test]
async fn test_classify_sqlite_schema_error_is_permanent() {
    use sqlx::Connection;
    sqlx::any::install_default_drivers();
    let mut conn = sqlx::AnyConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, payload BLOB)")
        .execute(&mut conn)
        .await
        .unwrap();

    // Missing column (the `locked_until` lease column queue mode expects). `AnyRow`
    // is not `Debug`, so extract the error via match rather than `unwrap_err`.
    let missing_col = match sqlx::query("SELECT locked_until FROM items")
        .fetch_all(&mut conn)
        .await
    {
        Ok(_) => panic!("expected a missing-column error"),
        Err(e) => e,
    };
    assert!(
        matches!(
            classify_sql_consumer_error(missing_col),
            ConsumerError::Permanent(_)
        ),
        "missing column must be permanent, not a retryable Connection error"
    );

    // Missing table.
    let missing_table = match sqlx::query("SELECT id FROM does_not_exist")
        .fetch_all(&mut conn)
        .await
    {
        Ok(_) => panic!("expected a missing-table error"),
        Err(e) => e,
    };
    assert!(
        matches!(
            classify_sql_consumer_error(missing_table),
            ConsumerError::Permanent(_)
        ),
        "missing table must be permanent"
    );

    conn.close().await.unwrap();
}

#[test]
fn test_resolve_source_no_fallback() {
    // Payload source must NOT fall back to metadata and vice versa.
    let mut msg = CanonicalMessage::new(b"not json".to_vec(), None);
    msg.metadata.insert("k".to_string(), "meta".to_string());
    let json: Option<serde_json::Value> = serde_json::from_slice(&msg.payload).ok();
    assert!(json.is_none());
    // payload:k -> Null even though metadata has "k"
    assert_eq!(
        resolve_source(&msg, &ColumnSource::Payload("k".to_string()), &json),
        BindValue::Null
    );
}

#[test]
fn test_insert_template_preserves_explicit_cast() {
    // Regression for numeric/timestamptz round-trips: an explicit `::type` cast written
    // next to a token must survive verbatim into the generated SQL. The sqlx `Any` driver
    // reads Postgres numeric/timestamptz as text, so the payload carries a JSON string that
    // Postgres won't implicitly cast on insert — the `::cast` is the supported escape hatch.
    let (sql, sources) = parse_insert_template(
        "INSERT INTO dst (id, amount, created_at) \
         VALUES (${payload:id}, ${payload:amount}::numeric, ${payload:created_at}::timestamptz)",
        "PostgreSQL",
    )
    .unwrap();
    assert_eq!(
        sql,
        "INSERT INTO dst (id, amount, created_at) VALUES ($1, $2::numeric, $3::timestamptz)"
    );
    assert_eq!(sources.len(), 3);
}

// Live round-trip proving text -> numeric/timestamptz works via the `::cast` escape hatch
// through the real publisher. Requires a Postgres reachable at MQB_PG_TEST_URL.
// e.g. MQB_PG_TEST_URL=postgres://postgres:pw@localhost:55432/t cargo test --features sqlx \
//        --lib sqlx_numeric_timestamptz_roundtrip -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn sqlx_numeric_timestamptz_roundtrip() {
    let Ok(url) = std::env::var("MQB_PG_TEST_URL") else {
        eprintln!("MQB_PG_TEST_URL not set; skipping");
        return;
    };
    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS dst_rt")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE dst_rt (id bigint, amount numeric, created_at timestamptz)")
        .execute(&pool)
        .await
        .unwrap();

    let config = SqlxConfig {
        url: url.clone(),
        table: "dst_rt".to_string(),
        insert_query: Some(
            "INSERT INTO dst_rt (id, amount, created_at) VALUES \
             (${payload:id}, ${payload:amount}::numeric, ${payload:created_at}::timestamptz)"
                .to_string(),
        ),
        ..Default::default()
    };
    let publisher = SqlxPublisher::new(&config).await.unwrap();

    // The sqlx source serializes numeric/timestamptz as JSON *strings* (text projection).
    let msg = CanonicalMessage::new(
        br#"{"id":1,"amount":"1.25","created_at":"2020-01-01 00:00:01+00"}"#.to_vec(),
        None,
    );
    publisher.send(msg).await.unwrap();

    let row = sqlx::query(
        "SELECT id, amount::text AS amount, created_at::text AS created_at FROM dst_rt",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let id: i64 = row.get("id");
    let amount: String = row.get("amount");
    let created_at: String = row.get("created_at");
    assert_eq!(id, 1);
    assert_eq!(amount, "1.25");
    assert!(
        created_at.starts_with("2020-01-01 00:00:01"),
        "got {created_at}"
    );
}

#[tokio::test]
async fn test_sqlx_multicolumn_insert() {
    let (_dir, url) = setup_db_file().await;
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE orders (sku TEXT, qty INTEGER, cust TEXT)")
        .execute(&pool)
        .await
        .unwrap();

    let config = SqlxConfig {
            url: url.clone(),
            table: "orders".to_string(),
            insert_query: Some(
                "INSERT INTO orders (sku, qty, cust) VALUES (${payload:sku}, ${payload:qty}, ${metadata:cust})"
                    .to_string(),
            ),
            ..Default::default()
        };
    let publisher = SqlxPublisher::new(&config).await.unwrap();

    let mut msg = CanonicalMessage::new(br#"{"sku":"abc","qty":7}"#.to_vec(), None);
    msg.metadata.insert("cust".to_string(), "c1".to_string());
    publisher.send(msg).await.unwrap();

    let row = sqlx::query("SELECT sku, qty, cust FROM orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    let sku: String = row.get("sku");
    let qty: i64 = row.get("qty");
    let cust: String = row.get("cust");
    assert_eq!(sku, "abc");
    assert_eq!(qty, 7);
    assert_eq!(cust, "c1");
}

#[tokio::test]
async fn test_sqlx_multicolumn_non_json_payload_nulls() {
    let (_dir, url) = setup_db_file().await;
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (a TEXT, b TEXT)")
        .execute(&pool)
        .await
        .unwrap();

    let config = SqlxConfig {
        url: url.clone(),
        table: "t".to_string(),
        insert_query: Some("INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})".to_string()),
        ..Default::default()
    };
    let publisher = SqlxPublisher::new(&config).await.unwrap();

    let mut msg = CanonicalMessage::new(b"raw non-json".to_vec(), None);
    msg.metadata.insert("a".to_string(), "meta_a".to_string());
    publisher.send(msg).await.unwrap();

    let row = sqlx::query("SELECT a, b FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    let a: String = row.get("a");
    let b: Option<String> = row.get("b");
    assert_eq!(a, "meta_a");
    assert_eq!(b, None); // payload not JSON -> NULL, no fallback to metadata
}

#[tokio::test]
async fn test_sqlx_multicolumn_batch() {
    let (_dir, url) = setup_db_file().await;
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (a TEXT, b INTEGER)")
        .execute(&pool)
        .await
        .unwrap();

    let config = SqlxConfig {
        url: url.clone(),
        table: "t".to_string(),
        insert_query: Some("INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})".to_string()),
        ..Default::default()
    };
    let publisher = SqlxPublisher::new(&config).await.unwrap();

    let mut msgs = Vec::new();
    for i in 0..3 {
        let mut m = CanonicalMessage::new(format!("{{\"b\":{}}}", i * 10).into_bytes(), None);
        m.metadata.insert("a".to_string(), format!("row{}", i));
        msgs.push(m);
    }
    publisher.send_batch(msgs).await.unwrap();

    let rows = sqlx::query("SELECT a, b FROM t ORDER BY b")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for (i, row) in rows.iter().enumerate() {
        let a: String = row.get("a");
        let b: i64 = row.get("b");
        assert_eq!(a, format!("row{}", i));
        assert_eq!(b, (i as i64) * 10);
    }
}

#[tokio::test]
async fn test_sqlx_auto_create_rejects_tokens() {
    let (_dir, url) = setup_db_file().await;
    let config = SqlxConfig {
        url,
        table: "t".to_string(),
        auto_create_table: true,
        insert_query: Some("INSERT INTO t (a) VALUES (${payload:a})".to_string()),
        ..Default::default()
    };
    assert!(SqlxPublisher::new(&config).await.is_err());
}

// Regression for the chaos-test message loss: only genuine constraint violations may be
// classified NonRetryable (dead-lettered). Every other database error — crucially the
// operational errors a broker restart/failover produces, e.g. MySQL 1053 "server shutdown in
// progress", which surface with `ErrorKind::Other` — must stay Retryable so in-flight messages
// are re-driven instead of silently dropped. Errors are produced by the real driver, which also
// verifies the `Any` driver actually reports `UniqueViolation` (the production fix relies on it).
#[tokio::test]
async fn test_classify_sql_error_constraint_is_nonretryable_others_retryable() {
    use sqlx::error::ErrorKind;
    sqlx::any::install_default_drivers();
    let dir = tempdir().unwrap();
    let path = dir.path().join("classify.db");
    let url = sqlite_url(&path);
    drop(tokio::fs::File::create(&path).await.unwrap());
    let pool = AnyPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t (id) VALUES (1)")
        .execute(&pool)
        .await
        .unwrap();

    // A duplicate primary key is deterministic: retrying can never succeed -> NonRetryable.
    let dup = sqlx::query("INSERT INTO t (id) VALUES (1)")
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(
        dup.as_database_error().unwrap().kind(),
        ErrorKind::UniqueViolation
    );
    assert!(matches!(
        classify_sql_error(dup),
        PublisherError::NonRetryable(_)
    ));

    // A missing table is a deterministic schema error: every retry fails identically, so it
    // dead-letters instead of wedging the route -> NonRetryable. (SQLite reports it as a bare
    // SQLITE_ERROR with no SQLSTATE, so this exercises the message-based fallback.)
    let schema = sqlx::query("INSERT INTO no_such_table (id) VALUES (1)")
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(schema.as_database_error().is_some());
    assert!(matches!(
        classify_sql_error(schema),
        PublisherError::NonRetryable(_)
    ));

    // Operational failures (a broker restart/failover, e.g. MySQL 1053 SQLSTATE `08S01`) and
    // transport-level errors (pool exhaustion, I/O) are transient -> must stay Retryable.
    assert!(matches!(
        classify_sql_error(sqlx::Error::PoolTimedOut),
        PublisherError::Retryable(_)
    ));
}

#[tokio::test]
async fn test_sqlx_status() {
    let (_dir, url) = setup_db_file().await;
    let config = SqlxConfig {
        url: url.clone(),
        table: "messages".to_string(),
        ..Default::default()
    };

    let publisher = SqlxPublisher::new(&config).await.unwrap();
    let status = publisher.status().await;
    assert!(status.healthy);
    assert_eq!(status.target, "messages");
    assert!(status.details.get("driver").is_some());
}

use std::env;
use std::fs;
use std::process::Command;

#[test]
#[ignore = "Requires git, internet access, and takes time. Runs downstream integration tests."]
fn armature_messaging_test() {
    // External downstream compatibility check. This clones the Armature repo,
    // patches its mq-bridge usage, and runs its tests; keep it explicit opt-in.
    // Define paths: Use system temp dir to avoid locking/conflicts with local target dir
    let target_dir = env::temp_dir().join("mq_bridge_integration_test");
    let test_dir = target_dir.join("armature_test");

    // Clean up previous run
    if test_dir.exists() {
        fs::remove_dir_all(&test_dir).expect("Failed to clean up previous test run");
    }
    fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    let test_dir = test_dir
        .canonicalize()
        .expect("Failed to canonicalize test dir");

    let repo_url = "https://github.com/pegasusheavy/armature.git";
    let branch = "develop";
    let subdirectory = "armature-messaging";

    // 1. Clone Repo
    println!("Cloning {} (branch: {})...", repo_url, branch);
    let status = Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            "--filter=blob:none",
            "--no-checkout",
            "--branch",
            branch,
            repo_url,
            ".",
        ])
        .current_dir(&test_dir)
        .status()
        .expect("Failed to execute git clone");
    assert!(status.success(), "Failed to clone armature repo");

    let status = Command::new("git")
        .args(["sparse-checkout", "set", "--no-cone", "/*", "!/benchmarks"])
        .current_dir(&test_dir)
        .status()
        .expect("Failed to set sparse-checkout");
    assert!(status.success(), "Failed to set sparse-checkout");

    let status = Command::new("git")
        .args(["checkout"])
        .current_dir(&test_dir)
        .status()
        .expect("Failed to checkout");
    assert!(status.success(), "Failed to checkout armature repo");

    let project_dir = test_dir.join(subdirectory);
    assert!(
        project_dir.exists(),
        "armature-messaging directory not found in cloned repo"
    );
    let project_dir = project_dir
        .canonicalize()
        .expect("Failed to canonicalize project dir");

    // 2. Get absolute path to current mq-bridge
    let mq_bridge_path = env::current_dir()
        .expect("Failed to get current dir")
        .canonicalize()
        .expect("Failed to canonicalize path");

    // 3. Patch dependency using cargo add to point to local version
    let cargo_bin = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    println!(
        "Patching mq-bridge dependency to local path: {:?}",
        mq_bridge_path
    );
    let status = Command::new(&cargo_bin)
        .args([
            "add",
            "mq-bridge",
            "--path",
            mq_bridge_path.to_str().unwrap(),
        ])
        .current_dir(&project_dir)
        .status()
        .expect("Failed to execute cargo add");
    assert!(status.success(), "Failed to patch mq-bridge dependency");

    // Patch armature-messaging source code to be compatible with mq-bridge 0.2.0 breaking changes
    // We need to convert Option<CanonicalMessage> to MessageDisposition using .into()
    let source_path = project_dir.join("src/mq_bridge.rs");
    assert!(
        source_path.exists(),
        "expected mq_bridge.rs at {:?}",
        source_path
    );

    println!("Patching {:?} for API compatibility...", source_path);
    let content = fs::read_to_string(&source_path).expect("Failed to read mq_bridge.rs");
    let replace_expected = |content: String,
                            from: &str,
                            to: &str,
                            expected: usize,
                            label: &str|
     -> String {
        let count = content.matches(from).count();
        assert_eq!(
                count, expected,
                "expected exactly {} `{}` patch target(s) in {:?}, found {} (upstream source may have changed)",
                expected, label, source_path, count
            );
        content.replacen(from, to, expected)
    };

    let new_content = replace_expected(
        content,
        "(received.commit)(None)",
        "(received.commit)(None.into())",
        1,
        "(received.commit)(None)",
    );
    let new_content = replace_expected(
        new_content,
        "(received.commit)(Some(response))",
        "(received.commit)(Some(response).into())",
        1,
        "(received.commit)(Some(response))",
    );
    let new_content = replace_expected(
        new_content,
        "topic: self.topic.clone(),\n                capacity: Some(self.buffer_size),",
        "topic: self.topic.clone(),\n                url: None,\n                capacity: Some(self.buffer_size),\n                request_reply: false,\n                request_timeout_ms: None,\n                subscribe_mode: false,\n                enable_nack: false,\n                enable_nack_overridden: false,",
        1,
        "memory config using self.topic.clone()",
    );
    let new_content = replace_expected(
        new_content,
        "topic: topic.into(),\n            capacity: Some(buffer_size),",
        "topic: topic.into(),\n            url: None,\n            capacity: Some(buffer_size),\n            request_reply: false,\n            request_timeout_ms: None,\n            subscribe_mode: false,\n            enable_nack: false,\n            enable_nack_overridden: false,",
        2,
        "memory config using topic.into()",
    );
    let new_content = replace_expected(
        new_content,
        "EndpointType::File(self.topic.clone())",
        "EndpointType::File(mq_bridge::models::FileConfig { path: self.topic.clone(), ..Default::default() })",
        1,
        "EndpointType::File(self.topic.clone())",
    );
    let new_content = replace_expected(
        new_content,
        "            concurrency: 1,\n            batch_size: 128,",
        "            options: mq_bridge::models::RouteOptions {\n                concurrency: 1,\n                batch_size: 128,\n                ..Default::default()\n            },",
        1,
        "route concurrency/batch_size fields",
    );
    fs::write(&source_path, new_content).expect("Failed to write patched mq_bridge.rs");

    // 4. Run tests
    println!(
        "Running armature-messaging tests in {:?} using {}...",
        project_dir, cargo_bin
    );
    assert!(project_dir.exists(), "Project directory missing");
    let status = Command::new(&cargo_bin)
        .arg("test")
        .arg("--features=mq-bridge")
        .arg("--")
        .arg("--ignored")
        .current_dir(&project_dir)
        .env("CARGO_TARGET_DIR", "target")
        .env_remove("RUSTC_WRAPPER")
        .status()
        .expect("Failed to execute cargo test");

    assert!(
        status.success(),
        "armature-messaging tests failed with local mq-bridge changes"
    );
}

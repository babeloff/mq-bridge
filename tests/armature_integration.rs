use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn replace_optional(
    content: &str,
    from: &str,
    to: &str,
    label: &str,
    source_path: &Path,
) -> String {
    let count = content.matches(from).count();
    if count == 0 {
        println!("No patch target found for {} in {:?}", label, source_path);
        content.to_string()
    } else {
        println!(
            "Patching {} occurrence(s) of {} in {:?}",
            count, label, source_path
        );
        content.replacen(from, to, count)
    }
}

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

    // Get absolute path to current mq-bridge
    let mq_bridge_path = env::current_dir()
        .expect("Failed to get current dir")
        .canonicalize()
        .expect("Failed to canonicalize path");

    // Patch dependency using cargo add to point to local version
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
    let new_content = replace_optional(
        &content,
        "(received.commit)(None)",
        "(received.commit)(None.into())",
        "(received.commit)(None)",
        &source_path,
    );
    let new_content = replace_optional(
        &new_content,
        "(received.commit)(Some(response))",
        "(received.commit)(Some(response).into())",
        "(received.commit)(Some(response))",
        &source_path,
    );
    let new_content = replace_optional(
        &new_content,
        "topic: self.topic.clone(),\n                capacity: Some(self.buffer_size),",
        "topic: self.topic.clone(),\n                url: None,\n                capacity: Some(self.buffer_size),\n                request_reply: false,\n                request_timeout_ms: None,\n                subscribe_mode: false,\n                enable_nack: false,\n                enable_nack_overridden: false,",
        "memory config using self.topic.clone()",
        &source_path,
    );
    let new_content = replace_optional(
        &new_content,
        "topic: topic.into(),\n            capacity: Some(buffer_size),",
        "topic: topic.into(),\n            url: None,\n            capacity: Some(buffer_size),\n            request_reply: false,\n            request_timeout_ms: None,\n            subscribe_mode: false,\n            enable_nack: false,\n            enable_nack_overridden: false,",
        "memory config using topic.into()",
        &source_path,
    );
    let new_content = replace_optional(
        &new_content,
        "EndpointType::File(self.topic.clone())",
        "EndpointType::File(mq_bridge::models::FileConfig { path: self.topic.clone(), ..Default::default() })",
        "EndpointType::File(self.topic.clone())",
        &source_path,
    );
    let new_content = replace_optional(
        &new_content,
        "            concurrency: 1,\n            batch_size: 128,",
        "            options: mq_bridge::models::RouteOptions {\n                concurrency: 1,\n                batch_size: 128,\n                ..Default::default()\n            },",
        "route concurrency/batch_size fields",
        &source_path,
    );
    fs::write(&source_path, new_content).expect("Failed to write patched mq_bridge.rs");

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

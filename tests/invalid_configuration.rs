use std::process::Command;

#[test]
fn invalid_configuration_exits_with_the_field_path() {
    let path = std::env::temp_dir().join(format!("chimera-invalid-{}.yaml", std::process::id()));
    let yaml = include_str!("../chimera.example.yaml").replace("model: gpt-6-luna", "model: ''");
    std::fs::write(&path, yaml).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_chimera"))
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ticket.implementation.model"), "{stderr}");
}

#[test]
fn valid_configuration_exits_successfully() {
    let output = Command::new(env!("CARGO_BIN_EXE_chimera"))
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/chimera.example.yaml"))
        .output()
        .unwrap();
    assert!(output.status.success());
}

fn run_with(replacement: (&str, &str)) -> (std::process::Output, String) {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("chimera-yaml-{}-{id}.yaml", std::process::id()));
    let yaml = include_str!("../chimera.example.yaml").replace(replacement.0, replacement.1);
    std::fs::write(&path, yaml).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_chimera"))
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output, stderr)
}

#[test]
fn wrong_field_type_names_the_field_and_the_type_error() {
    let (output, stderr) = run_with(("model: gpt-6-luna", "model: []"));
    assert!(!output.status.success());
    assert!(stderr.contains("model"), "{stderr}");
    assert!(stderr.contains("invalid type"), "{stderr}");
    assert!(stderr.contains("line"), "{stderr}");
    assert!(output.stdout.is_empty());
}

#[test]
fn nested_unknown_field_is_named_with_its_path() {
    let (output, stderr) = run_with((
        "    provider: codex\n",
        "    provider: codex\n    bogus_field: 1\n",
    ));
    assert!(!output.status.success());
    assert!(stderr.contains("unknown field `bogus_field`"), "{stderr}");
    assert!(stderr.contains("ticket.implementation"), "{stderr}");
    assert!(stderr.contains("line"), "{stderr}");
}

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

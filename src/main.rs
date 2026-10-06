use std::process::ExitCode;

const DEFAULT_CONFIG_PATH: &str = "chimera.yaml";

fn main() -> ExitCode {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG_PATH.into());
    // Validate before any Herdr workspace is created.
    match chimera_configuration::load(&path) {
        Ok(_configuration) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {path}: {error}");
            ExitCode::FAILURE
        }
    }
}

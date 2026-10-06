use std::ffi::OsString;
use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

use crate::error::GitError;

const SSH_COMMAND: &str = "ssh -o BatchMode=yes";

/// What an invocation does, which decides whether a failure can be uncertain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Reads only; a failure is never uncertain.
    Read,
    /// Changes the local repository only; a failure is never uncertain.
    Local,
    /// Changes the remote (push); a failure may be uncertain.
    Remote,
}

#[derive(Debug)]
pub(crate) struct Output {
    pub stdout: String,
    // Kept for operations that need git's diagnostics; none do yet.
    #[allow(dead_code)]
    pub stderr: String,
}

/// Runs `git` once per call. Never retries.
#[derive(Debug, Clone)]
pub(crate) struct Runner {
    program: OsString,
}

impl Runner {
    pub fn new() -> Self {
        Self::with_program("git")
    }

    pub fn with_program(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
        }
    }

    /// Runs `git <args>` in `dir`, a repository or worktree directory.
    pub fn run(&self, dir: &Path, args: &[&str], effect: Effect) -> Result<Output, GitError> {
        let output = Command::new(&self.program)
            .arg("--no-pager")
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::null())
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "true")
            .env("SSH_ASKPASS", "true")
            // OpenSSH can prompt through /dev/tty regardless of stdin; batch mode forbids every
            // password, passphrase and host-key prompt.
            .env("GIT_SSH_COMMAND", SSH_COMMAND)
            .env("GIT_PAGER", "cat")
            .env("GIT_EDITOR", "true")
            .env("GIT_SEQUENCE_EDITOR", "true")
            .env("GIT_MERGE_AUTOEDIT", "no")
            .env("LC_ALL", "C")
            .output()
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => GitError::NotFound(error),
                _ => GitError::Spawn(error),
            })?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if output.status.success() {
            return Ok(Output { stdout, stderr });
        }
        let command = args.first().copied().unwrap_or_default().to_owned();
        let status = describe(output.status);
        Err(if classify_uncertain(effect, output.status, &stderr) {
            GitError::Uncertain {
                command,
                status,
                stderr,
            }
        } else {
            GitError::Failed {
                command,
                status,
                stderr,
            }
        })
    }
}

fn describe(status: ExitStatus) -> String {
    status.to_string()
}

/// The server answered a specific ref with a refusal. Generic summaries such as "failed to push
/// some refs" are deliberately absent: git prints them for dropped connections too.
const REJECTED: &[&str] = &["[rejected]", "[remote rejected]"];

/// The connection broke after it was established, so the push may have been applied.
const DROPPED: &[&str] = &[
    "unexpected disconnect",
    "hung up unexpectedly",
    "RPC failed",
    "Connection reset",
    "Broken pipe",
    "early EOF",
];

/// The connection was never established, so nothing was sent.
const NEVER_CONNECTED: &[&str] = &[
    "Could not resolve host",
    "Connection refused",
    "Connection timed out",
    "No route to host",
    "Network is unreachable",
    "Failed to connect",
    "Couldn't connect",
    "unable to connect",
    "Permission denied",
    // SSH aborts before authentication, so nothing reaches receive-pack.
    "Host key verification failed",
    "Authentication failed",
    "does not appear to be a git repository",
    "terminal prompts disabled",
];

/// Whether a non-successful `git` run leaves the remote in an unknown state.
fn classify_uncertain(effect: Effect, status: ExitStatus, stderr: &str) -> bool {
    if effect != Effect::Remote {
        return false;
    }
    // Killed by a signal: the push may have started.
    if status.code().is_none() {
        return true;
    }
    let mentions = |markers: &[&str]| markers.iter().any(|marker| stderr.contains(marker));
    // Evidence of a dropped connection outranks everything else: the remote may have applied
    // the update before the connection broke.
    if mentions(DROPPED) {
        return true;
    }
    // Rejection and a refused connection are definitive; anything else (an unrecognised error)
    // is treated as unknown, which is the safe side for a push.
    !(mentions(REJECTED) || mentions(NEVER_CONNECTED))
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;

    use chimera_core::error::PortError;

    use super::*;
    use crate::testing::{TestRepo, write_executable};

    fn exit(code: i32) -> ExitStatus {
        ExitStatus::from_raw(code << 8)
    }

    fn signal(signal: i32) -> ExitStatus {
        ExitStatus::from_raw(signal)
    }

    #[test]
    fn missing_git_is_failed() {
        let repo = TestRepo::new();
        let error = Runner::with_program("/nonexistent/git")
            .run(repo.work(), &["status"], Effect::Remote)
            .unwrap_err();
        assert!(matches!(error, GitError::NotFound(_)));
        assert!(!error.is_uncertain());
    }

    #[test]
    fn spawn_failure_is_failed() {
        let repo = TestRepo::new();
        // A directory cannot be executed.
        let error = Runner::with_program(repo.work())
            .run(repo.work(), &["status"], Effect::Remote)
            .unwrap_err();
        assert!(matches!(error, GitError::Spawn(_)));
        assert!(!error.is_uncertain());
    }

    #[test]
    fn missing_working_directory_is_failed() {
        let repo = TestRepo::new();
        let error = Runner::new()
            .run(&repo.work().join("missing"), &["status"], Effect::Local)
            .unwrap_err();
        assert!(!error.is_uncertain());
    }

    #[test]
    fn captures_stdout() {
        let repo = TestRepo::new();
        let output = Runner::new()
            .run(
                repo.work(),
                &["rev-parse", "--abbrev-ref", "HEAD"],
                Effect::Read,
            )
            .unwrap();
        assert_eq!(output.stdout.trim(), "main");
    }

    #[test]
    fn local_nonzero_exit_is_failed_with_stderr() {
        let repo = TestRepo::new();
        let error = Runner::new()
            .run(repo.work(), &["checkout", "no-such-branch"], Effect::Local)
            .unwrap_err();
        match error {
            GitError::Failed { stderr, .. } => assert!(stderr.contains("no-such-branch")),
            other => panic!("expected failed, got {other:?}"),
        }
    }

    #[test]
    fn read_is_never_uncertain() {
        assert!(!classify_uncertain(Effect::Read, signal(9), ""));
        assert!(!classify_uncertain(
            Effect::Read,
            exit(128),
            "fatal: the remote end hung up unexpectedly"
        ));
    }

    #[test]
    fn local_is_never_uncertain() {
        assert!(!classify_uncertain(Effect::Local, signal(9), ""));
        assert!(!classify_uncertain(Effect::Local, exit(1), "unknown error"));
    }

    #[test]
    fn remote_killed_by_signal_is_uncertain() {
        assert!(classify_uncertain(Effect::Remote, signal(9), ""));
    }

    #[test]
    fn remote_dropped_connection_is_uncertain() {
        for stderr in [
            "send-pack: unexpected disconnect while reading sideband packet",
            "fatal: the remote end hung up unexpectedly",
            "error: RPC failed; curl 56 Recv failure: Connection reset by peer",
        ] {
            assert!(
                classify_uncertain(Effect::Remote, exit(128), stderr),
                "{stderr}"
            );
        }
    }

    #[test]
    fn remote_rejection_is_failed() {
        let stderr = " ! [rejected]        main -> main (fetch first)\n\
                      error: failed to push some refs to 'origin'";
        assert!(!classify_uncertain(Effect::Remote, exit(1), stderr));
        assert!(!classify_uncertain(
            Effect::Remote,
            exit(1),
            " ! [remote rejected] main -> main (pre-receive hook declined)"
        ));
    }

    #[test]
    fn remote_connection_never_established_is_failed() {
        for stderr in [
            "fatal: unable to access 'https://x/': Could not resolve host: x",
            "ssh: connect to host x port 22: Connection refused\n\
             fatal: Could not read from remote repository.",
            "git@x: Permission denied (publickey).\n\
             fatal: Could not read from remote repository.",
        ] {
            assert!(
                !classify_uncertain(Effect::Remote, exit(128), stderr),
                "{stderr}"
            );
        }
    }

    #[test]
    fn host_key_verification_failure_is_failed() {
        let stderr = "Host key verification failed.\n\
                      fatal: Could not read from remote repository.";
        assert!(!classify_uncertain(Effect::Remote, exit(128), stderr));
    }

    #[test]
    fn dropped_connection_outranks_host_key_message() {
        let stderr = "Host key verification failed.\n\
                      fatal: the remote end hung up unexpectedly";
        assert!(classify_uncertain(Effect::Remote, exit(128), stderr));
    }

    #[test]
    fn real_ssh_host_key_failure_is_failed() {
        let repo = TestRepo::new();
        let dir = repo.work().join("../ssh-stub");
        std::fs::create_dir_all(&dir).unwrap();
        let ssh = dir.join("ssh");
        write_executable(
            &ssh,
            "#!/bin/sh\necho 'Host key verification failed.' >&2\nexit 255\n",
        );
        // The runner sets GIT_SSH_COMMAND to plain `ssh`, which resolves through PATH.
        let git = repo.work().join("../ssh-git.sh");
        write_executable(
            &git,
            &format!(
                "#!/bin/sh\nPATH='{}':\"$PATH\" exec git \"$@\"\n",
                dir.display()
            ),
        );
        let error = Runner::with_program(&git)
            .run(
                repo.work(),
                &["push", "ssh://git@example.invalid/repo.git", "main"],
                Effect::Remote,
            )
            .unwrap_err();
        match error {
            GitError::Failed { stderr, .. } => {
                assert!(stderr.contains("Host key verification failed"), "{stderr}")
            }
            other => panic!("expected failed, got {other:?}"),
        }
    }

    #[test]
    fn real_rejected_push_is_failed() {
        let repo = TestRepo::new();
        let other = repo.clone_origin("other");
        repo.commit_file(&other, "a.txt", "other");
        Runner::new()
            .run(&other, &["push", "origin", "main"], Effect::Remote)
            .unwrap();
        repo.commit_file(repo.work(), "b.txt", "mine");
        let error = Runner::new()
            .run(repo.work(), &["push", "origin", "main"], Effect::Remote)
            .unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn real_unreachable_remote_is_failed() {
        let repo = TestRepo::new();
        let missing = repo.work().join("nowhere.git");
        let error = Runner::new()
            .run(
                repo.work(),
                &["push", missing.to_str().unwrap(), "main"],
                Effect::Remote,
            )
            .unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn real_killed_push_is_uncertain() {
        let repo = TestRepo::new();
        let script = repo.work().join("../fake-git.sh");
        write_executable(
            &script,
            "#!/bin/sh\necho 'Writing objects' >&2\nkill -9 $$\n",
        );
        let error = Runner::with_program(&script)
            .run(repo.work(), &["push"], Effect::Remote)
            .unwrap_err();
        assert!(error.is_uncertain(), "{error:?}");
    }

    #[test]
    fn generic_push_summary_does_not_override_dropped_connection() {
        let stderr = "send-pack: unexpected disconnect while reading sideband packet\n\
                      error: failed to push some refs to 'origin'";
        assert!(classify_uncertain(Effect::Remote, exit(1), stderr));
        assert!(classify_uncertain(
            Effect::Remote,
            exit(1),
            "remote: push declined\nfatal: the remote end hung up unexpectedly"
        ));
    }

    #[test]
    fn bare_push_summary_is_not_proof_of_rejection() {
        assert!(classify_uncertain(
            Effect::Remote,
            exit(1),
            "error: failed to push some refs to 'origin'"
        ));
    }

    #[test]
    fn real_push_with_killed_receiver_is_uncertain_and_remote_updated() {
        let repo = TestRepo::new();
        let hook = repo.origin().join("hooks/post-receive");
        write_executable(&hook, "#!/bin/sh\nkill -9 \"$PPID\"\n");
        repo.commit_file(repo.work(), "b.txt", "mine");
        let error = Runner::new()
            .run(repo.work(), &["push", "origin", "main"], Effect::Remote)
            .unwrap_err();
        let rev = |dir: &Path, rev: &str| {
            Runner::new()
                .run(dir, &["rev-parse", rev], Effect::Read)
                .unwrap()
                .stdout
        };
        assert_eq!(rev(&repo.origin(), "main"), rev(repo.work(), "HEAD"));
        assert!(error.is_uncertain(), "{error:?}");
        assert!(PortError::from(error).is_uncertain());
    }

    #[test]
    fn environment_is_non_interactive() {
        let repo = TestRepo::new();
        let script = repo.work().join("../env-git.sh");
        let dump = repo.work().join("../env.txt");
        write_executable(
            &script,
            &format!("#!/bin/sh\nenv > '{}'\ncat > /dev/null\n", dump.display()),
        );
        Runner::with_program(&script)
            .run(repo.work(), &["push"], Effect::Remote)
            .unwrap();
        let env = std::fs::read_to_string(&dump).unwrap();
        for expected in [
            "GIT_TERMINAL_PROMPT=0",
            "GIT_ASKPASS=true",
            "SSH_ASKPASS=true",
            "GIT_SSH_COMMAND=ssh -o BatchMode=yes",
            "GIT_PAGER=cat",
            "GIT_EDITOR=true",
            "GIT_SEQUENCE_EDITOR=true",
        ] {
            assert!(env.lines().any(|line| line == expected), "{expected}");
        }
    }
}

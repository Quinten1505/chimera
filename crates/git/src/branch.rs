//! Base branch resolution and feature branch creation.

use std::path::Path;

use chimera_core::BranchName;

use crate::error::GitError;
use crate::runner::{Effect, Runner};

const DEVELOP: &str = "develop";

/// Fetches `origin` and returns `develop` if `origin/develop` exists, otherwise the remote's
/// default branch. Fails if the default branch cannot be determined.
pub(crate) fn resolve_base_branch(runner: &Runner, dir: &Path) -> Result<BranchName, GitError> {
    runner.run(dir, &["fetch", "origin"], Effect::Read)?;
    if local_ref(runner, dir, &format!("refs/remotes/origin/{DEVELOP}"))?.is_some() {
        return Ok(branch(DEVELOP));
    }
    // Ask the remote: `origin/HEAD` is only recorded locally by `git clone`.
    let output = runner.run(
        dir,
        &["ls-remote", "--symref", "origin", "HEAD"],
        Effect::Read,
    )?;
    output
        .stdout
        .lines()
        .find_map(|line| {
            line.strip_prefix("ref: refs/heads/")?
                .strip_suffix("\tHEAD")
        })
        .map(branch)
        .ok_or_else(|| GitError::Failed {
            command: "ls-remote".into(),
            status: "no default branch".into(),
            stderr: "origin has no develop branch and does not report a default branch (HEAD)"
                .into(),
        })
}

/// Creates `feature` from the latest `origin/<base>` and pushes it to `origin` with upstream
/// tracking. Succeeds without change if the branch already exists on the remote (and locally, if
/// present there) at a commit consistent with `base`: the base tip, a descendant of it, or an
/// ancestor of it (the base moved on since creation). Any other existing branch is a failure.
pub(crate) fn create_feature_branch(
    runner: &Runner,
    dir: &Path,
    feature: &BranchName,
    base: &BranchName,
) -> Result<(), GitError> {
    let name = feature.as_str();
    runner.run(dir, &["check-ref-format", "--branch", name], Effect::Read)?;
    runner.run(dir, &["fetch", "origin"], Effect::Read)?;

    let local_ref_name = format!("refs/heads/{name}");
    let base_tip = local_ref(runner, dir, &format!("refs/remotes/origin/{base}"))?
        .ok_or_else(|| failed("branch", format!("origin has no branch {base}")))?;
    let local = local_ref(runner, dir, &local_ref_name)?;
    let remote = remote_ref(runner, dir, &local_ref_name)?;

    if let (Some(local), Some(remote)) = (&local, &remote)
        && local != remote
    {
        return Err(failed(
            "branch",
            format!("{name} is at {local} locally but {remote} on origin"),
        ));
    }
    if let Some(existing) = remote.as_ref().or(local.as_ref()) {
        check_consistent(runner, dir, name, existing, &base_tip)?;
    }

    match (&local, &remote) {
        (_, Some(remote)) => {
            if local.is_none() {
                runner.run(dir, &["branch", name, remote], Effect::Local)?;
            }
            let upstream = format!("origin/{name}");
            runner.run(
                dir,
                &["branch", "--set-upstream-to", &upstream, name],
                Effect::Local,
            )?;
        }
        (existing, None) => {
            if existing.is_none() {
                runner.run(dir, &["branch", name, &base_tip], Effect::Local)?;
            }
            let refspec = format!("{local_ref_name}:{local_ref_name}");
            runner.run(
                dir,
                &["push", "--set-upstream", "origin", &refspec],
                Effect::Remote,
            )?;
        }
    }
    Ok(())
}

/// Fails if `existing` is neither the base tip, a descendant of it, nor an ancestor of it.
fn check_consistent(
    runner: &Runner,
    dir: &Path,
    name: &str,
    existing: &str,
    base_tip: &str,
) -> Result<(), GitError> {
    // Exits 1 without output when the histories are unrelated.
    let merge_base = match runner.run(dir, &["merge-base", existing, base_tip], Effect::Read) {
        Ok(output) => output.stdout,
        Err(GitError::Failed { .. }) => String::new(),
        Err(error) => return Err(error),
    };
    let merge_base = merge_base.trim();
    if merge_base == base_tip || merge_base == existing {
        return Ok(());
    }
    Err(failed(
        "branch",
        format!("{name} already exists at {existing}, which is not based on origin's {base_tip}"),
    ))
}

/// The commit a ref in the local repository points to, if it exists.
fn local_ref(runner: &Runner, dir: &Path, name: &str) -> Result<Option<String>, GitError> {
    let output = runner.run(
        dir,
        &["for-each-ref", "--format=%(objectname)", name],
        Effect::Read,
    )?;
    Ok(first_field(&output.stdout))
}

/// The commit a ref on `origin` points to, read from the remote itself.
fn remote_ref(runner: &Runner, dir: &Path, name: &str) -> Result<Option<String>, GitError> {
    let output = runner.run(dir, &["ls-remote", "origin", name], Effect::Read)?;
    Ok(first_field(&output.stdout))
}

fn first_field(text: &str) -> Option<String> {
    text.split_whitespace().next().map(str::to_owned)
}

fn branch(name: &str) -> BranchName {
    BranchName::new(name).expect("branch names from git are not blank")
}

fn failed(command: &str, stderr: String) -> GitError {
    GitError::Failed {
        command: command.into(),
        status: "rejected".into(),
        stderr,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    use super::*;
    use crate::testing::TestRepo;

    fn name(value: &str) -> BranchName {
        BranchName::new(value).unwrap()
    }

    fn git(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn remote_head(repo: &TestRepo, branch: &str) -> String {
        git(
            &repo.origin(),
            &["rev-parse", &format!("refs/heads/{branch}")],
        )
    }

    #[test]
    fn base_is_develop_when_present() {
        let repo = TestRepo::new();
        git(repo.work(), &["push", "origin", "main:develop"]);
        let base = resolve_base_branch(&Runner::new(), repo.work()).unwrap();
        assert_eq!(base, name("develop"));
    }

    #[test]
    fn base_falls_back_to_default_branch() {
        let repo = TestRepo::new();
        let base = resolve_base_branch(&Runner::new(), repo.work()).unwrap();
        assert_eq!(base, name("main"));
    }

    #[test]
    fn undeterminable_default_branch_is_failed() {
        let repo = TestRepo::new();
        // Point the remote's HEAD at a branch that does not exist.
        git(
            &repo.origin(),
            &["symbolic-ref", "HEAD", "refs/heads/missing"],
        );
        let error = resolve_base_branch(&Runner::new(), repo.work()).unwrap_err();
        assert!(!error.is_uncertain());
        assert!(error.to_string().contains("default branch"));
    }

    #[test]
    fn creates_feature_branch_on_remote_with_tracking() {
        let repo = TestRepo::new();
        let runner = Runner::new();
        create_feature_branch(&runner, repo.work(), &name("feature/x"), &name("main")).unwrap();
        assert_eq!(remote_head(&repo, "feature/x"), remote_head(&repo, "main"));
        assert_eq!(
            git(
                repo.work(),
                &["rev-parse", "--abbrev-ref", "feature/x@{upstream}"]
            ),
            "origin/feature/x"
        );
    }

    #[test]
    fn creates_from_latest_remote_base() {
        let repo = TestRepo::new();
        let other = repo.clone_origin("other");
        repo.commit_file(&other, "new.txt", "new");
        git(&other, &["push", "origin", "main"]);
        create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("main")).unwrap();
        assert_eq!(remote_head(&repo, "f"), remote_head(&repo, "main"));
    }

    #[test]
    fn creating_twice_succeeds() {
        let repo = TestRepo::new();
        let runner = Runner::new();
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        let head = remote_head(&repo, "f");
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        assert_eq!(remote_head(&repo, "f"), head);
    }

    #[test]
    fn existing_remote_branch_without_local_is_adopted() {
        let repo = TestRepo::new();
        let runner = Runner::new();
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        git(repo.work(), &["branch", "-D", "f"]);
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        assert_eq!(
            git(repo.work(), &["rev-parse", "f"]),
            remote_head(&repo, "f")
        );
    }

    #[test]
    fn existing_branch_pointing_elsewhere_is_failed() {
        let repo = TestRepo::new();
        let runner = Runner::new();
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        // Local diverges from the remote.
        git(repo.work(), &["checkout", "f"]);
        repo.commit_file(repo.work(), "local.txt", "local");
        let error =
            create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap_err();
        assert!(!error.is_uncertain());
        assert!(error.to_string().contains("locally"));
    }

    #[test]
    fn existing_unrelated_remote_branch_is_failed() {
        let repo = TestRepo::new();
        let other = repo.clone_origin("other");
        git(&other, &["checkout", "--orphan", "f"]);
        repo.commit_file(&other, "x.txt", "x");
        git(&other, &["push", "origin", "f"]);
        let error = create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("main"))
            .unwrap_err();
        assert!(!error.is_uncertain());
        assert!(error.to_string().contains("not based on"));
    }

    #[test]
    fn missing_base_is_failed() {
        let repo = TestRepo::new();
        let error = create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("nope"))
            .unwrap_err();
        assert!(!error.is_uncertain());
    }
}

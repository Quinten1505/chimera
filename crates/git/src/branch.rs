//! Base branch resolution and feature branch creation.

use std::path::Path;

use chimera_core::BranchName;

use crate::error::GitError;
use crate::runner::{Effect, Runner};

const DEVELOP: &str = "develop";

/// Fetches `origin` and returns `develop` if `origin/develop` exists, otherwise the remote's
/// default branch. Fails if the default branch cannot be determined.
pub(crate) fn resolve_base_branch(runner: &Runner, dir: &Path) -> Result<BranchName, GitError> {
    fetch(runner, dir)?;
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
/// tracking.
///
/// Reconciliation policy: the expected commit is the tip of `origin/<base>` as just fetched. An
/// existing branch (on the remote, locally, or both) is accepted only if every copy points exactly
/// at that commit; the missing copy is then completed (local branch created from the remote, or
/// pushed to the remote). A branch at any other commit, including an ancestor or descendant of the
/// base tip, or with differing local and remote copies, is a failure. A branch that has since moved
/// on after a completed creation is reconciled from the remote head by the caller, not here.
pub(crate) fn create_feature_branch(
    runner: &Runner,
    dir: &Path,
    feature: &BranchName,
    base: &BranchName,
) -> Result<(), GitError> {
    let name = feature.as_str();
    runner.run(dir, &["check-ref-format", "--branch", name], Effect::Read)?;
    fetch(runner, dir)?;

    let local_ref_name = format!("refs/heads/{name}");
    let base_tip = local_ref(runner, dir, &format!("refs/remotes/origin/{base}"))?
        .ok_or_else(|| failed("branch", format!("origin has no branch {base}")))?;
    let local = local_ref(runner, dir, &local_ref_name)?;
    let remote = remote_ref(runner, dir, &local_ref_name)?;

    for (place, commit) in [("locally", &local), ("on origin", &remote)] {
        if let Some(commit) = commit
            && *commit != base_tip
        {
            return Err(failed(
                "branch",
                format!(
                    "{name} already exists {place} at {commit}, not at the expected origin/{base} commit {base_tip}"
                ),
            ));
        }
    }

    if remote.is_some() {
        if local.is_none() {
            runner.run(dir, &["branch", name, &base_tip], Effect::Local)?;
        }
        let upstream = format!("origin/{name}");
        runner.run(
            dir,
            &["branch", "--set-upstream-to", &upstream, name],
            Effect::Local,
        )?;
    } else {
        if local.is_none() {
            runner.run(dir, &["branch", name, &base_tip], Effect::Local)?;
        }
        let refspec = format!("{local_ref_name}:{local_ref_name}");
        runner.run(
            dir,
            &["push", "--set-upstream", "origin", &refspec],
            Effect::Remote,
        )?;
    }
    Ok(())
}

/// Fetches `origin`, pruning tracking refs for branches deleted on the remote.
fn fetch(runner: &Runner, dir: &Path) -> Result<(), GitError> {
    runner.run(dir, &["fetch", "--prune", "origin"], Effect::Read)?;
    Ok(())
}

/// The commit a ref in the local repository points to, if exactly that ref exists.
fn local_ref(runner: &Runner, dir: &Path, name: &str) -> Result<Option<String>, GitError> {
    // `for-each-ref` treats the pattern as a prefix, so compare the returned ref names.
    let output = runner.run(
        dir,
        &["for-each-ref", "--format=%(objectname) %(refname)", name],
        Effect::Read,
    )?;
    Ok(exact_ref(&output.stdout, name, ' '))
}

/// The commit a ref on `origin` points to, read from the remote itself.
fn remote_ref(runner: &Runner, dir: &Path, name: &str) -> Result<Option<String>, GitError> {
    // `ls-remote` patterns also match ref-name suffixes, so compare the returned ref names.
    let output = runner.run(dir, &["ls-remote", "origin", name], Effect::Read)?;
    Ok(exact_ref(&output.stdout, name, '\t'))
}

fn exact_ref(output: &str, name: &str, separator: char) -> Option<String> {
    output.lines().find_map(|line| {
        let (commit, refname) = line.split_once(separator)?;
        (refname.trim() == name).then(|| commit.to_owned())
    })
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
    fn base_ignores_child_ref_of_develop() {
        let repo = TestRepo::new();
        git(repo.work(), &["push", "origin", "main:develop/child"]);
        let base = resolve_base_branch(&Runner::new(), repo.work()).unwrap();
        assert_eq!(base, name("main"));
        let error =
            create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("develop"))
                .unwrap_err();
        assert!(!error.is_uncertain());
    }

    #[test]
    fn remote_suffix_match_is_not_the_feature_branch() {
        let repo = TestRepo::new();
        git(
            repo.work(),
            &["push", "origin", "main:refs/heads/other/refs/heads/f"],
        );
        let lookup = git(repo.work(), &["ls-remote", "origin", "refs/heads/f"]);
        assert!(
            lookup.ends_with("refs/heads/other/refs/heads/f"),
            "{lookup}"
        );
        create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("main")).unwrap();
        assert_eq!(remote_head(&repo, "f"), remote_head(&repo, "main"));
        assert_eq!(git(repo.work(), &["config", "branch.f.remote"]), "origin");
        assert_eq!(
            git(repo.work(), &["config", "branch.f.merge"]),
            "refs/heads/f"
        );
    }

    #[test]
    fn externally_deleted_base_is_not_used() {
        let repo = TestRepo::new();
        git(repo.work(), &["push", "origin", "main:develop"]);
        let runner = Runner::new();
        assert_eq!(
            resolve_base_branch(&runner, repo.work()).unwrap(),
            name("develop")
        );
        git(&repo.origin(), &["update-ref", "-d", "refs/heads/develop"]);
        assert_eq!(
            resolve_base_branch(&runner, repo.work()).unwrap(),
            name("main")
        );
        let error =
            create_feature_branch(&runner, repo.work(), &name("f"), &name("develop")).unwrap_err();
        assert!(!error.is_uncertain());
    }

    #[test]
    fn local_only_branch_ahead_of_base_is_failed() {
        let repo = TestRepo::new();
        git(repo.work(), &["checkout", "-b", "f"]);
        repo.commit_file(repo.work(), "local.txt", "local");
        let error = create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("main"))
            .unwrap_err();
        assert!(!error.is_uncertain());
        assert!(error.to_string().contains("expected"));
    }

    #[test]
    fn local_only_branch_at_base_is_pushed() {
        let repo = TestRepo::new();
        git(repo.work(), &["branch", "f", "main"]);
        create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("main")).unwrap();
        assert_eq!(remote_head(&repo, "f"), remote_head(&repo, "main"));
    }

    #[test]
    fn matching_ancestor_or_descendant_branches_are_failed() {
        let repo = TestRepo::new();
        let runner = Runner::new();
        // Remote branch ahead of base, with local at the same commit.
        let other = repo.clone_origin("other");
        git(&other, &["checkout", "-b", "ahead"]);
        repo.commit_file(&other, "a.txt", "a");
        git(&other, &["push", "origin", "ahead:ahead"]);
        git(repo.work(), &["fetch", "origin"]);
        git(repo.work(), &["branch", "ahead", "origin/ahead"]);
        let error =
            create_feature_branch(&runner, repo.work(), &name("ahead"), &name("main")).unwrap_err();
        assert!(!error.is_uncertain());
        // Both advance: base moves past an existing feature branch.
        create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap();
        repo.commit_file(&other, "b.txt", "b");
        git(&other, &["push", "origin", "HEAD:main"]);
        let error =
            create_feature_branch(&runner, repo.work(), &name("f"), &name("main")).unwrap_err();
        assert!(!error.is_uncertain());
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
        assert!(error.to_string().contains("expected"));
    }

    #[test]
    fn missing_base_is_failed() {
        let repo = TestRepo::new();
        let error = create_feature_branch(&Runner::new(), repo.work(), &name("f"), &name("nope"))
            .unwrap_err();
        assert!(!error.is_uncertain());
    }
}

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use chimera_core::{Outcome, TurnOutcome, TurnResult};

use crate::StoreError;
use crate::atomic::sync_directory;

const HISTORY_FILE: &str = "history.md";

/// An entry carries its turn as one line of this form, after the readable summary.
const RECORD_PREFIX: &str = "<!-- chimera-turn ";
const RECORD_SUFFIX: &str = " -->";

/// A line starting with this outside a fence opens an HTML comment that hides everything up to the
/// line containing [`COMMENT_CLOSE`] when the Markdown is rendered.
const COMMENT_OPEN: &str = "<!--";
const COMMENT_CLOSE: &str = "-->";

/// Serializes appends within this process so entries from different pipeline instances never
/// interleave; each entry is also written with a single `write_all` to a file opened for append.
static APPEND_LOCK: Mutex<()> = Mutex::new(());

/// Why the turn took place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// The port does not say why a turn took place, so the store records no kind for its turns yet.
#[allow(dead_code)]
pub(crate) enum TurnKind {
    /// The first turn of an assignment.
    Initial,
    /// A turn asking the agent to correct invalid output.
    Correction,
}

/// One completed agent turn, as recorded in `history.md`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HistoryEntry<'a> {
    pub time: SystemTime,
    /// The pipeline instance that ran the turn, when known.
    pub pipeline: Option<&'a str>,
    /// Why the turn took place, when known.
    pub kind: Option<TurnKind>,
    pub turn: &'a TurnResult,
}

/// Appends `entry` to `history.md` in `run_directory`, creating the file on first append. Existing
/// content is never rewritten; the entry and the file's directory entry are flushed to disk before
/// this returns. On failure the entry is removed from the file again, durably, or the error is
/// [`StoreError::Unreverted`]; if the process dies mid-append instead, the next append first closes
/// what the interrupted entry left open, so that entry is never loaded and later entries are, and
/// are visible when rendered.
pub(crate) fn append_history(
    run_directory: &Path,
    entry: &HistoryEntry<'_>,
) -> Result<(), StoreError> {
    append_history_with(run_directory, entry, &mut |_| Ok(()))
}

/// A file system operation of an append, before which tests can inject a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Write,
    SyncFile,
    SyncDirectory,
    Truncate,
    SyncTruncation,
}

/// `fault` runs before each step; an error it returns is that step's failure.
fn append_history_with(
    run_directory: &Path,
    entry: &HistoryEntry<'_>,
    fault: &mut dyn FnMut(Step) -> io::Result<()>,
) -> Result<(), StoreError> {
    let path: PathBuf = run_directory.join(HISTORY_FILE);
    let _guard = APPEND_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let existing = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(StoreError::io(&path, e)),
    };
    let mut text = repair(&existing);
    text.push_str(&render(entry));
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|e| StoreError::io(&path, e))?;
    let length = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
    let written = fault(Step::Write)
        .and_then(|()| file.write_all(text.as_bytes()))
        .and_then(|()| fault(Step::SyncFile))
        .and_then(|()| file.sync_all())
        .map_err(|e| StoreError::io(&path, e))
        // The file may be new, or left by an append that died before flushing its directory
        // entry, which is lost in a crash until then, so the directory is flushed every time.
        .and_then(|()| {
            fault(Step::SyncDirectory)
                .and_then(|()| sync_directory(run_directory))
                .map_err(|e| StoreError::io(run_directory, e))
        });
    let Err(error) = written else {
        return Ok(());
    };
    // A failed append must leave no entry behind, or a retry would record the turn twice; it is
    // only known not to have happened once its removal is on disk.
    let reverted = fault(Step::Truncate)
        .and_then(|()| file.set_len(length))
        .and_then(|()| fault(Step::SyncTruncation))
        .and_then(|()| file.sync_all());
    Err(match reverted {
        Ok(()) => error,
        Err(revert) => StoreError::Unreverted {
            path,
            source: Box::new(error),
            revert,
        },
    })
}

/// What must precede the next entry so that it starts on its own line outside any fence or HTML
/// comment, when the file ends in an interrupted entry. An interrupted entry has no complete
/// record, so it is never loaded as a turn; closing its comment on a line of its own keeps it so.
fn repair(existing: &[u8]) -> String {
    let mut text = String::new();
    if !existing.is_empty() && !existing.ends_with(b"\n") {
        text.push('\n');
    }
    let existing = String::from_utf8_lossy(existing);
    let scan = scan(&existing);
    if let Some(ticks) = scan.open_fence {
        text.push_str(&"`".repeat(ticks));
        text.push('\n');
    }
    if scan.open_comment {
        text.push_str(COMMENT_CLOSE);
        text.push('\n');
    }
    text
}

struct Scan<'a> {
    /// The JSON of each complete record, in file order.
    records: Vec<&'a str>,
    /// The number of backticks of the fence still open at the end of the text.
    open_fence: Option<usize>,
    /// Whether an HTML comment is still open at the end of the text.
    open_comment: bool,
}

/// Finds the records of complete entries. Turn output is never read as structure: lines inside a
/// fence are skipped, as are lines inside an HTML comment, which rendering hides. A record only
/// counts when followed by an empty line, the last thing an entry writes, so an entry cut anywhere
/// has none.
fn scan(text: &str) -> Scan<'_> {
    let lines: Vec<&str> = text.lines().collect();
    let mut records = Vec::new();
    let mut fence: Option<usize> = None;
    let mut comment = false;
    for (n, line) in lines.iter().enumerate() {
        let ticks = line.chars().take_while(|&c| c == '`').count();
        if comment {
            comment = !line.contains(COMMENT_CLOSE);
            continue;
        }
        match fence {
            Some(open) if ticks >= open && line.len() == ticks => fence = None,
            Some(_) => {}
            None if ticks >= 3 => fence = Some(ticks),
            None => {
                if let Some(json) = line
                    .strip_prefix(RECORD_PREFIX)
                    .and_then(|rest| rest.strip_suffix(RECORD_SUFFIX))
                    && lines.get(n + 1) == Some(&"")
                {
                    records.push(json);
                }
                comment = line.starts_with(COMMENT_OPEN) && !line.contains(COMMENT_CLOSE);
            }
        }
    }
    Scan {
        records,
        open_fence: fence,
        open_comment: comment,
    }
}

/// Reads the turns recorded in `history.md` in append order; a run without turns has none. The
/// turns come from the machine-readable record line of each entry, so `history.md` is the only
/// source of truth.
pub(crate) fn load_turns(run_directory: &Path) -> Result<Vec<TurnResult>, StoreError> {
    let path = run_directory.join(HISTORY_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(StoreError::io(&path, e)),
    };
    // An interrupted append can leave a partial character, always in an entry that is not loaded.
    let text = String::from_utf8_lossy(&bytes);
    scan(&text)
        .records
        .into_iter()
        .map(|json| {
            serde_json::from_str(json).map_err(|source| StoreError::Deserialize {
                path: path.clone(),
                source,
            })
        })
        .collect()
}

fn render(entry: &HistoryEntry<'_>) -> String {
    let turn = entry.turn;
    let (result, detail, output) = match &turn.outcome {
        TurnOutcome::Valid(outcome) => {
            let (name, explanation) = outcome_parts(outcome);
            (name, None, explanation)
        }
        TurnOutcome::Invalid { output, problem } => ("Invalid", Some(problem), output.as_str()),
    };
    let fence = "`".repeat((longest_backtick_run(output) + 1).max(3));
    let mut heading = format_time(entry.time);
    if let Some(pipeline) = entry.pipeline {
        heading.push_str(&format!(" · {}", inline(pipeline)));
    }
    heading.push_str(&format!(" · {:?}", turn.role));
    match entry.kind {
        Some(TurnKind::Initial) => heading.push_str(" · initial"),
        Some(TurnKind::Correction) => heading.push_str(" · correction"),
        None => {}
    }
    let mut text = format!(
        "## {heading}\n\n- Agent: {}\n- Result: {result}\n",
        inline(turn.agent.as_str()),
    );
    if let Some(problem) = detail {
        text.push_str(&format!("- Problem: {}\n", inline(problem)));
    }
    text.push_str(&format!("\n{fence}text\n{output}\n{fence}\n"));
    // The record comes last, and the empty line after it completes the entry.
    text.push_str(&format!(
        "\n{RECORD_PREFIX}{}{RECORD_SUFFIX}\n\n",
        record(turn)
    ));
    text
}

/// The turn as one line of JSON that cannot end the comment it is written in.
fn record(turn: &TurnResult) -> String {
    serde_json::to_string(turn)
        .expect("a turn serializes to JSON")
        .replace('>', "\\u003e")
}

fn outcome_parts(outcome: &Outcome) -> (&'static str, &str) {
    match outcome {
        Outcome::ImplementationReady(e) => ("ImplementationReady", e),
        Outcome::ReviewApproved(e) => ("ReviewApproved", e),
        Outcome::ChangesRequested(e) => ("ChangesRequested", e),
        Outcome::MergeReadyForConflictReview(e) => ("MergeReadyForConflictReview", e),
        Outcome::MergeSuccessful(e) => ("MergeSuccessful", e),
        Outcome::MergeBlocked(e) => ("MergeBlocked", e),
    }
}

/// A value for a single line: line breaks and other control characters become spaces.
fn inline(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn longest_backtick_run(text: &str) -> usize {
    text.split(|c| c != '`').map(str::len).max().unwrap_or(0)
}

/// RFC 3339 UTC with second precision.
fn format_time(time: SystemTime) -> String {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    };
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use chimera_core::{AgentId, Role};

    use super::*;

    fn valid(explanation: &str) -> TurnResult {
        TurnResult {
            agent: AgentId::new("agent-1").unwrap(),
            role: Role::Review,
            outcome: TurnOutcome::Valid(Outcome::ReviewApproved(explanation.into())),
        }
    }

    fn invalid(output: &str) -> TurnResult {
        TurnResult {
            agent: AgentId::new("agent-1").unwrap(),
            role: Role::Implementation,
            outcome: TurnOutcome::Invalid {
                output: output.into(),
                problem: "no handoff".into(),
            },
        }
    }

    fn append(dir: &Path, pipeline: &str, kind: TurnKind, turn: &TurnResult) {
        let entry = HistoryEntry {
            time: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            pipeline: Some(pipeline),
            kind: Some(kind),
            turn,
        };
        append_history(dir, &entry).unwrap();
    }

    fn read(dir: &Path) -> String {
        fs::read_to_string(dir.join("history.md")).unwrap()
    }

    fn headings(text: &str) -> Vec<&str> {
        text.lines().filter(|l| l.starts_with("## ")).collect()
    }

    #[test]
    fn formats_time_as_utc() {
        assert_eq!(format_time(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_time(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            format_time(UNIX_EPOCH + Duration::from_secs(1_709_164_800)),
            "2024-02-29T00:00:00Z"
        );
    }

    #[test]
    fn creates_file_and_records_entry_fields() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!dir.path().join("history.md").exists());
        append(
            dir.path(),
            "ticket-7",
            TurnKind::Initial,
            &valid("looks good"),
        );
        let text = read(dir.path());
        assert!(text.starts_with("## 2023-11-14T22:13:20Z · ticket-7 · Review · initial\n"));
        assert!(text.contains("- Agent: agent-1"));
        assert!(text.contains("- Result: ReviewApproved"));
        assert!(text.contains("looks good"));
    }

    #[test]
    fn entries_appear_in_order_and_existing_content_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        append(dir.path(), "p1", TurnKind::Initial, &valid("first"));
        let after_first = read(dir.path());
        append(dir.path(), "p2", TurnKind::Initial, &valid("second"));
        let after_second = read(dir.path());
        append(dir.path(), "p1", TurnKind::Initial, &valid("third"));
        let after_third = read(dir.path());

        assert!(after_second.starts_with(&after_first));
        assert!(after_third.starts_with(&after_second));
        let (first, second, third) = (
            after_third.find("first").unwrap(),
            after_third.find("second").unwrap(),
            after_third.find("third").unwrap(),
        );
        assert!(first < second && second < third);
    }

    #[test]
    fn does_not_truncate_preexisting_content() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("history.md"), "# earlier notes\n").unwrap();
        append(dir.path(), "p1", TurnKind::Initial, &valid("x"));
        assert!(read(dir.path()).starts_with("# earlier notes\n## "));
    }

    #[test]
    fn records_correction_and_no_handoff_turns() {
        let dir = tempfile::tempdir().unwrap();
        append(dir.path(), "p1", TurnKind::Initial, &invalid("rambling"));
        append(dir.path(), "p1", TurnKind::Correction, &valid("fixed"));
        let text = read(dir.path());
        assert_eq!(headings(&text).len(), 2);
        assert!(headings(&text)[0].ends_with("Implementation · initial"));
        assert!(text.contains("- Result: Invalid\n- Problem: no handoff"));
        assert!(text.contains("rambling"));
        assert!(headings(&text)[1].ends_with("Review · correction"));
    }

    #[test]
    fn output_with_markdown_cannot_break_later_entries() {
        let dir = tempfile::tempdir().unwrap();
        let hostile = "# fake heading\n```\n## another fake\n````\n```rust\nunclosed";
        append(dir.path(), "p1", TurnKind::Initial, &valid(hostile));
        append(dir.path(), "p2", TurnKind::Initial, &valid("after"));
        let text = read(dir.path());

        // Outside fences, only the two real headings remain.
        let mut in_fence: Option<usize> = None;
        let mut real = Vec::new();
        for line in text.lines() {
            let ticks = line.chars().take_while(|&c| c == '`').count();
            match in_fence {
                Some(open) if ticks >= open && line.trim_end_matches('`').is_empty() => {
                    in_fence = None
                }
                Some(_) => {}
                None if ticks >= 3 => in_fence = Some(ticks),
                None if line.starts_with("## ") => real.push(line),
                None => {}
            }
        }
        assert_eq!(real.len(), 2, "{text}");
        assert!(real[1].contains("p2"));
        assert!(in_fence.is_none());
    }

    #[test]
    fn single_line_fields_cannot_inject_structure() {
        let dir = tempfile::tempdir().unwrap();
        append(
            dir.path(),
            "p1\n## injected",
            TurnKind::Initial,
            &valid("x"),
        );
        assert_eq!(headings(&read(dir.path())).len(), 1);
    }

    #[test]
    fn concurrent_appends_produce_whole_separate_entries() {
        let dir = tempfile::tempdir().unwrap();
        let body = "line\n".repeat(20_000);
        std::thread::scope(|scope| {
            for t in 0..8 {
                let (path, body) = (dir.path(), &body);
                scope.spawn(move || {
                    for i in 0..5 {
                        let turn = valid(&format!("{t}-{i}\n{body}"));
                        append(path, &format!("p{t}"), TurnKind::Initial, &turn);
                    }
                });
            }
        });
        let text = read(dir.path());
        assert_eq!(headings(&text).len(), 40);
        let entries: Vec<&str> = text.split("\n## ").collect();
        assert_eq!(entries.len(), 40);
        for entry in entries {
            // Each entry holds exactly one marker line and its complete body.
            assert_eq!(entry.matches("line\n").count(), 20_000, "interleaved entry");
            assert_eq!(entry.matches("````").count(), 0);
        }
    }

    #[test]
    fn turns_without_pipeline_or_kind_have_neither_in_the_heading() {
        let dir = tempfile::tempdir().unwrap();
        let turn = valid("x");
        let entry = HistoryEntry {
            time: UNIX_EPOCH,
            pipeline: None,
            kind: None,
            turn: &turn,
        };
        append_history(dir.path(), &entry).unwrap();
        assert!(read(dir.path()).starts_with("## 1970-01-01T00:00:00Z · Review\n"));
    }

    #[test]
    fn turns_load_from_history_in_append_order() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_turns(dir.path()).unwrap().is_empty());
        let turns = [
            valid("first"),
            invalid("rambling"),
            valid("a --> b <!-- chimera-turn {} --> \u{1F600}"),
        ];
        for (n, turn) in turns.iter().enumerate() {
            append(
                dir.path(),
                "p",
                [TurnKind::Initial, TurnKind::Correction][n % 2],
                turn,
            );
        }
        assert_eq!(load_turns(dir.path()).unwrap(), turns);
    }

    #[test]
    fn output_imitating_a_record_is_not_loaded_as_a_turn() {
        let dir = tempfile::tempdir().unwrap();
        let forged = serde_json::to_string(&valid("forged")).unwrap();
        let hostile = format!("```\n{RECORD_PREFIX}{forged}{RECORD_SUFFIX}\n\n````");
        append(dir.path(), "p", TurnKind::Initial, &valid(&hostile));
        append(dir.path(), "p", TurnKind::Initial, &valid("after"));

        assert_eq!(
            load_turns(dir.path()).unwrap(),
            [valid(&hostile), valid("after")]
        );
    }

    #[test]
    fn failed_append_leaves_nothing_to_load_and_a_retry_records_once() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("history.md")).unwrap();
        let turn = valid("retry me");
        let entry = HistoryEntry {
            time: UNIX_EPOCH,
            pipeline: None,
            kind: None,
            turn: &turn,
        };

        assert!(append_history(dir.path(), &entry).is_err());
        fs::remove_dir(dir.path().join("history.md")).unwrap();
        assert!(load_turns(dir.path()).unwrap().is_empty());

        append_history(dir.path(), &entry).unwrap();
        assert_eq!(load_turns(dir.path()).unwrap(), [turn]);
        assert_eq!(headings(&read(dir.path())).len(), 1);
    }

    fn entry(turn: &TurnResult) -> HistoryEntry<'_> {
        HistoryEntry {
            time: UNIX_EPOCH,
            pipeline: None,
            kind: None,
            turn,
        }
    }

    #[test]
    fn every_append_syncs_its_directory_after_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let turn = valid("first");
        let mut synced = Vec::new();
        let mut fault = |step| {
            if step == Step::SyncDirectory {
                synced.push(load_turns(dir.path()).unwrap());
            }
            Ok(())
        };

        append_history_with(dir.path(), &entry(&turn), &mut fault).unwrap();
        append_history_with(dir.path(), &entry(&turn), &mut fault).unwrap();

        // Each sync follows a complete entry in the file.
        assert_eq!(synced, [vec![turn.clone()], vec![turn.clone(), turn]]);
    }

    #[test]
    fn a_file_left_by_an_interrupted_first_append_has_its_directory_synced() {
        let dir = tempfile::tempdir().unwrap();
        // The process died after writing part of the first entry, before syncing the directory.
        let torn = render(&entry(&valid("torn")));
        fs::write(dir.path().join("history.md"), &torn[..torn.len() / 2]).unwrap();
        let turn = valid("next");
        let mut steps = Vec::new();

        append_history_with(dir.path(), &entry(&turn), &mut |step| {
            steps.push(step);
            Ok(())
        })
        .unwrap();

        assert_eq!(steps, [Step::Write, Step::SyncFile, Step::SyncDirectory]);
        assert_eq!(load_turns(dir.path()).unwrap(), [turn]);
    }

    /// Runs an append whose `failing` steps fail, after `before` was appended; returns the error,
    /// the steps that ran and whether the file changed.
    fn fail_append(before: &[TurnResult], failing: &[Step]) -> (StoreError, Vec<Step>, bool) {
        let dir = tempfile::tempdir().unwrap();
        for turn in before {
            append_history(dir.path(), &entry(turn)).unwrap();
        }
        let path = dir.path().join("history.md");
        let previous = fs::read(&path).ok();
        let mut steps = Vec::new();
        let turn = valid("retry me");

        let error = append_history_with(dir.path(), &entry(&turn), &mut |step| {
            steps.push(step);
            match failing.contains(&step) {
                true => Err(io::Error::other(format!("{step:?} on fire"))),
                false => Ok(()),
            }
        })
        .unwrap_err();

        let content = fs::read(&path).unwrap();
        let changed = content != previous.unwrap_or_default();
        if !changed {
            // The retry records the turn once.
            append_history(dir.path(), &entry(&turn)).unwrap();
            let mut expected = before.to_vec();
            expected.push(turn);
            assert_eq!(load_turns(dir.path()).unwrap(), expected);
        }
        (error, steps, changed)
    }

    #[test]
    fn a_failed_append_is_removed_durably_and_reported_as_failed() {
        for before in [vec![], vec![valid("earlier")]] {
            for (failing, ran) in [
                (Step::Write, 1),
                (Step::SyncFile, 2),
                (Step::SyncDirectory, 3),
            ] {
                let (error, steps, changed) = fail_append(&before, &[failing]);

                assert!(matches!(error, StoreError::Io { .. }), "{failing:?}");
                assert!(error.to_string().contains(&format!("{failing:?} on fire")));
                let all = [Step::Write, Step::SyncFile, Step::SyncDirectory];
                let mut expected = all[..ran].to_vec();
                expected.extend([Step::Truncate, Step::SyncTruncation]);
                assert_eq!(steps, expected, "{failing:?}");
                assert!(!changed, "{failing:?}");
                assert!(chimera_core::error::PortError::from(error).is_failed());
            }
        }
    }

    #[test]
    fn a_failed_append_whose_removal_fails_is_uncertain() {
        for before in [vec![], vec![valid("earlier")]] {
            for failing in [Step::Write, Step::SyncFile, Step::SyncDirectory] {
                for revert in [Step::Truncate, Step::SyncTruncation] {
                    let (error, steps, _) = fail_append(&before, &[failing, revert]);

                    assert_eq!(steps.last(), Some(&revert));
                    let text = error.to_string();
                    assert!(matches!(error, StoreError::Unreverted { .. }), "{text}");
                    assert!(text.contains(&format!("{failing:?} on fire")), "{text}");
                    assert!(text.contains(&format!("{revert:?} on fire")), "{text}");
                    assert!(chimera_core::error::PortError::from(error).is_uncertain());
                }
            }
        }
        // When the entry could not be removed, it may well be in the file.
        let (_, _, changed) = fail_append(&[], &[Step::SyncDirectory, Step::Truncate]);
        assert!(changed);
    }

    #[test]
    fn interrupted_record_comment_is_closed_on_its_own_line() {
        let cut = format!("{RECORD_PREFIX}{{\"agent\":");
        assert_eq!(repair(cut.as_bytes()), "\n-->\n");
        assert_eq!(repair(format!("{cut} --").as_bytes()), "\n-->\n");
        // A complete record line, or a comment inside a fence, needs no terminator.
        assert_eq!(repair(format!("{cut}{RECORD_SUFFIX}").as_bytes()), "\n");
        assert_eq!(repair(b"```text\n<!-- x\n"), "```\n");
        // A comment stays open over later lines until one closes it.
        assert_eq!(repair(format!("{cut}\n-").as_bytes()), "\n-->\n");
        assert_eq!(repair(format!("{cut}\n-\n--").as_bytes()), "\n-->\n");
        assert_eq!(repair(format!("{cut}\n-\n-->").as_bytes()), "\n");
    }

    /// Adds to `states` the text, then every text its repair leaves when interrupted, up to
    /// `depth` interruptions deep.
    fn interrupted_repairs(text: String, depth: usize, states: &mut Vec<String>) {
        let repair = repair(text.as_bytes());
        if depth > 0 {
            for cut in 1..repair.len() {
                interrupted_repairs(format!("{text}{}", &repair[..cut]), depth - 1, states);
            }
        }
        states.push(text);
    }

    /// The text of each level-2 heading and code block of the rendered Markdown.
    fn rendered(markdown: &str) -> (Vec<String>, Vec<String>) {
        use pulldown_cmark::{Event, HeadingLevel, Parser, Tag, TagEnd};

        let (mut headings, mut blocks) = (Vec::new(), Vec::new());
        let mut current: Option<String> = None;
        for event in Parser::new(markdown) {
            match event {
                Event::Start(Tag::Heading {
                    level: HeadingLevel::H2,
                    ..
                })
                | Event::Start(Tag::CodeBlock(_)) => current = Some(String::new()),
                Event::Text(text) => {
                    if let Some(current) = &mut current {
                        current.push_str(&text);
                    }
                }
                Event::End(TagEnd::Heading(HeadingLevel::H2)) => headings.extend(current.take()),
                Event::End(TagEnd::CodeBlock) => blocks.extend(current.take()),
                _ => {}
            }
        }
        (headings, blocks)
    }

    #[test]
    fn repeatedly_interrupted_repairs_keep_later_entries_loaded_and_visible() {
        let committed = valid("committed");
        let after = valid("after");
        let prefix = render(&entry(&committed));
        let torn = render(&entry(&invalid("torn\n```\n## fake")));
        let mut states = Vec::new();
        // Cuts inside a character are covered by the restart tests.
        for cut in (0..torn.len()).filter(|&cut| torn.is_char_boundary(cut)) {
            interrupted_repairs(format!("{prefix}{}", &torn[..cut]), 3, &mut states);
        }

        for state in states {
            let text = format!(
                "{state}{}{}",
                repair(state.as_bytes()),
                render(&entry(&after))
            );
            let loaded: Result<Vec<TurnResult>, _> = scan(&text)
                .records
                .into_iter()
                .map(serde_json::from_str)
                .collect();
            assert_eq!(
                loaded.unwrap(),
                [committed.clone(), after.clone()],
                "{text}"
            );
            let (headings, blocks) = rendered(&text);
            assert_eq!(
                headings.last().unwrap(),
                "1970-01-01T00:00:00Z · Review",
                "{text}"
            );
            assert_eq!(blocks.last().unwrap(), "after\n", "{text}");
        }
    }

    #[test]
    fn corrupt_record_is_a_deserialize_error() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("history.md"),
            format!("{RECORD_PREFIX}{{{RECORD_SUFFIX}\n\n"),
        )
        .unwrap();

        let error = load_turns(dir.path()).unwrap_err();

        assert!(matches!(error, StoreError::Deserialize { .. }));
    }
}

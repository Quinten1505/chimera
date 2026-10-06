use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use chimera_core::{Outcome, TurnOutcome, TurnResult};

use crate::StoreError;

const HISTORY_FILE: &str = "history.md";

/// Serializes appends within this process so entries from different pipeline instances never
/// interleave; each entry is also written with a single `write_all` to a file opened for append.
static APPEND_LOCK: Mutex<()> = Mutex::new(());

/// Why the turn took place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnKind {
    /// The first turn of an assignment.
    Initial,
    /// A turn asking the agent to correct invalid output.
    Correction,
}

/// One completed agent turn, as recorded in `history.md`.
#[derive(Debug, Clone, Copy)]
pub struct HistoryEntry<'a> {
    pub time: SystemTime,
    /// The pipeline instance that ran the turn.
    pub pipeline: &'a str,
    pub kind: TurnKind,
    pub turn: &'a TurnResult,
}

/// Appends `entry` to `history.md` in `run_directory`, creating the file on first append. Existing
/// content is never rewritten; the entry is flushed to disk before this returns.
pub fn append_history(run_directory: &Path, entry: &HistoryEntry<'_>) -> Result<(), StoreError> {
    let path: PathBuf = run_directory.join(HISTORY_FILE);
    let text = render(entry);
    let _guard = APPEND_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|e| StoreError::io(&path, e))?;
    file.write_all(text.as_bytes())
        .map_err(|e| StoreError::io(&path, e))?;
    file.sync_all().map_err(|e| StoreError::io(&path, e))
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
    let kind = match entry.kind {
        TurnKind::Initial => "initial",
        TurnKind::Correction => "correction",
    };
    let fence = "`".repeat((longest_backtick_run(output) + 1).max(3));
    let mut text = format!(
        "## {} · {} · {:?} · {kind}\n\n- Agent: {}\n- Result: {result}\n",
        format_time(entry.time),
        inline(entry.pipeline),
        turn.role,
        inline(turn.agent.as_str()),
    );
    if let Some(problem) = detail {
        text.push_str(&format!("- Problem: {}\n", inline(problem)));
    }
    text.push_str(&format!("\n{fence}text\n{output}\n{fence}\n\n"));
    text
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
            pipeline,
            kind,
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
}

// Exercises durable run nesting and recovery.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn root() -> PathBuf {
    let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("pippo-runs-{}-{nonce}", std::process::id()))
}

fn input(parent: Option<String>) -> RunCreate {
    RunCreate {
        parent_id: parent,
        task_id: Some("t_1234abcd".into()),
        project_id: Some("pippo_123abc".into()),
        role: RunRole::Planner,
        title: "inspect project".into(),
        request: "Find the relevant code.".into(),
        constraints: vec!["read only".into()],
        media: vec![1],
        related: vec!["t_87654321".into()],
        highlight: vec![PathBuf::from("report.md")],
    }
}

#[test]
fn creates_nested_run_trees_with_durable_metadata() {
    let root = root();
    let runs = Runs::open(root.clone()).unwrap();
    let parent = runs.create(input(None)).unwrap();
    let child = runs.create(input(Some(parent.id.clone()))).unwrap();
    assert!(parent.id.starts_with("r_") && parent.id.len() == 10);
    assert!(child.id.starts_with("r_") && child.id.len() == 10);
    let entries = runs.snapshot().unwrap();
    let parent_path = entries
        .iter()
        .find(|(meta, _)| meta.id == parent.id)
        .unwrap()
        .1
        .clone();
    let child_path = entries
        .iter()
        .find(|(meta, _)| meta.id == child.id)
        .unwrap()
        .1
        .clone();
    let parent_path = parent_path.unwrap();
    let child_path = child_path.unwrap();
    assert_eq!(child_path, parent_path.join("runs").join(&child.id));
    for path in [&parent_path, &child_path] {
        assert!(path.join("meta.json").is_file());
        assert!(path.join("messages.jsonl").is_file());
        assert_eq!(fs::read(path.join("replay.json")).unwrap(), b"[]\n");
    }
    let saved: RunMeta =
        serde_json::from_slice(&fs::read(child_path.join("meta.json")).unwrap()).unwrap();
    assert_eq!(saved.parent_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(saved.constraints, vec!["read only"]);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn enforces_scout_and_parent_scope_rules() {
    let root = root();
    let runs = Runs::open(root.clone()).unwrap();
    let mut scout = input(None);
    scout.task_id = None;
    scout.project_id = None;
    scout.role = RunRole::Explorer;
    let scout = runs.create(scout).unwrap();
    assert!(runs
        .snapshot()
        .unwrap()
        .iter()
        .find(|(meta, _)| meta.id == scout.id)
        .unwrap()
        .1
        .is_none());
    assert_eq!(fs::read_dir(root.join("session/runs")).unwrap().count(), 0);
    let mut invalid = input(Some(scout.id));
    invalid.task_id = Some("t_ffffffff".into());
    assert!(runs.create(invalid).is_err());
    let mut worker = input(None);
    worker.task_id = None;
    worker.project_id = None;
    worker.role = RunRole::Worker;
    assert!(runs.create(worker).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn persists_transitions_and_interrupts_running_runs_on_reopen() {
    let root = root();
    let runs = Runs::open(root.clone()).unwrap();
    let paused = runs.create(input(None)).unwrap();
    runs.update(&paused.id, RunStatus::Paused, 1, None).unwrap();
    assert!(runs.update(&paused.id, RunStatus::Done, 1, None).is_err());
    runs.update(&paused.id, RunStatus::Running, 1, None)
        .unwrap();
    runs.update(&paused.id, RunStatus::Done, 1, Some("first".into()))
        .unwrap();
    runs.update(&paused.id, RunStatus::Running, 2, None)
        .unwrap();
    runs.update(&paused.id, RunStatus::Done, 2, Some("second".into()))
        .unwrap();
    let live = runs.create(input(None)).unwrap();
    drop(runs);

    let restored = Runs::open(root.clone()).unwrap();
    let entries = restored.snapshot().unwrap();
    let resumed = entries
        .iter()
        .find(|(meta, _)| meta.id == paused.id)
        .unwrap();
    let interrupted = entries.iter().find(|(meta, _)| meta.id == live.id).unwrap();
    assert_eq!(resumed.0.status, RunStatus::Done);
    assert_eq!(resumed.0.attempt, 2);
    assert_eq!(
        fs::read_to_string(root.join(resumed.0.report_path.as_ref().unwrap())).unwrap(),
        "second"
    );
    assert!(root
        .join("projects/pippo_123abc/reports/t_1234abcd/inspect_project.md")
        .is_file());
    assert_eq!(interrupted.0.status, RunStatus::Interrupted);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resolves_only_registered_report_versions_in_stable_order() {
    let root = root();
    let runs = Runs::open(root.clone()).unwrap();
    let mut parent = input(None);
    parent.title = "parent findings".into();
    parent.related.clear();
    parent.highlight.clear();
    let parent = runs.create(parent).unwrap();
    let mut child = input(Some(parent.id.clone()));
    child.title = "child findings".into();
    child.related.clear();
    child.highlight.clear();
    let child = runs.create(child).unwrap();
    runs.update(&child.id, RunStatus::Done, 1, Some("child body".into()))
        .unwrap();
    runs.update(&parent.id, RunStatus::Done, 1, Some("old body".into()))
        .unwrap();
    runs.update(&parent.id, RunStatus::Running, 2, None)
        .unwrap();
    runs.update(&parent.id, RunStatus::Done, 2, Some("latest body".into()))
        .unwrap();

    let old = PathBuf::from("projects/pippo_123abc/reports/t_1234abcd/parent_findings.md");
    let child_path = PathBuf::from("projects/pippo_123abc/reports/t_1234abcd/child_findings.md");
    let reports = runs
        .reports(
            "pippo_123abc",
            &["t_1234abcd".into()],
            &[old.clone(), child_path.clone(), old.clone()],
        )
        .unwrap();
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[0].path, old);
    assert!(reports[0].central && reports[1].central);
    assert_eq!(reports[1].path, child_path);
    assert_eq!(
        reports[2].path,
        PathBuf::from("projects/pippo_123abc/reports/t_1234abcd/parent_findings_(2).md")
    );
    assert!(!reports[2].central);
    for invalid in [
        PathBuf::from("../parent_findings.md"),
        PathBuf::from("projects/other_ffffff/reports/t_1234abcd/parent_findings.md"),
        PathBuf::from("projects/pippo_123abc/reports/t_1234abcd/unknown.md"),
    ] {
        assert!(runs.reports("pippo_123abc", &[], &[invalid]).is_err());
    }
    fs::remove_dir_all(root).unwrap();
}

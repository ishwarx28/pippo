// Exercises approval correlation and side-effect gating at the runtime boundary.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Harness {
    root: PathBuf,
    work: PathBuf,
    shared: Arc<Shared>,
    events: mpsc::Receiver<Interaction>,
}

impl Harness {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "pippo-policy-rpc-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let work = root.join("work");
        std::fs::create_dir_all(root.join("cfg")).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(root.join("cfg/rules.yaml"), rule::DEFAULTS).unwrap();
        let (notices, _notice_input) = mpsc::channel();
        let (interactions, events) = mpsc::channel();
        let shared = Shared {
            pending: Mutex::new(Pending {
                waits: HashMap::new(),
                closed: false,
            }),
            notices: Mutex::new(Some(notices)),
            interactions: Mutex::new(Some(interactions)),
            clarify: Mutex::new(ClarifyState::default()),
            approval: Mutex::new(ApprovalState::default()),
            sheet: Mutex::new(SheetState::default()),
            key: Key,
            proj: Arc::new(Proj::open(root.clone()).unwrap()),
            rules: rule::Book::open(&root, &root).unwrap(),
            reads: write::Reads::default(),
            shells: shell::Shells::default(),
        };
        task(
            &shared,
            Some(serde_json::json!({
                "action": "create", "title": "test policy boundary", "path": work
            })),
        )
        .unwrap();
        Self {
            root,
            work,
            shared: Arc::new(shared),
            events,
        }
    }

    fn prompt(&self) -> rule::Prompt {
        match self.events.recv_timeout(Duration::from_secs(2)).unwrap() {
            Interaction::ApprovalOpened { prompt } => prompt,
            event => panic!("unexpected interaction: {event:?}"),
        }
    }

    fn closed(&self, id: &str) {
        assert!(matches!(
            self.events.recv_timeout(Duration::from_secs(2)).unwrap(),
            Interaction::ApprovalClosed { id: closed, .. } if closed == id
        ));
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn write_request(path: &Path, call: &str, content: &str) -> Value {
    serde_json::json!({
        "turn_id": "run-a", "request_id": "request-a", "call_id": call,
        "path": path, "content": content
    })
}

#[test]
fn write_waits_before_side_effect_and_session_allow_is_exact() {
    let harness = Harness::new();
    let outside = harness.root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let path = outside.join("file.txt");
    let shared = Arc::clone(&harness.shared);
    let request = write_request(&path, "write-one", "first\n");
    let running = thread::spawn(move || write(&shared, Some(request)).unwrap());

    let prompt = harness.prompt();
    assert_eq!(prompt.tool, rule::Tool::Write);
    assert_eq!(
        prompt.subject,
        std::fs::canonicalize(&outside)
            .unwrap()
            .join("file.txt")
            .to_string_lossy()
    );
    assert_eq!(prompt.rule_id, "unmatched");
    assert!(!path.exists(), "write ran before approval");
    resolve_approval(
        &harness.shared,
        &prompt.id,
        Ok(ApprovalChoice::AllowSession),
    )
    .unwrap();
    harness.closed(&prompt.id);
    assert_eq!(running.join().unwrap()["ok"], true);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\n");

    let repeated = write(
        &harness.shared,
        Some(write_request(&path, "write-two", "first\n")),
    )
    .unwrap();
    assert_eq!(repeated["ok"], true);
    assert!(harness.events.try_recv().is_err());

    let shared = Arc::clone(&harness.shared);
    let changed = write_request(&path, "write-three", "changed\n");
    let running = thread::spawn(move || write(&shared, Some(changed)).unwrap());
    let prompt = harness.prompt();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\n");
    resolve_approval(&harness.shared, &prompt.id, Ok(ApprovalChoice::Deny)).unwrap();
    harness.closed(&prompt.id);
    let result = running.join().unwrap();
    assert_eq!(result["error"]["reason"], "denied");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\n");
}

#[test]
fn cancellation_late_answers_and_concurrent_sheets_fail_closed() {
    let harness = Harness::new();
    let changed = harness.work.join("changed");
    let shared = Arc::clone(&harness.shared);
    let running = thread::spawn(move || {
        shell(
            &shared,
            Some(serde_json::json!({
                "turn_id": "run-b", "request_id": "request-b", "call_id": "shell-one",
                "command": "touch changed"
            })),
        )
        .unwrap()
    });
    let prompt = harness.prompt();
    assert_eq!(prompt.subject, "touch changed");
    assert_eq!(
        prompt.path,
        std::fs::canonicalize(&harness.work)
            .unwrap()
            .to_string_lossy()
    );
    assert!(!changed.exists(), "command ran before approval");

    let outside = harness.root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let second = write(
        &harness.shared,
        Some(write_request(&outside.join("other"), "write-other", "no\n")),
    )
    .unwrap();
    assert_eq!(second["error"]["reason"], "busy");
    assert!(harness.events.try_recv().is_err());

    cancel_approval(
        &harness.shared,
        Some(serde_json::json!({
            "turn_id": "run-b", "request_id": "request-b", "call_id": "shell-one"
        })),
    )
    .unwrap();
    harness.closed(&prompt.id);
    assert_eq!(running.join().unwrap()["error"]["reason"], "denied");
    assert!(!changed.exists());
    assert!(resolve_approval(&harness.shared, &prompt.id, Ok(ApprovalChoice::AllowOnce)).is_err());
}

#[test]
fn disconnect_unblocks_an_approval_without_a_side_effect() {
    let harness = Harness::new();
    let outside = harness.root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let path = outside.join("disconnected");
    let shared = Arc::clone(&harness.shared);
    let request = write_request(&path, "write-close", "never\n");
    let running = thread::spawn(move || write(&shared, Some(request)).unwrap());
    let prompt = harness.prompt();
    close_shared(&harness.shared, "connection lost");
    harness.closed(&prompt.id);
    assert_eq!(running.join().unwrap()["error"]["reason"], "denied");
    assert!(!path.exists());
}

// Exercises RPC lifecycle, task tools and shared clarification behavior.

use super::*;

fn shared(root: &Path) -> (Shared, mpsc::Receiver<Interaction>) {
    std::fs::create_dir_all(root.join("cfg")).unwrap();
    std::fs::write(root.join("cfg/rules.yaml"), rule::DEFAULTS).unwrap();
    let (notices, _notice_input) = mpsc::channel();
    let (interactions, interaction_input) = mpsc::channel();
    (
        Shared {
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
            proj: Arc::new(Proj::open(root.to_path_buf()).unwrap()),
            rules: rule::Book::open(root, root).unwrap(),
            reads: write::Reads::default(),
            shells: shell::Shells::default(),
        },
        interaction_input,
    )
}

#[test]
fn closing_connection_unblocks_pending_callers() {
    let (answer, received) = mpsc::sync_channel(1);
    let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-closing", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (shared, interactions) = shared(&root);
    shared.pending.lock().unwrap().waits.insert(7, answer);

    close_shared(&shared, "stopped");
    let error = received.recv().unwrap().unwrap_err();
    assert_eq!(error, "stopped");
    close_shared(&shared, "stopped again");
    assert!(shared.pending.lock().unwrap().closed);
    assert!(shared.notices.lock().unwrap().is_none());
    assert!(shared.interactions.lock().unwrap().is_none());
    assert!(interactions.try_recv().is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn task_requests_reach_durable_project_state() {
    let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-task", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let work = root.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let (shared, _interactions) = shared(&root);
    let created = task(
        &shared,
        Some(serde_json::json!({
            "action": "create", "title": "add upload retry", "path": work
        })),
    )
    .unwrap();
    let id = created["task_id"].as_str().unwrap();
    assert!(id.starts_with("t_") && id.len() == 10);
    let current = live(&shared, Some(serde_json::json!({}))).unwrap();
    assert_eq!(current["task"]["id"], id);
    assert_eq!(
        current["project"]["path"],
        std::fs::canonicalize(&work)
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    assert!(task(
        &shared,
        Some(serde_json::json!({
            "action": "update", "id": id, "status": "done", "note": "verified"
        }))
    )
    .is_ok());
    drop(shared);
    assert!(Proj::open(root.clone()).is_ok());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn find_requests_use_the_active_task_scope() {
    let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-find", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let work = root.join("work");
    std::fs::create_dir_all(work.join("src")).unwrap();
    std::fs::write(work.join("src/main.rs"), b"first\nneedle\nlast\n").unwrap();
    let (shared, _interactions) = shared(&root);
    task(
        &shared,
        Some(serde_json::json!({
            "action": "create", "title": "search project text", "path": work.clone()
        })),
    )
    .unwrap();
    let result = find(
        &shared,
        Some(serde_json::json!({
            "turn_id": "run-a", "request_id": "request-a",
            "query": "needle", "in": "content", "context": 1
        })),
    )
    .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["kind"], "search");
    assert_eq!(result["hits"][0]["path"], "src/main.rs");
    assert_eq!(result["hits"][0]["line"], 2);
    assert_eq!(result["hits"][0]["context"].as_array().unwrap().len(), 3);
    let denied = edit(
        &shared,
        Some(serde_json::json!({
            "turn_id": "run-b", "request_id": "request-a", "call_id": "edit-b",
            "path": "src/main.rs",
            "target": "needle", "replacement": "changed"
        })),
    )
    .unwrap();
    assert_eq!(denied["error"]["reason"], "denied");
    let edited = edit(
        &shared,
        Some(serde_json::json!({
            "turn_id": "run-a", "request_id": "request-a", "call_id": "edit-a",
            "path": "src/main.rs",
            "target": "needle", "replacement": "changed"
        })),
    )
    .unwrap();
    assert_eq!(edited["ok"], true);
    assert_eq!(edited["replacements"], 1);
    assert_eq!(
        std::fs::read_to_string(work.join("src/main.rs")).unwrap(),
        "first\nchanged\nlast\n"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn shell_requests_execute_in_the_active_task_scope() {
    let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-shell", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let work = root.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let (shared, _interactions) = shared(&root);
    task(
        &shared,
        Some(serde_json::json!({
            "action": "create", "title": "run project command", "path": work.clone()
        })),
    )
    .unwrap();
    let result = shell(
        &shared,
        Some(serde_json::json!({
            "turn_id": "run-a", "request_id": "request-a", "call_id": "shell-a",
            "command": "pwd", "env": {"RPC_VALUE": "present"}
        })),
    )
    .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["kind"], "shell");
    assert_eq!(result["exit_code"], 0);
    assert_eq!(
        result["stdout"].as_str().unwrap().trim(),
        std::fs::canonicalize(&work)
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn clarification_answer_is_correlated_and_only_accepted_once() {
    let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-clarify", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (shared, interactions) = shared(&root);
    let shared = Arc::new(shared);
    let (output, mut responses) = async_mpsc::unbounded_channel();
    start_clarify(
        Arc::clone(&shared),
        output,
        Some(9),
        Some(serde_json::json!({
            "turn_id": "turn-a",
            "request_id": "request-a",
            "call_id": "call-a",
            "question": "Which failures should retry?",
            "options": [
                {"label": "Transient failures", "recommended": true},
                {"label": "All failures"}
            ]
        })),
    );
    let prompt = match interactions.recv().unwrap() {
        Interaction::ClarifyOpened { prompt } => prompt,
        event => panic!("unexpected event: {event:?}"),
    };
    assert_eq!(
        prompt
            .options
            .iter()
            .filter(|option| option.recommended)
            .count(),
        1
    );
    resolve_clarify(&shared, &prompt.id, Ok("Transient failures".into())).unwrap();
    assert!(resolve_clarify(&shared, &prompt.id, Ok("All failures".into())).is_err());
    assert!(matches!(
        interactions.recv().unwrap(),
        Interaction::ClarifyClosed { error: None, .. }
    ));
    let Command::Message(text) = responses.blocking_recv().unwrap() else {
        panic!("clarification response closed the socket")
    };
    let response: Wire = serde_json::from_str(&text).unwrap();
    assert_eq!(response.id, Some(9));
    assert_eq!(response.result.unwrap()["answer"], "Transient failures");
    close_shared(&shared, "stopped");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn clarification_disconnect_unblocks_waiter() {
    let root = std::env::temp_dir().join(format!(
        "pippo-rpc-proj-{}-clarify-close",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let (shared, interactions) = shared(&root);
    let (answer, received) = mpsc::sync_channel(1);
    shared.clarify.lock().unwrap().active = Some(ClarifyPending {
        id: "clarify-test".into(),
        key: ClarifyKey {
            turn: "turn-a".into(),
            request: "request-a".into(),
            call: "call-a".into(),
        },
        answer,
    });
    close_shared(&shared, "connection lost");
    assert_eq!(received.recv().unwrap().unwrap_err(), "connection lost");
    assert!(matches!(
        interactions.recv().unwrap(),
        Interaction::ClarifyClosed {
            error: Some(error),
            ..
        } if error == "Connection lost"
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn clarification_cancel_matches_the_originating_call() {
    let root = std::env::temp_dir().join(format!(
        "pippo-rpc-proj-{}-clarify-cancel",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let (shared, interactions) = shared(&root);
    let (answer, received) = mpsc::sync_channel(1);
    shared.clarify.lock().unwrap().active = Some(ClarifyPending {
        id: "clarify-test".into(),
        key: ClarifyKey {
            turn: "turn-a".into(),
            request: "request-a".into(),
            call: "call-a".into(),
        },
        answer,
    });
    let input = |turn: &str| {
        Some(serde_json::json!({
            "turn_id": turn,
            "request_id": "request-a",
            "call_id": "call-a",
            "question": "Continue?"
        }))
    };
    cancel_clarify(&shared, input("turn-b")).unwrap();
    assert!(shared.clarify.lock().unwrap().active.is_some());
    assert!(received.try_recv().is_err());
    cancel_clarify(&shared, input("turn-a")).unwrap();
    assert_eq!(
        received.recv().unwrap().unwrap_err(),
        "clarification cancelled"
    );
    assert!(shared.clarify.lock().unwrap().active.is_none());
    assert!(matches!(
        interactions.recv().unwrap(),
        Interaction::ClarifyClosed {
            error: Some(error),
            ..
        } if error == "Clarification cancelled"
    ));
    close_shared(&shared, "stopped");
    std::fs::remove_dir_all(root).unwrap();
}

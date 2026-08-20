// Exercises durable turn state and correlated cancellation.

use super::*;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn root() -> PathBuf {
    let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("pippo-sess-{}-{nonce}", std::process::id()))
}

fn session(root: &Path) -> Sess {
    Sess::new(Store::open(root.to_path_buf()).unwrap()).unwrap()
}

#[test]
fn records_open_chunks_and_close_in_order() {
    let root = root();
    let session = session(&root);
    let start = session.open("hello".into()).unwrap();
    let first = session.chunk(&start.call, "one ".into()).unwrap().unwrap();
    let second = session.chunk(&start.call, "two".into()).unwrap().unwrap();
    let closed = session
        .close(&start.call, Status::Done, None)
        .unwrap()
        .unwrap();
    let messages = session.snapshot().unwrap();
    assert_eq!(messages[0].text, "hello");
    assert_eq!(messages[1].text, "one two");
    assert_eq!(messages[1].status, Some(Status::Done));
    drop(session);
    let store = Store::open(root.clone()).unwrap();
    assert_eq!(
        store.messages::<Event>().unwrap(),
        vec![start.event, first, second, closed]
    );
    assert_eq!(store.replay::<Vec<Message>>().unwrap(), messages);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_stays_correlated_until_terminal_close() {
    let root = root();
    let session = session(&root);
    let start = session.open("wait".into()).unwrap();
    assert_eq!(session.request_cancel().unwrap(), Some(start.call.clone()));
    assert!(session.started(&start.call).unwrap());
    let event = session
        .close(&start.call, Status::Cancelled, Some("hidden".into()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        event,
        Event::Closed {
            status: Status::Cancelled,
            error: None,
            ..
        }
    ));
    assert_eq!(session.request_cancel().unwrap(), None);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn shutdown_durably_cancels_once_and_rejects_late_close() {
    let root = root();
    let current = session(&root);
    let start = current.open("wait".into()).unwrap();
    current.chunk(&start.call, "partial".into()).unwrap();
    let closed = current.shutdown().unwrap().unwrap();
    assert!(matches!(
        closed,
        Event::Closed {
            status: Status::Cancelled,
            ..
        }
    ));
    assert_eq!(current.shutdown().unwrap(), None);
    assert_eq!(
        current.close(&start.call, Status::Done, None).unwrap(),
        None
    );
    drop(current);
    let reopened = session(&root);
    let messages = reopened.snapshot().unwrap();
    assert_eq!(messages[1].text, "partial");
    assert_eq!(messages[1].status, Some(Status::Cancelled));
    assert_eq!(
        Store::open(root.clone())
            .unwrap()
            .messages::<Event>()
            .unwrap()
            .len(),
        3
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn replay_never_invents_a_terminal_state() {
    let root = root();
    let session = session(&root);
    let start = session.open("unfinished".into()).unwrap();
    session.chunk(&start.call, "partial".into()).unwrap();
    drop(session);
    let store = Store::open(root.clone()).unwrap();
    let replay = store.replay::<Vec<Message>>().unwrap();
    assert_eq!(replay[1].text, "partial");
    assert_eq!(replay[1].status, Some(Status::Running));
    assert_eq!(store.messages::<Event>().unwrap().len(), 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ignores_duplicate_and_late_notifications() {
    let root = root();
    let session = session(&root);
    let first = session.open("first".into()).unwrap();
    session.chunk(&first.call, "kept".into()).unwrap();
    session.close(&first.call, Status::Done, None).unwrap();
    assert_eq!(
        session.close(&first.call, Status::Done, None).unwrap(),
        None
    );
    let second = session.open("second".into()).unwrap();
    assert_eq!(session.chunk(&first.call, "late".into()).unwrap(), None);
    assert_eq!(
        session
            .close(&first.call, Status::Failed, Some("late".into()))
            .unwrap(),
        None
    );
    session.chunk(&second.call, "current".into()).unwrap();
    let messages = session.snapshot().unwrap();
    assert_eq!(messages[1].text, "kept");
    assert_eq!(messages[3].text, "current");
    assert_eq!(messages[3].status, Some(Status::Running));
    assert_eq!(
        Store::open(root.clone())
            .unwrap()
            .messages::<Event>()
            .unwrap()
            .len(),
        5
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn restores_message_projection_from_replay() {
    let root = root();
    let messages = vec![
        Message {
            id: "message-user".into(),
            turn_id: "turn-one".into(),
            role: Role::User,
            text: "Keep this exactly.".into(),
            status: None,
            error: None,
        },
        Message {
            id: "message-done".into(),
            turn_id: "turn-one".into(),
            role: Role::Assistant,
            text: "It is kept.".into(),
            status: Some(Status::Done),
            error: None,
        },
        Message {
            id: "message-failed".into(),
            turn_id: "turn-two".into(),
            role: Role::Assistant,
            text: "Partial reply".into(),
            status: Some(Status::Failed),
            error: Some("connection closed".into()),
        },
        Message {
            id: "message-cancelled".into(),
            turn_id: "turn-three".into(),
            role: Role::Assistant,
            text: "Stopped reply".into(),
            status: Some(Status::Cancelled),
            error: None,
        },
    ];
    let store = Store::open(root.clone()).unwrap();
    store.replace_replay(&messages).unwrap();
    drop(store);
    let restored = session(&root);
    assert_eq!(restored.snapshot().unwrap(), messages);
    fs::remove_dir_all(root).unwrap();
}

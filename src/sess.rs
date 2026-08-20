// Owns turn state, durable message transitions and live context.

use crate::store::Store;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Write as _,
    sync::{Mutex, MutexGuard},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Call {
    pub turn_id: String,
    pub request_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Done,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Message {
    pub id: String,
    pub turn_id: String,
    pub role: Role,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    Opened {
        #[serde(flatten)]
        call: Call,
        user: Message,
        assistant: Message,
    },
    Chunk {
        #[serde(flatten)]
        call: Call,
        message_id: String,
        text: String,
    },
    Closed {
        #[serde(flatten)]
        call: Call,
        message_id: String,
        status: Status,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

pub struct Start {
    pub call: Call,
    pub query: String,
    pub transcript: String,
    pub event: Event,
}

#[derive(Clone)]
struct Active {
    call: Call,
    assistant: String,
    cancel: bool,
}

#[derive(Clone)]
struct State {
    messages: Vec<Message>,
    active: Option<Active>,
}

pub struct Sess {
    store: Store,
    state: Mutex<State>,
}

impl Sess {
    pub fn new(store: Store) -> Result<Self> {
        let messages = store.replay::<Vec<Message>>()?;
        Ok(Self {
            store,
            state: Mutex::new(State {
                messages,
                active: None,
            }),
        })
    }

    pub fn open(&self, query: String) -> Result<Start> {
        if query.trim().is_empty() {
            anyhow::bail!("message is empty");
        }
        let mut state = self.lock()?;
        if state.active.is_some() {
            anyhow::bail!("a turn is already running");
        }
        let transcript = transcript(&state.messages)?;
        let call = Call {
            turn_id: id("turn")?,
            request_id: id("request")?,
        };
        let user = Message {
            id: format!("{}_user", call.turn_id),
            turn_id: call.turn_id.clone(),
            role: Role::User,
            text: query.clone(),
            status: None,
            error: None,
        };
        let assistant = Message {
            id: format!("{}_assistant", call.turn_id),
            turn_id: call.turn_id.clone(),
            role: Role::Assistant,
            text: String::new(),
            status: Some(Status::Running),
            error: None,
        };
        let event = Event::Opened {
            call: call.clone(),
            user: user.clone(),
            assistant: assistant.clone(),
        };
        let mut next = state.clone();
        next.messages.extend([user, assistant.clone()]);
        next.active = Some(Active {
            call: call.clone(),
            assistant: assistant.id,
            cancel: false,
        });
        self.commit(&mut state, next, &event)?;
        Ok(Start {
            call,
            query,
            transcript,
            event,
        })
    }

    pub fn started(&self, call: &Call) -> Result<bool> {
        let state = self.lock()?;
        let Some(active) = state.active.as_ref().filter(|active| active.call == *call) else {
            return Ok(false);
        };
        Ok(active.cancel)
    }

    pub fn request_cancel(&self) -> Result<Option<Call>> {
        let mut state = self.lock()?;
        let Some(active) = state.active.as_mut() else {
            return Ok(None);
        };
        active.cancel = true;
        Ok(Some(active.call.clone()))
    }

    pub fn chunk(&self, call: &Call, text: String) -> Result<Option<Event>> {
        if text.is_empty() {
            return Ok(None);
        }
        let mut state = self.lock()?;
        let Some(active) = state.active.as_ref().filter(|active| active.call == *call) else {
            return Ok(None);
        };
        let assistant = active.assistant.clone();
        let event = Event::Chunk {
            call: call.clone(),
            message_id: assistant.clone(),
            text: text.clone(),
        };
        let mut next = state.clone();
        let message = next
            .messages
            .iter_mut()
            .find(|message| message.id == assistant)
            .context("active assistant message is missing")?;
        if message.status != Some(Status::Running) {
            return Ok(None);
        }
        message.text.push_str(&text);
        self.commit(&mut state, next, &event)?;
        Ok(Some(event))
    }

    pub fn close(
        &self,
        call: &Call,
        status: Status,
        error: Option<String>,
    ) -> Result<Option<Event>> {
        if status == Status::Running {
            anyhow::bail!("a running turn cannot close");
        }
        let mut state = self.lock()?;
        let Some(active) = state.active.as_ref().filter(|active| active.call == *call) else {
            return Ok(None);
        };
        let assistant = active.assistant.clone();
        let error = match status {
            Status::Failed => error.filter(|value| !value.trim().is_empty()),
            _ => None,
        };
        let event = Event::Closed {
            call: call.clone(),
            message_id: assistant.clone(),
            status,
            error: error.clone(),
        };
        let mut next = state.clone();
        let message = next
            .messages
            .iter_mut()
            .find(|message| message.id == assistant)
            .context("active assistant message is missing")?;
        if message.status != Some(Status::Running) {
            return Ok(None);
        }
        message.status = Some(status);
        message.error = error;
        next.active = None;
        self.commit(&mut state, next, &event)?;
        Ok(Some(event))
    }

    pub fn snapshot(&self) -> Result<Vec<Message>> {
        Ok(self.lock()?.messages.clone())
    }

    pub fn shutdown(&self) -> Result<Option<Event>> {
        let mut state = self.lock()?;
        let event = if let Some(active) = state.active.as_ref() {
            let event = Event::Closed {
                call: active.call.clone(),
                message_id: active.assistant.clone(),
                status: Status::Cancelled,
                error: None,
            };
            let mut next = state.clone();
            let message = next
                .messages
                .iter_mut()
                .find(|message| message.id == active.assistant)
                .context("active assistant message is missing")?;
            message.status = Some(Status::Cancelled);
            message.error = None;
            next.active = None;
            self.commit(&mut state, next, &event)?;
            Some(event)
        } else {
            None
        };
        drop(state);
        self.store.flush()?;
        Ok(event)
    }

    fn commit(&self, state: &mut MutexGuard<'_, State>, next: State, event: &Event) -> Result<()> {
        self.store.record(event, &next.messages)?;
        **state = next;
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("session lock poisoned"))
    }
}

#[derive(Serialize)]
struct PromptMessage<'a> {
    role: Role,
    text: &'a str,
}

fn transcript(messages: &[Message]) -> Result<String> {
    if messages.is_empty() {
        return Ok(String::new());
    }
    let messages: Vec<_> = messages
        .iter()
        .map(|message| PromptMessage {
            role: message.role,
            text: &message.text,
        })
        .collect();
    serde_json::to_string(&messages).context("serialize live transcript")
}

fn id(prefix: &str) -> Result<String> {
    let mut bytes = [0_u8; 12];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generate {prefix} id: {error}"))?;
    let mut value = String::with_capacity(prefix.len() + 1 + bytes.len() * 2);
    write!(value, "{prefix}_").context("write id prefix")?;
    for byte in bytes {
        write!(value, "{byte:02x}").context("encode id")?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
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

        let store = Store::open(root.clone()).unwrap();
        assert_eq!(store.messages::<Event>().unwrap().len(), 5);
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
}

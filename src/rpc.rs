// Owns the websocket connection and concurrent JSON-RPC dispatch.

use crate::{
    cfg::Config,
    key::Key,
    proj::{Proj, TaskStatus},
    sess::{Call, Status},
};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};
use tokio::sync::mpsc as async_mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::AUTHORIZATION, HeaderValue},
        Message,
    },
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Paths {
    pub runtime: PathBuf,
    pub cache: PathBuf,
    pub agent: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Hello {
    pub paths: Paths,
    pub platform: Platform,
    pub settings: Config,
}

impl Hello {
    pub fn new(root: &Path, settings: &Config) -> Result<Self> {
        let agent = env::current_exe()
            .context("locate application executable")?
            .parent()
            .context("application executable has no parent")?
            .to_path_buf();
        Ok(Self {
            paths: Paths {
                runtime: root.to_path_buf(),
                cache: root.join("cache"),
                agent,
            },
            platform: Platform {
                os: env::consts::OS.into(),
                arch: env::consts::ARCH.into(),
            },
            settings: settings.clone(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Failure {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Wire {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Failure>,
}

enum Command {
    Message(String),
    Close,
}

type Answer = std::result::Result<Value, String>;

#[derive(Debug)]
pub enum Notice {
    Chunk {
        call: Call,
        text: String,
    },
    Closed {
        call: Call,
        status: Status,
        error: Option<String>,
    },
}

pub struct Stamped {
    pub order: u64,
    pub notice: std::result::Result<Notice, String>,
}

struct Shared {
    pending: Mutex<Pending>,
    notices: Mutex<Option<mpsc::Sender<Stamped>>>,
    key: Key,
    proj: Arc<Proj>,
}

struct Pending {
    waits: HashMap<u64, mpsc::SyncSender<Answer>>,
    closed: bool,
}

struct Inner {
    shared: Arc<Shared>,
    send: async_mpsc::UnboundedSender<Command>,
    next: AtomicU64,
    notices: Mutex<Option<mpsc::Receiver<Stamped>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct Rpc {
    inner: Arc<Inner>,
}

impl Rpc {
    pub fn connect(
        addr: SocketAddr,
        token: String,
        hello: &Hello,
        key: Key,
        proj: Arc<Proj>,
    ) -> Result<Self> {
        let rpc = Self::open(addr, token, key, proj)?;
        let ready: Ready = rpc.call("hello", hello)?;
        if !ready.ready {
            anyhow::bail!("child process did not accept startup hello");
        }
        Ok(rpc)
    }

    fn open(addr: SocketAddr, token: String, key: Key, proj: Arc<Proj>) -> Result<Self> {
        let (notice_send, notice_receive) = mpsc::channel();
        let shared = Arc::new(Shared {
            pending: Mutex::new(Pending {
                waits: HashMap::new(),
                closed: false,
            }),
            notices: Mutex::new(Some(notice_send)),
            key,
            proj,
        });
        let (send, receive) = async_mpsc::unbounded_channel();
        let (started, status) = mpsc::sync_channel(1);
        let background = Arc::clone(&shared);
        let failed = Arc::clone(&shared);
        let output = send.clone();
        let handle = thread::spawn(move || {
            let result = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("start rpc runtime: {error}"))
                .and_then(|runtime| {
                    runtime.block_on(connection(
                        addr, token, background, output, receive, started,
                    ))
                });
            fail_all(
                &failed,
                result
                    .err()
                    .unwrap_or_else(|| "rpc connection closed".into()),
            );
        });
        let inner = Arc::new(Inner {
            shared,
            send,
            next: AtomicU64::new(0),
            notices: Mutex::new(Some(notice_receive)),
            thread: Mutex::new(Some(handle)),
        });
        match status.recv_timeout(TIMEOUT) {
            Ok(Ok(())) => Ok(Self { inner }),
            Ok(Err(error)) => Err(anyhow::anyhow!(error)),
            Err(error) => Err(anyhow::anyhow!("wait for rpc connection: {error}")),
        }
    }

    pub fn call<P, R>(&self, method: &str, params: &P) -> Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = self.inner.next.fetch_add(1, Ordering::Relaxed) + 1;
        let request = Wire {
            jsonrpc: "2.0".into(),
            id: Some(id),
            method: Some(method.into()),
            params: Some(serde_json::to_value(params).context("encode rpc parameters")?),
            result: None,
            error: None,
        };
        let text = serde_json::to_string(&request).context("encode rpc request")?;
        let (sent, received) = mpsc::sync_channel(1);
        {
            let mut pending = self
                .inner
                .shared
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("rpc pending lock poisoned"))?;
            if pending.closed {
                anyhow::bail!("rpc connection is closed");
            }
            pending.waits.insert(id, sent);
        }
        if self.inner.send.send(Command::Message(text)).is_err() {
            self.forget(id)?;
            anyhow::bail!("rpc connection is closed");
        }
        let answer = match received.recv_timeout(TIMEOUT) {
            Ok(answer) => answer.map_err(anyhow::Error::msg)?,
            Err(error) => {
                self.forget(id)?;
                return Err(anyhow::anyhow!("wait for {method}: {error}"));
            }
        };
        serde_json::from_value(answer).with_context(|| format!("decode {method} result"))
    }

    pub fn take_notices(&self) -> Result<mpsc::Receiver<Stamped>> {
        self.inner
            .notices
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc notice lock poisoned"))?
            .take()
            .context("rpc notices already taken")
    }

    pub fn shutdown(&self) -> Result<()> {
        close_shared(&self.inner.shared, "rpc connection closed");
        let _ = self.inner.send.send(Command::Close);
        let mut thread = self
            .inner
            .thread
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc thread lock poisoned"))?;
        if let Some(thread) = thread.take() {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("rpc thread panicked"))?;
        }
        Ok(())
    }

    fn forget(&self, id: u64) -> Result<()> {
        self.inner
            .shared
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc pending lock poisoned"))?
            .waits
            .remove(&id);
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        close_shared(&self.shared, "rpc connection closed");
        let _ = self.send.send(Command::Close);
        match self.thread.get_mut() {
            Ok(thread) => {
                if let Some(thread) = thread.take() {
                    if thread.join().is_err() {
                        eprintln!("rpc thread panicked");
                    }
                }
            }
            Err(_) => eprintln!("rpc thread lock poisoned"),
        }
    }
}

#[derive(Deserialize)]
struct Ready {
    ready: bool,
}

async fn connection(
    addr: SocketAddr,
    token: String,
    shared: Arc<Shared>,
    output: async_mpsc::UnboundedSender<Command>,
    mut input: async_mpsc::UnboundedReceiver<Command>,
    started: mpsc::SyncSender<std::result::Result<(), String>>,
) -> std::result::Result<(), String> {
    let mut request = format!("ws://{addr}/rpc")
        .into_client_request()
        .map_err(|error| format!("build rpc request: {error}"))?;
    let auth = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|error| format!("build rpc authorization: {error}"))?;
    drop(token);
    request.headers_mut().insert(AUTHORIZATION, auth);
    let (socket, _) = match connect_async(request).await {
        Ok(connected) => connected,
        Err(error) => {
            let detail = format!("connect rpc websocket: {error}");
            let _ = started.send(Err(detail.clone()));
            return Err(detail);
        }
    };
    if started.send(Ok(())).is_err() {
        return Err("rpc starter closed".into());
    }

    let (mut writer, mut reader) = socket.split();
    let write = tokio::spawn(async move {
        while let Some(command) = input.recv().await {
            match command {
                Command::Message(text) => writer
                    .send(Message::Text(text.into()))
                    .await
                    .map_err(|error| format!("write rpc message: {error}"))?,
                Command::Close => {
                    writer
                        .send(Message::Close(None))
                        .await
                        .map_err(|error| format!("close rpc websocket: {error}"))?;
                    return Ok(());
                }
            }
        }
        Ok::<_, String>(())
    });
    let read_shared = Arc::clone(&shared);
    let read = tokio::spawn(async move {
        let mut notice_order = 0;
        while let Some(message) = reader.next().await {
            match message.map_err(|error| format!("read rpc message: {error}"))? {
                Message::Text(text) => {
                    let input: Wire = match serde_json::from_slice(text.as_bytes()) {
                        Ok(input) => input,
                        Err(error) => {
                            eprintln!("invalid rpc message: {error}");
                            continue;
                        }
                    };
                    let order = match (input.jsonrpc.as_str(), input.method.as_deref()) {
                        ("2.0", Some("turn.chunk" | "turn.closed")) => {
                            notice_order += 1;
                            Some(notice_order)
                        }
                        _ => None,
                    };
                    let shared = Arc::clone(&read_shared);
                    let output = output.clone();
                    tokio::spawn(async move { dispatch(shared, output, input, order) });
                }
                Message::Close(_) => return Ok(()),
                _ => {}
            }
        }
        Ok::<_, String>(())
    });
    tokio::pin!(write, read);
    let result = tokio::select! {
        result = &mut write => result.map_err(|error| format!("rpc writer task: {error}"))?,
        result = &mut read => result.map_err(|error| format!("rpc reader task: {error}"))?,
    };
    write.abort();
    read.abort();
    result
}

fn dispatch(
    shared: Arc<Shared>,
    output: async_mpsc::UnboundedSender<Command>,
    input: Wire,
    order: Option<u64>,
) {
    if input.jsonrpc != "2.0" {
        if let Some(id) = input.id {
            respond(
                output,
                id,
                None,
                Some(Failure {
                    code: -32600,
                    message: "invalid request".into(),
                }),
            );
        }
        return;
    }
    if let Some(method) = input.method {
        let id = input.id;
        let result = match method.as_str() {
            "runtime.ping" => Ok(serde_json::json!({"ready": true})),
            "runtime.model_key" => match shared.key.read() {
                Ok(Some(secret)) => Ok(serde_json::json!({"value": secret.expose()})),
                Ok(None) => Err(Failure {
                    code: -32001,
                    message: "model key is missing".into(),
                }),
                Err(error) => Err(Failure {
                    code: -32603,
                    message: error.to_string(),
                }),
            },
            "runtime.task" => task(&shared, input.params),
            "runtime.live_env" => live(&shared, input.params),
            "turn.chunk" => {
                send_notice(&shared, order, input.params, |value: Chunk| Notice::Chunk {
                    call: value.call,
                    text: value.text,
                })
            }
            "turn.closed" => send_notice(&shared, order, input.params, |value: Closed| {
                Notice::Closed {
                    call: value.call,
                    status: value.status,
                    error: value.error,
                }
            }),
            _ => Err(Failure {
                code: -32601,
                message: "method not found".into(),
            }),
        };
        if let Some(id) = id {
            match result {
                Ok(value) => respond(output, id, Some(value), None),
                Err(error) => respond(output, id, None, Some(error)),
            }
        }
        return;
    }
    let Some(id) = input.id else {
        eprintln!("rpc response is missing an id");
        return;
    };
    let answer = match input.error {
        Some(error) => Err(format!("{} ({})", error.message, error.code)),
        None => Ok(input.result.unwrap_or(Value::Null)),
    };
    match shared.pending.lock() {
        Ok(mut pending) => {
            if let Some(wait) = pending.waits.remove(&id) {
                if wait.send(answer).is_err() {
                    eprintln!("rpc caller stopped waiting for {id}");
                }
            }
        }
        Err(_) => eprintln!("rpc pending lock poisoned"),
    }
}

#[derive(Deserialize)]
struct Chunk {
    #[serde(flatten)]
    call: Call,
    text: String,
}

#[derive(Deserialize)]
struct Closed {
    #[serde(flatten)]
    call: Call,
    status: Status,
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
enum TaskInput {
    Create {
        title: String,
        path: PathBuf,
    },
    Update {
        id: String,
        status: TaskStatus,
        note: String,
    },
}

fn task(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: TaskInput =
        serde_json::from_value(params.unwrap_or(Value::Null)).map_err(|error| Failure {
            code: -32602,
            message: format!("decode task request: {error}"),
        })?;
    let result = match input {
        TaskInput::Create { title, path } => shared.proj.create(title, path),
        TaskInput::Update { id, status, note } => shared.proj.update(id, status, note),
    }
    .map_err(|error| Failure {
        code: -32602,
        message: error.to_string(),
    })?;
    serde_json::to_value(result).map_err(|error| Failure {
        code: -32603,
        message: format!("encode task result: {error}"),
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveInput {
    task_id: Option<String>,
}

fn live(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: LiveInput =
        serde_json::from_value(params.unwrap_or(Value::Null)).map_err(|error| Failure {
            code: -32602,
            message: format!("decode live environment request: {error}"),
        })?;
    let value = shared
        .proj
        .live(input.task_id.as_deref())
        .map_err(|error| Failure {
            code: -32603,
            message: error.to_string(),
        })?;
    serde_json::to_value(value).map_err(|error| Failure {
        code: -32603,
        message: format!("encode live environment: {error}"),
    })
}

fn send_notice<T: for<'de> Deserialize<'de>>(
    shared: &Shared,
    order: Option<u64>,
    params: Option<Value>,
    map: impl FnOnce(T) -> Notice,
) -> std::result::Result<Value, Failure> {
    let order = order.ok_or_else(|| Failure {
        code: -32600,
        message: "turn notification is unordered".into(),
    })?;
    let notice = serde_json::from_value(params.unwrap_or(Value::Null))
        .map(map)
        .map_err(|error| format!("decode turn notification: {error}"));
    let failed = notice.as_ref().err().cloned();
    let notices = shared.notices.lock().map_err(|_| Failure {
        code: -32603,
        message: "rpc notice lock poisoned".into(),
    })?;
    if notices
        .as_ref()
        .ok_or_else(|| Failure {
            code: -32603,
            message: "turn notification receiver is closed".into(),
        })?
        .send(Stamped { order, notice })
        .is_err()
    {
        return Err(Failure {
            code: -32603,
            message: "turn notification receiver is closed".into(),
        });
    }
    match failed {
        Some(message) => Err(Failure {
            code: -32602,
            message,
        }),
        None => Ok(Value::Null),
    }
}

fn respond(
    output: async_mpsc::UnboundedSender<Command>,
    id: u64,
    result: Option<Value>,
    error: Option<Failure>,
) {
    let message = Wire {
        jsonrpc: "2.0".into(),
        id: Some(id),
        method: None,
        params: None,
        result,
        error,
    };
    match serde_json::to_string(&message) {
        Ok(text) => {
            if output.send(Command::Message(text)).is_err() {
                eprintln!("rpc connection closed before response {id}");
            }
        }
        Err(error) => eprintln!("encode rpc response {id}: {error}"),
    }
}

fn fail_all(shared: &Shared, error: String) {
    close_shared(shared, &error);
}

fn close_shared(shared: &Shared, error: &str) {
    let waits = {
        let mut pending = match shared.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.closed = true;
        pending
            .waits
            .drain()
            .map(|(_, wait)| wait)
            .collect::<Vec<_>>()
    };
    match shared.notices.lock() {
        Ok(mut notices) => notices.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    };
    for wait in waits {
        if wait.send(Err(error.into())).is_err() {
            eprintln!("rpc caller stopped waiting");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closing_connection_unblocks_pending_callers() {
        let (notices, _input) = mpsc::channel();
        let (answer, received) = mpsc::sync_channel(1);
        let root =
            std::env::temp_dir().join(format!("pippo-rpc-proj-{}-closing", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let shared = Shared {
            pending: Mutex::new(Pending {
                waits: HashMap::from([(7, answer)]),
                closed: false,
            }),
            notices: Mutex::new(Some(notices)),
            key: Key,
            proj: Arc::new(Proj::open(root.clone()).unwrap()),
        };

        close_shared(&shared, "stopped");
        let error = received.recv().unwrap().unwrap_err();
        assert_eq!(error, "stopped");
        close_shared(&shared, "stopped again");
        assert!(shared.pending.lock().unwrap().closed);
        assert!(shared.notices.lock().unwrap().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn task_requests_reach_durable_project_state() {
        let root = std::env::temp_dir().join(format!("pippo-rpc-proj-{}-task", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let (notices, _input) = mpsc::channel();
        let shared = Shared {
            pending: Mutex::new(Pending {
                waits: HashMap::new(),
                closed: false,
            }),
            notices: Mutex::new(Some(notices)),
            key: Key,
            proj: Arc::new(Proj::open(root.clone()).unwrap()),
        };
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
}

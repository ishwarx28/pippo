// Owns the websocket connection and concurrent JSON-RPC dispatch.

use crate::{cfg::Config, key::Key};
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

struct Shared {
    waits: Mutex<HashMap<u64, mpsc::SyncSender<Answer>>>,
    key: Key,
}

struct Inner {
    shared: Arc<Shared>,
    send: async_mpsc::UnboundedSender<Command>,
    next: AtomicU64,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct Rpc {
    inner: Arc<Inner>,
}

impl Rpc {
    pub fn connect(addr: SocketAddr, token: String, hello: &Hello, key: Key) -> Result<Self> {
        let rpc = Self::open(addr, token, key)?;
        let ready: Ready = rpc.call("hello", hello)?;
        if !ready.ready {
            anyhow::bail!("child process did not accept startup hello");
        }
        Ok(rpc)
    }

    fn open(addr: SocketAddr, token: String, key: Key) -> Result<Self> {
        let shared = Arc::new(Shared {
            waits: Mutex::new(HashMap::new()),
            key,
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
            if let Err(error) = result {
                fail_all(&failed, error);
            }
        });
        let inner = Arc::new(Inner {
            shared,
            send,
            next: AtomicU64::new(0),
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
        self.inner
            .shared
            .waits
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc pending lock poisoned"))?
            .insert(id, sent);
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

    fn forget(&self, id: u64) -> Result<()> {
        self.inner
            .shared
            .waits
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc pending lock poisoned"))?
            .remove(&id);
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.send.send(Command::Close).is_err() {
            eprintln!("rpc connection already closed");
        }
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
        while let Some(message) = reader.next().await {
            match message.map_err(|error| format!("read rpc message: {error}"))? {
                Message::Text(text) => {
                    let shared = Arc::clone(&read_shared);
                    let output = output.clone();
                    tokio::spawn(async move { dispatch(shared, output, text.as_bytes()) });
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

fn dispatch(shared: Arc<Shared>, output: async_mpsc::UnboundedSender<Command>, bytes: &[u8]) {
    let input: Wire = match serde_json::from_slice(bytes) {
        Ok(input) => input,
        Err(error) => {
            eprintln!("invalid rpc message: {error}");
            return;
        }
    };
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
    match shared.waits.lock() {
        Ok(mut waits) => {
            if let Some(wait) = waits.remove(&id) {
                if wait.send(answer).is_err() {
                    eprintln!("rpc caller stopped waiting for {id}");
                }
            }
        }
        Err(_) => eprintln!("rpc pending lock poisoned"),
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
    match shared.waits.lock() {
        Ok(mut waits) => {
            for (_, wait) in waits.drain() {
                if wait.send(Err(error.clone())).is_err() {
                    eprintln!("rpc caller stopped waiting");
                }
            }
        }
        Err(_) => eprintln!("rpc pending lock poisoned"),
    }
}

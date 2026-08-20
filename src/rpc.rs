// Owns the websocket connection and concurrent JSON-RPC dispatch.

use crate::{
    cfg::{self, Config},
    key::Key,
    proj::{Plan, Proj, Step, StepStatus, TaskStatus},
    rule::{self, Decision, Request as RuleRequest},
    sess::{Call, RunCreate, RunMeta, RunReport, RunRole, RunStatus, Runs, Status},
    tool::{find, shell, write},
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
    pub preset: cfg::Preset,
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
            preset: cfg::preset_at(root, &settings.preset)?,
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
type ClarifyAnswer = std::result::Result<String, String>;
type ApprovalAnswer = std::result::Result<ApprovalChoice, String>;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalChoice {
    AllowOnce,
    AllowSession,
    Deny,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClarifyOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub recommended: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClarifyPrompt {
    pub id: String,
    pub question: String,
    pub options: Vec<ClarifyOption>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Interaction {
    ClarifyOpened {
        prompt: ClarifyPrompt,
    },
    ClarifyClosed {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    ApprovalOpened {
        prompt: rule::Prompt,
    },
    ApprovalClosed {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    PlanChanged {
        plan: Plan,
    },
}

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
    interactions: Mutex<Option<mpsc::Sender<Interaction>>>,
    clarify: Mutex<ClarifyState>,
    approval: Mutex<ApprovalState>,
    sheet: Mutex<SheetState>,
    key: Key,
    proj: Arc<Proj>,
    runs: Runs,
    rules: rule::Book,
    reads: write::Reads,
    shells: shell::Shells,
}

#[derive(Clone, PartialEq)]
struct ClarifyKey {
    turn: String,
    request: String,
    call: String,
}

struct ClarifyPending {
    id: String,
    key: ClarifyKey,
    answer: mpsc::SyncSender<ClarifyAnswer>,
}

#[derive(Default)]
struct ClarifyState {
    active: Option<ClarifyPending>,
}

#[derive(Clone, PartialEq)]
struct ApprovalKey {
    turn: String,
    request: String,
    call: String,
}

struct ApprovalPending {
    id: String,
    key: ApprovalKey,
    ask: rule::Ask,
    answer: mpsc::SyncSender<ApprovalAnswer>,
}

#[derive(Default)]
struct ApprovalState {
    active: Option<ApprovalPending>,
}

#[derive(Default)]
struct SheetState {
    next: u64,
    active: Option<String>,
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
    interactions: Mutex<Option<mpsc::Receiver<Interaction>>>,
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
        rules: rule::Book,
    ) -> Result<Self> {
        let rpc = Self::open(addr, token, key, proj, rules, hello.paths.runtime.clone())?;
        let ready: Ready = rpc.call("hello", hello)?;
        if !ready.ready {
            anyhow::bail!("child process did not accept startup hello");
        }
        Ok(rpc)
    }

    fn open(
        addr: SocketAddr,
        token: String,
        key: Key,
        proj: Arc<Proj>,
        rules: rule::Book,
        root: PathBuf,
    ) -> Result<Self> {
        let (notice_send, notice_receive) = mpsc::channel();
        let (interaction_send, interaction_receive) = mpsc::channel();
        let shared = Arc::new(Shared {
            pending: Mutex::new(Pending {
                waits: HashMap::new(),
                closed: false,
            }),
            notices: Mutex::new(Some(notice_send)),
            interactions: Mutex::new(Some(interaction_send)),
            clarify: Mutex::new(ClarifyState::default()),
            approval: Mutex::new(ApprovalState::default()),
            sheet: Mutex::new(SheetState::default()),
            key,
            proj,
            runs: Runs::open(root)?,
            rules,
            reads: write::Reads::default(),
            shells: shell::Shells::default(),
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
            interactions: Mutex::new(Some(interaction_receive)),
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

    pub fn take_interactions(&self) -> Result<mpsc::Receiver<Interaction>> {
        self.inner
            .interactions
            .lock()
            .map_err(|_| anyhow::anyhow!("rpc interaction lock poisoned"))?
            .take()
            .context("rpc interactions already taken")
    }

    pub fn answer_clarify(&self, id: &str, answer: String) -> Result<()> {
        let answer = answer.trim();
        if answer.is_empty() {
            anyhow::bail!("clarification answer is empty");
        }
        resolve_clarify(&self.inner.shared, id, Ok(answer.to_owned()))
    }

    pub fn cancel_clarify(&self, id: &str) -> Result<()> {
        resolve_clarify(
            &self.inner.shared,
            id,
            Err("clarification cancelled".into()),
        )
    }

    pub fn answer_approval(&self, id: &str, choice: ApprovalChoice) -> Result<()> {
        resolve_approval(&self.inner.shared, id, Ok(choice))
    }

    pub fn cancel_approval(&self, id: &str) -> Result<()> {
        resolve_approval(&self.inner.shared, id, Err("approval denied".into()))
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
                    if matches!(
                        input.method.as_deref(),
                        Some("runtime.write" | "runtime.edit" | "runtime.shell")
                    ) {
                        tokio::task::spawn_blocking(move || dispatch(shared, output, input, order));
                    } else {
                        tokio::spawn(async move { dispatch(shared, output, input, order) });
                    }
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
        if method == "runtime.clarify" {
            start_clarify(shared, output, id, input.params);
            return;
        }
        let result = match method.as_str() {
            "runtime.ping" => Ok(serde_json::json!({"ready": true})),
            "runtime.model_key" => match shared.key.read() {
                Ok(Some(secret)) => Ok(serde_json::json!({"value": secret.expose()})),
                Ok(None) => Err(Failure {
                    code: -32001,
                    message: "model key is missing".into(),
                }),
                Err(error) => Err(internal_failure(error)),
            },
            "runtime.task" => task(&shared, input.params),
            "runtime.live_env" => live(&shared, input.params),
            "runtime.run" => run(&shared, input.params),
            "runtime.plan" => plan(&shared, input.params),
            "runtime.find" => find(&shared, input.params),
            "runtime.write" => write(&shared, input.params),
            "runtime.edit" => edit(&shared, input.params),
            "runtime.shell" => shell(&shared, input.params),
            "runtime.shell.cancel" => cancel_shell(&shared, input.params),
            "runtime.approval.cancel" => cancel_approval(&shared, input.params),
            "runtime.clarify.cancel" => cancel_clarify(&shared, input.params),
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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClarifyInput {
    turn_id: String,
    request_id: String,
    call_id: String,
    question: String,
    #[serde(default)]
    options: Vec<ClarifyOption>,
}

fn start_clarify(
    shared: Arc<Shared>,
    output: async_mpsc::UnboundedSender<Command>,
    id: Option<u64>,
    params: Option<Value>,
) {
    let Some(id) = id else {
        return;
    };
    let input: ClarifyInput = match decode(params, "clarify request") {
        Ok(input) => input,
        Err(error) => {
            respond(output, id, None, Some(error));
            return;
        }
    };
    if let Err(error) = check_clarify(&input) {
        respond(output, id, None, Some(error));
        return;
    }
    let prompt_id = match open_sheet(&shared, "clarify") {
        Ok(id) => id,
        Err(error) => {
            respond(output, id, None, Some(error));
            return;
        }
    };
    let (answer, received) = mpsc::sync_channel(1);
    let prompt = ClarifyPrompt {
        id: prompt_id,
        question: input.question,
        options: input.options,
    };
    let pending = ClarifyPending {
        id: prompt.id.clone(),
        key: ClarifyKey {
            turn: input.turn_id,
            request: input.request_id,
            call: input.call_id,
        },
        answer,
    };
    match shared.clarify.lock() {
        Ok(mut state) => state.active = Some(pending),
        Err(_) => {
            close_sheet(&shared, &prompt.id);
            respond(
                output,
                id,
                None,
                Some(internal_failure("clarification lock poisoned")),
            );
            return;
        }
    }
    if let Err(error) = emit_interaction(
        &shared,
        Interaction::ClarifyOpened {
            prompt: prompt.clone(),
        },
    ) {
        let _ = resolve_clarify(&shared, &prompt.id, Err(error.clone()));
        respond(output, id, None, Some(internal_failure(error)));
        return;
    }
    thread::spawn(move || {
        let result = match received.recv() {
            Ok(Ok(answer)) => Ok(serde_json::json!({"answer": answer})),
            Ok(Err(error)) => Err(Failure {
                code: -32003,
                message: error,
            }),
            Err(_) => Err(internal_failure("clarification waiter closed")),
        };
        match result {
            Ok(value) => respond(output, id, Some(value), None),
            Err(error) => respond(output, id, None, Some(error)),
        }
    });
}

fn check_clarify(input: &ClarifyInput) -> std::result::Result<(), Failure> {
    if input.turn_id.trim().is_empty() || input.request_id.trim().is_empty() {
        return Err(invalid_failure(
            "clarification requires turn and request ids",
        ));
    }
    let question = input.question.trim();
    if question.is_empty() || question.contains(['\r', '\n']) || input.options.len() > 8 {
        return Err(invalid_failure(
            "clarification question or options are invalid",
        ));
    }
    let mut recommended = 0;
    let mut labels = std::collections::HashSet::new();
    for option in &input.options {
        let label = option.label.trim();
        if label.is_empty()
            || label.contains(['\r', '\n'])
            || !labels.insert(label)
            || option.recommended && {
                recommended += 1;
                recommended > 1
            }
        {
            return Err(invalid_failure("clarification options are invalid"));
        }
    }
    Ok(())
}

fn cancel_clarify(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: ClarifyInput = decode(params, "clarification cancellation")?;
    let key = ClarifyKey {
        turn: input.turn_id,
        request: input.request_id,
        call: input.call_id,
    };
    let pending = {
        let mut state = shared
            .clarify
            .lock()
            .map_err(|_| internal_failure("clarification lock poisoned"))?;
        match state.active.as_ref().filter(|pending| pending.key == key) {
            Some(_) => state.active.take(),
            None => None,
        }
    };
    if let Some(pending) = pending {
        let id = pending.id;
        let _ = emit_interaction(
            shared,
            Interaction::ClarifyClosed {
                id: id.clone(),
                error: Some("Clarification cancelled".into()),
            },
        );
        close_sheet(shared, &id);
        let _ = pending.answer.send(Err("clarification cancelled".into()));
    }
    Ok(serde_json::json!({"cancelled": true}))
}

fn resolve_clarify(shared: &Shared, id: &str, answer: ClarifyAnswer) -> Result<()> {
    let pending = {
        let mut state = shared
            .clarify
            .lock()
            .map_err(|_| anyhow::anyhow!("clarification lock poisoned"))?;
        match state.active.as_ref().filter(|pending| pending.id == id) {
            Some(_) => state.active.take(),
            None => None,
        }
    }
    .context("clarification is no longer active")?;
    let error = answer.as_ref().err().map(|value| sentence(value));
    if let Err(error) = emit_interaction(
        shared,
        Interaction::ClarifyClosed {
            id: pending.id.clone(),
            error,
        },
    ) {
        eprintln!("emit clarification close: {error}");
    }
    close_sheet(shared, &pending.id);
    let sent = pending.answer.send(answer);
    sent.map_err(|_| anyhow::anyhow!("clarification requester stopped waiting"))
}

fn sentence(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn open_sheet(shared: &Shared, prefix: &str) -> std::result::Result<String, Failure> {
    let mut sheet = shared
        .sheet
        .lock()
        .map_err(|_| internal_failure("interaction lock poisoned"))?;
    if sheet.active.is_some() {
        return Err(Failure {
            code: -32002,
            message: "another sheet is already open".into(),
        });
    }
    sheet.next += 1;
    let id = format!("{prefix}_{}", sheet.next);
    sheet.active = Some(id.clone());
    Ok(id)
}

fn close_sheet(shared: &Shared, id: &str) {
    let mut sheet = match shared.sheet.lock() {
        Ok(sheet) => sheet,
        Err(poisoned) => poisoned.into_inner(),
    };
    if sheet.active.as_deref() == Some(id) {
        sheet.active = None;
    }
}

fn emit_interaction(shared: &Shared, event: Interaction) -> std::result::Result<(), String> {
    shared
        .interactions
        .lock()
        .map_err(|_| "rpc interaction lock poisoned".to_string())?
        .as_ref()
        .ok_or_else(|| "interaction receiver is closed".to_string())?
        .send(event)
        .map_err(|_| "interaction receiver is closed".to_string())
}

struct GateFailure {
    reason: find::Reason,
    message: String,
}

fn busy_gate(message: impl Into<String>) -> GateFailure {
    GateFailure {
        reason: find::Reason::Busy,
        message: message.into(),
    }
}

fn gate(
    shared: &Shared,
    request: RuleRequest<'_>,
    key: ApprovalKey,
) -> std::result::Result<(), GateFailure> {
    let outside = !request.path.starts_with(request.project);
    let ask = match shared.rules.decide(request) {
        Decision::Allow => return Ok(()),
        Decision::Deny(message) => {
            return Err(GateFailure {
                reason: if message.contains("unavailable") {
                    find::Reason::Busy
                } else {
                    find::Reason::Denied
                },
                message,
            })
        }
        Decision::Ask(ask) => ask,
    };
    let id = open_sheet(shared, "approval").map_err(|error| busy_gate(error.message))?;
    let prompt = ask.prompt(id.clone());
    let (answer, received) = mpsc::sync_channel(1);
    let pending = ApprovalPending {
        id: id.clone(),
        key,
        ask,
        answer,
    };
    match shared.approval.lock() {
        Ok(mut state) => state.active = Some(pending),
        Err(_) => {
            close_sheet(shared, &id);
            return Err(busy_gate("approval lock poisoned"));
        }
    }
    if let Err(error) = emit_interaction(shared, Interaction::ApprovalOpened { prompt }) {
        let _ = resolve_approval(shared, &id, Err(error.clone()));
        return Err(busy_gate(error));
    }
    match received.recv() {
        Ok(Ok(ApprovalChoice::AllowOnce | ApprovalChoice::AllowSession)) => Ok(()),
        Ok(Ok(ApprovalChoice::Deny)) | Ok(Err(_)) | Err(_) => Err(GateFailure {
            reason: if outside {
                find::Reason::OutsideScope
            } else {
                find::Reason::Denied
            },
            message: "Approval denied".into(),
        }),
    }
}

fn resolve_approval(shared: &Shared, id: &str, answer: ApprovalAnswer) -> Result<()> {
    let pending = {
        let mut state = shared
            .approval
            .lock()
            .map_err(|_| anyhow::anyhow!("approval lock poisoned"))?;
        match state.active.as_ref().filter(|pending| pending.id == id) {
            Some(_) => state.active.take(),
            None => None,
        }
    }
    .context("approval is no longer active")?;
    let answer = match answer {
        Ok(ApprovalChoice::AllowSession) => shared
            .rules
            .allow_session(&pending.ask)
            .map(|_| ApprovalChoice::AllowSession)
            .map_err(|error| error.to_string()),
        other => other,
    };
    let error = answer.as_ref().err().map(|value| sentence(value));
    if let Err(emit) = emit_interaction(
        shared,
        Interaction::ApprovalClosed {
            id: pending.id.clone(),
            error,
        },
    ) {
        eprintln!("emit approval close: {emit}");
    }
    close_sheet(shared, &pending.id);
    let sent = pending.answer.send(answer);
    sent.map_err(|_| anyhow::anyhow!("approval requester stopped waiting"))
}

fn cancel_approval(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: shell::Cancel = decode(params, "approval cancellation")?;
    let key = ApprovalKey {
        turn: input.turn_id,
        request: input.request_id,
        call: input.call_id,
    };
    let id = shared
        .approval
        .lock()
        .map_err(|_| internal_failure("approval lock poisoned"))?
        .active
        .as_ref()
        .filter(|pending| pending.key == key)
        .map(|pending| pending.id.clone());
    if let Some(id) = id {
        let _ = resolve_approval(shared, &id, Err("approval cancelled".into()));
    }
    Ok(serde_json::json!({"cancelled": true}))
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
    let input: TaskInput = decode(params, "task request")?;
    let result = match input {
        TaskInput::Create { title, path } => shared.proj.create(title, path),
        TaskInput::Update { id, status, note } => shared.proj.update(id, status, note),
    }
    .map_err(invalid_failure)?;
    encode(result, "task result")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveInput {
    task_id: Option<String>,
}

fn live(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: LiveInput = decode(params, "live environment request")?;
    let value = shared
        .proj
        .live(input.task_id.as_deref())
        .map_err(internal_failure)?;
    encode(value, "live environment")
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
enum RunInput {
    Create {
        parent_id: Option<String>,
        task_id: Option<String>,
        role: RunRole,
        title: String,
        request: String,
        #[serde(default)]
        constraints: Vec<String>,
        #[serde(default)]
        media: Vec<u16>,
        #[serde(default)]
        related: Vec<String>,
        #[serde(default)]
        highlight: Vec<PathBuf>,
    },
    Update {
        id: String,
        status: RunStatus,
        attempt: u32,
        report: Option<String>,
    },
}

#[derive(Serialize)]
struct RunReply {
    #[serde(flatten)]
    meta: RunMeta,
    reports: Vec<RunReport>,
}

fn run(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: RunInput = decode(params, "run request")?;
    match input {
        RunInput::Create {
            parent_id,
            task_id,
            role,
            title,
            request,
            constraints,
            media,
            related,
            highlight,
        } => {
            let (task_id, project_id) = match task_id {
                Some(id) => {
                    let task = shared.proj.task(&id).map_err(invalid_failure)?;
                    (Some(task.id), Some(task.project_id))
                }
                None => (None, None),
            };
            let reports = match project_id.as_deref() {
                Some(project) => {
                    shared
                        .proj
                        .related(project, &related)
                        .map_err(invalid_failure)?;
                    shared
                        .runs
                        .reports(project, &related, &highlight)
                        .map_err(invalid_failure)?
                }
                None if related.is_empty() && highlight.is_empty() => Vec::new(),
                None => {
                    return Err(invalid_failure(anyhow::anyhow!(
                        "a scout cannot receive reports"
                    )))
                }
            };
            let meta = shared
                .runs
                .create(RunCreate {
                    parent_id,
                    task_id,
                    project_id,
                    role,
                    title,
                    request,
                    constraints,
                    media,
                    related,
                    highlight,
                })
                .map_err(invalid_failure)?;
            encode(RunReply { meta, reports }, "run metadata")
        }
        RunInput::Update {
            id,
            status,
            attempt,
            report,
        } => {
            let meta = shared
                .runs
                .update(&id, status, attempt, report)
                .map_err(invalid_failure)?;
            encode(meta, "run metadata")
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
enum PlanInput {
    Create {
        turn_id: String,
        task_id: String,
        goal: String,
        steps: Vec<Step>,
    },
    Update {
        task_id: String,
        step_id: String,
        status: StepStatus,
        note: String,
    },
}

fn plan(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let plan = match decode(params, "plan request")? {
        PlanInput::Create {
            turn_id,
            task_id,
            goal,
            steps,
        } => shared.proj.plan_create(&task_id, &turn_id, goal, steps),
        PlanInput::Update {
            task_id,
            step_id,
            status,
            note,
        } => shared.proj.plan_update(&task_id, &step_id, status, note),
    }
    .map_err(invalid_failure)?;
    let path = plan.path.clone();
    let _ = emit_interaction(shared, Interaction::PlanChanged { plan });
    encode(serde_json::json!({"ok": true, "path": path}), "plan result")
}

fn find(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: find::Input = decode(params, "find request")?;
    let scope = shared
        .proj
        .scope(input.task_id.as_deref())
        .map_err(invalid_failure)?;
    let key = match (input.turn_id.clone(), input.request_id.clone()) {
        (Some(turn), Some(request)) => Some(write::Key::new(turn, request).map_err(tool_failure)?),
        (None, None) => None,
        _ => return Err(invalid_failure("find run identity is incomplete")),
    };
    let target = scope.resolve(input.path.as_deref().or(input.root.as_deref()));
    if let Err(error) = gate(
        shared,
        RuleRequest {
            tool: rule::Tool::Read,
            role: None,
            command: None,
            path: &target,
            project: scope.root(),
            detail: "",
        },
        ApprovalKey {
            turn: input.turn_id.clone().unwrap_or_default(),
            request: input.request_id.clone().unwrap_or_default(),
            call: input.call_id.clone().unwrap_or_default(),
        },
    ) {
        return Ok(gated("find", error));
    }
    let mut outcome = find::run(&scope, input);
    if outcome.result.ok {
        if let Some(key) = key {
            if let Err(error) = shared.reads.mark(key, outcome.reads) {
                outcome.result = find::Result {
                    ok: false,
                    value: None,
                    error: Some(error),
                };
            }
        }
    }
    encode(outcome.result, "find result")
}

fn write(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: write::WriteInput = decode(params, "write request")?;
    let scope = shared
        .proj
        .scope(input.task_id.as_deref())
        .map_err(internal_failure)?;
    write::Key::new(input.turn_id.clone(), input.request_id.clone()).map_err(tool_failure)?;
    let path = write::policy_path(&scope, &input.path, false).map_err(tool_failure)?;
    let detail = format!("{:016x}", write::sig(input.content.as_bytes()));
    if let Err(error) = gate(
        shared,
        RuleRequest {
            tool: rule::Tool::Write,
            role: None,
            command: None,
            path: &path,
            project: scope.root(),
            detail: &detail,
        },
        ApprovalKey {
            turn: input.turn_id.clone(),
            request: input.request_id.clone(),
            call: input.call_id.clone(),
        },
    ) {
        return Ok(gated("write", error));
    }
    encode(
        shared.reads.write(&scope, &path, input.content),
        "write result",
    )
}

fn edit(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: write::EditInput = decode(params, "edit request")?;
    let scope = shared
        .proj
        .scope(input.task_id.as_deref())
        .map_err(internal_failure)?;
    let key =
        write::Key::new(input.turn_id.clone(), input.request_id.clone()).map_err(tool_failure)?;
    let path = write::policy_path(&scope, &input.path, true).map_err(tool_failure)?;
    let detail = format!(
        "{:016x}:{:016x}:{}",
        write::sig(input.target.as_bytes()),
        write::sig(input.replacement.as_bytes()),
        input.all
    );
    if let Err(error) = gate(
        shared,
        RuleRequest {
            tool: rule::Tool::Edit,
            role: None,
            command: None,
            path: &path,
            project: scope.root(),
            detail: &detail,
        },
        ApprovalKey {
            turn: input.turn_id.clone(),
            request: input.request_id.clone(),
            call: input.call_id.clone(),
        },
    ) {
        return Ok(gated("edit", error));
    }
    encode(
        shared.reads.edit(
            &scope,
            key,
            &path,
            &input.target,
            &input.replacement,
            input.all,
        ),
        "edit result",
    )
}

fn shell(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let mut input: shell::Input = decode(params, "shell request")?;
    let scope = shared
        .proj
        .scope(input.task_id.as_deref())
        .map_err(internal_failure)?;
    let cwd = shell::policy_cwd(&scope, &input).map_err(tool_failure)?;
    let detail = if input.env.is_empty() {
        String::new()
    } else {
        format!("{:016x}", write::sig(format!("{:?}", input.env).as_bytes()))
    };
    if let Err(error) = gate(
        shared,
        RuleRequest {
            tool: rule::Tool::Shell,
            role: Some(input.role),
            command: Some(&input.command),
            path: &cwd,
            project: scope.root(),
            detail: &detail,
        },
        ApprovalKey {
            turn: input.turn_id.clone(),
            request: input.request_id.clone(),
            call: input.call_id.clone(),
        },
    ) {
        return Ok(gated("shell", error));
    }
    input.cwd = Some(cwd);
    encode(shared.shells.run(&scope, input), "shell result")
}

fn gated(kind: &str, error: GateFailure) -> Value {
    let error = serde_json::json!({"reason": error.reason, "message": error.message});
    if kind == "shell" {
        serde_json::json!({
            "ok": false, "kind": kind, "stdout": "", "stderr": "", "error": error
        })
    } else {
        serde_json::json!({"ok": false, "error": error})
    }
}

fn cancel_shell(shared: &Shared, params: Option<Value>) -> std::result::Result<Value, Failure> {
    let input: shell::Cancel = decode(params, "shell cancellation")?;
    let cancelled = shared.shells.cancel(input).map_err(tool_failure)?;
    Ok(serde_json::json!({"cancelled": cancelled}))
}

fn tool_failure(error: find::Failure) -> Failure {
    invalid_failure(error.message)
}

fn internal_failure(error: impl std::fmt::Display) -> Failure {
    Failure {
        code: -32603,
        message: error.to_string(),
    }
}

fn invalid_failure(error: impl std::fmt::Display) -> Failure {
    Failure {
        code: -32602,
        message: error.to_string(),
    }
}

fn decode<T: DeserializeOwned>(params: Option<Value>, what: &str) -> Result<T, Failure> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| invalid_failure(format!("decode {what}: {error}")))
}

fn encode(value: impl Serialize, what: &str) -> Result<Value, Failure> {
    serde_json::to_value(value).map_err(|error| internal_failure(format!("encode {what}: {error}")))
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
    let closed = internal_failure("turn notification receiver is closed");
    let notices = shared
        .notices
        .lock()
        .map_err(|_| internal_failure("rpc notice lock poisoned"))?;
    if notices
        .as_ref()
        .ok_or(closed)?
        .send(Stamped { order, notice })
        .is_err()
    {
        return Err(internal_failure("turn notification receiver is closed"));
    }
    match failed {
        Some(message) => Err(invalid_failure(message)),
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
    shared.shells.cancel_all();
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
    let clarify = match shared.clarify.lock() {
        Ok(mut state) => state.active.take(),
        Err(poisoned) => poisoned.into_inner().active.take(),
    };
    if let Some(pending) = clarify {
        let id = pending.id;
        close_sheet(shared, &id);
        let _ = pending.answer.send(Err(error.into()));
        let _ = emit_interaction(
            shared,
            Interaction::ClarifyClosed {
                id,
                error: Some(sentence(error)),
            },
        );
    }
    let approval = match shared.approval.lock() {
        Ok(mut state) => state.active.take(),
        Err(poisoned) => poisoned.into_inner().active.take(),
    };
    if let Some(pending) = approval {
        let id = pending.id;
        close_sheet(shared, &id);
        let _ = pending.answer.send(Err(error.into()));
        let _ = emit_interaction(
            shared,
            Interaction::ApprovalClosed {
                id,
                error: Some(sentence(error)),
            },
        );
    }
    match shared.interactions.lock() {
        Ok(mut interactions) => interactions.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    };
    for wait in waits {
        if wait.send(Err(error.into())).is_err() {
            eprintln!("rpc caller stopped waiting");
        }
    }
}

#[cfg(test)]
#[path = "rpc_policy_test.rs"]
mod policy_tests;

#[cfg(test)]
#[path = "rpc_test.rs"]
mod tests;

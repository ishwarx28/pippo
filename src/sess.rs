// Owns turn state, durable message transitions and live context.

use crate::store::{atomic, replace, Store};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Paused,
    Blocked,
    Done,
    Failed,
    Stopped,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunRole {
    Planner,
    Explorer,
    Worker,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunMeta {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub role: RunRole,
    pub title: String,
    pub request: String,
    pub constraints: Vec<String>,
    pub media: Vec<u16>,
    pub related: Vec<String>,
    pub highlight: Vec<PathBuf>,
    pub status: RunStatus,
    pub attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report_path: Option<PathBuf>,
}

pub struct RunCreate {
    pub parent_id: Option<String>,
    pub task_id: Option<String>,
    pub project_id: Option<String>,
    pub role: RunRole,
    pub title: String,
    pub request: String,
    pub constraints: Vec<String>,
    pub media: Vec<u16>,
    pub related: Vec<String>,
    pub highlight: Vec<PathBuf>,
}

#[derive(Serialize)]
pub struct RunReport {
    pub path: PathBuf,
    pub central: bool,
}

struct RunEntry {
    meta: RunMeta,
    path: Option<PathBuf>,
}

pub struct Runs {
    root: PathBuf,
    projects: PathBuf,
    entries: Mutex<BTreeMap<String, RunEntry>>,
}

impl Runs {
    pub fn open(root: PathBuf) -> Result<Self> {
        let runs = root.join("session/runs");
        fs::create_dir_all(&runs)
            .with_context(|| format!("create runs directory {}", runs.display()))?;
        let mut entries = BTreeMap::new();
        load_runs(&runs, None, &mut entries)?;
        Ok(Self {
            root: runs,
            projects: root.join("projects"),
            entries: Mutex::new(entries),
        })
    }

    pub fn create(&self, input: RunCreate) -> Result<RunMeta> {
        let title = input.title.trim();
        let request = input.request.trim();
        if title.is_empty() || title.contains(['\r', '\n', '/', '\0']) || request.is_empty() {
            anyhow::bail!("run title and request are required");
        }
        let mut entries = self.lock()?;
        if input.task_id.is_none()
            && (input.role != RunRole::Explorer
                || input.parent_id.is_some()
                || input.project_id.is_some())
        {
            anyhow::bail!("only a root explorer scout may omit its task");
        }
        if let Some(parent_id) = input.parent_id.as_deref() {
            let parent = entries
                .get(parent_id)
                .with_context(|| format!("parent run {parent_id} is not registered"))?;
            if parent.meta.task_id != input.task_id || parent.meta.project_id != input.project_id {
                anyhow::bail!("child run must keep its parent's task and project");
            }
        }
        let id = loop {
            let id = run_id()?;
            if !entries.contains_key(&id) {
                break id;
            }
        };
        let meta = RunMeta {
            id: id.clone(),
            parent_id: input.parent_id.clone(),
            task_id: input.task_id,
            project_id: input.project_id,
            role: input.role,
            title: title.into(),
            request: request.into(),
            constraints: input.constraints,
            media: input.media,
            related: input.related,
            highlight: input.highlight,
            status: RunStatus::Running,
            attempt: 1,
            report_path: None,
        };
        let base = match input.parent_id.as_deref() {
            Some(parent) => entries[parent]
                .path
                .as_ref()
                .context("a scout cannot own child runs")?
                .join("runs"),
            None => self.root.clone(),
        };
        let path = meta
            .task_id
            .as_ref()
            .map(|_| create_run(&base, &meta))
            .transpose()?;
        entries.insert(
            id,
            RunEntry {
                meta: meta.clone(),
                path,
            },
        );
        Ok(meta)
    }

    pub fn update(
        &self,
        id: &str,
        status: RunStatus,
        attempt: u32,
        report: Option<String>,
    ) -> Result<RunMeta> {
        let mut entries = self.lock()?;
        let entry = entries
            .get_mut(id)
            .with_context(|| format!("run {id} is not registered"))?;
        let valid = match (entry.meta.status, status) {
            (
                RunStatus::Running,
                RunStatus::Paused | RunStatus::Blocked | RunStatus::Done | RunStatus::Failed,
            )
            | (RunStatus::Running, RunStatus::Stopped | RunStatus::Interrupted)
            | (
                RunStatus::Paused,
                RunStatus::Running | RunStatus::Stopped | RunStatus::Interrupted,
            ) => attempt == entry.meta.attempt,
            (
                RunStatus::Blocked
                | RunStatus::Done
                | RunStatus::Failed
                | RunStatus::Stopped
                | RunStatus::Interrupted,
                RunStatus::Running,
            ) => attempt == entry.meta.attempt + 1,
            (RunStatus::Blocked, RunStatus::Stopped | RunStatus::Interrupted) => {
                attempt == entry.meta.attempt
            }
            _ => false,
        };
        if !valid {
            anyhow::bail!("invalid run transition for {id}");
        }
        let mut next = entry.meta.clone();
        next.status = status;
        next.attempt = attempt;
        if let (Some(report), Some(project), Some(task)) =
            (report, next.project_id.as_deref(), next.task_id.as_deref())
        {
            let relative = report_path(project, task, &next.title, attempt)?;
            let path = self.runtime()?.join(&relative);
            if path.exists() {
                anyhow::bail!("report version already exists for run {id}");
            }
            replace(&path, report.as_bytes())?;
            next.report_path = Some(relative);
        }
        if let Some(path) = entry.path.as_ref() {
            atomic(&path.join("meta.json"), &next)?;
        }
        entry.meta = next.clone();
        Ok(next)
    }

    pub fn reports(
        &self,
        project: &str,
        related: &[String],
        highlight: &[PathBuf],
    ) -> Result<Vec<RunReport>> {
        let entries = self.lock()?;
        let root = self.runtime()?;
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        for path in highlight {
            if path.is_absolute()
                || path
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
                || !entries.values().any(|entry| {
                    let meta = &entry.meta;
                    meta.project_id.as_deref() == Some(project)
                        && (1..=meta.attempt).any(|attempt| {
                            meta.task_id.as_deref().is_some_and(|task| {
                                report_path(project, task, &meta.title, attempt)
                                    .is_ok_and(|expected| expected == *path)
                            })
                        })
                })
                || !root.join(path).is_file()
            {
                anyhow::bail!("highlighted report {} is not registered", path.display());
            }
            if seen.insert(path.clone()) {
                result.push(RunReport {
                    path: path.clone(),
                    central: true,
                });
            }
        }
        let related: BTreeSet<&str> = related.iter().map(String::as_str).collect();
        let mut latest = entries
            .values()
            .filter(|entry| {
                entry.meta.project_id.as_deref() == Some(project)
                    && entry
                        .meta
                        .task_id
                        .as_deref()
                        .is_some_and(|task| related.contains(task))
            })
            .filter(|entry| entry.meta.report_path.is_some())
            .map(|entry| {
                let meta = &entry.meta;
                let path = meta
                    .report_path
                    .as_ref()
                    .context("completed run has no report")?;
                let task = meta
                    .task_id
                    .as_deref()
                    .context("reported run has no task")?;
                if !(1..=meta.attempt).any(|attempt| {
                    report_path(project, task, &meta.title, attempt)
                        .is_ok_and(|expected| expected == *path)
                }) {
                    anyhow::bail!("registered report {} is invalid", path.display());
                }
                Ok(path.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        latest.sort();
        latest.dedup();
        for path in latest {
            if !root.join(&path).is_file() {
                anyhow::bail!("registered report {} is missing", path.display());
            }
            if seen.insert(path.clone()) {
                result.push(RunReport {
                    path,
                    central: false,
                });
            }
        }
        Ok(result)
    }

    #[cfg(test)]
    fn snapshot(&self) -> Result<Vec<(RunMeta, Option<PathBuf>)>> {
        Ok(self
            .lock()?
            .values()
            .map(|entry| (entry.meta.clone(), entry.path.clone()))
            .collect())
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<String, RunEntry>>> {
        self.entries
            .lock()
            .map_err(|_| anyhow::anyhow!("run state lock poisoned"))
    }

    fn runtime(&self) -> Result<&Path> {
        self.projects.parent().context("runtime root is missing")
    }
}

fn report_path(project: &str, task: &str, title: &str, attempt: u32) -> Result<PathBuf> {
    let mut name = title.split_whitespace().collect::<Vec<_>>().join("_");
    if attempt > 1 {
        write!(name, "_({attempt})").context("version report name")?;
    }
    Ok(PathBuf::from("projects")
        .join(project)
        .join("reports")
        .join(task)
        .join(name + ".md"))
}

fn create_run(base: &Path, meta: &RunMeta) -> Result<PathBuf> {
    fs::create_dir_all(base).with_context(|| format!("create run parent {}", base.display()))?;
    let path = base.join(&meta.id);
    let temp = base.join(format!(".{}.tmp", meta.id));
    fs::create_dir(&temp).with_context(|| format!("create run directory {}", temp.display()))?;
    fs::create_dir(temp.join("runs"))
        .with_context(|| format!("create child runs directory {}", temp.display()))?;
    atomic(&temp.join("meta.json"), meta)?;
    for (name, bytes) in [("messages.jsonl", b"".as_slice()), ("replay.json", b"[]\n")] {
        let file = temp.join(name);
        fs::write(&file, bytes).with_context(|| format!("create run file {}", file.display()))?;
        File::open(&file)
            .with_context(|| format!("open run file {}", file.display()))?
            .sync_all()
            .with_context(|| format!("flush run file {}", file.display()))?;
    }
    fs::rename(&temp, &path).with_context(|| format!("publish run {}", path.display()))?;
    File::open(base)
        .with_context(|| format!("open run parent {}", base.display()))?
        .sync_all()
        .with_context(|| format!("flush run parent {}", base.display()))?;
    Ok(path)
}

fn load_runs(
    root: &Path,
    parent: Option<&str>,
    entries: &mut BTreeMap<String, RunEntry>,
) -> Result<()> {
    let mut paths = fs::read_dir(root)
        .with_context(|| format!("read runs directory {}", root.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort_by_key(|entry| entry.file_name());
    for entry in paths {
        if !entry.file_type()?.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let path = entry.path();
        let file = path.join("meta.json");
        let mut meta: RunMeta = serde_json::from_slice(
            &fs::read(&file).with_context(|| format!("read run state {}", file.display()))?,
        )
        .with_context(|| format!("parse run state {}", file.display()))?;
        if !valid_run_id(&meta.id)
            || entry.file_name().to_string_lossy() != meta.id
            || meta.parent_id.as_deref() != parent
            || entries.contains_key(&meta.id)
        {
            anyhow::bail!("invalid run state {}", file.display());
        }
        if meta.status == RunStatus::Running {
            meta.status = RunStatus::Interrupted;
            atomic(&file, &meta)?;
        }
        let id = meta.id.clone();
        entries.insert(
            id.clone(),
            RunEntry {
                meta,
                path: Some(path.clone()),
            },
        );
        load_runs(&path.join("runs"), Some(&id), entries)?;
    }
    Ok(())
}

fn run_id() -> Result<String> {
    let mut bytes = [0_u8; 4];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("generate run id: {error}"))?;
    let mut id = String::from("r_");
    for byte in bytes {
        write!(id, "{byte:02x}").context("encode run id")?;
    }
    Ok(id)
}

fn valid_run_id(id: &str) -> bool {
    id.len() == 10 && id.starts_with("r_") && id[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

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
#[path = "sess_run_test.rs"]
mod run_tests;

#[cfg(test)]
#[path = "sess_test.rs"]
mod tests;

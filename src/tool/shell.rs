// Owns bounded foreground command execution and process-group termination.

use crate::{proj::Scope, tool::find};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    io::{self, Read},
    os::unix::process::{CommandExt, ExitStatusExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const DEFAULT_TIMEOUT: u64 = 30;
const MAX_TIMEOUT: u64 = 380;
const MAX_OUTPUT: usize = 200 * 1024;
const HALF_OUTPUT: usize = MAX_OUTPUT / 2;
const POLL: Duration = Duration::from_millis(10);
const KILL_GRACE: Duration = Duration::from_secs(5);
const MARKER: &str = "\n... output middle elided ...\n";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Key {
    turn: String,
    request: String,
    call: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub turn_id: String,
    pub request_id: String,
    pub call_id: String,
    pub task_id: Option<String>,
    pub command: String,
    pub cwd: Option<PathBuf>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cancel {
    pub turn_id: String,
    pub request_id: String,
    pub call_id: String,
}

#[derive(Debug, Serialize)]
pub struct Result {
    pub ok: bool,
    pub kind: &'static str,
    pub stdout: String,
    pub stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<find::Failure>,
}

#[derive(Default)]
pub struct Shells {
    running: Mutex<HashMap<Key, Arc<AtomicBool>>>,
}

struct Capture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total: usize,
}

impl Shells {
    pub fn run(&self, scope: &Scope, input: Input) -> Result {
        let key = match key(&input.turn_id, &input.request_id, &input.call_id) {
            Ok(key) => key,
            Err(error) => return failed(error),
        };
        let stop = match self.running.lock() {
            Ok(mut running) => running
                .entry(key.clone())
                .or_insert_with(|| Arc::new(AtomicBool::new(false)))
                .clone(),
            Err(_) => return failed(fail(find::Reason::Busy, "shell state lock poisoned")),
        };
        let result = execute(scope, input, &stop);
        match self.running.lock() {
            Ok(mut running) => {
                running.remove(&key);
                result
            }
            Err(_) if result.ok => failed(fail(find::Reason::Busy, "shell state lock poisoned")),
            Err(_) => result,
        }
    }

    pub fn cancel(&self, input: Cancel) -> std::result::Result<bool, find::Failure> {
        let key = key(&input.turn_id, &input.request_id, &input.call_id)?;
        let mut running = self
            .running
            .lock()
            .map_err(|_| fail(find::Reason::Busy, "shell state lock poisoned"))?;
        let stop = running
            .entry(key)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)));
        stop.store(true, Ordering::Release);
        Ok(true)
    }

    pub fn cancel_all(&self) {
        let running = match self.running.lock() {
            Ok(running) => running,
            Err(poisoned) => poisoned.into_inner(),
        };
        for stop in running.values() {
            stop.store(true, Ordering::Release);
        }
    }
}

fn execute(scope: &Scope, input: Input, stop: &AtomicBool) -> Result {
    let (cwd, timeout) = match validate(scope, &input) {
        Ok(value) => value,
        Err(error) => return failed(error),
    };
    if stop.load(Ordering::Acquire) {
        return stopped(false, String::new(), String::new(), None, None);
    }
    let mut command = Command::new("/bin/sh");
    command
        .args(["-lc", &input.command])
        .current_dir(cwd)
        .envs(&input.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return failed(find::io_failure(error)),
    };
    let pid = child.id() as i32;
    let stdout = match child.stdout.take() {
        Some(pipe) => thread::spawn(move || capture(pipe)),
        None => return broken_child(&mut child, pid, "open command stdout"),
    };
    let stderr = match child.stderr.take() {
        Some(pipe) => thread::spawn(move || capture(pipe)),
        None => return broken_child(&mut child, pid, "open command stderr"),
    };
    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let mut cancelled = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if stop.load(Ordering::Acquire) => {
                cancelled = true;
                terminate(&mut child, pid);
                break child.wait();
            }
            Ok(None) if Instant::now() >= deadline => {
                timed_out = true;
                terminate(&mut child, pid);
                break child.wait();
            }
            Ok(None) => thread::sleep(POLL),
            Err(error) => break Err(error),
        }
    };
    if !timed_out && !cancelled && group_exists(pid) {
        terminate(&mut child, pid);
    }
    let status = match status {
        Ok(status) => status,
        Err(error) => return failed(find::io_failure(error)),
    };
    let out = match join(stdout) {
        Ok(output) => output,
        Err(error) => return failed(find::io_failure(error)),
    };
    let err = match join(stderr) {
        Ok(output) => output,
        Err(error) => return failed(find::io_failure(error)),
    };
    let (stdout, stderr) = bounded(out, err);
    if timed_out || cancelled {
        return stopped(timed_out, stdout, stderr, status.code(), status.signal());
    }
    Result {
        ok: true,
        kind: "shell",
        stdout,
        stderr,
        exit_code: status.code(),
        signal: status.signal(),
        job_id: None,
        timed_out: false,
        cancelled: false,
        error: None,
    }
}

fn validate(
    scope: &Scope,
    input: &Input,
) -> std::result::Result<(PathBuf, Duration), find::Failure> {
    if input.command.trim().is_empty() || input.command.contains('\0') {
        return Err(fail(
            find::Reason::BadArgs,
            "shell command is empty or invalid",
        ));
    }
    if input.background {
        return Err(fail(
            find::Reason::BadArgs,
            "background commands are not available yet",
        ));
    }
    let seconds = input.timeout.unwrap_or(DEFAULT_TIMEOUT);
    if seconds == 0 || seconds > MAX_TIMEOUT {
        return Err(fail(
            find::Reason::BadArgs,
            "shell timeout must be between 1 and 380 seconds",
        ));
    }
    for (name, value) in &input.env {
        if name.is_empty() || name.contains(['=', '\0']) || value.contains('\0') {
            return Err(fail(
                find::Reason::BadArgs,
                "shell environment contains an invalid name or value",
            ));
        }
    }
    let cwd = scope.resolve(input.cwd.as_deref());
    let cwd = fs::canonicalize(&cwd).map_err(find::io_failure)?;
    if !cwd.is_dir() {
        return Err(fail(
            find::Reason::BadArgs,
            "shell working directory is not a directory",
        ));
    }
    Ok((cwd, Duration::from_secs(seconds)))
}

pub(crate) fn policy_cwd(
    scope: &Scope,
    input: &Input,
) -> std::result::Result<PathBuf, find::Failure> {
    validate(scope, input).map(|value| value.0)
}

fn terminate(child: &mut Child, pgid: i32) {
    signal(pgid, libc::SIGTERM);
    let deadline = Instant::now() + KILL_GRACE;
    while Instant::now() < deadline {
        let _ = child.try_wait();
        if !group_exists(pgid) {
            return;
        }
        thread::sleep(POLL);
    }
    signal(pgid, libc::SIGKILL);
}

fn signal(pgid: i32, signal: i32) {
    // A negative pid addresses the whole group created before exec.
    unsafe {
        libc::kill(-pgid, signal);
    }
}

fn group_exists(pgid: i32) -> bool {
    unsafe { libc::kill(-pgid, 0) == 0 }
}

fn broken_child(child: &mut Child, pid: i32, message: &str) -> Result {
    terminate(child, pid);
    let _ = child.wait();
    failed(fail(find::Reason::Busy, message))
}

fn capture(mut input: impl Read) -> io::Result<Capture> {
    let mut output = Capture {
        head: Vec::with_capacity(HALF_OUTPUT),
        tail: VecDeque::with_capacity(HALF_OUTPUT),
        total: 0,
    };
    let mut bytes = [0; 8192];
    loop {
        let count = input.read(&mut bytes)?;
        if count == 0 {
            return Ok(output);
        }
        output.push(&bytes[..count]);
    }
}

impl Capture {
    fn push(&mut self, bytes: &[u8]) {
        self.total = self.total.saturating_add(bytes.len());
        let head = (HALF_OUTPUT - self.head.len()).min(bytes.len());
        self.head.extend_from_slice(&bytes[..head]);
        let bytes = &bytes[head..];
        if bytes.len() >= HALF_OUTPUT {
            self.tail.clear();
            self.tail
                .extend(bytes[bytes.len() - HALF_OUTPUT..].iter().copied());
        } else {
            let excess = self
                .tail
                .len()
                .saturating_add(bytes.len())
                .saturating_sub(HALF_OUTPUT);
            self.tail.drain(..excess);
            self.tail.extend(bytes.iter().copied());
        }
    }

    fn render(self, limit: usize) -> String {
        if limit == 0 {
            return String::new();
        }
        let mut kept = self.head;
        kept.extend(self.tail);
        if self.total <= limit {
            return bound(String::from_utf8_lossy(&kept).into_owned(), limit);
        }
        let keep = limit.saturating_sub(MARKER.len());
        let prefix = keep / 2;
        let suffix = keep - prefix;
        let mut bytes = kept[..prefix.min(kept.len())].to_vec();
        bytes.extend_from_slice(MARKER.as_bytes());
        if suffix != 0 {
            bytes.extend_from_slice(&kept[kept.len().saturating_sub(suffix)..]);
        }
        bound(String::from_utf8_lossy(&bytes).into_owned(), limit)
    }
}

fn bounded(stdout: Capture, stderr: Capture) -> (String, String) {
    let (out, err) = if stdout.total.saturating_add(stderr.total) <= MAX_OUTPUT {
        let spare = MAX_OUTPUT - stdout.total - stderr.total;
        if stdout.total == 0 {
            (0, MAX_OUTPUT)
        } else if stderr.total == 0 {
            (MAX_OUTPUT, 0)
        } else {
            (stdout.total + spare / 2, stderr.total + spare - spare / 2)
        }
    } else if stdout.total <= HALF_OUTPUT {
        (stdout.total, MAX_OUTPUT - stdout.total)
    } else if stderr.total <= HALF_OUTPUT {
        (MAX_OUTPUT - stderr.total, stderr.total)
    } else {
        (HALF_OUTPUT, HALF_OUTPUT)
    };
    (stdout.render(out), stderr.render(err))
}

fn bound(value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }
    if limit <= MARKER.len() {
        let mut end = limit;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        return value[..end].into();
    }
    let keep = limit.saturating_sub(MARKER.len());
    let mut left = keep / 2;
    while !value.is_char_boundary(left) {
        left -= 1;
    }
    let mut right = value.len().saturating_sub(keep - left);
    while right < value.len() && !value.is_char_boundary(right) {
        right += 1;
    }
    format!("{}{}{}", &value[..left], MARKER, &value[right..])
}

fn join(thread: thread::JoinHandle<io::Result<Capture>>) -> io::Result<Capture> {
    thread
        .join()
        .map_err(|_| io::Error::other("command output reader panicked"))?
}

fn key(turn: &str, request: &str, call: &str) -> std::result::Result<Key, find::Failure> {
    if turn.trim().is_empty() || request.trim().is_empty() || call.trim().is_empty() {
        Err(fail(
            find::Reason::BadArgs,
            "shell call identity is incomplete",
        ))
    } else {
        Ok(Key {
            turn: turn.into(),
            request: request.into(),
            call: call.into(),
        })
    }
}

fn stopped(
    timed_out: bool,
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
    signal: Option<i32>,
) -> Result {
    Result {
        ok: false,
        kind: "shell",
        stdout,
        stderr,
        exit_code,
        signal,
        job_id: None,
        timed_out,
        cancelled: !timed_out,
        error: Some(fail(
            if timed_out {
                find::Reason::Timeout
            } else {
                find::Reason::Busy
            },
            if timed_out {
                "shell command timed out"
            } else {
                "shell command was cancelled"
            },
        )),
    }
}

fn failed(error: find::Failure) -> Result {
    with_output(error, String::new(), String::new())
}

fn with_output(error: find::Failure, stdout: String, stderr: String) -> Result {
    Result {
        ok: false,
        kind: "shell",
        stdout,
        stderr,
        exit_code: None,
        signal: None,
        job_id: None,
        timed_out: false,
        cancelled: false,
        error: Some(error),
    }
}

fn fail(reason: find::Reason, message: &str) -> find::Failure {
    find::Failure {
        reason,
        message: message.into(),
        total_lines: None,
        matches: None,
    }
}

#[cfg(test)]
#[path = "shell_test.rs"]
mod tests;

// Owns atomic whole-file writes, exact edits and per-run read evidence.

use crate::{proj::Scope, tool::find};
use serde::{Deserialize, Serialize};
use similar::TextDiff;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIFF: usize = 64 * 1024;
type Seen = HashMap<Key, HashMap<PathBuf, u64>>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    turn: String,
    request: String,
}

impl Key {
    pub fn new(turn: String, request: String) -> std::result::Result<Self, find::Failure> {
        if turn.trim().is_empty() || request.trim().is_empty() {
            return Err(fail(
                find::Reason::BadArgs,
                "tool call requires run identity",
            ));
        }
        Ok(Self { turn, request })
    }
}

#[derive(Clone, Debug)]
pub struct Mark {
    pub path: PathBuf,
    pub sig: u64,
}

#[derive(Default)]
pub struct Reads {
    seen: Mutex<Seen>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteInput {
    pub turn_id: String,
    pub request_id: String,
    pub task_id: Option<String>,
    pub path: PathBuf,
    pub content: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditInput {
    pub turn_id: String,
    pub request_id: String,
    pub task_id: Option<String>,
    pub path: PathBuf,
    pub target: String,
    pub replacement: String,
    #[serde(default)]
    pub all: bool,
}

#[derive(Debug, Serialize)]
pub struct Result {
    pub ok: bool,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<find::Failure>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Value {
    Write {
        diff: String,
        created: bool,
        line_count: usize,
    },
    Edit {
        diff: String,
        replacements: usize,
        line_count: usize,
    },
}

impl Reads {
    pub fn mark(&self, key: Key, marks: Vec<Mark>) -> std::result::Result<(), find::Failure> {
        if marks.is_empty() {
            return Ok(());
        }
        let mut seen = self.lock()?;
        let run = seen.entry(key).or_default();
        for mark in marks {
            run.insert(mark.path, mark.sig);
        }
        Ok(())
    }

    pub fn write(&self, scope: &Scope, path: &Path, content: String) -> Result {
        output((|| {
            check_size(&content)?;
            let mut seen = self.lock()?;
            let target = target(scope, path, false)?;
            let created = !target.exists();
            let old = if created {
                String::new()
            } else {
                read(&target)?
            };
            let shown = find::display(scope, &target);
            let diff = diff(&shown, &old, &content);
            atomic(&target, content.as_bytes(), !created)?;
            for run in seen.values_mut() {
                run.remove(&target);
            }
            Ok(Value::Write {
                diff,
                created,
                line_count: lines(&content),
            })
        })())
    }

    pub fn edit(
        &self,
        scope: &Scope,
        key: Key,
        path: &Path,
        target_text: &str,
        replacement: &str,
        all: bool,
    ) -> Result {
        output((|| {
            if target_text.is_empty() {
                return Err(fail(find::Reason::BadArgs, "edit target is empty"));
            }
            let path = target(scope, path, true)?;
            let mut seen = self.lock()?;
            let expected = seen
                .get(&key)
                .and_then(|paths| paths.get(&path))
                .copied()
                .ok_or_else(|| fail(find::Reason::Denied, "edit requires a read in this run"))?;
            let old = read(&path)?;
            if sig(old.as_bytes()) != expected {
                return Err(fail(
                    find::Reason::Busy,
                    "file changed since it was read; read it again",
                ));
            }
            let count = old.matches(target_text).count();
            if count == 0 || !all && count != 1 {
                return Err(find::Failure {
                    reason: find::Reason::BadArgs,
                    message: if count == 0 {
                        "edit target was not found".into()
                    } else {
                        "edit target is not unique".into()
                    },
                    total_lines: None,
                    matches: Some(count),
                });
            }
            let new = if all {
                old.replace(target_text, replacement)
            } else {
                old.replacen(target_text, replacement, 1)
            };
            check_size(&new)?;
            let shown = find::display(scope, &path);
            let diff = diff(&shown, &old, &new);
            atomic(&path, new.as_bytes(), true)?;
            seen.entry(key)
                .or_default()
                .insert(path, sig(new.as_bytes()));
            Ok(Value::Edit {
                diff,
                replacements: count,
                line_count: lines(&new),
            })
        })())
    }

    fn lock(&self) -> std::result::Result<MutexGuard<'_, Seen>, find::Failure> {
        self.seen
            .lock()
            .map_err(|_| fail(find::Reason::Busy, "read evidence lock poisoned"))
    }
}

pub fn sig(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn target(
    scope: &Scope,
    input: &Path,
    must_exist: bool,
) -> std::result::Result<PathBuf, find::Failure> {
    let path = scope.resolve(Some(input));
    find::guard(&path)?;
    if path.exists() {
        let path = fs::canonicalize(&path).map_err(find::io_failure)?;
        find::guard(&path)?;
        if !path.is_file() {
            return Err(fail(find::Reason::BadArgs, "path is not a file"));
        }
        return Ok(path);
    }
    if must_exist {
        return Err(fail(find::Reason::NotFound, "file does not exist"));
    }
    let name = path
        .file_name()
        .ok_or_else(|| fail(find::Reason::BadArgs, "path has no file name"))?;
    let parent = path
        .parent()
        .ok_or_else(|| fail(find::Reason::BadArgs, "path has no parent"))?;
    let parent = fs::canonicalize(parent).map_err(find::io_failure)?;
    find::guard(&parent)?;
    Ok(parent.join(name))
}

fn read(path: &Path) -> std::result::Result<String, find::Failure> {
    let bytes = fs::read(path).map_err(find::io_failure)?;
    if bytes.len() > MAX_BYTES {
        return Err(fail(find::Reason::Limit, "file exceeds the write limit"));
    }
    String::from_utf8(bytes)
        .map_err(|_| fail(find::Reason::BadArgs, "existing file is not UTF-8 text"))
}

fn atomic(path: &Path, bytes: &[u8], preserve: bool) -> std::result::Result<(), find::Failure> {
    let parent = path
        .parent()
        .ok_or_else(|| fail(find::Reason::BadArgs, "path has no parent"))?;
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random)
        .map_err(|error| fail(find::Reason::Busy, &format!("create temp name: {error}")))?;
    let tmp = parent.join(format!(
        ".pippo-write-{}-{:016x}",
        std::process::id(),
        u64::from_ne_bytes(random)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .map_err(find::io_failure)?;
        if preserve {
            file.set_permissions(fs::metadata(path).map_err(find::io_failure)?.permissions())
                .map_err(find::io_failure)?;
        }
        file.write_all(bytes).map_err(find::io_failure)?;
        file.sync_all().map_err(find::io_failure)?;
        fs::rename(&tmp, path).map_err(find::io_failure)?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(find::io_failure)
    })();
    if result.is_err() && tmp.exists() {
        if let Err(cleanup) = fs::remove_file(&tmp) {
            return Err(fail(
                find::Reason::Busy,
                &format!("atomic write failed and temp cleanup failed: {cleanup}"),
            ));
        }
    }
    result
}

fn diff(path: &str, old: &str, new: &str) -> String {
    if old.len().saturating_add(new.len()) > MAX_DIFF {
        return large_diff(path, old, new);
    }
    let value = TextDiff::from_lines(old, new)
        .unified_diff()
        .context_radius(3)
        .header(&format!("a/{path}"), &format!("b/{path}"))
        .to_string();
    if value.len() <= MAX_DIFF {
        return value;
    }
    let half = (MAX_DIFF - 32) / 2;
    let mut head = half;
    while !value.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = value.len() - half;
    while !value.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}\n... diff elided ...\n{}",
        &value[..head],
        &value[tail..]
    )
}

fn large_diff(path: &str, old: &str, new: &str) -> String {
    let mut value = format!("--- a/{path}\n+++ b/{path}\n@@ diff too large; showing edges @@\n");
    let space = (MAX_DIFF - value.len() - 32) / 4;
    for (prefix, text) in [("-", old), ("+", new)] {
        let (head, tail) = edges(text, space);
        for line in head.lines() {
            value.push_str(prefix);
            value.push_str(line);
            value.push('\n');
        }
        value.push_str("... diff elided ...\n");
        for line in tail.lines() {
            value.push_str(prefix);
            value.push_str(line);
            value.push('\n');
        }
    }
    value.truncate(value.floor_char_boundary(MAX_DIFF));
    value
}

fn edges(value: &str, cap: usize) -> (&str, &str) {
    let mut head = cap.min(value.len());
    while !value.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = value.len().saturating_sub(cap);
    while !value.is_char_boundary(tail) {
        tail += 1;
    }
    (&value[..head], &value[tail..])
}

fn lines(content: &str) -> usize {
    content.lines().count()
}

fn check_size(content: &str) -> std::result::Result<(), find::Failure> {
    if content.len() > MAX_BYTES {
        Err(fail(find::Reason::Limit, "content exceeds the write limit"))
    } else {
        Ok(())
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

fn output(result: std::result::Result<Value, find::Failure>) -> Result {
    match result {
        Ok(value) => Result {
            ok: true,
            value: Some(value),
            error: None,
        },
        Err(error) => Result {
            ok: false,
            value: None,
            error: Some(error),
        },
    }
}

#[cfg(test)]
#[path = "write_test.rs"]
mod tests;

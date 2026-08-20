// Owns bounded text search and reads under a run's project-relative scope.

use crate::{
    proj::Scope,
    tool::write::{self, Mark},
};
use ignore::WalkBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

const DEFAULT_CAP: usize = 50;
const MAX_CAP: usize = 200;
const DEFAULT_CONTEXT: usize = 1;
const MAX_CONTEXT: usize = 20;
const LONG_FILE_LINES: usize = 200;
const MAX_BYTES: u64 = 8 * 1024 * 1024;
const IGNORED_DIRS: &[&str] = &[
    ".git",
    ".next",
    "build",
    "coverage",
    "dist",
    "node_modules",
    "target",
    "vendor",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub turn_id: Option<String>,
    pub request_id: Option<String>,
    pub task_id: Option<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub regex: bool,
    #[serde(rename = "in")]
    pub place: Option<Place>,
    pub root: Option<PathBuf>,
    pub context: Option<usize>,
    pub cap: Option<usize>,
    pub offset: Option<usize>,
    pub path: Option<PathBuf>,
    pub range: Option<Range>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Place {
    Content,
    Path,
    Both,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Serialize)]
pub struct Result {
    pub ok: bool,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Failure>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Value {
    Search {
        hits: Vec<Hit>,
        offset: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_offset: Option<usize>,
    },
    Read {
        path: String,
        start: usize,
        end: usize,
        total_lines: usize,
        lines: Vec<Line>,
    },
}

#[derive(Debug, Serialize)]
pub struct Hit {
    pub path: String,
    pub line: Option<usize>,
    pub total_lines: usize,
    pub source: &'static str,
    pub context: Vec<Line>,
    #[serde(skip)]
    mark: Option<Mark>,
}

#[derive(Debug, Serialize)]
pub struct Line {
    pub line: usize,
    pub text: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub matched: bool,
}

#[derive(Debug, Serialize)]
pub struct Failure {
    pub reason: Reason,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_lines: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matches: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    NotFound,
    Denied,
    BadArgs,
    Limit,
    Busy,
    Timeout,
}

pub struct Outcome {
    pub result: Result,
    pub reads: Vec<Mark>,
}

pub fn run(scope: &Scope, input: Input) -> Outcome {
    match validate(scope, input).and_then(|op| match op {
        Op::Search(args) => search(scope, args),
        Op::Read(args) => read(scope, args),
    }) {
        Ok((value, reads)) => Outcome {
            result: Result {
                ok: true,
                value: Some(value),
                error: None,
            },
            reads,
        },
        Err(error) => Outcome {
            result: Result {
                ok: false,
                value: None,
                error: Some(error),
            },
            reads: Vec::new(),
        },
    }
}

struct Search {
    query: String,
    regex: Option<Regex>,
    place: Place,
    root: PathBuf,
    context: usize,
    cap: usize,
    offset: usize,
}

struct Read {
    path: PathBuf,
    range: Option<Range>,
}

enum Op {
    Search(Search),
    Read(Read),
}

fn validate(scope: &Scope, input: Input) -> std::result::Result<Op, Failure> {
    match (input.query, input.path) {
        (Some(query), None) => {
            if query.is_empty() || input.range.is_some() {
                return Err(bad("search requires a non-empty query and no range", None));
            }
            let context = input.context.unwrap_or(DEFAULT_CONTEXT);
            let cap = input.cap.unwrap_or(DEFAULT_CAP);
            if context > MAX_CONTEXT || cap == 0 || cap > MAX_CAP {
                return Err(bad("search context or cap is outside its limit", None));
            }
            let regex = input
                .regex
                .then(|| Regex::new(&query).map_err(|error| bad(&error.to_string(), None)))
                .transpose()?;
            Ok(Op::Search(Search {
                query,
                regex,
                place: input.place.unwrap_or(Place::Content),
                root: scope.resolve(input.root.as_deref()),
                context,
                cap,
                offset: input.offset.unwrap_or(0),
            }))
        }
        (None, Some(path)) => {
            if input.regex
                || input.place.is_some()
                || input.root.is_some()
                || input.context.is_some()
                || input.cap.is_some()
                || input.offset.is_some()
            {
                return Err(bad("read accepts only path and range", None));
            }
            Ok(Op::Read(Read {
                path: scope.resolve(Some(&path)),
                range: input.range,
            }))
        }
        _ => Err(bad("find requires either query or path", None)),
    }
}

fn search(scope: &Scope, args: Search) -> std::result::Result<(Value, Vec<Mark>), Failure> {
    guard(&args.root)?;
    let meta = fs::metadata(&args.root).map_err(io_failure)?;
    let mut hits = Vec::new();
    let needed = args.offset.saturating_add(args.cap).saturating_add(1);
    if meta.is_file() {
        collect(scope, &args, &args.root, &mut hits, needed)?;
    } else if meta.is_dir() {
        let mut walk = WalkBuilder::new(&args.root);
        walk.hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .require_git(false)
            .ignore(true)
            .parents(true)
            .follow_links(false)
            .sort_by_file_path(|left, right| left.cmp(right))
            .filter_entry(|entry| {
                entry.depth() == 0
                    || !denied(entry.path())
                        && !entry
                            .file_type()
                            .is_some_and(|kind| kind.is_dir() && ignored_dir(entry.path()))
            });
        for entry in walk.build() {
            let entry = entry.map_err(|error| Failure {
                reason: Reason::Busy,
                message: error.to_string(),
                total_lines: None,
                matches: None,
            })?;
            if entry.depth() == 0 || denied(entry.path()) {
                continue;
            }
            if fs::metadata(entry.path()).is_ok_and(|meta| meta.is_file()) {
                collect(scope, &args, entry.path(), &mut hits, needed)?;
                if hits.len() >= needed {
                    break;
                }
            }
        }
    } else {
        return Err(bad("search root is not a file or directory", None));
    }
    let more = hits.len() > args.offset.saturating_add(args.cap);
    let page = hits
        .into_iter()
        .skip(args.offset)
        .take(args.cap)
        .collect::<Vec<_>>();
    let reads = page.iter().filter_map(|hit| hit.mark.clone()).collect();
    let next_offset = more.then_some(args.offset + page.len());
    Ok((
        Value::Search {
            hits: page,
            offset: args.offset,
            next_offset,
        },
        reads,
    ))
}

fn collect(
    scope: &Scope,
    args: &Search,
    path: &Path,
    hits: &mut Vec<Hit>,
    needed: usize,
) -> std::result::Result<(), Failure> {
    guard_resolved(path)?;
    let shown = display(scope, path);
    let path_match = matches!(args.place, Place::Path | Place::Both) && matched(args, &shown);
    let content = matches!(args.place, Place::Content | Place::Both);
    if !path_match && !content {
        return Ok(());
    }
    let meta = fs::metadata(path).map_err(io_failure)?;
    if meta.len() > MAX_BYTES {
        if path_match {
            hits.push(Hit {
                path: shown,
                line: None,
                total_lines: count_lines(path)?,
                source: "path",
                context: Vec::new(),
                mark: None,
            });
        }
        return Ok(());
    }
    let bytes = read_file(path)?;
    let total = line_count(&bytes);
    if path_match {
        hits.push(Hit {
            path: shown.clone(),
            line: None,
            total_lines: total,
            source: "path",
            context: Vec::new(),
            mark: None,
        });
        if hits.len() >= needed {
            return Ok(());
        }
    }
    if !content {
        return Ok(());
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(());
    };
    let lines = text.lines().collect::<Vec<_>>();
    let mark = Mark {
        path: fs::canonicalize(path).map_err(io_failure)?,
        sig: write::sig(&bytes),
    };
    for (index, line) in lines.iter().enumerate() {
        if !matched(args, line) {
            continue;
        }
        let start = index.saturating_sub(args.context);
        let end = (index + args.context + 1).min(lines.len());
        hits.push(Hit {
            path: shown.clone(),
            line: Some(index + 1),
            total_lines: total,
            source: "content",
            context: lines[start..end]
                .iter()
                .enumerate()
                .map(|(at, value)| Line {
                    line: start + at + 1,
                    text: (*value).to_owned(),
                    matched: start + at == index,
                })
                .collect(),
            mark: Some(mark.clone()),
        });
        if hits.len() >= needed {
            break;
        }
    }
    Ok(())
}

fn read(scope: &Scope, args: Read) -> std::result::Result<(Value, Vec<Mark>), Failure> {
    guard_resolved(&args.path)?;
    let meta = fs::metadata(&args.path).map_err(io_failure)?;
    if !meta.is_file() {
        return Err(bad("read path is not a file", None));
    }
    if meta.len() > MAX_BYTES {
        return Err(Failure {
            reason: Reason::Limit,
            message: "file exceeds the text read limit".into(),
            total_lines: None,
            matches: None,
        });
    }
    let bytes = read_file(&args.path)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| bad("file is not UTF-8 text", None))?;
    let lines = text.lines().collect::<Vec<_>>();
    let total = lines.len();
    if args.range.is_none() && total > LONG_FILE_LINES {
        return Err(bad("long file requires a line range", Some(total)));
    }
    let range = args.range.unwrap_or(Range {
        start: usize::from(total > 0),
        end: total,
    });
    if total == 0 && args.range.is_none() {
        return Ok((
            Value::Read {
                path: display(scope, &args.path),
                start: 0,
                end: 0,
                total_lines: 0,
                lines: Vec::new(),
            },
            vec![Mark {
                path: fs::canonicalize(&args.path).map_err(io_failure)?,
                sig: write::sig(&bytes),
            }],
        ));
    }
    if range.start == 0 || range.end < range.start || range.end > total {
        return Err(bad("line range is outside the file", Some(total)));
    }
    let selected = if total == 0 {
        Vec::new()
    } else {
        lines[range.start - 1..range.end]
            .iter()
            .enumerate()
            .map(|(index, text)| Line {
                line: range.start + index,
                text: (*text).to_owned(),
                matched: false,
            })
            .collect()
    };
    Ok((
        Value::Read {
            path: display(scope, &args.path),
            start: range.start,
            end: range.end,
            total_lines: total,
            lines: selected,
        },
        vec![Mark {
            path: fs::canonicalize(&args.path).map_err(io_failure)?,
            sig: write::sig(&bytes),
        }],
    ))
}

fn matched(args: &Search, value: &str) -> bool {
    args.regex.as_ref().map_or_else(
        || value.contains(&args.query),
        |regex| regex.is_match(value),
    )
}

pub(crate) fn display(scope: &Scope, path: &Path) -> String {
    path.strip_prefix(scope.root())
        .ok()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(path)
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

pub(crate) fn guard(path: &Path) -> std::result::Result<(), Failure> {
    if denied(path) {
        Err(deny(path))
    } else {
        Ok(())
    }
}

fn guard_resolved(path: &Path) -> std::result::Result<(), Failure> {
    guard(path)?;
    match fs::canonicalize(path) {
        Ok(path) => guard(&path),
        Err(error) => Err(io_failure(error)),
    }
}

fn denied(path: &Path) -> bool {
    path.components().any(|part| {
        let name = part.as_os_str().to_string_lossy().to_ascii_lowercase();
        name == ".ssh"
            || name.starts_with(".env")
            || matches!(
                name.as_str(),
                "id_dsa" | "id_ecdsa" | "id_ed25519" | "id_rsa" | "keystore"
            )
            || [".key", ".pem", ".p12", ".pfx"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
    })
}

fn ignored_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| IGNORED_DIRS.contains(&name))
}

fn read_file(path: &Path) -> std::result::Result<Vec<u8>, Failure> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) =>
        {
            fs::read(path).map_err(io_failure)
        }
        Err(error) => Err(io_failure(error)),
    }
}

fn line_count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\n').count()
        + usize::from(!bytes.is_empty() && !bytes.ends_with(b"\n"))
}

fn count_lines(path: &Path) -> std::result::Result<usize, Failure> {
    let file = fs::File::open(path).map_err(io_failure)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut count = 0;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).map_err(io_failure)? {
            0 => return Ok(count),
            _ => count += 1,
        }
    }
}

pub(crate) fn io_failure(error: io::Error) -> Failure {
    let reason = match error.kind() {
        io::ErrorKind::NotFound => Reason::NotFound,
        io::ErrorKind::PermissionDenied => Reason::Denied,
        _ => Reason::Busy,
    };
    Failure {
        reason,
        message: error.to_string(),
        total_lines: None,
        matches: None,
    }
}

fn bad(message: &str, total_lines: Option<usize>) -> Failure {
    Failure {
        reason: Reason::BadArgs,
        message: message.into(),
        total_lines,
        matches: None,
    }
}

fn deny(path: &Path) -> Failure {
    Failure {
        reason: Reason::Denied,
        message: format!("read denied for {}", path.display()),
        total_lines: None,
        matches: None,
    }
}

#[cfg(test)]
#[path = "find_test.rs"]
mod tests;

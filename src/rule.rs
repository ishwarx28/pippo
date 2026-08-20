// Owns rulebook matching and the in-memory approval allowlist.

use anyhow::{Context, Result};
use globset::Glob;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const DEFAULTS: &str = r#"# Patterns use { kind: literal|regex|glob, value: ... } and {project} {home} {cache}.
rules:
- { id: deny-sudo, tool: shell, command: { kind: regex, value: '(^|[[:space:]])sudo([[:space:]]|$)' }, action: deny, reason: Privilege escalation is never run }
- { id: deny-network-shell, tool: shell, command: { kind: regex, value: '(curl|wget)[^|]*[|][[:space:]]*(sh|bash)([[:space:]]|$)' }, action: deny, reason: Downloaded code cannot be piped into a shell }
- { id: ask-package-install, tool: shell, command: { kind: regex, value: '^(npm|pnpm|yarn|bun)[[:space:]]+(install|add|ci)|^cargo[[:space:]]+(add|install)|^go[[:space:]]+(get|install)|^(pip|pip3|uv)[[:space:]]+(install|add|sync)|^python(3)?[[:space:]]+-m[[:space:]]+pip[[:space:]]+install|^(brew|gem)[[:space:]]+install|^composer[[:space:]]+require' }, action: ask, reason: Package installation changes the machine or dependency tree }
- { id: ask-git-push, tool: shell, command: { kind: regex, value: '^git[[:space:]]+push([[:space:]]|$)' }, action: ask, reason: Pushing changes a remote repository }
- { id: ask-hard-reset, tool: shell, command: { kind: regex, value: '^git[[:space:]]+reset[[:space:]]+--hard([[:space:]]|$)' }, action: ask, reason: A hard reset can discard work }
- { id: ask-recursive-delete, tool: shell, command: { kind: regex, value: '^rm[[:space:]]+(-[^[:space:]]*r[^[:space:]]*f|-[^[:space:]]*f[^[:space:]]*r)([[:space:]]|$)' }, action: ask, reason: Recursive force deletion is destructive }
- { id: allow-inspection, tool: shell, command: { kind: regex, value: '^(pwd|ls|rg|grep|head|tail|cat|wc|stat|file)([[:space:]][^><;$`]*|)$|^git[[:space:]]+(status|diff|log|show)([[:space:]][^><;$`]*|)$|^git[[:space:]]+branch$' }, path: { kind: glob, value: '{project}/**' }, action: allow, reason: Project inspection is read-only }
- { id: allow-build-test, tool: shell, command: { kind: regex, value: '^(cargo[[:space:]]+(build|check|test|clippy|fmt)|go[[:space:]]+(build|test|vet)|npm[[:space:]]+(test|run[[:space:]]+(build|test|lint))|pnpm[[:space:]]+(build|test|lint)|yarn[[:space:]]+(build|test|lint))([[:space:]][^><;$`]*|)$' }, path: { kind: glob, value: '{project}/**' }, action: allow, reason: Project build and test commands are expected verification }
- { id: allow-project-write, tool: write, path: { kind: glob, value: '{project}/**' }, action: allow, reason: Writes inside the active project are expected }
- { id: allow-project-edit, tool: edit, path: { kind: glob, value: '{project}/**' }, action: allow, reason: Edits inside the active project are expected }
- { id: allow-global-skills-write, tool: write, path: { kind: glob, value: '{home}/.pippo/skills/**' }, action: allow, reason: Global skill files are agent procedures }
- { id: allow-global-skills-edit, tool: edit, path: { kind: glob, value: '{home}/.pippo/skills/**' }, action: allow, reason: Global skill files are agent procedures }
- { id: allow-project-skills-write, tool: write, path: { kind: glob, value: '{home}/.pippo/projects/*/skills/**' }, action: allow, reason: Project skill files are agent procedures }
- { id: allow-project-skills-edit, tool: edit, path: { kind: glob, value: '{home}/.pippo/projects/*/skills/**' }, action: allow, reason: Project skill files are agent procedures }
"#;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tool {
    Shell,
    Write,
    Edit,
}

impl Tool {
    fn name(self) -> &'static str {
        match self {
            Self::Shell => "shell",
            Self::Write => "write",
            Self::Edit => "edit",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Action {
    Allow,
    Ask,
    Deny,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pattern {
    kind: Kind,
    value: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Literal,
    Regex,
    Glob,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    id: String,
    tool: Tool,
    command: Option<Pattern>,
    path: Option<Pattern>,
    action: Action,
    reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rules {
    rules: Vec<Rule>,
}

pub struct Book {
    rules: Vec<Rule>,
    home: PathBuf,
    cache: PathBuf,
    allowed: Mutex<HashSet<String>>,
}

pub struct Request<'a> {
    pub tool: Tool,
    pub role: Option<&'a str>,
    pub command: Option<&'a str>,
    pub path: &'a Path,
    pub project: &'a Path,
    pub detail: &'a str,
}

#[derive(Clone, Debug)]
pub struct Ask {
    pub rule_id: String,
    pub tool: Tool,
    pub subject: String,
    pub path: String,
    pub reason: String,
    key: String,
}

#[derive(Clone, Debug)]
pub enum Decision {
    Allow,
    Ask(Ask),
    Deny(String),
}

#[derive(Clone, Debug, Serialize)]
pub struct Prompt {
    pub id: String,
    pub rule_id: String,
    pub tool: Tool,
    pub subject: String,
    pub path: String,
    pub reason: String,
}

impl Ask {
    pub fn prompt(&self, id: String) -> Prompt {
        Prompt {
            id,
            rule_id: self.rule_id.clone(),
            tool: self.tool,
            subject: self.subject.clone(),
            path: self.path.clone(),
            reason: self.reason.clone(),
        }
    }
}

impl Book {
    pub fn open(root: &Path, home: &Path) -> Result<Self> {
        let path = root.join("cfg/rules.yaml");
        let source = fs::read_to_string(&path)
            .with_context(|| format!("read rulebook {}", path.display()))?;
        Self::parse(&source, home.to_path_buf(), root.join("cache"))
            .with_context(|| format!("parse rulebook {}", path.display()))
    }

    fn parse(source: &str, home: PathBuf, cache: PathBuf) -> Result<Self> {
        let rules: Rules = serde_yaml::from_str(source)?;
        let home = fs::canonicalize(&home).unwrap_or_else(|_| clean(&home));
        let cache = fs::canonicalize(&cache).unwrap_or_else(|_| clean(&cache));
        let mut ids = HashSet::new();
        for rule in &rules.rules {
            if rule.id.trim().is_empty()
                || rule.reason.trim().is_empty()
                || !ids.insert(rule.id.as_str())
                || rule.command.is_none() && rule.path.is_none()
                || rule.tool != Tool::Shell && rule.command.is_some()
            {
                anyhow::bail!("invalid or duplicate rule {}", rule.id);
            }
            let env = Env {
                home: &home,
                cache: &cache,
                project: &home,
            };
            for pattern in [&rule.command, &rule.path].into_iter().flatten() {
                pattern.validate(&env)?;
            }
        }
        Ok(Self {
            rules: rules.rules,
            home,
            cache,
            allowed: Mutex::new(HashSet::new()),
        })
    }

    pub fn decide(&self, request: Request<'_>) -> Decision {
        let path = clean(request.path);
        if !within(&path, &self.home) {
            return Decision::Deny("Paths outside the home directory are refused".into());
        }
        let env = Env {
            home: &self.home,
            cache: &self.cache,
            project: request.project,
        };
        let matched = self
            .rules
            .iter()
            .filter(|rule| rule.matches(&request, &path, &env))
            .collect::<Vec<_>>();
        if let Some(rule) = matched.iter().find(|rule| rule.action == Action::Deny) {
            return Decision::Deny(rule.reason.clone());
        }
        let fingerprint = fingerprint(&request, &path);
        let ask = matched.iter().find(|rule| rule.action == Action::Ask);
        let key = allow_key(ask.map_or("unmatched", |rule| &rule.id), &fingerprint);
        match self.allowed.lock() {
            Ok(allowed) if allowed.contains(&key) => return Decision::Allow,
            Err(_) => return Decision::Deny("Approval state is unavailable".into()),
            _ => {}
        }
        if matched.iter().any(|rule| rule.action == Action::Allow) {
            return Decision::Allow;
        }
        let (id, reason) = ask.map_or_else(
            || ("unmatched".into(), "No rule allows this action".into()),
            |rule| (rule.id.clone(), rule.reason.clone()),
        );
        Decision::Ask(Ask {
            rule_id: id,
            tool: request.tool,
            subject: request
                .command
                .unwrap_or_else(|| path.to_str().unwrap_or("<invalid path>"))
                .into(),
            path: path.to_string_lossy().into_owned(),
            reason,
            key,
        })
    }

    pub fn allow_session(&self, ask: &Ask) -> Result<()> {
        self.allowed
            .lock()
            .map_err(|_| anyhow::anyhow!("approval state lock poisoned"))?
            .insert(ask.key.clone());
        Ok(())
    }
}

impl Rule {
    fn matches(&self, request: &Request<'_>, path: &Path, env: &Env<'_>) -> bool {
        if self.tool != request.tool
            || self
                .path
                .as_ref()
                .is_some_and(|value| !value.matches(&path.to_string_lossy(), env))
        {
            return false;
        }
        let Some(pattern) = &self.command else {
            return true;
        };
        let Some(command) = request.command else {
            return false;
        };
        let parts = segments(command);
        if self.action == Action::Allow {
            !parts.is_empty() && parts.iter().all(|part| pattern.matches(part, env))
        } else {
            pattern.matches(command, env) || parts.iter().any(|part| pattern.matches(part, env))
        }
    }
}

struct Env<'a> {
    home: &'a Path,
    cache: &'a Path,
    project: &'a Path,
}

impl Pattern {
    fn value(&self, env: &Env<'_>) -> String {
        self.value
            .replace("{project}", &slash(env.project))
            .replace("{home}", &slash(env.home))
            .replace("{cache}", &slash(env.cache))
    }

    fn validate(&self, env: &Env<'_>) -> Result<()> {
        let value = self.value(env);
        match self.kind {
            Kind::Literal => Ok(()),
            Kind::Regex => Regex::new(&value).map(|_| ()).map_err(Into::into),
            Kind::Glob => Glob::new(&value).map(|_| ()).map_err(Into::into),
        }
    }

    fn matches(&self, input: &str, env: &Env<'_>) -> bool {
        let value = self.value(env);
        match self.kind {
            Kind::Literal => input == value,
            Kind::Regex => Regex::new(&value).is_ok_and(|regex| regex.is_match(input)),
            Kind::Glob => {
                value.strip_suffix("/**") == Some(input)
                    || Glob::new(&value).is_ok_and(|glob| glob.compile_matcher().is_match(input))
            }
        }
    }
}

fn segments(command: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let bytes = command.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'\\' if quote != Some(b'\'') => at += 1,
            value @ (b'\'' | b'"') if quote == Some(value) => quote = None,
            value @ (b'\'' | b'"') if quote.is_none() => quote = Some(value),
            b'|' | b';' | b'&' if quote.is_none() => {
                let part = command[start..at].trim();
                if !part.is_empty() {
                    values.push(part.to_owned());
                }
                at += usize::from(at + 1 < bytes.len() && bytes[at + 1] == bytes[at]);
                start = at + 1;
            }
            _ => {}
        }
        at += 1;
    }
    let part = command[start..].trim();
    if !part.is_empty() {
        values.push(part.to_owned());
    }
    values
}

fn clean(path: &Path) -> PathBuf {
    path.components().collect()
}

fn within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn slash(path: &Path) -> String {
    path.to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

fn fingerprint(request: &Request<'_>, path: &Path) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}",
        request.tool.name(),
        request.role.unwrap_or(""),
        request.command.unwrap_or(""),
        slash(path),
        request.detail
    )
}

fn allow_key(id: &str, fingerprint: &str) -> String {
    format!("{id}\0{fingerprint}")
}

#[cfg(test)]
#[path = "rule_test.rs"]
mod tests;

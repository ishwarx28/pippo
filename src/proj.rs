// Owns project registration and durable task state.

use crate::store::atomic;
use anyhow::{Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{Mutex, MutexGuard},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Running,
    Done,
    Failed,
    Abandoned,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub project_id: String,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TaskReply {
    pub task_id: String,
    pub project_id: String,
    pub project_registered: bool,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Scope {
    root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LiveTask {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Live {
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<LiveTask>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<Project>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<Vec<String>>,
    pub agents: Vec<PathBuf>,
    pub projects: Vec<Project>,
}

impl Scope {
    fn new(root: PathBuf) -> Result<Self> {
        if !root.is_absolute() {
            anyhow::bail!("project path must be absolute");
        }
        let root = if root.exists() {
            fs::canonicalize(&root)
                .with_context(|| format!("resolve project path {}", root.display()))?
        } else {
            lexical(&root)
        };
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn resolve(&self, path: Option<&Path>) -> PathBuf {
        match path {
            None => self.root().to_path_buf(),
            Some(path) if path.is_absolute() => lexical(path),
            Some(path) => lexical(&self.root.join(path)),
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct SessionMeta {
    active_task: Option<String>,
    tasks: BTreeMap<String, Task>,
}

struct State {
    meta: SessionMeta,
    projects: BTreeMap<String, Project>,
}

pub struct Proj {
    root: PathBuf,
    state: Mutex<State>,
}

impl Proj {
    pub fn open(root: PathBuf) -> Result<Self> {
        let projects = load_projects(&root)?;
        let path = root.join("session/meta.json");
        let meta = if path.exists() {
            serde_json::from_slice(
                &fs::read(&path).with_context(|| format!("read task state {}", path.display()))?,
            )
            .with_context(|| format!("parse task state {}", path.display()))?
        } else {
            SessionMeta::default()
        };
        validate(&meta, &projects)?;
        let this = Self {
            root,
            state: Mutex::new(State { meta, projects }),
        };
        if !path.exists() {
            this.save(&this.lock()?.meta)?;
        }
        Ok(this)
    }

    pub fn create(&self, title: String, path: PathBuf) -> Result<TaskReply> {
        let title = title.trim();
        if !(2..=8).contains(&title.split_whitespace().count()) {
            anyhow::bail!("task title must contain 2 to 8 words");
        }
        let path = Scope::new(path)?.resolve(None);
        let mut state = self.lock()?;
        if let Some(active) = state.meta.active_task.as_deref() {
            anyhow::bail!("task {active} is still active");
        }
        let project = project(&path)?;
        let registered = match state.projects.get(&project.id) {
            Some(saved) if saved.path == project.path => false,
            Some(_) => anyhow::bail!("project id collision for {}", project.id),
            None => {
                create_project(&self.root, &project)?;
                state.projects.insert(project.id.clone(), project.clone());
                true
            }
        };
        let id = loop {
            let id = task_id()?;
            if !state.meta.tasks.contains_key(&id) {
                break id;
            }
        };
        let task = Task {
            id: id.clone(),
            title: title.into(),
            project_id: project.id.clone(),
            status: TaskStatus::Running,
            note: None,
        };
        let mut next = SessionMeta {
            active_task: Some(id.clone()),
            tasks: state.meta.tasks.clone(),
        };
        next.tasks.insert(id.clone(), task);
        fs::create_dir_all(
            self.root
                .join("projects")
                .join(&project.id)
                .join("reports")
                .join(&id),
        )
        .with_context(|| format!("create report directory for task {id}"))?;
        self.save(&next)?;
        state.meta = next;
        drop(state);
        if self.scope(Some(&id))?.resolve(None) != path {
            anyhow::bail!("task {id} has an inconsistent project scope");
        }
        Ok(TaskReply {
            task_id: id,
            project_id: project.id,
            project_registered: registered,
            status: TaskStatus::Running,
        })
    }

    pub fn update(&self, id: String, status: TaskStatus, note: String) -> Result<TaskReply> {
        if status == TaskStatus::Running {
            anyhow::bail!("task update requires a terminal status");
        }
        let note = note.trim();
        if note.is_empty() {
            anyhow::bail!("task update note is required");
        }
        let mut state = self.lock()?;
        if state.meta.active_task.as_deref() != Some(id.as_str()) {
            anyhow::bail!("task {id} is not active");
        }
        let mut next = SessionMeta {
            active_task: None,
            tasks: state.meta.tasks.clone(),
        };
        let task = next
            .tasks
            .get_mut(&id)
            .with_context(|| format!("task {id} is not registered"))?;
        if task.status != TaskStatus::Running {
            anyhow::bail!("task {id} is already closed");
        }
        task.status = status;
        task.note = Some(note.into());
        let project_id = task.project_id.clone();
        self.save(&next)?;
        state.meta = next;
        Ok(TaskReply {
            task_id: id,
            project_id,
            project_registered: false,
            status,
        })
    }

    pub fn scope(&self, task_id: Option<&str>) -> Result<Scope> {
        let state = self.lock()?;
        let id = match task_id {
            Some(id) => id,
            None => state
                .meta
                .active_task
                .as_deref()
                .context("no active task")?,
        };
        let task = state
            .meta
            .tasks
            .get(id)
            .with_context(|| format!("task {id} is not registered"))?;
        let project = state
            .projects
            .get(&task.project_id)
            .with_context(|| format!("project {} is not registered", task.project_id))?;
        Ok(Scope {
            root: project.path.clone(),
        })
    }

    pub fn task(&self, id: &str) -> Result<Task> {
        let state = self.lock()?;
        if state.meta.active_task.as_deref() != Some(id) {
            anyhow::bail!("task {id} is not active");
        }
        state
            .meta
            .tasks
            .get(id)
            .cloned()
            .with_context(|| format!("task {id} is not registered"))
    }

    pub fn related(&self, project: &str, ids: &[String]) -> Result<()> {
        let state = self.lock()?;
        for id in ids {
            let task = state
                .meta
                .tasks
                .get(id)
                .with_context(|| format!("related task {id} is not registered"))?;
            if task.project_id != project {
                anyhow::bail!("related task {id} belongs to another project");
            }
        }
        Ok(())
    }

    pub fn live(&self, task_id: Option<&str>) -> Result<Live> {
        let (task, project, projects) = {
            let state = self.lock()?;
            let selected = match task_id {
                Some(id) => Some(
                    state
                        .meta
                        .tasks
                        .get(id)
                        .with_context(|| format!("task {id} is not registered"))?,
                ),
                None => state
                    .meta
                    .active_task
                    .as_ref()
                    .and_then(|id| state.meta.tasks.get(id)),
            };
            let task = selected.map(|task| LiveTask {
                id: task.id.clone(),
                title: task.title.clone(),
                status: task.status,
                active: state.meta.active_task.as_deref() == Some(task.id.as_str()),
            });
            let project = selected
                .map(|task| {
                    state
                        .projects
                        .get(&task.project_id)
                        .cloned()
                        .with_context(|| format!("project {} is not registered", task.project_id))
                })
                .transpose()?;
            let projects = state.projects.values().cloned().collect();
            (task, project, projects)
        };
        let (git, agents) = match project.as_ref() {
            Some(project) => (git_status(&project.path)?, agents(&project.path)?),
            None => (None, Vec::new()),
        };
        Ok(Live {
            date: Local::now().format("%Y-%m-%d").to_string(),
            task,
            project,
            git,
            agents,
            projects,
        })
    }

    fn save(&self, meta: &SessionMeta) -> Result<()> {
        atomic(&self.root.join("session/meta.json"), meta)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("project state lock poisoned"))
    }
}

fn load_projects(root: &Path) -> Result<BTreeMap<String, Project>> {
    let dir = root.join("projects");
    fs::create_dir_all(&dir)
        .with_context(|| format!("create projects directory {}", dir.display()))?;
    let mut paths = fs::read_dir(&dir)
        .with_context(|| format!("read projects directory {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort_by_key(|entry| entry.file_name());
    let mut projects = BTreeMap::new();
    for entry in paths {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let meta = entry.path().join("meta.json");
        if !meta.exists() {
            continue;
        }
        let saved: Project = serde_json::from_slice(
            &fs::read(&meta).with_context(|| format!("read project {}", meta.display()))?,
        )
        .with_context(|| format!("parse project {}", meta.display()))?;
        let expected = project(&saved.path)?;
        if saved.id != entry.file_name().to_string_lossy() || saved != expected {
            anyhow::bail!("invalid project metadata {}", meta.display());
        }
        projects.insert(saved.id.clone(), saved);
    }
    Ok(projects)
}

fn validate(meta: &SessionMeta, projects: &BTreeMap<String, Project>) -> Result<()> {
    let running: Vec<_> = meta
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Running)
        .collect();
    if running.len() > 1
        || running.first().map(|task| task.id.as_str()) != meta.active_task.as_deref()
    {
        anyhow::bail!("task state has an inconsistent active task");
    }
    for (id, task) in &meta.tasks {
        if id != &task.id || !valid_task_id(id) || !projects.contains_key(&task.project_id) {
            anyhow::bail!("task state contains invalid task {id}");
        }
    }
    Ok(())
}

fn create_project(root: &Path, project: &Project) -> Result<()> {
    let dir = root.join("projects").join(&project.id);
    fs::create_dir_all(dir.join("skills"))
        .with_context(|| format!("create project skills directory {}", dir.display()))?;
    fs::create_dir_all(dir.join("reports"))
        .with_context(|| format!("create project reports directory {}", dir.display()))?;
    fs::create_dir_all(dir.join("plans"))
        .with_context(|| format!("create project plans directory {}", dir.display()))?;
    let prefs = dir.join("prefs.md");
    if !prefs.exists() {
        fs::write(&prefs, b"").with_context(|| format!("create {}", prefs.display()))?;
    }
    atomic(&dir.join("meta.json"), project)
}

fn project(path: &Path) -> Result<Project> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .context("project path has no valid directory name")?;
    let mut slug = String::new();
    let mut separator = false;
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() {
            if separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.push((byte as char).to_ascii_lowercase());
            separator = false;
        } else {
            separator = true;
        }
    }
    if slug.is_empty() {
        slug.push_str("project");
    }
    let hash = path
        .to_str()
        .context("project path must be valid Unicode")?
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    Ok(Project {
        id: format!("{slug}_{:06x}", hash & 0xff_ffff),
        name: name.into(),
        path: path.to_path_buf(),
    })
}

fn task_id() -> Result<String> {
    let mut bytes = [0_u8; 4];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("generate task id: {error}"))?;
    let mut id = String::from("t_");
    for byte in bytes {
        write!(id, "{byte:02x}").context("encode task id")?;
    }
    Ok(id)
}

fn valid_task_id(id: &str) -> bool {
    id.len() == 10 && id.starts_with("t_") && id[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn lexical(path: &Path) -> PathBuf {
    let mut clean = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                clean.push(part.as_os_str());
            }
        }
    }
    clean
}

fn git_status(path: &Path) -> Result<Option<Vec<String>>> {
    let meta = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    if !meta.is_dir() {
        anyhow::bail!("project path is not a directory: {}", path.display());
    }
    let probe = Command::new("git")
        .args([
            "-C",
            path.to_str().context("project path is not valid Unicode")?,
        ])
        .args(["rev-parse", "--show-toplevel"])
        .env("LC_ALL", "C")
        .output()
        .with_context(|| format!("inspect git repository at {}", path.display()))?;
    if !probe.status.success() {
        let error = String::from_utf8(probe.stderr).context("decode git repository error")?;
        if error.contains("not a git repository") {
            return Ok(None);
        }
        anyhow::bail!(
            "inspect git repository at {}: {}",
            path.display(),
            error.trim()
        );
    }
    let output = Command::new("git")
        .args([
            "-C",
            path.to_str().context("project path is not valid Unicode")?,
        ])
        .args([
            "status",
            "--porcelain=v1",
            "--branch",
            "--untracked-files=normal",
        ])
        .env("LC_ALL", "C")
        .output()
        .with_context(|| format!("read git status at {}", path.display()))?;
    if !output.status.success() {
        let error = String::from_utf8(output.stderr).context("decode git status error")?;
        anyhow::bail!("read git status at {}: {}", path.display(), error.trim());
    }
    let status = String::from_utf8(output.stdout).context("decode git status")?;
    Ok(Some(
        status
            .replace("\r\n", "\n")
            .lines()
            .map(str::to_owned)
            .collect(),
    ))
}

fn agents(path: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs: Vec<_> = path.ancestors().collect();
    dirs.reverse();
    let mut found = Vec::new();
    for dir in dirs {
        for name in ["AGENTS.md", "CLAUDE.md"] {
            let candidate = dir.join(name);
            match fs::metadata(&candidate) {
                Ok(meta) if meta.is_file() => found.push(candidate),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspect agent file {}", candidate.display()));
                }
            }
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn root() -> PathBuf {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pippo-proj-{}-{nonce}", std::process::id()))
    }

    fn git(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(path)
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn creates_registers_closes_and_reloads_tasks() {
        let root = root();
        let work = root.join("work/My Project");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.clone()).unwrap();
        let created = proj
            .create("add upload retry".into(), work.clone())
            .unwrap();
        assert!(valid_task_id(&created.task_id));
        assert!(created.project_registered);
        assert!(created.project_id.starts_with("my-project_"));
        assert_eq!(created.project_id.len(), "my-project_".len() + 6);
        let dir = root.join("projects").join(&created.project_id);
        for path in [
            dir.join("meta.json"),
            dir.join("prefs.md"),
            dir.join("skills"),
            dir.join("reports").join(&created.task_id),
            dir.join("plans"),
        ] {
            assert!(path.exists(), "{} was not created", path.display());
        }
        let closed = proj
            .update(created.task_id.clone(), TaskStatus::Done, "verified".into())
            .unwrap();
        assert_eq!(closed.status, TaskStatus::Done);
        drop(proj);

        let reopened = Proj::open(root.clone()).unwrap();
        {
            let state = reopened.lock().unwrap();
            let saved = &state.meta.tasks[&created.task_id];
            assert_eq!(saved.status, TaskStatus::Done);
            assert_eq!(saved.note.as_deref(), Some("verified"));
            assert_eq!(state.meta.active_task, None);
        }
        let next = reopened.create("fix another issue".into(), work).unwrap();
        assert!(!next.project_registered);
        assert_ne!(next.task_id, created.task_id);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn enforces_arguments_and_one_active_task() {
        let root = root();
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.clone()).unwrap();
        assert!(proj.create("one".into(), work.clone()).is_err());
        assert!(proj
            .create("valid title".into(), PathBuf::from("relative"))
            .is_err());
        let created = proj
            .create("valid task title".into(), work.clone())
            .unwrap();
        assert!(proj.create("second task title".into(), work).is_err());
        assert!(proj
            .update(created.task_id.clone(), TaskStatus::Running, "note".into())
            .is_err());
        assert!(proj
            .update(created.task_id.clone(), TaskStatus::Done, " ".into())
            .is_err());
        assert!(proj
            .update("t_00000000".into(), TaskStatus::Failed, "missing".into())
            .is_err());
        proj.update(
            created.task_id.clone(),
            TaskStatus::Abandoned,
            "user changed direction".into(),
        )
        .unwrap();
        assert!(proj
            .update(created.task_id, TaskStatus::Done, "again".into())
            .is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_inconsistent_durable_state() {
        let root = root();
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.clone()).unwrap();
        let created = proj.create("valid task title".into(), work).unwrap();
        drop(proj);
        let meta = root.join("session/meta.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&meta).unwrap()).unwrap();
        value["active_task"] = serde_json::Value::Null;
        fs::write(&meta, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(Proj::open(root.clone()).is_err());
        assert!(created.task_id.starts_with("t_"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scope_resolves_every_tool_path_from_one_base() {
        let root = root();
        let real = root.join("work/real");
        let link = root.join("work/link");
        fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let proj = Proj::open(root.clone()).unwrap();
        proj.create("scope path behavior".into(), link).unwrap();

        let scope = proj.scope(None).unwrap();
        let canonical = fs::canonicalize(&real).unwrap();
        assert_eq!(scope.root(), canonical);
        assert_eq!(scope.resolve(None), canonical);
        assert_eq!(
            scope.resolve(Some(Path::new("src/./main.rs"))),
            canonical.join("src/main.rs")
        );
        assert_eq!(
            scope.resolve(Some(Path::new("../shared/file.txt"))),
            canonical.parent().unwrap().join("shared/file.txt")
        );
        let outside = root.join("outside/../other/file.txt");
        assert_eq!(scope.resolve(Some(&outside)), root.join("other/file.txt"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scope_tracks_sequential_projects_without_losing_closed_tasks() {
        let root = root();
        let first_path = root.join("first");
        let second_path = root.join("second");
        fs::create_dir_all(&first_path).unwrap();
        fs::create_dir_all(&second_path).unwrap();
        let proj = Proj::open(root.clone()).unwrap();
        assert!(proj
            .scope(None)
            .unwrap_err()
            .to_string()
            .contains("no active task"));
        assert!(proj
            .scope(Some("t_00000000"))
            .unwrap_err()
            .to_string()
            .contains("not registered"));

        let first = proj
            .create("work in first".into(), first_path.clone())
            .unwrap();
        let first_scope = proj.scope(None).unwrap();
        assert_eq!(first_scope.root(), fs::canonicalize(&first_path).unwrap());
        proj.update(first.task_id.clone(), TaskStatus::Done, "verified".into())
            .unwrap();
        assert!(proj.scope(None).is_err());
        assert_eq!(proj.scope(Some(&first.task_id)).unwrap(), first_scope);

        let second = proj
            .create("work in second".into(), second_path.clone())
            .unwrap();
        assert_eq!(
            proj.scope(None).unwrap().root(),
            fs::canonicalize(&second_path).unwrap()
        );
        assert_eq!(proj.scope(Some(&first.task_id)).unwrap(), first_scope);
        assert_ne!(first.project_id, second.project_id);
        drop(proj);

        let reopened = Proj::open(root.clone()).unwrap();
        assert_eq!(
            reopened.scope(None).unwrap().root(),
            fs::canonicalize(&second_path).unwrap()
        );
        assert_eq!(reopened.scope(Some(&first.task_id)).unwrap(), first_scope);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_environment_refreshes_project_git_and_agent_paths() {
        let root = root();
        let zeta = root.join("work/zeta");
        let repo = root.join("work/alpha");
        fs::create_dir_all(&zeta).unwrap();
        fs::create_dir_all(repo.join("nested")).unwrap();
        fs::write(root.join("AGENTS.md"), "private instruction contents").unwrap();
        git(&repo, &["init", "-q"]);
        git(&repo, &["config", "user.name", "Test"]);
        git(&repo, &["config", "user.email", "test@example.com"]);
        git(&repo, &["config", "commit.gpgsign", "false"]);
        fs::write(repo.join("tracked.txt"), "first\n").unwrap();
        git(&repo, &["add", "tracked.txt"]);
        git(&repo, &["commit", "-qm", "initial"]);

        let proj = Proj::open(root.clone()).unwrap();
        let old = proj.create("work in zeta".into(), zeta).unwrap();
        proj.update(old.task_id, TaskStatus::Done, "verified".into())
            .unwrap();
        let current = proj.create("work in alpha".into(), repo.clone()).unwrap();
        let clean = proj.live(None).unwrap();
        assert_eq!(clean.task.as_ref().unwrap().id, current.task_id);
        assert!(clean.task.as_ref().unwrap().active);
        assert_eq!(
            clean.project.as_ref().unwrap().path,
            fs::canonicalize(&repo).unwrap()
        );
        assert_eq!(clean.git.as_ref().unwrap().len(), 1);
        assert!(clean.git.as_ref().unwrap()[0].starts_with("## "));
        assert!(
            clean.date.len() == 10
                && clean.date.as_bytes()[4] == b'-'
                && clean.date.as_bytes()[7] == b'-'
        );

        fs::write(repo.join("tracked.txt"), "changed\n").unwrap();
        fs::write(repo.join("new.txt"), "new\n").unwrap();
        fs::write(repo.join("CLAUDE.md"), "more private contents").unwrap();
        fs::write(repo.join("nested/AGENTS.md"), "not project-wide").unwrap();
        let dirty = proj.live(None).unwrap();
        let status = dirty.git.as_ref().unwrap();
        assert!(status.iter().any(|line| line == " M tracked.txt"));
        assert!(status.iter().any(|line| line == "?? new.txt"));
        let canonical_root = fs::canonicalize(&root).unwrap();
        let local_agents: Vec<_> = dirty
            .agents
            .iter()
            .filter(|path| path.starts_with(&canonical_root))
            .cloned()
            .collect();
        assert_eq!(
            local_agents,
            vec![
                canonical_root.join("AGENTS.md"),
                fs::canonicalize(&repo).unwrap().join("CLAUDE.md")
            ]
        );
        assert!(!dirty
            .agents
            .iter()
            .any(|path| path.ends_with("nested/AGENTS.md")));
        let encoded = serde_json::to_string(&dirty).unwrap();
        assert!(!encoded.contains("private instruction contents"));
        assert!(dirty
            .projects
            .windows(2)
            .all(|pair| pair[0].id < pair[1].id));

        proj.update(current.task_id.clone(), TaskStatus::Done, "verified".into())
            .unwrap();
        let empty = proj.live(None).unwrap();
        assert!(empty.task.is_none() && empty.project.is_none() && empty.git.is_none());
        assert!(empty.agents.is_empty());
        let closed = proj.live(Some(&current.task_id)).unwrap();
        assert_eq!(closed.task.as_ref().unwrap().status, TaskStatus::Done);
        assert!(!closed.task.as_ref().unwrap().active);
        assert!(closed
            .git
            .as_ref()
            .unwrap()
            .iter()
            .any(|line| line == "?? new.txt"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_environment_handles_missing_projects_and_unknown_tasks() {
        let root = root();
        let proj = Proj::open(root.clone()).unwrap();
        let missing = root.join("work/missing/../gone");
        let task = proj
            .create("inspect missing project".into(), missing)
            .unwrap();
        let first = proj.live(None).unwrap();
        assert!(first.git.is_none());
        assert_eq!(first.project.as_ref().unwrap().path, root.join("work/gone"));
        assert!(proj.live(Some("t_00000000")).is_err());
        proj.update(
            task.task_id,
            TaskStatus::Abandoned,
            "path disappeared".into(),
        )
        .unwrap();
        let second = proj.live(None).unwrap();
        assert!(second.task.is_none());
        assert_eq!(first.projects, second.projects);
        fs::remove_dir_all(root).unwrap();
    }
}

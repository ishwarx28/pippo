// Owns project registration and durable task state.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, File},
    path::{Component, Path, PathBuf},
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

    fn save(&self, meta: &SessionMeta) -> Result<()> {
        atomic_json(&self.root.join("session/meta.json"), meta)
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
    atomic_json(&dir.join("meta.json"), project)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state file has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create state directory {}", parent.display()))?;
    let tmp = path.with_extension("json.tmp");
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize state")?;
    bytes.push(b'\n');
    fs::write(&tmp, bytes).with_context(|| format!("write state temp {}", tmp.display()))?;
    File::open(&tmp)
        .with_context(|| format!("open state temp {}", tmp.display()))?
        .sync_all()
        .with_context(|| format!("flush state temp {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("replace state {}", path.display()))?;
    File::open(parent)
        .with_context(|| format!("open state directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("flush state directory {}", parent.display()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn root() -> PathBuf {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pippo-proj-{}-{nonce}", std::process::id()))
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
}

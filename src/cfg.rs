// Owns cfg/ loading, defaults and runtime-root creation.

use crate::rule;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct Config {
    pub preset: String,
    pub compact_at: f32,
    pub prefs_lines_global: u16,
    pub prefs_lines_project: u16,
    pub max_parallel_runs: u8,
    pub max_background_jobs: u8,
    pub max_steps: MaxSteps,
    pub max_depth: u8,
    pub sync: bool,
    pub telemetry: bool,
    pub ui: Ui,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            preset: "default".into(),
            compact_at: 0.6,
            prefs_lines_global: 60,
            prefs_lines_project: 40,
            max_parallel_runs: 4,
            max_background_jobs: 4,
            max_steps: MaxSteps::default(),
            max_depth: 3,
            sync: false,
            telemetry: false,
            ui: Ui::default(),
        }
    }
}

impl Config {
    fn clamp(&mut self) {
        self.compact_at = self.compact_at.clamp(0.3, 0.75);
        self.max_steps.clamp();
        self.ui.left_width = self.ui.left_width.clamp(200, 420);
        self.ui.right_width = self.ui.right_width.clamp(200, 420);
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct MaxSteps {
    pub orchestrator: u16,
    pub planner: u16,
    pub explorer: u16,
    pub worker: u16,
}

impl Default for MaxSteps {
    fn default() -> Self {
        Self {
            orchestrator: 300,
            planner: 200,
            explorer: 150,
            worker: 200,
        }
    }
}

impl MaxSteps {
    fn clamp(&mut self) {
        self.orchestrator = self.orchestrator.max(100);
        self.planner = self.planner.max(100);
        self.explorer = self.explorer.max(100);
        self.worker = self.worker.max(100);
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct Ui {
    pub theme: Theme,
    pub left_sidebar_open: bool,
    pub right_sidebar_open: bool,
    pub left_width: u16,
    pub right_width: u16,
}

impl Default for Ui {
    fn default() -> Self {
        Self {
            theme: Theme::System,
            left_sidebar_open: true,
            right_sidebar_open: true,
            left_width: 240,
            right_width: 280,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Theme {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Presets(pub BTreeMap<String, Preset>);

impl Default for Presets {
    fn default() -> Self {
        let mut roles = BTreeMap::new();
        roles.insert(
            "all".into(),
            Model {
                model: Some("gemini-3.5-flash".into()),
                reasoning: Some(Reasoning::Low),
                temperature: None,
            },
        );
        for role in ["orchestrator", "planner"] {
            roles.insert(
                role.into(),
                Model {
                    model: Some("gemini-3.7-flash".into()),
                    reasoning: Some(Reasoning::High),
                    temperature: None,
                },
            );
        }
        roles.insert(
            "curator".into(),
            Model {
                model: Some("gemini-3.7-flash".into()),
                reasoning: Some(Reasoning::Off),
                temperature: None,
            },
        );
        Self(BTreeMap::from([("default".into(), Preset { roles })]))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Preset {
    #[serde(flatten)]
    pub roles: BTreeMap<String, Model>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Model {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Reasoning {
    Off,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Account {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

pub fn root() -> Result<PathBuf> {
    Ok(home()?.join(".pippo"))
}

pub fn home() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .context("home directory is unavailable")?;
    Ok(PathBuf::from(home))
}

pub fn load_at(root: PathBuf) -> Result<Config> {
    let dir = root.join("cfg");
    fs::create_dir_all(&dir)
        .with_context(|| format!("create runtime config directory {}", dir.display()))?;

    let config_path = dir.join("config.json");
    let mut config: Config = read_json(&config_path, Config::default())?;
    config.clamp();
    write_if_changed(&config_path, &config)?;

    preset_at(&root, &config.preset)?;
    read_json(&dir.join("account.json"), Account::default())?;

    let rules_path = dir.join("rules.yaml");
    if !rules_path.exists() {
        fs::write(&rules_path, rule::DEFAULTS)
            .with_context(|| format!("write default config {}", rules_path.display()))?;
    }
    fs::read(&rules_path).with_context(|| format!("read config {}", rules_path.display()))?;

    Ok(config)
}

pub fn preset_at(root: &Path, name: &str) -> Result<Preset> {
    let presets: Presets = read_json(&root.join("cfg/presets.json"), Presets::default())?;
    presets
        .0
        .get(name)
        .cloned()
        .with_context(|| format!("active preset {name:?} is not defined"))
}

fn read_json<T>(path: &PathBuf, default: T) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    if !path.exists() {
        write_json(path, &default)?;
        return Ok(default);
    }
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read config {}", path.display()))?,
    )
    .with_context(|| format!("parse config {}", path.display()))
}

fn write_if_changed<T: Serialize>(path: &PathBuf, value: &T) -> Result<()> {
    let saved: serde_json::Value = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read config {}", path.display()))?,
    )
    .with_context(|| format!("parse config {}", path.display()))?;
    if saved != serde_json::to_value(value)? {
        write_json(path, value)?;
    }
    Ok(())
}

fn write_json<T: Serialize>(path: &PathBuf, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes).with_context(|| format!("write config {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn root() -> PathBuf {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        env::temp_dir().join(format!("pippo-cfg-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn creates_and_reads_every_config_file() {
        let root = root();
        let cfg = load_at(root.clone()).unwrap();

        assert_eq!(cfg, Config::default());
        for name in ["config.json", "presets.json", "rules.yaml", "account.json"] {
            assert!(root.join("cfg").join(name).is_file(), "missing {name}");
        }

        let loaded = load_at(root.clone()).unwrap();
        assert_eq!(loaded, cfg);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fills_missing_keys_and_clamps_documented_limits() {
        let root = root();
        let dir = root.join("cfg");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("config.json"),
            r#"{
                "preset": "default",
                "compact_at": 0.9,
                "max_steps": {"worker": 12},
                "sync": true,
                "ui": {"left_width": 100, "right_width": 900}
            }"#,
        )
        .unwrap();
        let cfg = load_at(root.clone()).unwrap();
        assert_eq!(cfg.compact_at, 0.75);
        assert_eq!(cfg.max_steps.worker, 100);
        assert_eq!(cfg.max_steps.orchestrator, 300);
        assert_eq!(cfg.ui.left_width, 200);
        assert_eq!(cfg.ui.right_width, 420);
        assert!(cfg.sync);
        assert_eq!(
            fs::read_to_string(dir.join("rules.yaml")).unwrap(),
            rule::DEFAULTS
        );

        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join("config.json")).unwrap()).unwrap();
        assert!(saved.get("telemetry").is_some());
        assert!(saved["ui"].get("theme").is_some());
        fs::remove_dir_all(root).unwrap();
    }
}

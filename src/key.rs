// Owns the model credential in a private file under the runtime root.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};
use tauri::{AppHandle, Emitter, State};

const FILE: &str = "auth.json";
const EVENT: &str = "model-key-status";
// Owner-only: a credential is never readable by another account on the machine.
#[cfg(unix)]
const MODE: u32 = 0o600;

#[derive(Clone)]
pub struct Key {
    path: PathBuf,
}

#[derive(Deserialize, Serialize)]
struct Auth {
    #[serde(skip_serializing_if = "Option::is_none")]
    gemini_api_key: Option<String>,
}

pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Missing,
    Stored,
}

impl Key {
    pub fn new(root: PathBuf) -> Self {
        Self {
            path: root.join(FILE),
        }
    }

    pub fn read(&self) -> Result<Option<Secret>> {
        let Some(auth) = self.load()? else {
            return Ok(None);
        };
        auth.gemini_api_key
            .filter(|value| !value.trim().is_empty())
            .map(validate)
            .transpose()
    }

    fn status(&self) -> Result<Status> {
        Ok(if self.read()?.is_some() {
            Status::Stored
        } else {
            Status::Missing
        })
    }

    pub fn is_stored(&self) -> Result<bool> {
        Ok(self.status()? == Status::Stored)
    }

    fn store(&self, value: String) -> Result<Status> {
        let value = validate(value)?;
        write_private(
            &self.path,
            &Auth {
                gemini_api_key: Some(value.0),
            },
        )?;
        Ok(Status::Stored)
    }

    fn clear(&self) -> Result<Status> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(Status::Missing),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Status::Missing),
            Err(error) => {
                Err(error).with_context(|| format!("clear model key {}", self.path.display()))
            }
        }
    }

    fn load(&self) -> Result<Option<Auth>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read model key {}", self.path.display()))
            }
        };
        protect(&self.path)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("parse model key {}", self.path.display()))
    }
}

#[tauri::command]
pub fn model_key_status(key: State<'_, Key>) -> std::result::Result<Status, String> {
    key.status().map_err(message)
}

#[tauri::command]
pub fn store_model_key(
    app: AppHandle,
    key: State<'_, Key>,
    value: String,
) -> std::result::Result<Status, String> {
    changed(&app, key.store(value).map_err(message)?)
}

#[tauri::command]
pub fn clear_model_key(app: AppHandle, key: State<'_, Key>) -> std::result::Result<Status, String> {
    changed(&app, key.clear().map_err(message)?)
}

fn changed(app: &AppHandle, status: Status) -> std::result::Result<Status, String> {
    app.emit(EVENT, status).map_err(message)?;
    Ok(status)
}

fn write_private(path: &Path, auth: &Auth) -> Result<()> {
    let parent = path.parent().context("model key file has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create model key directory {}", parent.display()))?;
    let mut bytes = serde_json::to_vec_pretty(auth).context("serialize model key")?;
    bytes.push(b'\n');
    let temp = path.with_extension("json.tmp");
    // The temp file carries the final mode from creation, so the secret is never world readable.
    let mut file =
        private(&temp).with_context(|| format!("create model key temp {}", temp.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("write model key temp {}", temp.display()))?;
    file.sync_all()
        .with_context(|| format!("flush model key temp {}", temp.display()))?;
    drop(file);
    fs::rename(&temp, path).with_context(|| format!("publish model key {}", path.display()))?;
    File::open(parent)
        .with_context(|| format!("open model key directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("flush model key directory {}", parent.display()))
}

fn private(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(MODE)
        .open(path)
}

fn protect(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path)
        .with_context(|| format!("inspect model key {}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    if mode != MODE {
        fs::set_permissions(path, fs::Permissions::from_mode(MODE))
            .with_context(|| format!("restrict model key {}", path.display()))?;
    }
    Ok(())
}

fn validate(value: String) -> Result<Secret> {
    let value = value.trim();
    if value.is_empty() {
        anyhow::bail!("model key is empty");
    }
    if value.chars().any(char::is_control) {
        anyhow::bail!("model key contains control characters");
    }
    Ok(Secret(value.into()))
}

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn key() -> (Key, PathBuf) {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pippo-key-{}-{nonce}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        (Key::new(root.clone()), root)
    }

    #[test]
    fn validates_without_exposing_a_key() {
        let key = validate("  secret-value  ".into()).unwrap();
        assert_eq!(key.expose(), "secret-value");
        assert!(validate(" \n\t ".into()).is_err());
        assert!(validate("secret\nvalue".into()).is_err());
    }

    #[test]
    fn status_is_an_unambiguous_ui_contract() {
        assert_eq!(
            serde_json::to_string(&Status::Missing).unwrap(),
            r#""missing""#
        );
        assert_eq!(
            serde_json::to_string(&Status::Stored).unwrap(),
            r#""stored""#
        );
    }

    #[test]
    fn stores_reads_and_clears_an_owner_only_file() {
        let (key, root) = key();
        assert_eq!(key.status().unwrap(), Status::Missing);
        assert!(key.read().unwrap().is_none());

        assert_eq!(
            key.store("  secret-value  ".into()).unwrap(),
            Status::Stored
        );
        assert_eq!(key.read().unwrap().unwrap().expose(), "secret-value");
        assert_eq!(key.status().unwrap(), Status::Stored);
        assert!(key.is_stored().unwrap());
        assert!(!root.join("auth.json.tmp").exists());

        assert_eq!(key.clear().unwrap(), Status::Missing);
        assert_eq!(key.clear().unwrap(), Status::Missing);
        assert!(key.read().unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn repairs_permissions_that_were_widened_outside_the_app() {
        use std::os::unix::fs::PermissionsExt;
        let (key, root) = key();
        key.store("secret-value".into()).unwrap();
        let path = root.join("auth.json");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            MODE
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(key.read().unwrap().unwrap().expose(), "secret-value");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            MODE
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_blank_or_absent_value_reads_as_missing() {
        let (key, root) = key();
        fs::write(root.join("auth.json"), b"{}\n").unwrap();
        assert_eq!(key.status().unwrap(), Status::Missing);
        fs::write(root.join("auth.json"), br#"{"gemini_api_key":"  "}"#).unwrap();
        assert_eq!(key.status().unwrap(), Status::Missing);
        fs::remove_dir_all(root).unwrap();
    }
}

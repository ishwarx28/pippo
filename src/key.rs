// Owns the model credential in the operating system keychain.

use anyhow::{Context, Result};
use keyring::{Entry, Error};
use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

const SERVICE: &str = "app.pippo.desktop";
const USER: &str = "gemini-api-key";
const EVENT: &str = "model-key-status";

pub struct Key;

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
    pub fn read(&self) -> Result<Option<Secret>> {
        match entry()?.get_password() {
            Ok(value) => Ok(Some(validate(value)?)),
            Err(Error::NoEntry) => Ok(None),
            Err(error) => Err(error).context("read model key from system keychain"),
        }
    }

    fn status(&self) -> Result<Status> {
        Ok(if self.read()?.is_some() {
            Status::Stored
        } else {
            Status::Missing
        })
    }

    fn store(&self, value: String) -> Result<Status> {
        let value = validate(value)?;
        entry()?
            .set_password(value.expose())
            .context("store model key in system keychain")?;
        Ok(Status::Stored)
    }

    fn clear(&self) -> Result<Status> {
        match entry()?.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(Status::Missing),
            Err(error) => Err(error).context("clear model key from system keychain"),
        }
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

fn entry() -> Result<Entry> {
    Entry::new(SERVICE, USER).context("open system keychain entry")
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
}

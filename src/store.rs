// Owns the runtime tree, append-only JSONL and atomic rewrites.

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

#[derive(Debug)]
pub struct Store {
    session: PathBuf,
    io: Mutex<()>,
}

impl Store {
    pub fn open(root: PathBuf) -> Result<Self> {
        let session = root.join("session");
        fs::create_dir_all(&session)
            .with_context(|| format!("create session directory {}", session.display()))?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(session.join("messages.jsonl"))
            .with_context(|| format!("create history in {}", session.display()))?;

        let store = Self {
            session,
            io: Mutex::new(()),
        };
        store.messages::<serde_json::Value>()?;
        if store.replay_path().exists() {
            store.replay::<serde_json::Value>()?;
        } else {
            store.replace_replay(&Vec::<serde_json::Value>::new())?;
        }
        Ok(store)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn append<T: Serialize>(&self, message: &T) -> Result<()> {
        let _guard = self.lock()?;
        self.append_locked(message)
    }

    pub fn record<T: Serialize, R: Serialize>(&self, message: &T, replay: &R) -> Result<()> {
        let _guard = self.lock()?;
        self.append_locked(message)?;
        self.replace_replay_locked(replay)
    }

    fn append_locked<T: Serialize>(&self, message: &T) -> Result<()> {
        let path = self.history_path();
        let mut bytes = serde_json::to_vec(message).context("serialize history message")?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open history {}", path.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("append history {}", path.display()))?;
        file.sync_data()
            .with_context(|| format!("flush history {}", path.display()))
    }

    pub fn messages<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let _guard = self.lock()?;
        self.read_messages()
    }

    pub fn replay<T: DeserializeOwned>(&self) -> Result<T> {
        let _guard = self.lock()?;
        let path = self.replay_path();
        serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("read replay {}", path.display()))?,
        )
        .with_context(|| format!("parse replay {}", path.display()))
    }

    pub fn replace_replay<T: Serialize>(&self, replay: &T) -> Result<()> {
        let _guard = self.lock()?;
        self.replace_replay_locked(replay)
    }

    fn replace_replay_locked<T: Serialize>(&self, replay: &T) -> Result<()> {
        let path = self.replay_path();
        let tmp = self.session.join(".replay.json.tmp");
        let mut bytes = serde_json::to_vec_pretty(replay).context("serialize replay")?;
        bytes.push(b'\n');

        let mut file = File::create(&tmp)
            .with_context(|| format!("create replay temp file {}", tmp.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("write replay temp file {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("flush replay temp file {}", tmp.display()))?;
        fs::rename(&tmp, &path)
            .with_context(|| format!("replace replay {} from {}", path.display(), tmp.display()))?;
        File::open(&self.session)
            .with_context(|| format!("open session directory {}", self.session.display()))?
            .sync_all()
            .with_context(|| format!("flush session directory {}", self.session.display()))
    }

    fn read_messages<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let path = self.history_path();
        let bytes = fs::read(&path).with_context(|| format!("read history {}", path.display()))?;
        let complete = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let mut messages = Vec::new();
        if complete > 0 {
            for (index, line) in bytes[..complete - 1]
                .split(|byte| *byte == b'\n')
                .enumerate()
            {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                messages.push(serde_json::from_slice(line).with_context(|| {
                    format!("parse history {} line {}", path.display(), index + 1)
                })?);
            }
        }
        if complete < bytes.len() {
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .with_context(|| format!("open history for recovery {}", path.display()))?;
            file.set_len(complete as u64)
                .with_context(|| format!("repair history {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("flush repaired history {}", path.display()))?;
        }
        Ok(messages)
    }

    fn lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.io
            .lock()
            .map_err(|_| anyhow::anyhow!("store lock poisoned"))
    }

    fn history_path(&self) -> PathBuf {
        self.session.join("messages.jsonl")
    }

    fn replay_path(&self) -> PathBuf {
        self.session.join("replay.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Message {
        id: u8,
        text: String,
    }

    fn root() -> PathBuf {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pippo-store-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn appends_and_reloads_history() {
        let root = root();
        let store = Store::open(root.clone()).unwrap();
        let first = Message {
            id: 1,
            text: "hello".into(),
        };
        let second = Message {
            id: 2,
            text: "line one\nline two".into(),
        };

        store.append(&first).unwrap();
        store.append(&second).unwrap();
        drop(store);

        let store = Store::open(root.clone()).unwrap();
        assert_eq!(store.messages::<Message>().unwrap(), vec![first, second]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn atomically_replaces_replay() {
        let root = root();
        let store = Store::open(root.clone()).unwrap();
        store.replace_replay(&vec!["first"]).unwrap();
        store.replace_replay(&vec!["second", "third"]).unwrap();

        assert_eq!(
            store.replay::<Vec<String>>().unwrap(),
            vec!["second", "third"]
        );
        assert!(!root.join("session/.replay.json.tmp").exists());
        drop(store);
        assert!(Store::open(root.clone()).is_ok());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn drops_only_a_truncated_final_line() {
        let root = root();
        let session = root.join("session");
        fs::create_dir_all(&session).unwrap();
        fs::write(
            session.join("messages.jsonl"),
            b"{\"id\":1,\"text\":\"kept\"}\n{\"id\":2,\"text\":\"cut",
        )
        .unwrap();

        let store = Store::open(root.clone()).unwrap();
        assert_eq!(
            store.messages::<Message>().unwrap(),
            vec![Message {
                id: 1,
                text: "kept".into()
            }]
        );
        assert_eq!(
            fs::read(session.join("messages.jsonl")).unwrap(),
            b"{\"id\":1,\"text\":\"kept\"}\n"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_corruption_before_the_final_line() {
        let root = root();
        let session = root.join("session");
        fs::create_dir_all(&session).unwrap();
        fs::write(session.join("messages.jsonl"), b"not json\n{\"id\":2").unwrap();

        let error = Store::open(root.clone()).unwrap_err();
        assert!(error.to_string().contains("line 1"));
        assert_eq!(
            fs::read(session.join("messages.jsonl")).unwrap(),
            b"not json\n{\"id\":2"
        );
        fs::remove_dir_all(root).unwrap();
    }
}

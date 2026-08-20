// Owns desktop bootstrap, the single window and child process.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod cfg;
mod key;
mod rpc;
mod store;

use anyhow::{Context, Result};
use std::{
    fmt::Write as _,
    io::{BufRead, BufReader, Write},
    net::{IpAddr, SocketAddr},
    process::{Child, Command, Stdio},
    sync::{mpsc, Mutex},
    thread,
    time::Duration,
};

const CHILD_PATH: &str = env!("PIPPO_PIPPOD_PATH");
const START_TIMEOUT: Duration = Duration::from_secs(3);

pub struct Service {
    child: Mutex<Child>,
}

struct Spawned {
    service: Service,
    addr: SocketAddr,
    token: String,
}

impl Service {
    fn spawn() -> Result<Spawned> {
        let token = token()?;
        let mut child = Command::new(CHILD_PATH)
            .args(["--listen", "127.0.0.1:0"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn child process {CHILD_PATH}"))?;
        let mut stdin = child.stdin.take().context("open child token pipe")?;
        stdin
            .write_all(token.as_bytes())
            .context("send child token")?;
        stdin.write_all(b"\n").context("finish child token")?;
        drop(stdin);

        let stdout = child.stdout.take().context("open child readiness pipe")?;
        let (sent, received) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            if sent.send(result).is_err() {
                eprintln!("child readiness receiver closed");
            }
        });
        let mut service = Self {
            child: Mutex::new(child),
        };
        let line = received
            .recv_timeout(START_TIMEOUT)
            .context("child process did not become ready")?
            .context("read child process address")?;
        reader
            .join()
            .map_err(|_| anyhow::anyhow!("child readiness reader panicked"))?;
        let addr: SocketAddr = line.trim().parse().context("parse child process address")?;
        if !matches!(addr.ip(), IpAddr::V4(ip) if ip.is_loopback()) {
            anyhow::bail!("child process did not bind IPv4 loopback");
        }
        if let Some(status) = service
            .child_mut()?
            .try_wait()
            .context("check child process")?
        {
            anyhow::bail!("child process exited during startup with {status}");
        }
        Ok(Spawned {
            service,
            addr,
            token,
        })
    }

    fn stop(&mut self) -> Result<()> {
        let child = self.child_mut()?;
        if child.try_wait().context("check child process")?.is_none() {
            child.kill().context("stop child process")?;
        }
        child.wait().context("wait for child process")?;
        Ok(())
    }

    fn child_mut(&mut self) -> Result<&mut Child> {
        self.child
            .get_mut()
            .map_err(|_| anyhow::anyhow!("child process lock poisoned"))
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("failed to stop child process: {error:#}");
        }
    }
}

fn token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generate child token: {error}"))?;
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(token, "{byte:02x}").context("encode child token")?;
    }
    Ok(token)
}

fn main() {
    run().expect("failed to run pippo");
}

fn run() -> Result<()> {
    let root = cfg::root()?;
    let cfg = cfg::load_at(root.clone())?;
    let store = store::Store::open(root.clone())?;
    let spawned = Service::spawn()?;
    let rpc = rpc::Rpc::connect(spawned.addr, spawned.token, &rpc::Hello::new(&root, &cfg)?)?;
    tauri::Builder::default()
        .manage(cfg)
        .manage(key::Key)
        .manage(store)
        .manage(rpc)
        .manage(spawned.service)
        .invoke_handler(tauri::generate_handler![
            key::model_key_status,
            key::store_model_key,
            key::clear_model_key
        ])
        .run(tauri::generate_context!())
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, TcpStream};

    #[test]
    fn tokens_are_random_hex() {
        let first = token().unwrap();
        let second = token().unwrap();
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn child_rpc_is_authenticated_and_concurrent() {
        let spawned = Service::spawn().unwrap();
        assert_eq!(spawned.addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        let hello = rpc::Hello::new(&std::env::temp_dir(), &cfg::Config::default()).unwrap();
        let rpc = rpc::Rpc::connect(spawned.addr, spawned.token.clone(), &hello).unwrap();
        assert!(rpc::Rpc::connect(spawned.addr, spawned.token, &hello).is_err());

        thread::scope(|scope| {
            let calls: Vec<_> = (0..16)
                .map(|_| {
                    let rpc = rpc.clone();
                    scope.spawn(move || {
                        let ready: serde_json::Value = rpc.call("health", &()).unwrap();
                        assert_eq!(ready["ready"], true);
                    })
                })
                .collect();
            for call in calls {
                call.join().unwrap();
            }
        });
        drop(rpc);
        let addr = spawned.addr;
        let mut service = spawned.service;
        service.stop().unwrap();
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err());
    }
}

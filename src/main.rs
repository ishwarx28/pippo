// Owns desktop bootstrap, the single window and child process.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod cfg;
mod ipc;
mod key;
mod proj;
mod rpc;
mod rule;
mod sess;
mod store;
mod tool {
    pub mod find;
    pub mod shell;
    pub mod write;
}

use anyhow::{Context, Result};
use std::{
    fmt::Write as _,
    io::{BufRead, BufReader, Write},
    net::{IpAddr, SocketAddr},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tauri::Manager;

const CHILD_PATH: &str = env!("PIPPO_PIPPOD_PATH");
const START_TIMEOUT: Duration = Duration::from_secs(3);
const STOP_TIMEOUT: Duration = Duration::from_secs(3);
const STOP_POLL: Duration = Duration::from_millis(10);

pub struct Service {
    child: Mutex<Option<Child>>,
}

#[derive(Default)]
struct Shutdown {
    started: AtomicBool,
    events: Mutex<Vec<thread::JoinHandle<()>>>,
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
            child: Mutex::new(Some(child)),
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

    fn stop(&self) -> Result<bool> {
        let Some(mut child) = self
            .child
            .lock()
            .map_err(|_| anyhow::anyhow!("child process lock poisoned"))?
            .take()
        else {
            return Ok(true);
        };
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(true),
                Ok(None) if Instant::now() < deadline => thread::sleep(STOP_POLL),
                Ok(None) => break,
                Err(error) => {
                    let kill = child.kill();
                    let wait = child.wait();
                    if let Err(kill) = kill {
                        return Err(error)
                            .context(format!("check child process; stop failed: {kill}"));
                    }
                    wait.context("wait for child process after failed status check")?;
                    return Err(error).context("check child process");
                }
            }
        }
        child.kill().context("force child process to stop")?;
        child.wait().context("wait for child process")?;
        Ok(false)
    }

    fn child_mut(&mut self) -> Result<&mut Child> {
        self.child
            .get_mut()
            .map_err(|_| anyhow::anyhow!("child process lock poisoned"))
            .and_then(|child| child.as_mut().context("child process already stopped"))
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("failed to stop child process: {error:#}");
        }
    }
}

impl Shutdown {
    fn set_events(&self, events: Vec<thread::JoinHandle<()>>) -> Result<()> {
        let mut slot = self
            .events
            .lock()
            .map_err(|_| anyhow::anyhow!("turn event thread lock poisoned"))?;
        if !slot.is_empty() {
            anyhow::bail!("turn event thread already set");
        }
        *slot = events;
        Ok(())
    }

    fn run(&self, sess: &sess::Sess, rpc: &rpc::Rpc, service: &Service) -> Result<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let mut errors = Vec::new();
        keep(&mut errors, sess.shutdown().map(|_| ()), "close session");
        keep(
            &mut errors,
            rpc.call::<_, ShutdownAccepted>("shutdown", &())
                .and_then(|reply| {
                    if reply.accepted {
                        Ok(())
                    } else {
                        anyhow::bail!("child process was already shutting down")
                    }
                }),
            "request child shutdown",
        );
        keep(&mut errors, rpc.shutdown(), "close rpc");
        let events = self
            .events
            .lock()
            .map(|mut events| events.drain(..).collect::<Vec<_>>());
        match events {
            Ok(events) => {
                for event in events {
                    if event.join().is_err() {
                        errors.push("join interface events: thread panicked".into());
                    }
                }
            }
            Err(_) => errors.push("join turn events: lock poisoned".into()),
        }
        keep(
            &mut errors,
            service.stop().map(|_| ()),
            "stop child process",
        );
        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(errors.join("; "))
        }
    }
}

#[derive(serde::Deserialize)]
struct ShutdownAccepted {
    accepted: bool,
}

fn keep(errors: &mut Vec<String>, result: Result<()>, action: &str) {
    if let Err(error) = result {
        errors.push(format!("{action}: {error:#}"));
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
    let sess = Arc::new(sess::Sess::new(store)?);
    let proj = Arc::new(proj::Proj::open(root.clone())?);
    let rules = rule::Book::open(&root, &cfg::home()?)?;
    let spawned = Service::spawn()?;
    let key = key::Key;
    let rpc = rpc::Rpc::connect(
        spawned.addr,
        spawned.token,
        &rpc::Hello::new(&root, &cfg)?,
        key,
        Arc::clone(&proj),
        rules,
    )?;
    let notices = rpc.take_notices()?;
    let interactions = rpc.take_interactions()?;
    let event_sess = Arc::clone(&sess);
    let app = tauri::Builder::default()
        .manage(cfg)
        .manage(key)
        .manage(sess)
        .manage(proj)
        .manage(rpc)
        .manage(spawned.service)
        .manage(Shutdown::default())
        .setup(move |app| {
            let events = ipc::listen(app.handle().clone(), event_sess, notices, interactions)?;
            app.state::<Shutdown>().set_events(events)?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            key::model_key_status,
            key::store_model_key,
            key::clear_model_key,
            ipc::session_snapshot,
            ipc::send_message,
            ipc::stop_turn,
            ipc::answer_clarify,
            ipc::cancel_clarify,
            ipc::answer_approval,
            ipc::cancel_approval
        ])
        .build(tauri::generate_context!())?;
    app.run(|app, event| {
        if matches!(
            event,
            tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit
        ) {
            let result = app.state::<Shutdown>().run(
                app.state::<Arc<sess::Sess>>().inner(),
                app.state::<rpc::Rpc>().inner(),
                app.state::<Service>().inner(),
            );
            if let Err(error) = result {
                eprintln!("shutdown failed: {error:#}");
            }
        }
    });
    Ok(())
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
        let root = std::env::temp_dir().join(format!(
            "pippo-child-rpc-{}-{}",
            std::process::id(),
            token().unwrap()
        ));
        let hello = rpc::Hello::new(&root, &cfg::Config::default()).unwrap();
        let proj = Arc::new(proj::Proj::open(root.clone()).unwrap());
        cfg::load_at(root.clone()).unwrap();
        let rpc = rpc::Rpc::connect(
            spawned.addr,
            spawned.token.clone(),
            &hello,
            key::Key,
            Arc::clone(&proj),
            rule::Book::open(&root, &root).unwrap(),
        )
        .unwrap();
        assert!(rpc::Rpc::connect(
            spawned.addr,
            spawned.token,
            &hello,
            key::Key,
            proj,
            rule::Book::open(&root, &root).unwrap()
        )
        .is_err());

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
        let stopped: ShutdownAccepted = rpc.call("shutdown", &()).unwrap();
        assert!(stopped.accepted);
        rpc.shutdown().unwrap();
        let addr = spawned.addr;
        let service = spawned.service;
        assert!(service.stop().unwrap());
        assert!(service.stop().unwrap());
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repeated_shutdown_flushes_the_cancelled_turn() {
        let root = std::env::temp_dir().join(format!(
            "pippo-shutdown-{}-{}",
            std::process::id(),
            token().unwrap()
        ));
        let store = store::Store::open(root.clone()).unwrap();
        let sess = sess::Sess::new(store).unwrap();
        let turn = sess.open("persist me".into()).unwrap();
        sess.chunk(&turn.call, "partial".into()).unwrap();
        let spawned = Service::spawn().unwrap();
        let hello = rpc::Hello::new(&root, &cfg::Config::default()).unwrap();
        let proj = Arc::new(proj::Proj::open(root.clone()).unwrap());
        cfg::load_at(root.clone()).unwrap();
        let rpc = rpc::Rpc::connect(
            spawned.addr,
            spawned.token,
            &hello,
            key::Key,
            proj,
            rule::Book::open(&root, &root).unwrap(),
        )
        .unwrap();
        let shutdown = Shutdown::default();

        shutdown.run(&sess, &rpc, &spawned.service).unwrap();
        shutdown.run(&sess, &rpc, &spawned.service).unwrap();

        let restored = sess::Sess::new(store::Store::open(root.clone()).unwrap()).unwrap();
        let messages = restored.snapshot().unwrap();
        assert_eq!(messages[1].text, "partial");
        assert_eq!(messages[1].status, Some(sess::Status::Cancelled));
        std::fs::remove_dir_all(root).unwrap();
    }
}

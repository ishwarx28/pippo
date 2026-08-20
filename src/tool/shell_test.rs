// Exercises scoped execution, process cleanup and bounded output.

use super::*;
use crate::proj::Proj;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Tree {
    root: PathBuf,
    work: PathBuf,
    scope: Scope,
}

impl Tree {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "pippo-shell-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.join("runtime")).unwrap();
        let task = proj
            .create("test shell contract".into(), work.clone())
            .unwrap();
        let scope = proj.scope(Some(&task.task_id)).unwrap();
        Self { root, work, scope }
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn input(command: &str) -> Input {
    Input {
        turn_id: "run-a".into(),
        request_id: "request-a".into(),
        call_id: "shell-a".into(),
        task_id: None,
        command: command.into(),
        cwd: None,
        timeout: None,
        background: false,
        env: BTreeMap::new(),
    }
}

fn reason(result: &Result) -> find::Reason {
    result.error.as_ref().unwrap().reason
}

#[test]
fn runs_in_scope_with_environment_and_separate_streams() {
    let tree = Tree::new();
    fs::create_dir(tree.work.join("sub")).unwrap();
    let mut request = input("printf '%s:%s' \"$PIPPO_WORD\" \"$PWD\"; printf warning >&2; exit 7");
    request.cwd = Some("sub".into());
    request.env.insert("PIPPO_WORD".into(), "hello".into());
    let result = Shells::default().run(&tree.scope, request);
    assert!(result.ok, "{:?}", result.error);
    assert_eq!(result.exit_code, Some(7));
    assert!(result.stdout.starts_with("hello:"));
    assert!(result.stdout.ends_with("/sub"));
    assert_eq!(result.stderr, "warning");
    assert_eq!(result.signal, None);
}

#[test]
fn accepts_absolute_outside_cwd_and_closes_stdin() {
    let tree = Tree::new();
    let outside = tree.root.join("outside");
    fs::create_dir(&outside).unwrap();
    let mut request = input("if read line; then printf open; else printf '%s' \"$PWD:closed\"; fi");
    request.cwd = Some(outside.clone());
    let result = Shells::default().run(&tree.scope, request);
    assert!(result.ok);
    assert_eq!(
        result.stdout,
        format!("{}:closed", fs::canonicalize(outside).unwrap().display())
    );
}

#[test]
fn reports_signal_and_invalid_output_safely() {
    let tree = Tree::new();
    let signal = Shells::default().run(&tree.scope, input("kill -TERM $$"));
    assert!(signal.ok);
    assert_eq!(signal.exit_code, None);
    assert_eq!(signal.signal, Some(libc::SIGTERM));

    let invalid = Shells::default().run(&tree.scope, input("printf '\\377ok'"));
    assert!(invalid.ok);
    assert!(
        invalid.stdout.ends_with("ok"),
        "invalid output became {:?}",
        invalid.stdout
    );
    assert!(invalid.stdout.contains('\u{fffd}'));
}

#[test]
fn rejects_bad_arguments_before_spawning() {
    let tree = Tree::new();
    let mut cases = vec![
        input(" "),
        input("printf nope"),
        input("printf nope"),
        input("printf nope"),
    ];
    cases[1].timeout = Some(0);
    cases[2].timeout = Some(381);
    cases[3].background = true;
    let mut bad_env = input("printf nope");
    bad_env.env.insert("BAD=NAME".into(), "value".into());
    cases.push(bad_env);
    let mut bad_cwd = input("printf nope");
    bad_cwd.cwd = Some("missing".into());
    cases.push(bad_cwd);
    for request in cases {
        let result = Shells::default().run(&tree.scope, request);
        assert!(!result.ok);
        assert!(matches!(
            reason(&result),
            find::Reason::BadArgs | find::Reason::NotFound
        ));
    }
    assert_eq!(
        validate(&tree.scope, &input("true")).unwrap().1,
        Duration::from_secs(30)
    );
    let mut longest = input("true");
    longest.timeout = Some(380);
    assert_eq!(
        validate(&tree.scope, &longest).unwrap().1,
        Duration::from_secs(380)
    );
}

#[test]
fn caps_both_streams_together_and_keeps_their_ends() {
    let tree = Tree::new();
    let command = "printf OUT_BEGIN; yes o | head -c 210000; printf OUT_END; \
                   printf ERR_BEGIN >&2; yes e | head -c 210000 >&2; printf ERR_END >&2";
    let result = Shells::default().run(&tree.scope, input(command));
    assert!(result.ok, "{:?}", result.error);
    assert!(result.stdout.starts_with("OUT_BEGIN"));
    assert!(result.stdout.ends_with("OUT_END"));
    assert!(result.stderr.starts_with("ERR_BEGIN"));
    assert!(result.stderr.ends_with("ERR_END"));
    assert!(result.stdout.contains(MARKER));
    assert!(result.stderr.contains(MARKER));
    assert!(result.stdout.len() + result.stderr.len() <= MAX_OUTPUT);
}

#[test]
fn timeout_terminates_the_descendant_process_group() {
    let tree = Tree::new();
    let mut request = input("sleep 30 & echo $! > child.pid; wait");
    request.timeout = Some(1);
    let result = Shells::default().run(&tree.scope, request);
    assert!(!result.ok);
    assert!(result.timed_out);
    assert_eq!(reason(&result), find::Reason::Timeout);
    let pid = wait_pid(&tree.work.join("child.pid"));
    assert!(!alive(pid), "timed-out descendant {pid} survived");
}

#[test]
fn cancellation_terminates_the_descendant_process_group() {
    let tree = Tree::new();
    let shells = Arc::new(Shells::default());
    let running = Arc::clone(&shells);
    let scope = tree.scope.clone();
    let handle = thread::spawn(move || {
        running.run(&scope, input("sleep 30 & echo $! > cancel-child.pid; wait"))
    });
    let path = tree.work.join("cancel-child.pid");
    let pid = wait_pid(&path);
    assert!(shells
        .cancel(Cancel {
            turn_id: "run-a".into(),
            request_id: "request-a".into(),
            call_id: "shell-a".into(),
        })
        .unwrap());
    let result = handle.join().unwrap();
    assert!(!result.ok);
    assert!(result.cancelled);
    assert!(!alive(pid), "cancelled descendant {pid} survived");
}

fn alive(pid: i32) -> bool {
    for _ in 0..100 {
        if unsafe { libc::kill(pid, 0) } != 0 {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
    true
}

fn wait_pid(path: &PathBuf) -> i32 {
    for _ in 0..100 {
        if let Ok(value) = fs::read_to_string(path) {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("pid file {} was not completed", path.display());
}

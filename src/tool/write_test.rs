// Exercises atomic writes, read evidence, exact edits and bounded diffs.

use super::*;
use crate::proj::Proj;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Tree {
    root: PathBuf,
    work: PathBuf,
    scope: Scope,
}

impl Tree {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "pippo-write-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.join("runtime")).unwrap();
        let task = proj
            .create("test write contract".into(), work.clone())
            .unwrap();
        let scope = proj.scope(Some(&task.task_id)).unwrap();
        Self { root, work, scope }
    }

    fn put(&self, path: &str, bytes: &[u8]) {
        let path = self.work.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn key(name: &str) -> Key {
    Key::new(name.into(), "request".into()).unwrap()
}

fn read(reads: &Reads, scope: &Scope, key: Key, path: &str) {
    let outcome = find::run(
        scope,
        find::Input {
            turn_id: None,
            request_id: None,
            task_id: None,
            query: None,
            regex: false,
            place: None,
            root: None,
            context: None,
            cap: None,
            offset: None,
            path: Some(path.into()),
            range: Some(find::Range { start: 1, end: 1 }),
        },
    );
    assert!(outcome.result.ok, "read failed: {:?}", outcome.result.error);
    reads.mark(key, outcome.reads).unwrap();
}

#[test]
fn write_creates_and_whole_replaces_without_creating_parents() {
    let tree = Tree::new();
    let reads = Reads::default();
    let created = reads.write(&tree.scope, Path::new("new.txt"), "one\n".into());
    let Some(Value::Write {
        diff,
        created,
        line_count,
    }) = created.value
    else {
        panic!("write failed: {:?}", created.error)
    };
    assert!(created);
    assert_eq!(line_count, 1);
    assert!(diff.contains("--- a/new.txt") && diff.contains("+one"));
    assert_eq!(
        fs::read_to_string(tree.work.join("new.txt")).unwrap(),
        "one\n"
    );

    let replaced = reads.write(&tree.scope, Path::new("new.txt"), "two".into());
    let Some(Value::Write { diff, created, .. }) = replaced.value else {
        panic!("replacement failed: {:?}", replaced.error)
    };
    assert!(!created);
    assert!(diff.contains("-one") && diff.contains("+two"));
    assert!(diff.contains("No newline at end of file"));
    assert_eq!(
        fs::read_to_string(tree.work.join("new.txt")).unwrap(),
        "two"
    );

    let failed = reads.write(&tree.scope, Path::new("missing/new.txt"), "hidden".into());
    assert_eq!(failed.error.unwrap().reason, find::Reason::NotFound);
    assert!(!tree.work.join("missing").exists());

    let outside = reads.write(&tree.scope, Path::new("../outside.txt"), "outside\n".into());
    assert!(outside.ok);
    assert_eq!(
        fs::read_to_string(tree.root.join("outside.txt")).unwrap(),
        "outside\n"
    );
}

#[test]
fn edit_requires_fresh_same_run_evidence() {
    let tree = Tree::new();
    tree.put("file.txt", b"alpha beta\n");
    let reads = Reads::default();
    let denied = reads.edit(
        &tree.scope,
        key("first"),
        Path::new("file.txt"),
        "alpha",
        "omega",
        false,
    );
    assert_eq!(denied.error.unwrap().reason, find::Reason::Denied);

    read(&reads, &tree.scope, key("first"), "file.txt");
    tree.put("other.txt", b"alpha\n");
    let wrong_path = reads.edit(
        &tree.scope,
        key("first"),
        Path::new("other.txt"),
        "alpha",
        "omega",
        false,
    );
    assert_eq!(wrong_path.error.unwrap().reason, find::Reason::Denied);
    let wrong = reads.edit(
        &tree.scope,
        key("other"),
        Path::new("file.txt"),
        "alpha",
        "omega",
        false,
    );
    assert_eq!(wrong.error.unwrap().reason, find::Reason::Denied);
    let edited = reads.edit(
        &tree.scope,
        key("first"),
        Path::new("file.txt"),
        "alpha",
        "omega",
        false,
    );
    assert!(edited.ok);
    assert!(
        reads
            .edit(
                &tree.scope,
                key("first"),
                Path::new("file.txt"),
                "beta",
                "gamma",
                false,
            )
            .ok
    );

    read(&reads, &tree.scope, key("stale"), "file.txt");
    fs::write(tree.work.join("file.txt"), b"external change\n").unwrap();
    let stale = reads.edit(
        &tree.scope,
        key("stale"),
        Path::new("file.txt"),
        "external",
        "internal",
        false,
    );
    assert_eq!(stale.error.unwrap().reason, find::Reason::Busy);
    assert_eq!(
        fs::read_to_string(tree.work.join("file.txt")).unwrap(),
        "external change\n"
    );
}

#[test]
fn edit_reports_zero_many_and_all_match_counts() {
    let tree = Tree::new();
    tree.put("file.txt", b"same and same\n");
    let reads = Reads::default();
    let run = key("matches");
    read(&reads, &tree.scope, run.clone(), "file.txt");
    let zero = reads.edit(
        &tree.scope,
        run.clone(),
        Path::new("file.txt"),
        "missing",
        "new",
        false,
    );
    assert_eq!(zero.error.unwrap().matches, Some(0));
    let many = reads.edit(
        &tree.scope,
        run.clone(),
        Path::new("file.txt"),
        "same",
        "new",
        false,
    );
    assert_eq!(many.error.unwrap().matches, Some(2));
    assert_eq!(
        fs::read_to_string(tree.work.join("file.txt")).unwrap(),
        "same and same\n"
    );
    let all = reads.edit(&tree.scope, run, Path::new("file.txt"), "same", "new", true);
    let Some(Value::Edit { replacements, .. }) = all.value else {
        panic!("all edit failed: {:?}", all.error)
    };
    assert_eq!(replacements, 2);
    assert_eq!(
        fs::read_to_string(tree.work.join("file.txt")).unwrap(),
        "new and new\n"
    );
}

#[test]
fn invalid_text_sensitive_paths_and_aliases_are_safe() {
    let tree = Tree::new();
    tree.put("binary.bin", &[0xff, 0x00]);
    let reads = Reads::default();
    let binary = reads.write(&tree.scope, Path::new("binary.bin"), "text".into());
    assert_eq!(binary.error.unwrap().reason, find::Reason::BadArgs);
    assert_eq!(
        fs::read(tree.work.join("binary.bin")).unwrap(),
        [0xff, 0x00]
    );
    let denied = reads.write(&tree.scope, Path::new(".env.local"), "SECRET=x".into());
    assert_eq!(denied.error.unwrap().reason, find::Reason::Denied);
    assert!(!tree.work.join(".env.local").exists());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        tree.put("real.txt", b"old\n");
        symlink(tree.work.join("real.txt"), tree.work.join("alias.txt")).unwrap();
        read(&reads, &tree.scope, key("alias"), "real.txt");
        assert!(
            reads
                .edit(
                    &tree.scope,
                    key("alias"),
                    Path::new("alias.txt"),
                    "old",
                    "new",
                    false,
                )
                .ok
        );
        assert!(tree.work.join("alias.txt").is_symlink());
        assert_eq!(
            fs::read_to_string(tree.work.join("real.txt")).unwrap(),
            "new\n"
        );
    }
}

#[test]
fn diffs_are_bounded_and_unicode_safe() {
    let tree = Tree::new();
    tree.put("large.txt", "old 🦀\n".repeat(20_000).as_bytes());
    let reads = Reads::default();
    let result = reads.write(
        &tree.scope,
        Path::new("large.txt"),
        "new 🦀\n".repeat(20_000),
    );
    let Some(Value::Write { diff, .. }) = result.value else {
        panic!("large write failed: {:?}", result.error)
    };
    assert!(diff.len() <= MAX_DIFF);
    assert!(diff.contains("diff elided"));
    assert!(std::str::from_utf8(diff.as_bytes()).is_ok());
}

#[test]
fn concurrent_edits_never_commit_partial_or_lost_content() {
    let tree = Tree::new();
    tree.put("file.txt", b"value\n");
    let reads = Arc::new(Reads::default());
    let run = key("concurrent");
    read(&reads, &tree.scope, run.clone(), "file.txt");
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for replacement in ["first", "second"] {
        let reads = Arc::clone(&reads);
        let scope = tree.scope.clone();
        let run = run.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            reads.edit(
                &scope,
                run,
                Path::new("file.txt"),
                "value",
                replacement,
                false,
            )
        }));
    }
    barrier.wait();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.ok).count(), 1);
    let content = fs::read_to_string(tree.work.join("file.txt")).unwrap();
    assert!(content == "first\n" || content == "second\n");
}

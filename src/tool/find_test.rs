// Exercises search, paging, scope, ignore and sensitive-read behavior.

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
            "pippo-find-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();
        let proj = Proj::open(root.join("runtime")).unwrap();
        let task = proj
            .create("test find contract".into(), work.clone())
            .unwrap();
        let scope = proj.scope(Some(&task.task_id)).unwrap();
        Self { root, work, scope }
    }

    fn write(&self, path: &str, bytes: &[u8]) {
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

fn search(query: &str) -> Input {
    Input {
        turn_id: None,
        request_id: None,
        task_id: None,
        query: Some(query.into()),
        regex: false,
        place: None,
        root: None,
        context: None,
        cap: None,
        offset: None,
        path: None,
        range: None,
    }
}

fn read(path: &str, range: Option<Range>) -> Input {
    Input {
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
        range,
    }
}

fn value(outcome: Outcome) -> Value {
    let result = outcome.result;
    assert!(result.ok, "find failed: {:?}", result.error);
    result.value.unwrap()
}

#[test]
fn literal_regex_context_and_paging_are_stable() {
    let tree = Tree::new();
    tree.write("a.txt", b"before\nneedle one\nafter\nneedle two\n");
    tree.write("b.txt", b"needle three\n");
    let mut input = search("needle");
    input.context = Some(1);
    input.cap = Some(2);
    let Value::Search {
        hits, next_offset, ..
    } = value(run(&tree.scope, input))
    else {
        panic!("search returned a read")
    };
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].path, "a.txt");
    assert_eq!(hits[0].line, Some(2));
    assert_eq!(hits[0].total_lines, 4);
    assert_eq!(hits[0].context.len(), 3);
    assert!(hits[0].context[1].matched);
    assert_eq!(next_offset, Some(2));

    let mut next = search(r"needle (two|three)");
    next.regex = true;
    next.offset = Some(1);
    next.cap = Some(1);
    let Value::Search {
        hits, next_offset, ..
    } = value(run(&tree.scope, next))
    else {
        panic!("search returned a read")
    };
    assert_eq!(hits[0].path, "b.txt");
    assert_eq!(hits[0].line, Some(1));
    assert_eq!(next_offset, None);
}

#[test]
fn path_search_honours_project_ignores_and_explicit_roots() {
    let tree = Tree::new();
    tree.write("src/needle.rs", b"plain\n");
    tree.write("target/needle.rs", b"generated\n");
    tree.write("ignored/needle.log", b"ignored\n");
    tree.write(".gitignore", b"ignored/\n");
    let mut input = search("needle");
    input.place = Some(Place::Path);
    let Value::Search { hits, .. } = value(run(&tree.scope, input)) else {
        panic!("search returned a read")
    };
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].path, "src/needle.rs");
    assert_eq!(hits[0].source, "path");
    assert_eq!(hits[0].line, None);

    let mut explicit = search("generated");
    explicit.root = Some("target/needle.rs".into());
    let Value::Search { hits, .. } = value(run(&tree.scope, explicit)) else {
        panic!("search returned a read")
    };
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].path, "target/needle.rs");
}

#[test]
fn range_reads_are_line_aware_and_paths_may_leave_scope() {
    let tree = Tree::new();
    let long = (1..=205)
        .map(|line| format!("line {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    tree.write("long.txt", long.as_bytes());
    let failed = run(&tree.scope, read("long.txt", None)).result;
    assert!(!failed.ok);
    let error = failed.error.unwrap();
    assert!(matches!(error.reason, Reason::BadArgs));
    assert_eq!(error.total_lines, Some(205));

    let Value::Read {
        path,
        start,
        end,
        total_lines,
        lines,
    } = value(run(
        &tree.scope,
        read(
            "long.txt",
            Some(Range {
                start: 201,
                end: 205,
            }),
        ),
    ))
    else {
        panic!("read returned a search")
    };
    assert_eq!(
        (path.as_str(), start, end, total_lines),
        ("long.txt", 201, 205, 205)
    );
    assert_eq!(lines[0].line, 201);
    assert_eq!(lines[4].text, "line 205");

    let outside = tree.root.join("outside.txt");
    fs::write(&outside, b"outside\n").unwrap();
    let Value::Read { path, lines, .. } = value(run(
        &tree.scope,
        read("../outside.txt", Some(Range { start: 1, end: 1 })),
    )) else {
        panic!("read returned a search")
    };
    assert_eq!(path, fs::canonicalize(&outside).unwrap().to_string_lossy());
    assert_eq!(lines[0].text, "outside");

    let mut absolute = search("outside");
    absolute.root = Some(outside.clone());
    let Value::Search { hits, .. } = value(run(&tree.scope, absolute)) else {
        panic!("search returned a read")
    };
    assert_eq!(hits[0].path, outside.to_string_lossy());
}

#[test]
fn denylist_blocks_sensitive_names_and_symlink_aliases() {
    let tree = Tree::new();
    tree.write(".env.local", b"TOKEN=secret\n");
    tree.write("private.pem", b"secret\n");
    for path in [".env.local", "private.pem"] {
        let result = run(&tree.scope, read(path, None)).result;
        assert!(!result.ok);
        assert!(matches!(result.error.unwrap().reason, Reason::Denied));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let secret = tree.root.join(".ssh/id_ed25519");
        fs::create_dir_all(secret.parent().unwrap()).unwrap();
        fs::write(&secret, b"secret\n").unwrap();
        symlink(&secret, tree.work.join("alias.txt")).unwrap();
        let result = run(&tree.scope, read("alias.txt", None)).result;
        assert!(!result.ok);
        assert!(matches!(result.error.unwrap().reason, Reason::Denied));
    }
}

#[test]
fn binary_content_is_skipped_but_path_search_remains_available() {
    let tree = Tree::new();
    tree.write(
        "binary-needle.bin",
        &[0xff, 0x00, b'n', b'e', b'e', b'd', b'l', b'e'],
    );
    let Value::Search { hits, .. } = value(run(&tree.scope, search("needle"))) else {
        panic!("search returned a read")
    };
    assert!(hits.is_empty());

    let mut by_path = search("needle");
    by_path.place = Some(Place::Path);
    let Value::Search { hits, .. } = value(run(&tree.scope, by_path)) else {
        panic!("search returned a read")
    };
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].path, "binary-needle.bin");

    let result = run(&tree.scope, read("binary-needle.bin", None)).result;
    assert!(!result.ok);
    assert!(matches!(result.error.unwrap().reason, Reason::BadArgs));
}

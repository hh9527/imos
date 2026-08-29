use std::fs::{File, Permissions};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use assert_cmd::cargo::cargo_bin;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
    store: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join("store");
        Self { root, store }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn command(&self) -> Command {
        let mut command = Command::new(cargo_bin("imos"));
        command.arg("--store").arg(&self.store);
        command
    }

    fn create(&self, plan: &Path) -> std::process::Output {
        self.command().arg("create").arg(plan).output().unwrap()
    }
}

fn write_plan(path: &Path, key: &str, items: Vec<Value>) {
    let plan = json!({
        "imos": {
            "version": 1,
            "key": key,
            "items": items,
        },
        "upstream": { "test": true }
    });
    std::fs::write(path, serde_json::to_vec_pretty(&plan).unwrap()).unwrap();
}

fn item(key: &str, source: &Path, action: Value) -> Value {
    let bytes = std::fs::read(source).unwrap();
    json!({
        "key": key,
        "url": url::Url::from_file_path(source).unwrap().to_string(),
        "size": bytes.len(),
        "digest": format!("sha256:{}", hex::encode(Sha256::digest(&bytes))),
        "action": action,
    })
}

fn remote_item(key: &str, url: &str, bytes: &[u8], action: Value) -> Value {
    json!({
        "key": key,
        "url": url,
        "size": bytes.len(),
        "digest": format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
        "action": action,
    })
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn make_tar(path: &Path, entries: &[(&str, &[u8], u32)]) {
    let file = File::create(path).unwrap();
    let mut archive = tar::Builder::new(file);
    for (name, contents, mode) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_path(name).unwrap();
        header.set_size(contents.len() as u64);
        header.set_mode(*mode);
        header.set_cksum();
        archive.append(&header, *contents).unwrap();
    }
    archive.finish().unwrap();
}

#[test]
fn cli_help_is_english() {
    let output = Command::new(cargo_bin("imos"))
        .arg("--help")
        .output()
        .unwrap();
    assert_success(&output);
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("Deterministic artifact download"));
    assert!(help.contains("Submit an immutable plan"));
    assert!(
        !help
            .chars()
            .any(|character| ('\u{4e00}'..='\u{9fff}').contains(&character))
    );
}

#[test]
fn creates_all_action_types_and_normalizes_modes() {
    let fixture = Fixture::new();
    let plain = fixture.path("plain.txt");
    let binary = fixture.path("tool");
    let archive = fixture.path("package.tar");
    std::fs::write(&plain, b"plain contents\n").unwrap();
    std::fs::write(&binary, b"#!/bin/sh\necho tool\n").unwrap();
    make_tar(
        &archive,
        &[
            ("package/docs/readme.txt", b"readme\n", 0o600),
            ("package/lib/runner", b"runner\n", 0o700),
        ],
    );

    let plan = fixture.path("plan.json");
    write_plan(
        &plan,
        "all-actions-v1",
        vec![
            item(
                "plain-v1",
                &plain,
                json!({"type": "install_file", "to": "share/plain.txt"}),
            ),
            item(
                "binary-v1",
                &binary,
                json!({"type": "install_bin", "name": "tool"}),
            ),
            item(
                "archive-v1",
                &archive,
                json!({
                    "type": "unpack_dir",
                    "kind": "tar",
                    "strip": 1,
                    "to": "package"
                }),
            ),
            item(
                "archive-v1",
                &archive,
                json!({
                    "type": "unpack_file",
                    "kind": "tar",
                    "from": "package/docs/readme.txt",
                    "to": "selected.txt"
                }),
            ),
        ],
    );

    let first = fixture.create(&plan);
    assert_success(&first);
    let root = PathBuf::from(String::from_utf8_lossy(&first.stdout).trim());
    assert_eq!(
        std::fs::read(root.join("share/plain.txt")).unwrap(),
        b"plain contents\n"
    );
    assert_eq!(
        std::fs::read(root.join("bin/tool")).unwrap(),
        b"#!/bin/sh\necho tool\n"
    );
    assert_eq!(
        std::fs::read(root.join("package/docs/readme.txt")).unwrap(),
        b"readme\n"
    );
    assert_eq!(
        std::fs::read(root.join("package/lib/runner")).unwrap(),
        b"runner\n"
    );
    assert_eq!(
        std::fs::read(root.join("selected.txt")).unwrap(),
        b"readme\n"
    );
    assert_eq!(
        std::fs::metadata(root.join("share/plain.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    assert_eq!(
        std::fs::metadata(root.join("bin/tool"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::metadata(root.join("package/lib/runner"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );

    let second = fixture.create(&plan);
    assert_success(&second);
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn supports_gzip_and_zstd_archives() {
    let fixture = Fixture::new();
    let tar_path = fixture.path("source.tar");
    make_tar(&tar_path, &[("data.txt", b"compressed\n", 0o644)]);
    let tar_bytes = std::fs::read(&tar_path).unwrap();

    let gzip_path = fixture.path("source.tar.gz");
    let mut gzip = GzEncoder::new(File::create(&gzip_path).unwrap(), Compression::default());
    gzip.write_all(&tar_bytes).unwrap();
    gzip.finish().unwrap();

    let zstd_path = fixture.path("source.tar.zst");
    std::fs::write(
        &zstd_path,
        zstd::stream::encode_all(&tar_bytes[..], 1).unwrap(),
    )
    .unwrap();

    let plan = fixture.path("compressed-plan.json");
    write_plan(
        &plan,
        "compressed-v1",
        vec![
            item(
                "gzip-v1",
                &gzip_path,
                json!({
                    "type": "unpack_file",
                    "kind": "tar_gzip",
                    "from": "data.txt",
                    "to": "gzip.txt"
                }),
            ),
            item(
                "zstd-v1",
                &zstd_path,
                json!({
                    "type": "unpack_file",
                    "kind": "tar_zstd",
                    "from": "data.txt",
                    "to": "zstd.txt"
                }),
            ),
        ],
    );

    let output = fixture.create(&plan);
    assert_success(&output);
    let root = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
    assert_eq!(
        std::fs::read(root.join("gzip.txt")).unwrap(),
        b"compressed\n"
    );
    assert_eq!(
        std::fs::read(root.join("zstd.txt")).unwrap(),
        b"compressed\n"
    );
}

#[test]
fn keeps_shared_intent_until_the_last_request_disappears() {
    let fixture = Fixture::new();
    let source = fixture.path("source.txt");
    std::fs::write(&source, b"shared\n").unwrap();
    let plan_one = fixture.path("one.json");
    let plan_two = fixture.path("two.json");
    let items = vec![item(
        "shared-download-v1",
        &source,
        json!({"type": "install_file", "to": "shared.txt"}),
    )];
    write_plan(&plan_one, "shared-plan-v1", items.clone());
    write_plan(&plan_two, "shared-plan-v1", items);

    let first = fixture.create(&plan_one);
    let second = fixture.create(&plan_two);
    assert_success(&first);
    assert_success(&second);
    let installation = PathBuf::from(String::from_utf8(first.stdout).unwrap().trim());

    let removed = fixture
        .command()
        .arg("remove")
        .arg(&plan_one)
        .output()
        .unwrap();
    assert_success(&removed);
    let collected = fixture.command().arg("gc").output().unwrap();
    assert_success(&collected);
    assert!(installation.exists());

    std::fs::remove_file(&plan_two).unwrap();
    let collected = fixture.command().arg("gc").output().unwrap();
    assert_success(&collected);
    assert!(!installation.exists());
    assert!(
        String::from_utf8(collected.stdout)
            .unwrap()
            .contains("request=1")
    );
}

#[test]
fn digest_mismatch_does_not_publish_a_download() {
    let fixture = Fixture::new();
    let source = fixture.path("source.txt");
    std::fs::write(&source, b"wrong digest\n").unwrap();
    let plan = fixture.path("bad-digest.json");
    write_plan(
        &plan,
        "bad-digest-plan",
        vec![json!({
            "key": "bad-digest-download",
            "url": url::Url::from_file_path(&source).unwrap().to_string(),
            "digest": format!("sha256:{}", "0".repeat(64)),
            "action": {"type": "install_file", "to": "data.txt"}
        })],
    );

    let output = fixture.create(&plan);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("digest mismatch")
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("install"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn rejects_link_entries_in_archives() {
    let fixture = Fixture::new();
    let archive_path = fixture.path("link.tar");
    let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
    let mut header = tar::Header::new_gnu();
    header.set_path("escape").unwrap();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_link_name("../outside").unwrap();
    header.set_size(0);
    header.set_mode(0o777);
    header.set_cksum();
    archive.append(&header, std::io::empty()).unwrap();
    archive.finish().unwrap();

    let plan = fixture.path("link-plan.json");
    write_plan(
        &plan,
        "link-plan-v1",
        vec![item(
            "link-archive-v1",
            &archive_path,
            json!({"type": "unpack_dir", "kind": "tar", "to": "."}),
        )],
    );

    let output = fixture.create(&plan);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unsupported entry type")
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("install"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn rejects_mutable_or_multi_link_plan_files() {
    let fixture = Fixture::new();
    let source = fixture.path("source.txt");
    std::fs::write(&source, b"data\n").unwrap();
    let plan = fixture.path("plan.json");
    write_plan(
        &plan,
        "multi-link-plan",
        vec![item(
            "multi-link-download",
            &source,
            json!({"type": "install_file", "to": "data.txt"}),
        )],
    );
    std::fs::hard_link(&plan, fixture.path("another-link.json")).unwrap();

    let output = fixture.create(&plan);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("exactly one link")
    );
}

#[test]
fn store_is_private() {
    let fixture = Fixture::new();
    let output = fixture.command().arg("gc").output().unwrap();
    assert_success(&output);
    let mode = std::fs::metadata(&fixture.store)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o700);
    std::fs::set_permissions(&fixture.store, Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn concurrent_create_elects_one_http_downloader() {
    let fixture = Fixture::new();
    let body = vec![b'x'; 2 * 1024 * 1024];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let mut received = Vec::new();
        loop {
            let count = stream.read(&mut request).unwrap();
            if count == 0 {
                break;
            }
            received.extend_from_slice(&request[..count]);
            if received.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response_body.len()
        )
        .unwrap();
        for chunk in response_body.chunks(64 * 1024) {
            stream.write_all(chunk).unwrap();
            stream.flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    let plan = fixture.path("concurrent.json");
    write_plan(
        &plan,
        "concurrent-plan-v1",
        vec![remote_item(
            "concurrent-download-v1",
            &format!("http://{address}/artifact"),
            &body,
            json!({"type": "install_file", "to": "artifact.bin"}),
        )],
    );

    let mut first = fixture.command();
    first
        .arg("create")
        .arg(&plan)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let first = first.spawn().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let mut second = fixture.command();
    second
        .arg("create")
        .arg(&plan)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let second = second.spawn().unwrap();

    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_success(&first);
    assert_success(&second);
    assert_eq!(first.stdout, second.stdout);
    assert!(String::from_utf8_lossy(&second.stderr).contains("\"event\":\"started\""));
    server.join().unwrap();
}

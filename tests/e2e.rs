use std::fs::{File, Permissions};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;

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
            "name": key,
            "key": key,
            "items": items,
        },
        "upstream": { "test": true }
    });
    std::fs::write(path, serde_json::to_vec_pretty(&plan).unwrap()).unwrap();
}

fn item(key: &str, source: &Path, action: Value) -> Value {
    let bytes = std::fs::read(source).unwrap();
    let mut kind = normalized_kind(action);
    kind.insert(
        "url".into(),
        url::Url::from_file_path(source).unwrap().to_string().into(),
    );
    kind.insert("size".into(), bytes.len().into());
    kind.insert(
        "digest".into(),
        format!("sha256:{}", hex::encode(Sha256::digest(&bytes))).into(),
    );
    json!({
        "name": key,
        "key": key,
        "kind": kind,
    })
}

fn remote_item(key: &str, url: &str, bytes: &[u8], action: Value) -> Value {
    let mut kind = normalized_kind(action);
    kind.insert("url".into(), url.into());
    kind.insert("size".into(), bytes.len().into());
    kind.insert(
        "digest".into(),
        format!("sha256:{}", hex::encode(Sha256::digest(bytes))).into(),
    );
    json!({
        "name": key,
        "key": key,
        "kind": kind,
    })
}

fn normalized_kind(action: Value) -> serde_json::Map<String, Value> {
    let mut kind = action.as_object().unwrap().clone();
    let item_type = match kind["type"].as_str().unwrap() {
        "unpack_dir" => "UnpackDir",
        "unpack_file" => "UnpackFile",
        "install_file" => "InstallFile",
        "install_bin" => "InstallBin",
        other => panic!("unknown test item type: {other}"),
    };
    kind.insert("type".into(), item_type.into());
    if let Some(archive) = kind.remove("kind") {
        let archive = match archive.as_str().unwrap() {
            "tar" => "Tar",
            "tar_gzip" => "TarGzip",
            "tar_zstd" => "TarZstd",
            other => panic!("unknown test archive kind: {other}"),
        };
        kind.insert("archive".into(), archive.into());
    }
    kind
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
    assert!(help.contains("serve"));
    assert!(
        !help
            .chars()
            .any(|character| ('\u{4e00}'..='\u{9fff}').contains(&character))
    );
}

#[test]
fn rejects_the_old_serv_command_name() {
    let output = Command::new(cargo_bin("imos"))
        .arg("serv")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unrecognized subcommand 'serv'")
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
            "name": "bad digest download",
            "key": "bad-digest-download",
            "kind": {
                "type": "InstallFile",
                "url": url::Url::from_file_path(&source).unwrap().to_string(),
                "digest": format!("sha256:{}", "0".repeat(64)),
                "to": "data.txt"
            }
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
fn size_mismatch_does_not_publish_a_download() {
    let fixture = Fixture::new();
    let source = fixture.path("source.txt");
    let contents = b"wrong size\n";
    std::fs::write(&source, contents).unwrap();
    let plan = fixture.path("bad-size.json");
    write_plan(
        &plan,
        "bad-size-plan",
        vec![json!({
            "name": "bad size download",
            "key": "bad-size-download",
            "kind": {
                "type": "InstallFile",
                "url": url::Url::from_file_path(&source).unwrap().to_string(),
                "size": contents.len() + 1,
                "to": "data.txt"
            }
        })],
    );

    let output = fixture.create(&plan);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("size mismatch")
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
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
fn rejects_parent_paths_in_archive_headers() {
    let fixture = Fixture::new();
    let archive_path = fixture.path("parent-path.tar");
    let mut archive = tar::Builder::new(File::create(&archive_path).unwrap());
    let contents = b"must not escape\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("safe-name").unwrap();
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    let malicious_path = b"../outside";
    header.as_mut_bytes()[..100].fill(0);
    header.as_mut_bytes()[..malicious_path.len()].copy_from_slice(malicious_path);
    header.set_cksum();
    archive.append(&header, &contents[..]).unwrap();
    archive.finish().unwrap();

    let plan = fixture.path("parent-path-plan.json");
    write_plan(
        &plan,
        "parent-path-plan-v1",
        vec![item(
            "parent-path-archive-v1",
            &archive_path,
            json!({"type": "unpack_dir", "kind": "tar", "to": "."}),
        )],
    );

    let output = fixture.create(&plan);
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unsafe path")
    );
    assert!(!fixture.root.path().join("outside").exists());
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

#[test]
fn downloads_distinct_plan_items_concurrently() {
    let fixture = Fixture::new();
    let body = vec![b'p'; 64 * 1024];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = std::thread::spawn(move || {
        let read_request = |stream: &mut std::net::TcpStream| {
            let mut buffer = [0_u8; 4096];
            let mut received = Vec::new();
            loop {
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    break;
                }
                received.extend_from_slice(&buffer[..count]);
                if received.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
        };
        let write_response = |stream: &mut std::net::TcpStream| {
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            )
            .unwrap();
            stream.write_all(&response_body).unwrap();
        };

        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut second = loop {
            match listener.accept() {
                Ok((stream, _)) => break Some(stream),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if std::time::Instant::now() >= deadline {
                        break None;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("accept second download: {error}"),
            }
        };
        let Some(mut second) = second.take() else {
            write_response(&mut first);
            return false;
        };
        read_request(&mut second);
        write_response(&mut first);
        write_response(&mut second);
        true
    });

    let plan = fixture.path("parallel-downloads.json");
    write_plan(
        &plan,
        "parallel-plan-v1",
        vec![
            remote_item(
                "parallel-download-one",
                &format!("http://{address}/one"),
                &body,
                json!({"type": "install_file", "to": "one.bin"}),
            ),
            remote_item(
                "parallel-download-two",
                &format!("http://{address}/two"),
                &body,
                json!({"type": "install_file", "to": "two.bin"}),
            ),
        ],
    );

    let output = fixture.create(&plan);
    let ran_concurrently = server.join().unwrap();
    assert!(
        ran_concurrently,
        "second download did not start concurrently"
    );
    assert_success(&output);
    let root = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
    assert_eq!(std::fs::read(root.join("one.bin")).unwrap(), body);
    assert_eq!(std::fs::read(root.join("two.bin")).unwrap(), body);
}

#[test]
fn different_plans_share_one_download_object() {
    let fixture = Fixture::new();
    let source = fixture.path("shared-source.txt");
    std::fs::write(&source, b"shared download\n").unwrap();
    let first_plan = fixture.path("first-plan.json");
    let second_plan = fixture.path("second-plan.json");
    write_plan(
        &first_plan,
        "first-plan-v1",
        vec![item(
            "one-shared-download-v1",
            &source,
            json!({"type": "install_file", "to": "first.txt"}),
        )],
    );
    write_plan(
        &second_plan,
        "second-plan-v1",
        vec![item(
            "one-shared-download-v1",
            &source,
            json!({"type": "install_file", "to": "second.txt"}),
        )],
    );

    assert_success(&fixture.create(&first_plan));
    assert_success(&fixture.create(&second_plan));
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
        1
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("install"))
            .unwrap()
            .count(),
        2
    );

    assert_success(
        &fixture
            .command()
            .arg("remove")
            .arg(&first_plan)
            .output()
            .unwrap(),
    );
    assert_success(&fixture.command().arg("gc").output().unwrap());
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
        1
    );
    assert_eq!(
        std::fs::read_dir(fixture.store.join("install"))
            .unwrap()
            .count(),
        1
    );

    assert_success(
        &fixture
            .command()
            .arg("remove")
            .arg(&second_plan)
            .output()
            .unwrap(),
    );
    assert_success(&fixture.command().arg("gc").output().unwrap());
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
        0
    );
}

#[test]
fn recovers_after_the_first_downloader_is_killed() {
    let fixture = Fixture::new();
    let body = vec![b'z'; 4 * 1024 * 1024];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        for attempt in 0..2 {
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
            if attempt == 0 {
                started_tx.send(()).unwrap();
            }
            for chunk in response_body.chunks(64 * 1024) {
                if stream.write_all(chunk).is_err() || stream.flush().is_err() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(3));
            }
        }
    });

    let plan = fixture.path("recover.json");
    write_plan(
        &plan,
        "recover-plan-v1",
        vec![remote_item(
            "recover-download-v1",
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
    let mut first = first.spawn().unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    first.kill().unwrap();
    let first = first.wait_with_output().unwrap();
    assert!(!first.status.success());
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

    let recovered = fixture.create(&plan);
    assert_success(&recovered);
    let installed_root = PathBuf::from(String::from_utf8_lossy(&recovered.stdout).trim());
    assert_eq!(
        std::fs::metadata(installed_root.join("artifact.bin"))
            .unwrap()
            .len(),
        body.len() as u64
    );
    server.join().unwrap();

    assert_success(&fixture.command().arg("gc").output().unwrap());
    assert_eq!(
        std::fs::read_dir(fixture.store.join("tmp"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn serve_runs_concurrent_requests_and_reuses_one_download() {
    let fixture = Fixture::new();
    let body = vec![b's'; 2 * 1024 * 1024];
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
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    });

    let plan = fixture.path("serve-concurrent.json");
    write_plan(
        &plan,
        "serve-concurrent-plan-v1",
        vec![remote_item(
            "serve-concurrent-download-v1",
            &format!("http://{address}/artifact"),
            &body,
            json!({"type": "install_file", "to": "artifact.bin"}),
        )],
    );

    let mut child = fixture
        .command()
        .arg("serve")
        .arg("-e")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, "{}", json!({"id": "first", "plan_file": plan})).unwrap();
    writeln!(input, "{}", json!({"id": "second", "plan_file": plan})).unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);
    server.join().unwrap();

    let completions = parse_jsonl(&output.stdout);
    let events = parse_jsonl(&output.stderr);
    for id in ["first", "second"] {
        assert_eq!(
            completions
                .iter()
                .filter(|event| event["id"] == id && event["type"] == "result")
                .count(),
            1
        );
        assert!(events.iter().any(|event| {
            event["id"] == id && event["type"] == "progress" && event["stage"] == "download"
        }));
    }
    assert_eq!(
        std::fs::read_dir(fixture.store.join("dl")).unwrap().count(),
        1
    );
}

#[test]
fn serve_recovers_from_bad_lines_and_rejects_duplicate_ids() {
    let fixture = Fixture::new();
    let source = fixture.path("serve-source.txt");
    std::fs::write(&source, b"serve data\n").unwrap();
    let plan = fixture.path("serve-plan.json");
    write_plan(
        &plan,
        "serve-plan-v1",
        vec![item(
            "serve-download-v1",
            &source,
            json!({"type": "install_file", "to": "data.txt"}),
        )],
    );

    let mut child = fixture
        .command()
        .arg("serve")
        .arg("-e")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, "not json").unwrap();
    writeln!(input, r#"{{"id":"bad-shape","plan_file":1}}"#).unwrap();
    writeln!(input, "{}", json!({"id": "same", "plan_file": plan})).unwrap();
    writeln!(input, "{}", json!({"id": "same", "plan_file": plan})).unwrap();
    writeln!(input, "{}", json!({"id": "after", "plan_file": plan})).unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);

    let completions = parse_jsonl(&output.stdout);
    let events = parse_jsonl(&output.stderr);
    assert!(
        events
            .iter()
            .any(|event| event["id"].is_null() && event["type"] == "error")
    );
    assert!(
        events
            .iter()
            .any(|event| event["id"] == "bad-shape" && event["type"] == "error")
    );
    assert_eq!(
        completions
            .iter()
            .filter(|event| event["id"] == "same" && event["type"] == "result")
            .count(),
        1
    );
    assert!(events.iter().any(|event| {
        event["id"] == "same"
            && event["type"] == "error"
            && event["message"] == "request id is already in flight"
    }));
    assert!(
        completions
            .iter()
            .any(|event| event["id"] == "after" && event["type"] == "result")
    );
}

#[test]
fn serve_aborts_on_protocol_errors_without_event_mode() {
    let fixture = Fixture::new();
    let source = fixture.path("protocol-source.txt");
    std::fs::write(&source, b"must not install\n").unwrap();
    let plan = fixture.path("protocol-plan.json");
    write_plan(
        &plan,
        "protocol-plan-v1",
        vec![item(
            "protocol-download-v1",
            &source,
            json!({"type": "install_file", "to": "data.txt"}),
        )],
    );

    let mut child = fixture
        .command()
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, "not json").unwrap();
    writeln!(input, "{}", json!({"id": "after", "plan_file": plan})).unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());

    let events = parse_jsonl(&output.stdout);
    assert_eq!(events.len(), 1);
    assert!(events[0]["id"].is_null());
    assert_eq!(events[0]["type"], "error");
    assert!(
        fixture
            .store
            .join("install")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn serve_returns_expected_operation_failures_on_stdout_and_continues() {
    let fixture = Fixture::new();
    let source = fixture.path("failure-source.txt");
    std::fs::write(&source, b"not an archive\n").unwrap();

    let missing_plan = fixture.path("missing-plan.json");
    write_plan(
        &missing_plan,
        "missing-plan-v1",
        vec![json!({
            "name": "missing download",
            "key": "missing-download-v1",
            "kind": {
                "type": "InstallFile",
                "url": url::Url::from_file_path(fixture.path("missing.bin")).unwrap().to_string(),
                "to": "missing.bin"
            }
        })],
    );
    let digest_plan = fixture.path("digest-plan.json");
    write_plan(
        &digest_plan,
        "digest-failure-plan-v1",
        vec![json!({
            "name": "digest failure download",
            "key": "digest-failure-download-v1",
            "kind": {
                "type": "InstallFile",
                "url": url::Url::from_file_path(&source).unwrap().to_string(),
                "digest": format!("sha256:{}", "0".repeat(64)),
                "to": "data.txt"
            }
        })],
    );
    let unpack_plan = fixture.path("unpack-plan.json");
    write_plan(
        &unpack_plan,
        "unpack-failure-plan-v1",
        vec![item(
            "unpack-failure-download-v1",
            &source,
            json!({"type": "unpack_dir", "kind": "tar", "to": "."}),
        )],
    );
    let success_plan = fixture.path("success-after-failures.json");
    write_plan(
        &success_plan,
        "success-after-failures-plan-v1",
        vec![item(
            "success-after-failures-download-v1",
            &source,
            json!({"type": "install_file", "to": "data.txt"}),
        )],
    );

    let mut child = fixture
        .command()
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    for (id, plan) in [
        ("missing", missing_plan),
        ("digest", digest_plan),
        ("unpack", unpack_plan),
        ("success", success_plan),
    ] {
        writeln!(input, "{}", json!({"id": id, "plan_file": plan})).unwrap();
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);
    assert!(output.stderr.is_empty());

    let events = parse_jsonl(&output.stdout);
    assert!(
        events
            .iter()
            .all(|event| event["type"] == "result" || event["type"] == "error")
    );
    for id in ["missing", "digest", "unpack"] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event["id"] == id && event["type"] == "error")
                .count(),
            1,
            "missing error terminal for {id}: {events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event["id"] == id && (event["type"] == "result" || event["type"] == "error")
                })
                .count(),
            1,
            "expected exactly one terminal for {id}: {events:?}"
        );
    }
    assert!(events.iter().any(|event| {
        event["id"] == "missing"
            && event["type"] == "error"
            && event["message"]
                .as_str()
                .unwrap()
                .contains("open download source")
    }));
    assert!(events.iter().any(|event| {
        event["id"] == "digest"
            && event["type"] == "error"
            && event["message"]
                .as_str()
                .unwrap()
                .contains("digest mismatch")
    }));
    assert!(
        events
            .iter()
            .any(|event| event["id"] == "unpack" && event["type"] == "error")
    );
    assert!(
        events
            .iter()
            .any(|event| event["id"] == "success" && event["type"] == "result")
    );
}

#[test]
fn serve_routes_startup_errors_by_mode() {
    let fixture = Fixture::new();
    let unusable_store = fixture.path("not-a-directory");
    std::fs::write(&unusable_store, b"file").unwrap();
    let output = Command::new(cargo_bin("imos"))
        .arg("--store")
        .arg(&unusable_store)
        .arg("serve")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let events = parse_jsonl(&output.stdout);
    assert_eq!(events.len(), 1);
    assert!(events[0]["id"].is_null());
    assert_eq!(events[0]["type"], "error");

    let output = Command::new(cargo_bin("imos"))
        .arg("--store")
        .arg(&unusable_store)
        .arg("serve")
        .arg("-e")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let events = parse_jsonl(&output.stderr);
    assert_eq!(events.len(), 1);
    assert!(events[0]["id"].is_null());
    assert_eq!(events[0]["type"], "error");
}

#[test]
fn serve_stops_when_stdout_is_closed() {
    let fixture = Fixture::new();
    let source = fixture.path("closed-output-source.txt");
    std::fs::write(&source, b"closed output\n").unwrap();
    let plan = fixture.path("closed-output-plan.json");
    write_plan(
        &plan,
        "closed-output-plan-v1",
        vec![item(
            "closed-output-download-v1",
            &source,
            json!({"type": "install_file", "to": "data.txt"}),
        )],
    );

    let mut child = fixture
        .command()
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let mut input = child.stdin.take().unwrap();
    writeln!(input, "{}", json!({"id": "closed", "plan_file": plan})).unwrap();
    input.flush().unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("serve did not stop after stdout was closed");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn parse_jsonl(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

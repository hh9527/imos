use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use flate2::read::GzDecoder;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::plan::{ArchiveKind, Item, Plan, PlanItem, validate_relative_path};
use crate::progress::ProgressLock;

pub fn verify_download(object: &Path, item: &Item) -> Result<PathBuf> {
    let stored_key = std::fs::read_to_string(object.join("key"))
        .with_context(|| format!("read download object key: {}", object.display()))?;
    ensure!(stored_key == item.key, "download key hash collision");
    let data = object.join("data");
    let metadata = std::fs::metadata(&data)
        .with_context(|| format!("read download object: {}", data.display()))?;
    ensure!(metadata.is_file(), "download object is not a regular file");
    if let Some(expected) = item.size {
        ensure!(
            metadata.len() == expected,
            "download key {} size conflict: expected {expected}, got {}",
            item.key,
            metadata.len()
        );
    }
    if let Some(expected) = &item.digest {
        let actual = digest_file(&data)?;
        ensure!(
            &actual == expected,
            "download key {} digest conflict: expected {expected}, got {actual}",
            item.key
        );
    }
    Ok(data)
}

pub fn download_to(item: &Item, destination: &Path, progress: &mut ProgressLock) -> Result<()> {
    let url = url::Url::parse(&item.url)?;
    let mut reader: Box<dyn Read> = match url.scheme() {
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("invalid file URL: {}", item.url))?;
            Box::new(BufReader::new(File::open(&path).with_context(|| {
                format!("open download source {}", path.display())
            })?))
        }
        "http" | "https" => {
            let response = reqwest::blocking::Client::builder()
                .build()?
                .get(url)
                .send()
                .with_context(|| format!("download {}", item.url))?
                .error_for_status()
                .with_context(|| format!("download {}", item.url))?;
            Box::new(response)
        }
        scheme => bail!("unsupported download URL scheme: {scheme}"),
    };

    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .with_context(|| format!("create temporary download {}", destination.display()))?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut next_progress = 1024 * 1024;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count])?;
        hasher.update(&buffer[..count]);
        total += count as u64;
        if total >= next_progress {
            progress.event(&json!({
                "event": "download",
                "key": item.key,
                "current": total,
                "total": item.size,
            }))?;
            next_progress = total.saturating_add(1024 * 1024);
        }
    }
    output.sync_all()?;

    if let Some(expected) = item.size {
        ensure!(
            total == expected,
            "download key {} size mismatch: expected {expected}, got {total}",
            item.key
        );
    }
    let actual_digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    if let Some(expected) = &item.digest {
        ensure!(
            &actual_digest == expected,
            "download key {} digest mismatch: expected {expected}, got {actual_digest}",
            item.key
        );
    }
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o444))?;
    progress.event(&json!({
        "event": "downloaded",
        "key": item.key,
        "size": total,
        "digest": actual_digest,
    }))?;
    Ok(())
}

pub fn execute_plan(plan: &Plan, downloads: &[PathBuf], root: &Path) -> Result<()> {
    ensure!(
        plan.items.len() == downloads.len(),
        "plan download count does not match item count"
    );
    std::fs::create_dir_all(root)?;
    set_mode(root, 0o755)?;
    for (item, data) in plan.items.iter().zip(downloads) {
        execute_item(item, data, root)
            .with_context(|| format!("execute plan item {}", item.key))?;
    }
    normalize_directories(root)?;
    Ok(())
}

fn execute_item(item: &Item, data: &Path, root: &Path) -> Result<()> {
    match &item.action {
        PlanItem::InstallFile { to } => copy_new(data, &root.join(to), 0o644),
        PlanItem::InstallBin { name } => copy_new(data, &root.join("bin").join(name), 0o755),
        PlanItem::UnpackDir { kind, strip, to } => {
            let destination = if to == Path::new(".") {
                root.to_path_buf()
            } else {
                root.join(to)
            };
            unpack_dir(data, *kind, *strip, &destination)
        }
        PlanItem::UnpackFile { kind, from, to } => unpack_file(data, *kind, from, &root.join(to)),
    }
}

fn archive_reader(path: &Path, kind: ArchiveKind) -> Result<Box<dyn Read>> {
    let file = File::open(path)?;
    Ok(match kind {
        ArchiveKind::Tar => Box::new(BufReader::new(file)),
        ArchiveKind::TarGzip => Box::new(GzDecoder::new(BufReader::new(file))),
        ArchiveKind::TarZstd => Box::new(zstd::stream::read::Decoder::new(BufReader::new(file))?),
    })
}

fn unpack_dir(data: &Path, kind: ArchiveKind, strip: u32, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let reader = archive_reader(data, kind)?;
    let mut archive = tar::Archive::new(reader);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let original = entry.path()?.into_owned();
        validate_archive_path(&original)?;
        validate_entry_type(entry.header().entry_type(), &original)?;
        let Some(relative) = strip_components(&original, strip as usize) else {
            continue;
        };
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(&relative);
        if entry.header().entry_type().is_dir() {
            create_directory(&target)?;
        } else {
            let mode = normalized_archive_mode(entry.header().mode().unwrap_or(0));
            write_entry(&mut entry, &target, mode)?;
        }
    }
    Ok(())
}

fn unpack_file(data: &Path, kind: ArchiveKind, source: &Path, destination: &Path) -> Result<()> {
    let reader = archive_reader(data, kind)?;
    let mut archive = tar::Archive::new(reader);
    let mut found = false;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        validate_archive_path(&path)?;
        validate_entry_type(entry.header().entry_type(), &path)?;
        if path != source {
            continue;
        }
        ensure!(
            !found,
            "archive contains duplicate path: {}",
            source.display()
        );
        ensure!(
            entry.header().entry_type().is_file(),
            "archive entry is not a regular file"
        );
        let mode = normalized_archive_mode(entry.header().mode().unwrap_or(0));
        write_entry(&mut entry, destination, mode)?;
        found = true;
    }
    ensure!(found, "archive does not contain file: {}", source.display());
    Ok(())
}

fn validate_archive_path(path: &Path) -> Result<()> {
    validate_relative_path(path, false)
        .with_context(|| format!("archive contains an unsafe path: {}", path.display()))
}

fn validate_entry_type(entry_type: tar::EntryType, path: &Path) -> Result<()> {
    ensure!(
        entry_type.is_dir() || entry_type.is_file(),
        "archive contains an unsupported entry type: {}",
        path.display()
    );
    Ok(())
}

fn strip_components(path: &Path, count: usize) -> Option<PathBuf> {
    let components = path.components().collect::<Vec<_>>();
    if components.len() <= count {
        return None;
    }
    let mut result = PathBuf::new();
    for component in &components[count..] {
        if let Component::Normal(part) = component {
            result.push(part);
        }
    }
    Some(result)
}

fn copy_new(source: &Path, destination: &Path, mode: u32) -> Result<()> {
    let mut input = File::open(source)?;
    write_reader(&mut input, destination, mode)
}

fn write_entry<R: Read>(entry: &mut R, destination: &Path, mode: u32) -> Result<()> {
    write_reader(entry, destination, mode)
}

fn write_reader<R: Read>(reader: &mut R, destination: &Path, mode: u32) -> Result<()> {
    let parent = destination
        .parent()
        .context("installation target has no parent directory")?;
    std::fs::create_dir_all(parent)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .with_context(|| format!("installation target conflict: {}", destination.display()))?;
    std::io::copy(reader, &mut output)?;
    output.sync_all()?;
    set_mode(destination, mode)
}

fn create_directory(path: &Path) -> Result<()> {
    if path.exists() {
        ensure!(
            path.is_dir(),
            "installation target type conflict: {}",
            path.display()
        );
    } else {
        std::fs::create_dir_all(path)?;
    }
    set_mode(path, 0o755)
}

fn normalize_directories(root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            normalize_directories(&entry.path())?;
            set_mode(&entry.path(), 0o755)?;
        }
    }
    set_mode(root, 0o755)
}

fn normalized_archive_mode(mode: u32) -> u32 {
    if mode & 0o111 == 0 { 0o644 } else { 0o755 }
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn digest_file(path: &Path) -> Result<String> {
    let mut input = File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut input, &mut HashWriter(&mut hasher))?;
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

struct HashWriter<'a>(&'a mut Sha256);

impl Write for HashWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

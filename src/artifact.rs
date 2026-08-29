use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail, ensure};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::plan::{ArchiveKind, Item, ItemKind, validate_relative_path};
use crate::progress::{BlockingEventSender, Event};
use crate::status::{StatusType, timestamp};

pub fn verify_download(object: &Path, item: &Item) -> Result<PathBuf> {
    let stored_key = std::fs::read_to_string(object.join("key"))
        .with_context(|| format!("read download object key: {}", object.display()))?;
    ensure!(stored_key == item.key, "download key hash collision");
    let data = object.join("data");
    let metadata = std::fs::metadata(&data)
        .with_context(|| format!("read download object: {}", data.display()))?;
    ensure!(metadata.is_file(), "download object is not a regular file");
    if let Some(expected) = item.size() {
        ensure!(
            metadata.len() == expected,
            "download key {} size conflict: expected {expected}, got {}",
            item.key,
            metadata.len()
        );
    }
    if let Some(expected) = item.digest() {
        let actual = digest_file(&data)?;
        ensure!(
            actual == expected,
            "download key {} digest conflict: expected {expected}, got {actual}",
            item.key
        );
    }
    Ok(data)
}

pub async fn download_to<F, Fut>(item: &Item, destination: &Path, mut progress: F) -> Result<()>
where
    F: FnMut(Event) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let url = url::Url::parse(item.url())?;
    let mut output = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .await
        .with_context(|| format!("create temporary download {}", destination.display()))?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut next_progress = 1024 * 1024;

    match url.scheme() {
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("invalid file URL: {}", item.url()))?;
            let mut input = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("open download source {}", path.display()))?;
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let count = input.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                write_download_chunk(
                    &buffer[..count],
                    &mut output,
                    &mut hasher,
                    &mut total,
                    &mut next_progress,
                    &mut progress,
                    &item.key,
                )
                .await?;
            }
        }
        "http" | "https" => {
            let mut response = reqwest::Client::builder()
                .build()?
                .get(url)
                .send()
                .await
                .with_context(|| format!("download {}", item.url()))?
                .error_for_status()
                .with_context(|| format!("download {}", item.url()))?;
            while let Some(chunk) = response.chunk().await? {
                write_download_chunk(
                    &chunk,
                    &mut output,
                    &mut hasher,
                    &mut total,
                    &mut next_progress,
                    &mut progress,
                    &item.key,
                )
                .await?;
            }
        }
        scheme => bail!("unsupported download URL scheme: {scheme}"),
    }
    output.sync_all().await?;

    if let Some(expected) = item.size() {
        ensure!(
            total == expected,
            "download key {} size mismatch: expected {expected}, got {total}",
            item.key
        );
    }
    let actual_digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    if let Some(expected) = item.digest() {
        ensure!(
            actual_digest == expected,
            "download key {} digest mismatch: expected {expected}, got {actual_digest}",
            item.key
        );
    }
    tokio::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o444)).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_download_chunk<F, Fut>(
    chunk: &[u8],
    output: &mut tokio::fs::File,
    hasher: &mut Sha256,
    total: &mut u64,
    next_progress: &mut u64,
    progress: &mut F,
    key: &str,
) -> Result<()>
where
    F: FnMut(Event) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    output.write_all(chunk).await?;
    hasher.update(chunk);
    *total += chunk.len() as u64;
    if *total >= *next_progress {
        progress(Event::Progressed {
            key: key.to_owned(),
            bytes: *total,
        })
        .await?;
        *next_progress = total.saturating_add(1024 * 1024);
    }
    Ok(())
}

pub fn prepare_install_root(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root)?;
    set_mode(root, 0o755)
}

pub fn execute_item(
    item: &Item,
    data: &Path,
    root: &Path,
    events: BlockingEventSender,
) -> Result<()> {
    execute_item_inner(item, data, root, events)
        .with_context(|| format!("execute plan item {}", item.key))
}

pub fn finalize_install_root(root: &Path) -> Result<()> {
    normalize_directories(root)?;
    Ok(())
}

fn execute_item_inner(
    item: &Item,
    data: &Path,
    root: &Path,
    events: BlockingEventSender,
) -> Result<()> {
    match &item.kind {
        ItemKind::InstallFile { to, .. } => copy_new(data, &root.join(to), 0o644),
        ItemKind::InstallBin { name, .. } => copy_new(data, &root.join("bin").join(name), 0o755),
        ItemKind::UnpackDir {
            archive, strip, to, ..
        } => {
            let destination = if to == Path::new(".") {
                root.to_path_buf()
            } else {
                root.join(to)
            };
            unpack_with_status(item, data, events, |progress| {
                unpack_dir(data, *archive, *strip, &destination, progress)
            })
        }
        ItemKind::UnpackFile {
            archive, from, to, ..
        } => unpack_with_status(item, data, events, |progress| {
            unpack_file(data, *archive, from, &root.join(to), progress)
        }),
    }
}

#[derive(Clone)]
struct UnpackProgress {
    events: BlockingEventSender,
    key: String,
    bytes: Arc<AtomicU64>,
}

struct CountingReader<R> {
    inner: R,
    progress: UnpackProgress,
    next_status: u64,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        let bytes = self
            .progress
            .bytes
            .fetch_add(count as u64, Ordering::Relaxed)
            + count as u64;
        if bytes >= self.next_status {
            self.progress.events.send(Event::Progressed {
                key: self.progress.key.clone(),
                bytes,
            });
            self.next_status = bytes.saturating_add(1024 * 1024);
        }
        Ok(count)
    }
}

fn unpack_with_status(
    item: &Item,
    data: &Path,
    events: BlockingEventSender,
    unpack: impl FnOnce(UnpackProgress) -> Result<()>,
) -> Result<()> {
    let total = std::fs::metadata(data)?.len();
    let progress = UnpackProgress {
        events: events.clone(),
        key: item.key.clone(),
        bytes: Arc::new(AtomicU64::new(0)),
    };
    events.send(Event::AttemptStarted {
        ty: StatusType::Unpack,
        key: item.key.clone(),
        name: item.name.clone(),
        at: timestamp(),
        bytes: Some(0),
        total_bytes: Some(total),
    });
    let result = unpack(progress.clone());
    let bytes = progress.bytes.load(Ordering::Relaxed);
    let event = if result.is_ok() {
        Event::Completed {
            key: item.key.clone(),
            at: timestamp(),
            bytes: Some(bytes),
        }
    } else {
        Event::Failed {
            key: item.key.clone(),
            at: timestamp(),
            bytes: Some(bytes),
        }
    };
    events.send(event);
    result
}

fn archive_reader(
    path: &Path,
    kind: ArchiveKind,
    progress: UnpackProgress,
) -> Result<Box<dyn Read>> {
    let file = File::open(path)?;
    let reader = BufReader::new(CountingReader {
        inner: file,
        progress,
        next_status: 1024 * 1024,
    });
    Ok(match kind {
        ArchiveKind::Tar => Box::new(reader),
        ArchiveKind::TarGzip => Box::new(GzDecoder::new(reader)),
        ArchiveKind::TarZstd => Box::new(zstd::stream::read::Decoder::new(reader)?),
    })
}

fn unpack_dir(
    data: &Path,
    kind: ArchiveKind,
    strip: u32,
    destination: &Path,
    progress: UnpackProgress,
) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let reader = archive_reader(data, kind, progress)?;
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
    std::io::copy(&mut archive.into_inner(), &mut std::io::sink())?;
    Ok(())
}

fn unpack_file(
    data: &Path,
    kind: ArchiveKind,
    source: &Path,
    destination: &Path,
    progress: UnpackProgress,
) -> Result<()> {
    let reader = archive_reader(data, kind, progress)?;
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
    std::io::copy(&mut archive.into_inner(), &mut std::io::sink())?;
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

use std::fs::File;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use fs2::FileExt;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Clone, Default)]
pub struct ProgressSender(Option<mpsc::Sender<Value>>);

impl ProgressSender {
    pub fn new(sender: mpsc::Sender<Value>) -> Self {
        Self(Some(sender))
    }

    pub async fn send(&self, event: Value) {
        if let Some(sender) = &self.0 {
            let _ = sender.send(event).await;
        }
    }
}

pub struct ProgressLock {
    lock: File,
    writer: tokio::fs::File,
    progress: ProgressSender,
}

pub struct FileLock(File);

impl FileLock {
    pub async fn shared(path: &Path) -> Result<Self> {
        Self::acquire(path, false).await
    }

    pub async fn exclusive(path: &Path) -> Result<Self> {
        Self::acquire(path, true).await
    }

    async fn acquire(path: &Path, exclusive: bool) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .await
            .with_context(|| format!("open lock file {}", path.display()))?
            .into_std()
            .await;
        loop {
            let result = if exclusive {
                FileExt::try_lock_exclusive(&file)
            } else {
                FileExt::try_lock_shared(&file)
            };
            match result {
                Ok(()) => return Ok(Self(file)),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("acquire lock {}", path.display()));
                }
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

impl ProgressLock {
    pub async fn acquire(path: &Path, progress: ProgressSender) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .await
            .with_context(|| format!("open lock file {}", path.display()))?;
        let file = file.into_std().await;
        let mut followed = 0_u64;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    progress.send(serde_json::json!({"event": "waiting"})).await;
                    let bytes = tokio::fs::read(path).await?;
                    if (bytes.len() as u64) < followed {
                        followed = 0;
                    }
                    if bytes.len() as u64 > followed {
                        let new = &bytes[followed as usize..];
                        let complete = new
                            .iter()
                            .rposition(|byte| *byte == b'\n')
                            .map_or(0, |position| position + 1);
                        for line in new[..complete].split(|byte| *byte == b'\n') {
                            if !line.is_empty()
                                && let Ok(event) = serde_json::from_slice(line)
                            {
                                progress.send(event).await;
                            }
                        }
                        followed += complete as u64;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("acquire lock {}", path.display()));
                }
            }
        }
        let writer_file = file.try_clone()?;
        let mut writer = tokio::fs::File::from_std(writer_file);
        writer.set_len(0).await?;
        writer.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(Self {
            lock: file,
            writer,
            progress,
        })
    }

    pub async fn event<T: Serialize>(&mut self, event: &T) -> Result<()> {
        let value = serde_json::to_value(event)?;
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        self.writer.write_all(&line).await?;
        self.writer.flush().await?;
        self.progress.send(value).await;
        Ok(())
    }
}

impl Drop for ProgressLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock);
    }
}

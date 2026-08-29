use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use fs2::FileExt;
use serde_json::Value;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};

use crate::status::{Status, StatusState, StatusType};

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
    reporter: StatusReporter,
    waited: bool,
}

#[derive(Clone)]
pub struct StatusReporter {
    writer: Arc<Mutex<tokio::fs::File>>,
    progress: ProgressSender,
}

#[derive(Clone, Default)]
pub struct BlockingEventSender(Option<Arc<dyn Fn(Event) + Send + Sync>>);

impl BlockingEventSender {
    pub fn from_fn(send: impl Fn(Event) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(send)))
    }

    pub fn send(&self, event: Event) {
        if let Some(send) = &self.0 {
            send(event);
        }
    }
}

#[derive(Default)]
pub struct State {
    statuses: HashMap<String, Status>,
    failed: Option<String>,
}

impl State {
    pub fn take_failure(&mut self) -> Option<String> {
        self.failed.take()
    }

    pub fn observe(&mut self, status: Status) {
        self.statuses.insert(status.key.clone(), status);
    }
}

pub enum Event {
    Waiting {
        ty: StatusType,
        key: String,
        name: String,
        total_bytes: Option<u64>,
    },
    Resumed {
        key: String,
    },
    AttemptStarted {
        ty: StatusType,
        key: String,
        name: String,
        at: String,
        bytes: Option<u64>,
        total_bytes: Option<u64>,
    },
    Progressed {
        key: String,
        bytes: u64,
    },
    Completed {
        key: String,
        at: String,
        bytes: Option<u64>,
    },
    Failed {
        key: String,
        at: String,
        bytes: Option<u64>,
    },
    Cached {
        ty: StatusType,
        key: String,
        name: String,
        at: String,
        bytes: Option<u64>,
        total_bytes: Option<u64>,
    },
    EffectFailed(String),
}

pub enum Effect {
    EmitStatus(Status),
}

impl Event {
    pub fn reduce(self, state: &mut State, effects: &mut Vec<Effect>) {
        let status = match self {
            Self::Waiting {
                ty,
                key,
                name,
                total_bytes,
            } => {
                if let Some(status) = state
                    .statuses
                    .get_mut(&key)
                    .filter(|status| status.ty == ty)
                {
                    status.status = StatusState::Waiting;
                    status.clone()
                } else {
                    let status = Status {
                        ty,
                        key: key.clone(),
                        name,
                        status: StatusState::Waiting,
                        tried: 0,
                        started: None,
                        end: None,
                        bytes: None,
                        total_bytes,
                    };
                    state.statuses.insert(key, status.clone());
                    status
                }
            }
            Self::AttemptStarted {
                ty,
                key,
                name,
                at,
                bytes,
                total_bytes,
            } => {
                let tried = state
                    .statuses
                    .get(&key)
                    .filter(|status| status.ty == ty)
                    .map_or(1, |status| status.tried.saturating_add(1));
                let status = Status {
                    ty,
                    key: key.clone(),
                    name,
                    status: StatusState::Running,
                    tried,
                    started: Some(at),
                    end: None,
                    bytes,
                    total_bytes,
                };
                state.statuses.insert(key, status.clone());
                status
            }
            Self::Resumed { key } => {
                let Some(status) = state.statuses.get_mut(&key) else {
                    state.failed =
                        Some(format!("operation resumed before it started for key {key}"));
                    return;
                };
                status.status = StatusState::Running;
                status.end = None;
                status.clone()
            }
            Self::Progressed { key, bytes } => {
                let Some(status) = state.statuses.get_mut(&key) else {
                    state.failed = Some(format!(
                        "progress received before operation started for key {key}"
                    ));
                    return;
                };
                if matches!(status.status, StatusState::Completed | StatusState::Failed) {
                    return;
                }
                status.status = StatusState::Running;
                status.end = None;
                status.bytes = Some(bytes);
                status.clone()
            }
            Self::Completed { key, at, bytes } => {
                let Some(status) = state.statuses.get_mut(&key) else {
                    state.failed = Some(format!(
                        "operation finished before it started for key {key}"
                    ));
                    return;
                };
                if matches!(status.status, StatusState::Completed | StatusState::Failed) {
                    return;
                }
                status.status = StatusState::Completed;
                status.end = Some(at);
                if bytes.is_some() {
                    status.bytes = bytes;
                }
                status.clone()
            }
            Self::Failed { key, at, bytes } => {
                let Some(status) = state.statuses.get_mut(&key) else {
                    state.failed =
                        Some(format!("operation failed before it started for key {key}"));
                    return;
                };
                if matches!(status.status, StatusState::Completed | StatusState::Failed) {
                    return;
                }
                status.status = StatusState::Failed;
                status.end = Some(at);
                if bytes.is_some() {
                    status.bytes = bytes;
                }
                status.clone()
            }
            Self::Cached {
                ty,
                key,
                name,
                at,
                bytes,
                total_bytes,
            } => {
                let status = Status {
                    ty,
                    key: key.clone(),
                    name,
                    status: StatusState::Completed,
                    tried: 0,
                    started: None,
                    end: Some(at),
                    bytes,
                    total_bytes,
                };
                state.statuses.insert(key, status.clone());
                status
            }
            Self::EffectFailed(message) => {
                state.failed = Some(message);
                return;
            }
        };
        effects.push(Effect::EmitStatus(status));
    }
}

impl Effect {
    pub async fn apply(self, ctx: &StatusReporter) -> Vec<Event> {
        match self {
            Self::EmitStatus(status) => match ctx.emit(&status).await {
                Ok(()) => Vec::new(),
                Err(error) => vec![Event::EffectFailed(error.to_string())],
            },
        }
    }
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
    pub async fn acquire<F, Fut>(
        path: &Path,
        progress: ProgressSender,
        mut observed: F,
    ) -> Result<Self>
    where
        F: FnMut(Status) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
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
        let mut waited = false;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    waited = true;
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
                                && let Ok(status) = serde_json::from_slice::<Status>(line)
                            {
                                observed(status).await?;
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
            reporter: StatusReporter {
                writer: Arc::new(Mutex::new(writer)),
                progress,
            },
            waited,
        })
    }

    pub fn waited(&self) -> bool {
        self.waited
    }

    pub fn reporter(&self) -> StatusReporter {
        self.reporter.clone()
    }
}

impl StatusReporter {
    async fn emit(&self, status: &Status) -> Result<()> {
        let value = status.to_value()?;
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&line).await?;
        writer.flush().await?;
        drop(writer);
        self.progress.send(value).await;
        Ok(())
    }
}

impl Drop for ProgressLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reduce(state: &mut State, event: Event) -> Status {
        let mut effects = Vec::new();
        event.reduce(state, &mut effects);
        assert!(state.failed.is_none());
        assert_eq!(effects.len(), 1);
        let Effect::EmitStatus(status) = effects.pop().unwrap();
        status
    }

    #[test]
    fn reducer_preserves_a_try_across_resource_waiting() {
        let mut state = State::default();
        let running = reduce(
            &mut state,
            Event::AttemptStarted {
                ty: StatusType::Download,
                key: "archive-v1".into(),
                name: "Archive".into(),
                at: "2026-08-29T10:00:00Z".into(),
                bytes: Some(0),
                total_bytes: Some(10),
            },
        );
        assert_eq!(running.status, StatusState::Running);
        assert_eq!(running.tried, 1);

        let progressed = reduce(
            &mut state,
            Event::Progressed {
                key: "archive-v1".into(),
                bytes: 5,
            },
        );
        let waiting = reduce(
            &mut state,
            Event::Waiting {
                ty: StatusType::Download,
                key: "archive-v1".into(),
                name: "Archive".into(),
                total_bytes: Some(10),
            },
        );
        assert_eq!(waiting.status, StatusState::Waiting);
        assert_eq!(waiting.tried, 1);
        assert_eq!(waiting.started, progressed.started);
        assert_eq!(waiting.bytes, Some(5));

        let resumed = reduce(
            &mut state,
            Event::Resumed {
                key: "archive-v1".into(),
            },
        );
        assert_eq!(resumed.status, StatusState::Running);
        assert_eq!(resumed.tried, 1);
        assert_eq!(resumed.bytes, Some(5));
    }

    #[test]
    fn reducer_counts_retries_and_resets_for_a_new_operation_type() {
        let mut state = State::default();
        for tried in 1..=2 {
            let status = reduce(
                &mut state,
                Event::AttemptStarted {
                    ty: StatusType::Download,
                    key: "archive-v1".into(),
                    name: "Archive".into(),
                    at: format!("2026-08-29T10:00:0{tried}Z"),
                    bytes: Some(0),
                    total_bytes: Some(10),
                },
            );
            assert_eq!(status.tried, tried);
        }

        let unpack = reduce(
            &mut state,
            Event::AttemptStarted {
                ty: StatusType::Unpack,
                key: "archive-v1".into(),
                name: "Archive".into(),
                at: "2026-08-29T10:00:03Z".into(),
                bytes: Some(0),
                total_bytes: Some(10),
            },
        );
        assert_eq!(unpack.tried, 1);
        assert_eq!(unpack.ty, StatusType::Unpack);
    }

    #[test]
    fn reducer_ignores_late_progress_after_a_terminal_status() {
        let mut state = State::default();
        reduce(
            &mut state,
            Event::AttemptStarted {
                ty: StatusType::Unpack,
                key: "archive-v1".into(),
                name: "Archive".into(),
                at: "2026-08-29T10:00:00Z".into(),
                bytes: Some(0),
                total_bytes: Some(10),
            },
        );
        let completed = reduce(
            &mut state,
            Event::Completed {
                key: "archive-v1".into(),
                at: "2026-08-29T10:00:01Z".into(),
                bytes: Some(10),
            },
        );

        let mut effects = Vec::new();
        Event::Progressed {
            key: "archive-v1".into(),
            bytes: 9,
        }
        .reduce(&mut state, &mut effects);
        assert!(effects.is_empty());
        assert_eq!(
            state.statuses.get("archive-v1").expect("status"),
            &completed
        );
    }
}

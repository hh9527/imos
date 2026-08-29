use std::collections::{HashMap, HashSet};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use tempfile::Builder;

use crate::artifact::{download_to, execute_plan, verify_download};
use crate::db::IntentDb;
use crate::plan::{Item, Plan, PlanEnvelope};
use crate::progress::{BlockingEventSender, Event, FileLock, ProgressLock, ProgressSender};
use crate::status::{StatusType, timestamp};

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub installs: usize,
    pub downloads: usize,
    pub requests: usize,
    pub temporary: usize,
}

#[derive(Clone)]
struct PlanFileState {
    device: u64,
    inode: u64,
    links: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

struct PreparedCreate {
    state: PlanFileState,
    request_path: PathBuf,
    already_registered: bool,
    plan: Plan,
}

impl Store {
    pub async fn open(root: PathBuf) -> Result<Self> {
        blocking(move || Self::open_blocking(root)).await
    }

    fn open_blocking(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create store root {}", root.display()))?;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        for directory in [
            "requests",
            "dl",
            "install",
            "locks/dl",
            "locks/install",
            "tmp",
        ] {
            std::fs::create_dir_all(root.join(directory))
                .with_context(|| format!("create store directory {directory}"))?;
        }
        let store = Self { root };
        store.db()?;
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(store.gc_lock_path())
            .context("open GC lock")?;
        Ok(store)
    }

    pub async fn create(&self, plan_file: &Path) -> Result<PathBuf> {
        self.create_with_progress(plan_file, ProgressSender::default())
            .await
    }

    pub async fn create_with_progress(
        &self,
        plan_file: &Path,
        progress: ProgressSender,
    ) -> Result<PathBuf> {
        let store = self.clone();
        let plan_file_owned = plan_file.to_path_buf();
        let prepared = blocking(move || store.prepare_create(&plan_file_owned)).await?;
        let _gc_lock = FileLock::shared(&self.gc_lock_path()).await?;
        let result = self
            .ensure_install(&prepared.plan, progress.clone())
            .await?;

        let store = self.clone();
        let plan_file = plan_file.to_path_buf();
        blocking(move || store.register_create(&plan_file, prepared)).await?;
        Ok(result)
    }

    pub async fn remove(&self, plan_file: &Path) -> Result<()> {
        let metadata = tokio::fs::metadata(plan_file)
            .await
            .with_context(|| format!("read plan metadata {}", plan_file.display()))?;
        let request_ino = metadata.ino().to_string();
        let _gc_lock = FileLock::shared(&self.gc_lock_path()).await?;
        let store = self.clone();
        blocking(move || {
            store.db()?.remove_request(&request_ino)?;
            let request_path = store.root.join("requests").join(request_ino);
            if request_path.exists() {
                std::fs::remove_file(request_path)?;
            }
            Ok(())
        })
        .await
    }

    pub async fn gc(&self) -> Result<GcReport> {
        let _gc_lock = FileLock::exclusive(&self.gc_lock_path()).await?;
        let store = self.clone();
        blocking(move || store.gc_locked()).await
    }

    fn prepare_create(&self, plan_file: &Path) -> Result<PreparedCreate> {
        let metadata = std::fs::metadata(plan_file)
            .with_context(|| format!("read plan metadata {}", plan_file.display()))?;
        ensure!(metadata.is_file(), "plan must be a regular file");
        let store_device = std::fs::metadata(&self.root)?.dev();
        ensure!(
            metadata.dev() == store_device,
            "plan file and store must be on the same file system"
        );
        let request_path = self.root.join("requests").join(metadata.ino().to_string());
        let already_registered = request_path.exists();
        if !already_registered {
            ensure!(
                metadata.nlink() == 1,
                "a new plan file must have exactly one link"
            );
        }
        let envelope = PlanEnvelope::read(plan_file)?;
        Ok(PreparedCreate {
            state: PlanFileState {
                device: metadata.dev(),
                inode: metadata.ino(),
                links: metadata.nlink(),
                length: metadata.len(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
            },
            request_path,
            already_registered,
            plan: envelope.imos,
        })
    }

    fn register_create(&self, plan_file: &Path, prepared: PreparedCreate) -> Result<()> {
        let request_ino = prepared.state.inode.to_string();
        if !prepared.already_registered {
            match std::fs::hard_link(plan_file, &prepared.request_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let registered = std::fs::metadata(&prepared.request_path)?;
                    ensure!(
                        registered.ino() == prepared.state.inode,
                        "request inode path is already bound to another file"
                    );
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("register plan inode {request_ino}"));
                }
            }
        }
        let internal = std::fs::metadata(&prepared.request_path)?;
        ensure!(
            internal.dev() == prepared.state.device && internal.ino() == prepared.state.inode,
            "plan file was replaced while being registered"
        );
        ensure!(
            internal.len() == prepared.state.length
                && internal.mtime() == prepared.state.modified_seconds
                && internal.mtime_nsec() == prepared.state.modified_nanoseconds,
            "plan file was modified while create was running"
        );
        let minimum_links = if prepared.already_registered {
            prepared.state.links.max(2)
        } else {
            2
        };
        ensure!(
            internal.nlink() >= minimum_links,
            "upstream plan file was removed"
        );

        let mut db = self.db()?;
        if let Some(existing) = db.request_plan(&request_ino)? {
            ensure!(
                existing == prepared.plan.key,
                "request inode is already bound to another plan key"
            );
        }
        let download_keys = prepared
            .plan
            .download_keys()
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        db.add_request(&request_ino, &prepared.plan.key, &download_keys)
    }

    async fn ensure_install(&self, plan: &Plan, progress: ProgressSender) -> Result<PathBuf> {
        let object = self.root.join("install").join(key_name(&plan.key));
        let root = object.join("root");
        let lock_path = self.root.join("locks/install").join(key_name(&plan.key));
        let lock = ProgressLock::acquire(&lock_path, progress.clone()).await?;
        let object_owned = object.clone();
        let key = plan.key.clone();
        if blocking(move || valid_object(&object_owned, &key, true)).await? {
            if !lock.waited() {
                lock.dispatch(Event::Cached {
                    ty: StatusType::Install,
                    key: plan.key.clone(),
                    name: plan.name.clone(),
                    at: timestamp(),
                    bytes: None,
                    total_bytes: None,
                })
                .await?;
            }
            return Ok(root);
        }
        lock.dispatch(Event::AttemptStarted {
            ty: StatusType::Install,
            key: plan.key.clone(),
            name: plan.name.clone(),
            at: timestamp(),
            bytes: None,
            total_bytes: None,
        })
        .await?;

        let result = self
            .ensure_install_locked(plan, &object, &root, &lock, progress)
            .await;
        match result {
            Ok(path) => {
                lock.dispatch(Event::Completed {
                    key: plan.key.clone(),
                    at: timestamp(),
                    bytes: None,
                })
                .await?;
                Ok(path)
            }
            Err(error) => {
                let _ = lock
                    .dispatch(Event::Failed {
                        key: plan.key.clone(),
                        at: timestamp(),
                        bytes: None,
                    })
                    .await;
                Err(error)
            }
        }
    }

    async fn ensure_install_locked(
        &self,
        plan: &Plan,
        object: &Path,
        root: &Path,
        progress_lock: &ProgressLock,
        progress: ProgressSender,
    ) -> Result<PathBuf> {
        let mut unique = HashSet::new();
        let mut tasks = tokio::task::JoinSet::new();
        for item in &plan.items {
            if unique.insert(item.key.clone()) {
                let store = self.clone();
                let item = item.clone();
                let progress = progress.clone();
                tasks.spawn(async move {
                    let key = item.key.clone();
                    store
                        .ensure_download(&item, progress)
                        .await
                        .map(|path| (key, path))
                });
            }
        }
        let mut downloads_by_key = HashMap::with_capacity(unique.len());
        while let Some(result) = tasks.join_next().await {
            let (key, path) = result.context("download task failed")??;
            downloads_by_key.insert(key, path);
        }
        let downloads = plan
            .items
            .iter()
            .map(|item| {
                downloads_by_key
                    .get(&item.key)
                    .cloned()
                    .with_context(|| format!("missing download result for key {}", item.key))
            })
            .collect::<Result<Vec<_>>>()?;

        let tmp_root = self.root.join("tmp");
        let temporary = blocking(move || {
            Ok(Builder::new()
                .prefix("install-")
                .tempdir_in(tmp_root)?
                .keep())
        })
        .await?;
        tokio::fs::write(temporary.join("key"), &plan.key).await?;
        let plan_owned = plan.clone();
        let install_root = temporary.join("root");
        let (event_send, mut event_receive) = tokio::sync::mpsc::channel(64);
        let reporter = progress_lock.reporter();
        let status_bridge = tokio::spawn(async move {
            while let Some(event) = event_receive.recv().await {
                reporter.dispatch(event).await?;
            }
            Result::<()>::Ok(())
        });
        let result = blocking(move || {
            execute_plan(
                &plan_owned,
                &downloads,
                &install_root,
                BlockingEventSender::new(event_send),
            )
        })
        .await;
        status_bridge
            .await
            .context("unpack status bridge failed")??;
        result?;
        tokio::fs::rename(&temporary, object)
            .await
            .with_context(|| format!("publish installation {}", object.display()))?;
        Ok(root.to_path_buf())
    }

    async fn ensure_download(&self, item: &Item, progress: ProgressSender) -> Result<PathBuf> {
        let object = self.root.join("dl").join(key_name(&item.key));
        let lock_path = self.root.join("locks/dl").join(key_name(&item.key));
        let lock = ProgressLock::acquire(&lock_path, progress).await?;
        if tokio::fs::try_exists(&object).await? {
            let object_owned = object.clone();
            let item_owned = item.clone();
            match blocking(move || verify_download(&object_owned, &item_owned)).await {
                Ok(path) => {
                    if !lock.waited() {
                        let bytes = tokio::fs::metadata(&path).await?.len();
                        lock.dispatch(Event::Cached {
                            ty: StatusType::Download,
                            key: item.key.clone(),
                            name: item.name.clone(),
                            at: timestamp(),
                            bytes: Some(bytes),
                            total_bytes: Some(item.size().unwrap_or(bytes)),
                        })
                        .await?;
                    }
                    return Ok(path);
                }
                Err(error) => {
                    lock.dispatch(Event::AttemptStarted {
                        ty: StatusType::Download,
                        key: item.key.clone(),
                        name: item.name.clone(),
                        at: timestamp(),
                        bytes: Some(0),
                        total_bytes: item.size(),
                    })
                    .await?;
                    let _ = lock
                        .dispatch(Event::Failed {
                            key: item.key.clone(),
                            at: timestamp(),
                            bytes: None,
                        })
                        .await;
                    return Err(error);
                }
            }
        }
        lock.dispatch(Event::AttemptStarted {
            ty: StatusType::Download,
            key: item.key.clone(),
            name: item.name.clone(),
            at: timestamp(),
            bytes: Some(0),
            total_bytes: item.size(),
        })
        .await?;

        let result = self.ensure_download_locked(item, &object, &lock).await;
        match result {
            Ok(path) => {
                let bytes = tokio::fs::metadata(&path).await?.len();
                lock.dispatch(Event::Completed {
                    key: item.key.clone(),
                    at: timestamp(),
                    bytes: Some(bytes),
                })
                .await?;
                Ok(path)
            }
            Err(error) => {
                let _ = lock
                    .dispatch(Event::Failed {
                        key: item.key.clone(),
                        at: timestamp(),
                        bytes: None,
                    })
                    .await;
                Err(error)
            }
        }
    }

    async fn ensure_download_locked(
        &self,
        item: &Item,
        object: &Path,
        progress: &ProgressLock,
    ) -> Result<PathBuf> {
        let tmp_root = self.root.join("tmp");
        let temporary = blocking(move || {
            Ok(Builder::new()
                .prefix("download-")
                .tempdir_in(tmp_root)?
                .keep())
        })
        .await?;
        tokio::fs::write(temporary.join("key"), &item.key).await?;
        download_to(item, &temporary.join("data"), progress).await?;
        tokio::fs::rename(&temporary, object)
            .await
            .with_context(|| format!("publish download object {}", object.display()))?;
        Ok(object.join("data"))
    }

    fn gc_locked(&self) -> Result<GcReport> {
        let mut report = GcReport::default();
        let mut db = self.db()?;
        let known = db.request_inodes()?;

        for request_ino in &known {
            if !self.root.join("requests").join(request_ino).exists() {
                db.remove_request(request_ino)?;
                report.requests += 1;
            }
        }
        for entry in std::fs::read_dir(self.root.join("requests"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let metadata = entry.metadata()?;
            if metadata.nlink() == 1 || !known.contains(&name) {
                if known.contains(&name) {
                    db.remove_request(&name)?;
                }
                std::fs::remove_file(entry.path())?;
                report.requests += 1;
            }
        }
        db.remove_unreferenced_download_relations()?;

        report.installs = self.sweep_keyed_dir("install", &db.live_plan_keys()?)?;
        report.downloads = self.sweep_keyed_dir("dl", &db.live_download_keys()?)?;
        report.temporary = self.sweep_all("tmp")?;
        Ok(report)
    }

    fn db(&self) -> Result<IntentDb> {
        IntentDb::open(&self.root.join("state.sqlite"))
    }

    fn gc_lock_path(&self) -> PathBuf {
        self.root.join("locks/gc")
    }

    fn sweep_keyed_dir(&self, directory: &str, live_keys: &HashSet<String>) -> Result<usize> {
        let live_names = live_keys
            .iter()
            .map(|key| key_name(key))
            .collect::<HashSet<_>>();
        let mut removed = 0;
        for entry in std::fs::read_dir(self.root.join(directory))? {
            let entry = entry?;
            if !live_names.contains(&entry.file_name().to_string_lossy().into_owned()) {
                remove_entry(&entry.path())?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn sweep_all(&self, directory: &str) -> Result<usize> {
        let mut removed = 0;
        for entry in std::fs::read_dir(self.root.join(directory))? {
            remove_entry(&entry?.path())?;
            removed += 1;
        }
        Ok(removed)
    }
}

async fn blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .context("blocking task failed")?
}

fn valid_object(object: &Path, key: &str, directory_root: bool) -> Result<bool> {
    if !object.exists() {
        return Ok(false);
    }
    let stored_key = std::fs::read_to_string(object.join("key"))
        .with_context(|| format!("read object key: {}", object.display()))?;
    ensure!(stored_key == key, "object key hash collision");
    if directory_root {
        ensure!(
            object.join("root").is_dir(),
            "installation object is missing its root directory"
        );
    }
    Ok(true)
}

fn key_name(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

fn remove_entry(path: &Path) -> Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

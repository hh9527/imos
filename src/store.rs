use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use tempfile::Builder;

use crate::artifact::{
    download_to, execute_item, finalize_install_root, prepare_install_root, verify_download,
};
use crate::db::IntentDb;
use crate::plan::{Item, Plan, PlanEnvelope};
use crate::progress::{
    BlockingEventSender, Effect as ProgressEffect, Event as ProgressEvent, FileLock, ProgressLock,
    ProgressSender, State as ProgressState, StatusReporter,
};
use crate::status::{Status, StatusType, timestamp};

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

#[derive(Clone)]
struct PreparedCreate {
    state: PlanFileState,
    request_path: PathBuf,
    already_registered: bool,
    plan: Plan,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ProgressTarget(String);

enum PendingProgress {
    Local(StatusReporter, ProgressEvent),
    Observed(Status),
}

#[derive(Default)]
struct DownloadTracker {
    pending: HashSet<String>,
    completed: HashMap<String, PathBuf>,
    failed: bool,
}

enum DownloadDecision {
    Completed,
    Failed(String),
    Ignored,
}

impl DownloadTracker {
    fn start(items: &[Item]) -> Self {
        Self {
            pending: items.iter().map(|item| item.key.clone()).collect(),
            ..Self::default()
        }
    }

    fn finish(
        &mut self,
        key: String,
        result: std::result::Result<PathBuf, String>,
    ) -> DownloadDecision {
        if self.failed || !self.pending.remove(&key) {
            return DownloadDecision::Ignored;
        }
        match result {
            Ok(path) => {
                self.completed.insert(key, path);
                DownloadDecision::Completed
            }
            Err(error) => {
                self.failed = true;
                DownloadDecision::Failed(error)
            }
        }
    }
}

#[derive(Default)]
struct CreateState {
    store_root: PathBuf,
    plan_file: Option<PathBuf>,
    prepared: Option<PreparedCreate>,
    gc_lock: Option<std::sync::Arc<FileLock>>,
    install_lock: Option<std::sync::Arc<ProgressLock>>,
    downloads: DownloadTracker,
    temporary: Option<PathBuf>,
    next_item: usize,
    item_running: bool,
    publishing: bool,
    progress: ProgressState,
    progress_busy: HashSet<ProgressTarget>,
    progress_pending: HashMap<ProgressTarget, VecDeque<PendingProgress>>,
    result: Option<std::result::Result<PathBuf, String>>,
}

enum CreateEvent {
    Submitted(PathBuf),
    Prepared(std::result::Result<PreparedCreate, String>),
    InstallAcquired {
        prepared: PreparedCreate,
        gc_lock: std::sync::Arc<FileLock>,
        install_lock: std::sync::Arc<ProgressLock>,
        cached: bool,
    },
    DownloadFinished {
        key: String,
        result: std::result::Result<PathBuf, String>,
    },
    InstallPrepared(std::result::Result<PathBuf, String>),
    ItemFinished {
        index: usize,
        result: std::result::Result<(), String>,
    },
    Published(std::result::Result<PathBuf, String>),
    Registered(std::result::Result<PathBuf, String>),
    Progress {
        target: ProgressTarget,
        reporter: StatusReporter,
        event: ProgressEvent,
    },
    ObservedStatus(Status),
    ProgressApplied {
        target: ProgressTarget,
        events: Vec<ProgressEvent>,
    },
    StatusForwarded {
        target: ProgressTarget,
    },
    EffectFailed(String),
}

enum CreateEffect {
    Prepare(PathBuf),
    AcquireInstall(PreparedCreate),
    Download(Item),
    PrepareInstall {
        plan_key: String,
    },
    ExecuteItem {
        index: usize,
        item: Item,
        data: PathBuf,
        root: PathBuf,
        reporter: StatusReporter,
    },
    PublishInstall {
        temporary: PathBuf,
        object: PathBuf,
        root: PathBuf,
    },
    Register {
        plan_file: PathBuf,
        prepared: PreparedCreate,
        root: PathBuf,
    },
    EmitProgress {
        target: ProgressTarget,
        reporter: StatusReporter,
        effect: ProgressEffect,
    },
    ForwardStatus {
        target: ProgressTarget,
        status: Status,
    },
}

#[derive(Clone)]
struct CreateContext {
    store: Store,
    progress: ProgressSender,
    events: tokio::sync::mpsc::Sender<CreateEvent>,
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
        let (event_send, mut event_receive) = tokio::sync::mpsc::channel(128);
        let context = CreateContext {
            store: self.clone(),
            progress,
            events: event_send,
        };
        let mut state = CreateState {
            store_root: self.root.clone(),
            ..CreateState::default()
        };
        let mut tasks = tokio::task::JoinSet::new();
        dispatch_create(
            CreateEvent::Submitted(plan_file.to_path_buf()),
            &mut state,
            &mut tasks,
            &context,
        );

        loop {
            while let Ok(event) = event_receive.try_recv() {
                dispatch_create(event, &mut state, &mut tasks, &context);
            }
            if state.result.is_some() && tasks.is_empty() {
                break;
            }
            tokio::select! {
                Some(event) = event_receive.recv() => {
                    dispatch_create(event, &mut state, &mut tasks, &context);
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = result {
                        dispatch_create(
                            CreateEvent::EffectFailed(format!("create effect task failed: {error}")),
                            &mut state,
                            &mut tasks,
                            &context,
                        );
                    }
                }
            }
        }

        match state.result.expect("completed create has a result") {
            Ok(path) => Ok(path),
            Err(error) => anyhow::bail!(error),
        }
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

impl CreateEvent {
    fn reduce(self, state: &mut CreateState, effects: &mut Vec<CreateEffect>) {
        match self {
            Self::Submitted(plan_file) => {
                state.plan_file = Some(plan_file.clone());
                effects.push(CreateEffect::Prepare(plan_file));
            }
            Self::Prepared(Ok(prepared)) => {
                effects.push(CreateEffect::AcquireInstall(prepared));
            }
            Self::Prepared(Err(error)) | Self::EffectFailed(error) => {
                fail_create(state, effects, error);
            }
            Self::InstallAcquired {
                prepared,
                gc_lock,
                install_lock,
                cached,
            } => {
                let reporter = install_lock.reporter();
                state.gc_lock = Some(gc_lock);
                state.install_lock = Some(install_lock.clone());
                state.prepared = Some(prepared.clone());
                let root = install_root(&state.store_root, &prepared.plan.key);
                if cached {
                    if !install_lock.waited() {
                        enqueue_progress(
                            state,
                            effects,
                            ProgressTarget(prepared.plan.key.clone()),
                            reporter,
                            ProgressEvent::Cached {
                                ty: StatusType::Install,
                                key: prepared.plan.key.clone(),
                                name: prepared.plan.name.clone(),
                                at: timestamp(),
                                bytes: None,
                                total_bytes: None,
                            },
                        );
                    }
                    effects.push(CreateEffect::Register {
                        plan_file: state.plan_file.clone().expect("submitted plan file"),
                        prepared,
                        root,
                    });
                    return;
                }

                enqueue_progress(
                    state,
                    effects,
                    ProgressTarget(prepared.plan.key.clone()),
                    reporter,
                    ProgressEvent::AttemptStarted {
                        ty: StatusType::Install,
                        key: prepared.plan.key.clone(),
                        name: prepared.plan.name.clone(),
                        at: timestamp(),
                        bytes: None,
                        total_bytes: None,
                    },
                );
                state.downloads = DownloadTracker::start(&prepared.plan.items);
                let mut unique = HashSet::new();
                for item in &prepared.plan.items {
                    if unique.insert(item.key.clone()) {
                        effects.push(CreateEffect::Download(item.clone()));
                    }
                }
                effects.push(CreateEffect::PrepareInstall {
                    plan_key: prepared.plan.key.clone(),
                });
            }
            Self::InstallPrepared(Ok(temporary)) => {
                if state.result.is_none() {
                    state.temporary = Some(temporary);
                    advance_items(state, effects);
                }
            }
            Self::InstallPrepared(Err(error)) => fail_create(state, effects, error),
            Self::DownloadFinished { key, result } => match state.downloads.finish(key, result) {
                DownloadDecision::Completed => advance_items(state, effects),
                DownloadDecision::Failed(error) => fail_create(state, effects, error),
                DownloadDecision::Ignored => {}
            },
            Self::ItemFinished { index, result } => {
                if state.result.is_some() || !state.item_running || index != state.next_item {
                    return;
                }
                state.item_running = false;
                match result {
                    Ok(()) => {
                        state.next_item += 1;
                        advance_items(state, effects);
                    }
                    Err(error) => fail_create(state, effects, error),
                }
            }
            Self::Published(result) => match result {
                Ok(root) => {
                    if let (Some(prepared), Some(lock)) =
                        (state.prepared.clone(), state.install_lock.clone())
                    {
                        enqueue_progress(
                            state,
                            effects,
                            ProgressTarget(prepared.plan.key.clone()),
                            lock.reporter(),
                            ProgressEvent::Completed {
                                key: prepared.plan.key.clone(),
                                at: timestamp(),
                                bytes: None,
                            },
                        );
                        effects.push(CreateEffect::Register {
                            plan_file: state.plan_file.clone().expect("submitted plan file"),
                            prepared,
                            root,
                        });
                    }
                }
                Err(error) => fail_create(state, effects, error),
            },
            Self::Registered(result) => {
                if state.result.is_none() {
                    state.result = Some(result);
                }
            }
            Self::Progress {
                target,
                reporter,
                event,
            } => enqueue_progress(state, effects, target, reporter, event),
            Self::ObservedStatus(status) => {
                let target = ProgressTarget(status.key.clone());
                state
                    .progress_pending
                    .entry(target.clone())
                    .or_default()
                    .push_back(PendingProgress::Observed(status));
                pump_progress(state, effects, target);
            }
            Self::ProgressApplied { target, events } => {
                state.progress_busy.remove(&target);
                for event in events {
                    let mut progress_effects = Vec::new();
                    event.reduce(&mut state.progress, &mut progress_effects);
                }
                if let Some(error) = state.progress.take_failure() {
                    fail_create(state, effects, error);
                }
                pump_progress(state, effects, target);
            }
            Self::StatusForwarded { target } => {
                state.progress_busy.remove(&target);
                pump_progress(state, effects, target);
            }
        }
    }
}

fn enqueue_progress(
    state: &mut CreateState,
    effects: &mut Vec<CreateEffect>,
    target: ProgressTarget,
    reporter: StatusReporter,
    event: ProgressEvent,
) {
    state
        .progress_pending
        .entry(target.clone())
        .or_default()
        .push_back(PendingProgress::Local(reporter, event));
    pump_progress(state, effects, target);
}

fn pump_progress(state: &mut CreateState, effects: &mut Vec<CreateEffect>, target: ProgressTarget) {
    while !state.progress_busy.contains(&target) {
        let next = state
            .progress_pending
            .get_mut(&target)
            .and_then(VecDeque::pop_front);
        let Some(next) = next else {
            return;
        };
        match next {
            PendingProgress::Local(reporter, event) => {
                let mut progress_effects = Vec::new();
                event.reduce(&mut state.progress, &mut progress_effects);
                if let Some(error) = state.progress.take_failure() {
                    fail_create(state, effects, error);
                    return;
                }
                if let Some(effect) = progress_effects.pop() {
                    state.progress_busy.insert(target.clone());
                    effects.push(CreateEffect::EmitProgress {
                        target: target.clone(),
                        reporter,
                        effect,
                    });
                }
            }
            PendingProgress::Observed(status) => {
                state.progress.observe(status.clone());
                state.progress_busy.insert(target.clone());
                effects.push(CreateEffect::ForwardStatus {
                    target: target.clone(),
                    status,
                });
            }
        }
    }
}

fn fail_create(state: &mut CreateState, effects: &mut Vec<CreateEffect>, error: String) {
    if state.result.is_some() {
        return;
    }
    state.result = Some(Err(error));
    if let (Some(prepared), Some(lock)) = (&state.prepared, &state.install_lock) {
        enqueue_progress(
            state,
            effects,
            ProgressTarget(prepared.plan.key.clone()),
            lock.reporter(),
            ProgressEvent::Failed {
                key: prepared.plan.key.clone(),
                at: timestamp(),
                bytes: None,
            },
        );
    }
}

fn advance_items(state: &mut CreateState, effects: &mut Vec<CreateEffect>) {
    if state.result.is_some() || state.item_running || state.publishing {
        return;
    }
    let (Some(prepared), Some(temporary)) = (&state.prepared, &state.temporary) else {
        return;
    };
    if state.next_item == prepared.plan.items.len() {
        state.publishing = true;
        let object = state
            .store_root
            .join("install")
            .join(key_name(&prepared.plan.key));
        effects.push(CreateEffect::PublishInstall {
            temporary: temporary.clone(),
            root: object.join("root"),
            object,
        });
        return;
    }
    let item = &prepared.plan.items[state.next_item];
    let Some(data) = ready_item_data(&prepared.plan, state.next_item, &state.downloads) else {
        return;
    };
    state.item_running = true;
    let reporter = state
        .install_lock
        .as_ref()
        .expect("prepared installation has a lock")
        .reporter();
    effects.push(CreateEffect::ExecuteItem {
        index: state.next_item,
        item: item.clone(),
        data,
        root: temporary.join("root"),
        reporter,
    });
}

impl CreateEffect {
    async fn apply(self, context: CreateContext) -> Vec<CreateEvent> {
        match self {
            Self::Prepare(plan_file) => {
                let store = context.store.clone();
                vec![CreateEvent::Prepared(
                    blocking(move || store.prepare_create(&plan_file))
                        .await
                        .map_err(|error| format!("{error:#}")),
                )]
            }
            Self::AcquireInstall(prepared) => {
                let result = async {
                    let gc_lock =
                        std::sync::Arc::new(FileLock::shared(&context.store.gc_lock_path()).await?);
                    let lock_path = context
                        .store
                        .root
                        .join("locks/install")
                        .join(key_name(&prepared.plan.key));
                    let event_send = context.events.clone();
                    let install_lock = std::sync::Arc::new(
                        ProgressLock::acquire(
                            &lock_path,
                            context.progress.clone(),
                            move |status| {
                                let event_send = event_send.clone();
                                async move {
                                    event_send
                                        .send(CreateEvent::ObservedStatus(status))
                                        .await
                                        .map_err(|_| anyhow::anyhow!("create event queue closed"))
                                }
                            },
                        )
                        .await?,
                    );
                    let object = context
                        .store
                        .root
                        .join("install")
                        .join(key_name(&prepared.plan.key));
                    let key = prepared.plan.key.clone();
                    let cached = blocking(move || valid_object(&object, &key, true)).await?;
                    Result::<_>::Ok((gc_lock, install_lock, cached))
                }
                .await;
                match result {
                    Ok((gc_lock, install_lock, cached)) => vec![CreateEvent::InstallAcquired {
                        prepared,
                        gc_lock,
                        install_lock,
                        cached,
                    }],
                    Err(error) => vec![CreateEvent::EffectFailed(format!("{error:#}"))],
                }
            }
            Self::Download(item) => {
                let key = item.key.clone();
                let result = apply_download(&context, item)
                    .await
                    .map_err(|error| format!("{error:#}"));
                vec![CreateEvent::DownloadFinished { key, result }]
            }
            Self::PrepareInstall { plan_key } => {
                let temporary_root = context.store.root.join("tmp");
                let result = blocking(move || {
                    let temporary = Builder::new()
                        .prefix("install-")
                        .tempdir_in(temporary_root)?
                        .keep();
                    std::fs::write(temporary.join("key"), plan_key)?;
                    prepare_install_root(&temporary.join("root"))?;
                    Ok(temporary)
                })
                .await
                .map_err(|error| format!("{error:#}"));
                vec![CreateEvent::InstallPrepared(result)]
            }
            Self::ExecuteItem {
                index,
                item,
                data,
                root,
                reporter,
            } => {
                let target = ProgressTarget(item.key.clone());
                let event_send = context.events.clone();
                let result = blocking(move || {
                    let sender = BlockingEventSender::from_fn(move |event| {
                        let _ = event_send.blocking_send(CreateEvent::Progress {
                            target: target.clone(),
                            reporter: reporter.clone(),
                            event,
                        });
                    });
                    execute_item(&item, &data, &root, sender)
                })
                .await
                .map_err(|error| format!("{error:#}"));
                vec![CreateEvent::ItemFinished { index, result }]
            }
            Self::PublishInstall {
                temporary,
                object,
                root,
            } => {
                let result = blocking(move || {
                    finalize_install_root(&temporary.join("root"))?;
                    std::fs::rename(&temporary, &object)
                        .with_context(|| format!("publish installation {}", object.display()))?;
                    Ok(root)
                })
                .await
                .map_err(|error| format!("{error:#}"));
                vec![CreateEvent::Published(result)]
            }
            Self::Register {
                plan_file,
                prepared,
                root,
            } => {
                let store = context.store.clone();
                let result = blocking(move || {
                    store.register_create(&plan_file, prepared)?;
                    Ok(root)
                })
                .await
                .map_err(|error| format!("{error:#}"));
                vec![CreateEvent::Registered(result)]
            }
            Self::EmitProgress {
                target,
                reporter,
                effect,
            } => {
                let events = effect.apply(&reporter).await;
                vec![CreateEvent::ProgressApplied { target, events }]
            }
            Self::ForwardStatus { target, status } => {
                match status.to_value() {
                    Ok(value) => context.progress.send(value).await,
                    Err(error) => return vec![CreateEvent::EffectFailed(error.to_string())],
                }
                vec![CreateEvent::StatusForwarded { target }]
            }
        }
    }
}

async fn apply_download(context: &CreateContext, item: Item) -> Result<PathBuf> {
    let object = context.store.root.join("dl").join(key_name(&item.key));
    let lock_path = context
        .store
        .root
        .join("locks/dl")
        .join(key_name(&item.key));
    let event_send = context.events.clone();
    let lock = std::sync::Arc::new(
        ProgressLock::acquire(&lock_path, context.progress.clone(), move |status| {
            let event_send = event_send.clone();
            async move {
                event_send
                    .send(CreateEvent::ObservedStatus(status))
                    .await
                    .map_err(|_| anyhow::anyhow!("create event queue closed"))
            }
        })
        .await?,
    );
    let reporter = lock.reporter();
    let target = ProgressTarget(item.key.clone());
    if tokio::fs::try_exists(&object).await? {
        let object_owned = object.clone();
        let item_owned = item.clone();
        match blocking(move || verify_download(&object_owned, &item_owned)).await {
            Ok(path) => {
                if !lock.waited() {
                    let bytes = tokio::fs::metadata(&path).await?.len();
                    send_progress(
                        context,
                        target,
                        reporter,
                        ProgressEvent::Cached {
                            ty: StatusType::Download,
                            key: item.key.clone(),
                            name: item.name.clone(),
                            at: timestamp(),
                            bytes: Some(bytes),
                            total_bytes: Some(item.size().unwrap_or(bytes)),
                        },
                    )
                    .await?;
                }
                return Ok(path);
            }
            Err(error) => {
                send_progress(
                    context,
                    target.clone(),
                    reporter.clone(),
                    download_started(&item),
                )
                .await?;
                send_progress(
                    context,
                    target,
                    reporter,
                    ProgressEvent::Failed {
                        key: item.key.clone(),
                        at: timestamp(),
                        bytes: None,
                    },
                )
                .await?;
                return Err(error);
            }
        }
    }

    send_progress(
        context,
        target.clone(),
        reporter.clone(),
        download_started(&item),
    )
    .await?;
    let temporary_root = context.store.root.join("tmp");
    let temporary = blocking(move || {
        Ok(Builder::new()
            .prefix("download-")
            .tempdir_in(temporary_root)?
            .keep())
    })
    .await?;
    tokio::fs::write(temporary.join("key"), &item.key).await?;
    let data = temporary.join("data");
    let progress_context = context.clone();
    let progress_target = target.clone();
    let progress_reporter = reporter.clone();
    let result = download_to(&item, &data, move |event| {
        let context = progress_context.clone();
        let target = progress_target.clone();
        let reporter = progress_reporter.clone();
        async move { send_progress(&context, target, reporter, event).await }
    })
    .await;
    match result {
        Ok(()) => {
            tokio::fs::rename(&temporary, &object)
                .await
                .with_context(|| format!("publish download object {}", object.display()))?;
            let path = object.join("data");
            let bytes = tokio::fs::metadata(&path).await?.len();
            send_progress(
                context,
                target,
                reporter,
                ProgressEvent::Completed {
                    key: item.key.clone(),
                    at: timestamp(),
                    bytes: Some(bytes),
                },
            )
            .await?;
            Ok(path)
        }
        Err(error) => {
            send_progress(
                context,
                target,
                reporter,
                ProgressEvent::Failed {
                    key: item.key.clone(),
                    at: timestamp(),
                    bytes: None,
                },
            )
            .await?;
            Err(error)
        }
    }
}

fn download_started(item: &Item) -> ProgressEvent {
    ProgressEvent::AttemptStarted {
        ty: StatusType::Download,
        key: item.key.clone(),
        name: item.name.clone(),
        at: timestamp(),
        bytes: Some(0),
        total_bytes: item.size(),
    }
}

async fn send_progress(
    context: &CreateContext,
    target: ProgressTarget,
    reporter: StatusReporter,
    event: ProgressEvent,
) -> Result<()> {
    context
        .events
        .send(CreateEvent::Progress {
            target,
            reporter,
            event,
        })
        .await
        .map_err(|_| anyhow::anyhow!("create event queue closed"))
}

fn dispatch_create(
    event: CreateEvent,
    state: &mut CreateState,
    tasks: &mut tokio::task::JoinSet<()>,
    context: &CreateContext,
) {
    let mut effects = Vec::new();
    event.reduce(state, &mut effects);
    for effect in effects {
        let context = context.clone();
        let events = context.events.clone();
        tasks.spawn(async move {
            for event in effect.apply(context).await {
                if events.send(event).await.is_err() {
                    break;
                }
            }
        });
    }
}

fn install_root(store_root: &Path, key: &str) -> PathBuf {
    store_root.join("install").join(key_name(key)).join("root")
}

fn ready_item_data(plan: &Plan, next_item: usize, downloads: &DownloadTracker) -> Option<PathBuf> {
    downloads.completed.get(&plan.items[next_item].key).cloned()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ItemKind;

    fn item(key: &str) -> Item {
        Item {
            kind: ItemKind::InstallFile {
                url: format!("file:///tmp/{key}"),
                size: None,
                digest: None,
                to: PathBuf::from(format!("{key}.txt")),
            },
            name: key.into(),
            key: key.into(),
        }
    }

    #[test]
    fn ordered_items_start_as_soon_as_their_download_is_ready() {
        let plan = Plan {
            version: 1,
            name: "Plan".into(),
            key: "plan-v1".into(),
            items: vec![item("first"), item("second")],
        };
        let mut downloads = DownloadTracker::start(&plan.items);

        assert!(matches!(
            downloads.finish("second".into(), Ok(PathBuf::from("/dl/second"))),
            DownloadDecision::Completed
        ));
        assert_eq!(ready_item_data(&plan, 0, &downloads), None);

        assert!(matches!(
            downloads.finish("first".into(), Ok(PathBuf::from("/dl/first"))),
            DownloadDecision::Completed
        ));
        assert_eq!(
            ready_item_data(&plan, 0, &downloads),
            Some(PathBuf::from("/dl/first"))
        );
        assert_eq!(
            ready_item_data(&plan, 1, &downloads),
            Some(PathBuf::from("/dl/second"))
        );
    }

    #[test]
    fn a_download_failure_makes_late_results_inert() {
        let items = vec![item("first"), item("second")];
        let mut downloads = DownloadTracker::start(&items);
        assert!(matches!(
            downloads.finish("first".into(), Err("failed".into())),
            DownloadDecision::Failed(error) if error == "failed"
        ));
        assert!(matches!(
            downloads.finish("second".into(), Ok(PathBuf::from("/dl/second"))),
            DownloadDecision::Ignored
        ));
        assert!(downloads.completed.is_empty());
    }
}

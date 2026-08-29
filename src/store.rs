use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::Builder;

use crate::artifact::{download_to, execute_plan, verify_download};
use crate::db::IntentDb;
use crate::plan::{Item, Plan, PlanEnvelope};
use crate::progress::ProgressLock;

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

impl Store {
    pub fn open(root: PathBuf) -> Result<Self> {
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
        store.gc_lock_file()?;
        Ok(store)
    }

    pub fn create(&self, plan_file: &Path) -> Result<PathBuf> {
        let metadata = std::fs::metadata(plan_file)
            .with_context(|| format!("read plan metadata {}", plan_file.display()))?;
        ensure!(metadata.is_file(), "plan must be a regular file");
        let store_device = std::fs::metadata(&self.root)?.dev();
        ensure!(
            metadata.dev() == store_device,
            "plan file and store must be on the same file system"
        );
        let request_ino = metadata.ino().to_string();
        let request_path = self.root.join("requests").join(&request_ino);
        let already_registered = request_path.exists();
        if !already_registered {
            ensure!(
                metadata.nlink() == 1,
                "a new plan file must have exactly one link"
            );
        }

        let envelope = PlanEnvelope::read(plan_file)?;
        let plan = &envelope.imos;
        let gc_lock = self.lock_gc_shared()?;
        let result = self.ensure_install(plan)?;

        if !already_registered {
            match std::fs::hard_link(plan_file, &request_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let registered = std::fs::metadata(&request_path)?;
                    ensure!(
                        registered.ino() == metadata.ino(),
                        "request inode path is already bound to another file"
                    );
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("register plan inode {request_ino}"));
                }
            }
        }
        let internal_metadata = std::fs::metadata(&request_path)?;
        ensure!(
            internal_metadata.dev() == metadata.dev() && internal_metadata.ino() == metadata.ino(),
            "plan file was replaced while being registered"
        );
        ensure!(
            internal_metadata.len() == metadata.len()
                && internal_metadata.mtime() == metadata.mtime()
                && internal_metadata.mtime_nsec() == metadata.mtime_nsec(),
            "plan file was modified while create was running"
        );
        ensure!(
            internal_metadata.nlink() >= 2,
            "upstream plan file was removed"
        );

        let mut db = self.db()?;
        if let Some(existing) = db.request_plan(&request_ino)? {
            ensure!(
                existing == plan.key,
                "request inode is already bound to another plan key"
            );
        }
        let download_keys = plan
            .download_keys()
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        db.add_request(&request_ino, &plan.key, &download_keys)?;
        drop(gc_lock);
        Ok(result)
    }

    pub fn remove(&self, plan_file: &Path) -> Result<()> {
        let metadata = std::fs::metadata(plan_file)
            .with_context(|| format!("read plan metadata {}", plan_file.display()))?;
        let request_ino = metadata.ino().to_string();
        let _gc_lock = self.lock_gc_shared()?;
        self.db()?.remove_request(&request_ino)?;
        let request_path = self.root.join("requests").join(request_ino);
        if request_path.exists() {
            std::fs::remove_file(request_path)?;
        }
        Ok(())
    }

    pub fn gc(&self) -> Result<GcReport> {
        let _gc_lock = self.lock_gc_exclusive()?;
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

        let live_plans = db.live_plan_keys()?;
        let live_downloads = db.live_download_keys()?;
        report.installs = self.sweep_keyed_dir("install", &live_plans)?;
        report.downloads = self.sweep_keyed_dir("dl", &live_downloads)?;
        report.temporary = self.sweep_all("tmp")?;
        Ok(report)
    }

    fn ensure_install(&self, plan: &Plan) -> Result<PathBuf> {
        let object = self.root.join("install").join(key_name(&plan.key));
        let root = object.join("root");
        let lock_path = self.root.join("locks/install").join(key_name(&plan.key));
        let mut progress = ProgressLock::acquire(&lock_path)?;
        progress.event(&json!({"event": "started", "plan_key": plan.key}))?;
        let result: Result<(PathBuf, bool)> = (|| {
            let mut downloads = Vec::with_capacity(plan.items.len());
            for item in &plan.items {
                downloads.push(self.ensure_download(item)?);
            }
            if self.valid_object(&object, &plan.key, true)? {
                return Ok((root.clone(), true));
            }
            progress.event(&json!({"event": "install"}))?;

            let temporary = Builder::new()
                .prefix("install-")
                .tempdir_in(self.root.join("tmp"))?;
            std::fs::write(temporary.path().join("key"), &plan.key)?;
            execute_plan(plan, &downloads, &temporary.path().join("root"))?;
            let temporary_path = temporary.keep();
            std::fs::rename(&temporary_path, &object)
                .with_context(|| format!("publish installation {}", object.display()))?;
            Ok((root, false))
        })();

        match result {
            Ok((path, cached)) => {
                progress.event(&json!({"event": "completed", "cached": cached}))?;
                Ok(path)
            }
            Err(error) => {
                let _ = progress.event(&json!({
                    "event": "failed",
                    "message": error.to_string(),
                }));
                Err(error)
            }
        }
    }

    fn ensure_download(&self, item: &Item) -> Result<PathBuf> {
        let object = self.root.join("dl").join(key_name(&item.key));
        let lock_path = self.root.join("locks/dl").join(key_name(&item.key));
        let mut progress = ProgressLock::acquire(&lock_path)?;
        progress.event(&json!({"event": "started", "dl_key": item.key}))?;
        let result: Result<(PathBuf, bool)> = (|| {
            if object.exists() {
                return Ok((verify_download(&object, item)?, true));
            }

            let temporary = Builder::new()
                .prefix("download-")
                .tempdir_in(self.root.join("tmp"))?;
            std::fs::write(temporary.path().join("key"), &item.key)?;
            download_to(item, &temporary.path().join("data"), &mut progress)?;
            let temporary_path = temporary.keep();
            std::fs::rename(&temporary_path, &object)
                .with_context(|| format!("publish download object {}", object.display()))?;
            Ok((object.join("data"), false))
        })();

        match result {
            Ok((path, cached)) => {
                progress.event(&json!({"event": "completed", "cached": cached}))?;
                Ok(path)
            }
            Err(error) => {
                let _ = progress.event(&json!({
                    "event": "failed",
                    "message": error.to_string(),
                }));
                Err(error)
            }
        }
    }

    fn valid_object(&self, object: &Path, key: &str, directory_root: bool) -> Result<bool> {
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

    fn db(&self) -> Result<IntentDb> {
        IntentDb::open(&self.root.join("state.sqlite"))
    }

    fn gc_lock_file(&self) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join("locks/gc"))
            .context("open GC lock")
    }

    fn lock_gc_shared(&self) -> Result<File> {
        let file = self.gc_lock_file()?;
        FileExt::lock_shared(&file)?;
        Ok(file)
    }

    fn lock_gc_exclusive(&self) -> Result<File> {
        let file = self.gc_lock_file()?;
        FileExt::lock_exclusive(&file)?;
        Ok(file)
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

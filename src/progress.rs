use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use fs2::FileExt;
use serde::Serialize;

pub struct ProgressLock {
    file: File,
}

impl ProgressLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open lock file {}", path.display()))?;
        let mut followed = 0_u64;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    let length = file.metadata()?.len();
                    if length > followed {
                        file.seek(SeekFrom::Start(followed))?;
                        let mut bytes = Vec::new();
                        file.read_to_end(&mut bytes)?;
                        followed += bytes.len() as u64;
                        eprint!("{}", String::from_utf8_lossy(&bytes));
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("acquire lock {}", path.display()));
                }
            }
        }
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file })
    }

    pub fn event<T: Serialize>(&mut self, event: &T) -> Result<()> {
        serde_json::to_writer(&mut self.file, event)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        Ok(())
    }
}

impl Drop for ProgressLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanEnvelope {
    pub imos: Plan,
    #[serde(flatten)]
    pub extensions: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: u32,
    pub key: String,
    #[serde(default)]
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Item {
    pub key: String,
    pub url: String,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub digest: Option<String>,
    pub action: PlanItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanItem {
    UnpackDir {
        kind: ArchiveKind,
        #[serde(default)]
        strip: u32,
        #[serde(default = "default_current_dir")]
        to: PathBuf,
    },
    UnpackFile {
        kind: ArchiveKind,
        from: PathBuf,
        to: PathBuf,
    },
    InstallFile {
        to: PathBuf,
    },
    InstallBin {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveKind {
    Tar,
    TarGzip,
    TarZstd,
}

fn default_current_dir() -> PathBuf {
    PathBuf::from(".")
}

impl PlanEnvelope {
    pub fn read(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("read plan file {}", path.display()))?;
        let envelope: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse plan file {}", path.display()))?;
        envelope.imos.validate()?;
        Ok(envelope)
    }
}

impl Plan {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1,
            "unsupported plan version {}",
            self.version
        );
        ensure!(!self.key.is_empty(), "plan key must not be empty");

        for item in &self.items {
            item.validate()?;
        }
        Ok(())
    }

    pub fn download_keys(&self) -> impl Iterator<Item = &str> {
        self.items.iter().map(|item| item.key.as_str())
    }
}

impl Item {
    fn validate(&self) -> Result<()> {
        ensure!(!self.key.is_empty(), "download key must not be empty");
        let url = url::Url::parse(&self.url)
            .with_context(|| format!("invalid download URL: {}", self.url))?;
        ensure!(
            matches!(url.scheme(), "http" | "https" | "file"),
            "unsupported download URL scheme: {}",
            url.scheme()
        );
        if let Some(digest) = &self.digest {
            validate_digest(digest)?;
        }

        match &self.action {
            PlanItem::UnpackDir { to, .. } => validate_relative_path(to, true),
            PlanItem::UnpackFile { from, to, .. } => {
                validate_relative_path(from, false)?;
                validate_relative_path(to, false)
            }
            PlanItem::InstallFile { to } => validate_relative_path(to, false),
            PlanItem::InstallBin { name } => validate_file_name(name),
        }
    }
}

pub fn validate_relative_path(path: &Path, allow_current: bool) -> Result<()> {
    if allow_current && path == Path::new(".") {
        return Ok(());
    }
    ensure!(!path.as_os_str().is_empty(), "path must not be empty");
    ensure!(
        !path.is_absolute(),
        "path must be relative: {}",
        path.display()
    );
    let text = path.to_str().context("path must be valid UTF-8")?;
    ensure!(!text.contains('\0'), "path must not contain NUL");
    ensure!(
        text.split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".."),
        "path contains an unsafe component: {}",
        path.display()
    );
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("path contains an unsafe component: {}", path.display());
        }
    }
    Ok(())
}

fn validate_file_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "file name must not be empty");
    validate_relative_path(Path::new(name), false)?;
    ensure!(
        Path::new(name).components().count() == 1,
        "expected a single file name: {name}"
    );
    Ok(())
}

fn validate_digest(digest: &str) -> Result<()> {
    let Some(value) = digest.strip_prefix("sha256:") else {
        bail!("only sha256 digests are supported");
    };
    ensure!(
        value.len() == 64,
        "sha256 digest must contain 64 hexadecimal characters"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "sha256 digest must use lowercase hexadecimal"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_paths() {
        assert!(validate_relative_path(Path::new("../bin"), false).is_err());
    }

    #[test]
    fn accepts_current_unpack_destination() {
        assert!(validate_relative_path(Path::new("."), true).is_ok());
    }

    #[test]
    fn validates_sha256_digest() {
        assert!(validate_digest(&format!("sha256:{}", "a".repeat(64))).is_ok());
        assert!(validate_digest("sha256:ABC").is_err());
    }
}

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub version: u32,
    pub name: String,
    pub key: String,
    #[serde(default)]
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Item {
    pub kind: ItemKind,
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "type",
    rename_all = "PascalCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ItemKind {
    UnpackDir {
        url: String,
        #[serde(default)]
        size: Option<u64>,
        #[serde(default)]
        digest: Option<String>,
        archive: ArchiveKind,
        #[serde(default)]
        strip: u32,
        #[serde(default = "default_current_dir")]
        to: PathBuf,
    },
    UnpackFile {
        url: String,
        #[serde(default)]
        size: Option<u64>,
        #[serde(default)]
        digest: Option<String>,
        archive: ArchiveKind,
        from: PathBuf,
        to: PathBuf,
    },
    InstallFile {
        url: String,
        #[serde(default)]
        size: Option<u64>,
        #[serde(default)]
        digest: Option<String>,
        to: PathBuf,
    },
    InstallBin {
        url: String,
        #[serde(default)]
        size: Option<u64>,
        #[serde(default)]
        digest: Option<String>,
        name: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum ArchiveKind {
    Tar,
    TarGzip,
    TarZstd,
}

fn default_current_dir() -> PathBuf {
    PathBuf::from(".")
}

impl Plan {
    pub fn read(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("read plan file {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse plan file {}", path.display()))?;
        Self::from_value(value).with_context(|| format!("validate plan file {}", path.display()))
    }

    pub fn from_value(value: serde_json::Value) -> Result<Self> {
        let plan: Self = serde_json::from_value(value).context("deserialize plan")?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1,
            "unsupported plan version {}",
            self.version
        );
        validate_name(&self.name, "plan")?;
        validate_file_name(&self.name).context("invalid plan name")?;
        validate_key(&self.key).context("invalid plan key")?;

        let mut downloads = HashMap::new();
        for item in &self.items {
            item.validate()?;
            let source = item.download_source();
            if let Some(existing) = downloads.insert(&item.key, source) {
                ensure!(
                    existing == source,
                    "download key {} has conflicting definitions",
                    item.key
                );
            }
        }
        Ok(())
    }

    pub fn download_keys(&self) -> impl Iterator<Item = &str> {
        self.items.iter().map(|item| item.key.as_str())
    }
}

impl Item {
    fn validate(&self) -> Result<()> {
        validate_name(&self.name, "item")?;
        validate_key(&self.key).context("invalid download key")?;
        let source = self.download_source();
        let url = url::Url::parse(source.url)
            .with_context(|| format!("invalid download URL: {}", source.url))?;
        ensure!(
            matches!(url.scheme(), "http" | "https" | "file"),
            "unsupported download URL scheme: {}",
            url.scheme()
        );
        if let Some(digest) = source.digest {
            validate_digest(digest)?;
        }

        match &self.kind {
            ItemKind::UnpackDir { to, .. } => validate_relative_path(to, true),
            ItemKind::UnpackFile { from, to, .. } => {
                validate_relative_path(from, false)?;
                validate_relative_path(to, false)
            }
            ItemKind::InstallFile { to, .. } => validate_relative_path(to, false),
            ItemKind::InstallBin { name, .. } => validate_file_name(name),
        }
    }

    pub fn url(&self) -> &str {
        self.download_source().url
    }

    pub fn size(&self) -> Option<u64> {
        self.download_source().size
    }

    pub fn digest(&self) -> Option<&str> {
        self.download_source().digest
    }

    fn download_source(&self) -> DownloadSource<'_> {
        match &self.kind {
            ItemKind::UnpackDir {
                url, size, digest, ..
            }
            | ItemKind::UnpackFile {
                url, size, digest, ..
            }
            | ItemKind::InstallFile {
                url, size, digest, ..
            }
            | ItemKind::InstallBin {
                url, size, digest, ..
            } => DownloadSource {
                url,
                size: *size,
                digest: digest.as_deref(),
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct DownloadSource<'a> {
    url: &'a str,
    size: Option<u64>,
    digest: Option<&'a str>,
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

fn validate_key(key: &str) -> Result<()> {
    let bytes = key.as_bytes();
    ensure!(bytes.len() <= 64, "key must not exceed 64 bytes");
    ensure!(
        bytes.first().is_some_and(u8::is_ascii_lowercase),
        "key must start with an ASCII lowercase letter"
    );
    let mut previous_was_separator = false;
    for byte in &bytes[1..] {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
            previous_was_separator = false;
        } else if matches!(byte, b'-' | b'_') && !previous_was_separator {
            previous_was_separator = true;
        } else {
            bail!("key contains an invalid character or separator sequence");
        }
    }
    ensure!(!previous_was_separator, "key must not end with a separator");
    Ok(())
}

fn validate_name(name: &str, subject: &str) -> Result<()> {
    ensure!(!name.is_empty(), "{subject} name must not be empty");
    ensure!(
        name.len() <= 64,
        "{subject} name must not exceed 64 UTF-8 bytes"
    );
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
    use serde_json::json;

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

    #[test]
    fn validates_key_syntax() {
        let maximum = "a".repeat(64);
        for key in ["a", "a1", "tool-1_linux-x86_64", &maximum] {
            assert!(validate_key(key).is_ok(), "expected valid key: {key}");
        }
        let too_long = "a".repeat(65);
        for key in [
            "", "1tool", "Tool", "tool--x", "tool_", "tool.x", "tool/x", &too_long,
        ] {
            assert!(validate_key(key).is_err(), "expected invalid key: {key}");
        }
    }

    #[test]
    fn validates_utf8_name_length() {
        assert!(validate_name(&"a".repeat(64), "plan").is_ok());
        assert!(validate_name(&"名".repeat(21), "item").is_ok());
        assert!(validate_name(&"a".repeat(65), "plan").is_err());
        assert!(validate_name(&"名".repeat(22), "item").is_err());
    }

    #[test]
    fn accepts_top_level_extensions_and_rejects_unsafe_plan_names() {
        let extended = json!({
            "version": 1,
            "name": "tool.json",
            "key": "tool-v1",
            "items": [],
            "upstream": {"package": "tool"}
        });
        assert!(Plan::from_value(extended).is_ok());

        for name in [".", "..", "../escape", "dir/file", "bad\0name"] {
            let value = json!({
                "version": 1,
                "name": name,
                "key": "tool-v1",
                "items": []
            });
            assert!(
                Plan::from_value(value).is_err(),
                "expected invalid name: {name:?}"
            );
        }
    }

    #[test]
    fn rejects_dot_in_plan_and_download_keys() {
        let mut value = json!({
            "version": 1,
            "name": "example plan",
            "key": "plan.v1",
            "items": []
        });
        let plan: Plan = serde_json::from_value(value.clone()).unwrap();
        assert!(plan.validate().is_err());

        value["key"] = json!("plan-v1");
        value["items"] = json!([{
            "name": "example file",
            "key": "file.v1",
            "kind": {
                "type": "InstallFile",
                "url": "file:///example",
                "to": "example"
            }
        }]);
        let plan: Plan = serde_json::from_value(value).unwrap();
        assert!(plan.validate().is_err());
    }

    #[test]
    fn uses_pascal_case_for_enum_values() {
        let plan: Plan = serde_json::from_value(json!({
            "version": 1,
            "name": "example plan",
            "key": "plan-v1",
            "items": [{
                "name": "example archive",
                "key": "archive-v1",
                "kind": {
                    "type": "UnpackDir",
                    "url": "file:///example.tar.zst",
                    "archive": "TarZstd",
                    "to": "."
                }
            }]
        }))
        .unwrap();
        assert!(plan.validate().is_ok());

        let encoded = serde_json::to_value(plan).unwrap();
        assert_eq!(encoded["items"][0]["kind"]["type"], "UnpackDir");
        assert_eq!(encoded["items"][0]["kind"]["archive"], "TarZstd");
    }

    #[test]
    fn rejects_legacy_snake_case_enum_values() {
        let result = serde_json::from_value::<Plan>(json!({
            "version": 1,
            "name": "example plan",
            "key": "plan-v1",
            "items": [{
                "name": "example file",
                "key": "file-v1",
                "kind": {
                    "type": "install_file",
                    "url": "file:///example",
                    "to": "example"
                }
            }]
        }));
        assert!(result.is_err());
    }

    #[test]
    fn rejects_conflicting_definitions_for_a_global_download_key() {
        let plan: Plan = serde_json::from_value(json!({
            "version": 1,
            "name": "conflicting plan",
            "key": "plan-v1",
            "items": [
                {
                    "name": "first source",
                    "key": "global-download-v1",
                    "kind": {
                        "type": "InstallFile",
                        "url": "file:///first",
                        "to": "first"
                    }
                },
                {
                    "name": "second source",
                    "key": "global-download-v1",
                    "kind": {
                        "type": "InstallFile",
                        "url": "file:///second",
                        "to": "second"
                    }
                }
            ]
        }))
        .unwrap();
        assert!(plan.validate().is_err());
    }
}

// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;

use super::provider::ConfigProvider;
use super::ConfigDocument;
use crate::errors::Result;

/// A [`ConfigProvider`] backed by a YAML file on the local filesystem.
///
/// Each [`load`](ConfigProvider::load) compares the file's modification time
/// against the previously seen one and only re-reads/parses when it has changed
/// (returning `Ok(None)` otherwise). A document missing the top-level `version`
/// key is rejected (logged and treated as "no update"), matching the other
/// Aerospike clients.
///
/// Pass one to [`Client::new_with_config`](crate::Client::new_with_config) to
/// drive dynamic configuration from a chosen path (instead of the
/// `AEROSPIKE_CLIENT_CONFIG_URL` environment variable):
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use aerospike::config::YamlFileProvider;
///
/// let provider = Arc::new(YamlFileProvider::new("/etc/aerospike/config.yaml"));
/// // let client = Client::new_with_config(&policy, &hosts, provider).await?;
/// ```
#[derive(Debug)]
pub struct YamlFileProvider {
    path: PathBuf,
    last_modified: Mutex<Option<SystemTime>>,
}

impl YamlFileProvider {
    /// Creates a provider that reads the YAML config at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            last_modified: Mutex::new(None),
        }
    }

    /// Returns `true` if the file has changed since the last successful read,
    /// updating the stored modification time. A file with no available mtime is
    /// always treated as changed.
    fn changed_since_last(&self, modified: Option<SystemTime>) -> bool {
        let mut last = self.last_modified.lock().expect("yaml provider mutex poisoned");
        match (modified, *last) {
            (Some(m), Some(prev)) if m <= prev => false,
            _ => {
                *last = modified;
                true
            }
        }
    }
}

#[async_trait]
impl ConfigProvider for YamlFileProvider {
    async fn load(&self) -> Result<Option<ConfigDocument>> {
        let metadata = match std::fs::metadata(&self.path) {
            Ok(m) => m,
            Err(err) => {
                warn!("Dynamic-config file {} unavailable: {err}", self.path.display());
                return Ok(None);
            }
        };

        if !self.changed_since_last(metadata.modified().ok()) {
            return Ok(None);
        }

        let data = match std::fs::read_to_string(&self.path) {
            Ok(d) => d,
            Err(err) => {
                warn!("Failed to read dynamic-config file {}: {err}", self.path.display());
                return Ok(None);
            }
        };

        let doc: ConfigDocument = match serde_yml::from_str(&data) {
            Ok(doc) => doc,
            Err(err) => {
                warn!(
                    "Failed to parse dynamic-config file {}: {}",
                    self.path.display(),
                    err.to_string().replace('\n', " ")
                );
                return Ok(None);
            }
        };

        if doc.version.is_none() {
            warn!(
                "Dynamic-config file {} is missing the `version` key; ignoring it",
                self.path.display()
            );
            return Ok(None);
        }

        Ok(Some(doc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_path(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "aerospike-yaml-provider-{}-{tag}.yaml",
            std::process::id()
        ));
        path
    }

    fn write_file(path: &PathBuf, contents: &str) {
        let mut f = std::fs::File::create(path).expect("create temp config");
        f.write_all(contents.as_bytes()).expect("write temp config");
        f.flush().expect("flush temp config");
    }

    // First load returns the parsed document; a subsequent load with no file
    // change returns `None` (mirrors Go's mod-time gate / "loadedConfig is nil").
    #[aerospike_macro::test]
    async fn loads_then_returns_none_when_unchanged() {
        let path = temp_path("unchanged");
        write_file(&path, "version: \"1.0.0\"\ndynamic:\n  read:\n    max_retries: 4\n");
        let provider = YamlFileProvider::new(path.clone());

        let first = provider.load().await.unwrap();
        assert!(first.is_some(), "first load should return the document");
        assert_eq!(first.unwrap().version.as_deref(), Some("1.0.0"));

        let second = provider.load().await.unwrap();
        assert!(second.is_none(), "unchanged file should report no update");

        let _ = std::fs::remove_file(&path);
    }

    // A document missing the top-level `version` key is rejected (logged, ignored).
    #[aerospike_macro::test]
    async fn missing_version_is_ignored() {
        let path = temp_path("noversion");
        write_file(&path, "dynamic:\n  read:\n    max_retries: 4\n");
        let provider = YamlFileProvider::new(path.clone());
        assert!(provider.load().await.unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }

    // A missing file is not an error — the watcher just keeps the current config.
    #[aerospike_macro::test]
    async fn missing_file_is_ignored() {
        let provider = YamlFileProvider::new(temp_path("does-not-exist"));
        assert!(provider.load().await.unwrap().is_none());
    }

    // Malformed YAML is rejected without erroring out (logged, ignored).
    #[aerospike_macro::test]
    async fn malformed_yaml_is_ignored() {
        let path = temp_path("malformed");
        write_file(&path, "version: \"1.0.0\"\ndynamic: : : not valid\n");
        let provider = YamlFileProvider::new(path.clone());
        assert!(provider.load().await.unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }
}

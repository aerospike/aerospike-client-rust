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

// Items here are re-exported from the (private) `config` submodule tree; their
// effective visibility is set by those re-exports, so `pub(crate)` is intentional.
#![allow(clippy::redundant_pub_crate)]

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use super::provider::ConfigProvider;
use super::yaml::YamlFileProvider;

/// Environment variable that enables dynamic configuration for
/// [`Client::new`](crate::Client::new).
///
/// Holds a `scheme://path` DSN, e.g. `file:///etc/aerospike/config.yaml`. When
/// unset/empty, dynamic config is off unless a provider is injected via
/// [`Client::new_with_config`](crate::Client::new_with_config).
pub const CONFIG_URL_ENV: &str = "AEROSPIKE_CLIENT_CONFIG_URL";

/// Scheme assumed when a DSN has no `scheme://` prefix (a bare file path).
const DEFAULT_SCHEME: &str = "file://";

/// Builds a [`ConfigProvider`] for a given DSN path. Registered per URL scheme.
pub type ProviderFactory = Box<dyn Fn(&str) -> Arc<dyn ConfigProvider> + Send + Sync>;

static REGISTRY: LazyLock<Mutex<HashMap<String, ProviderFactory>>> = LazyLock::new(|| {
    let mut m: HashMap<String, ProviderFactory> = HashMap::new();
    // Built-in: a bare path or `file://` DSN maps to the YAML file provider.
    m.insert(
        DEFAULT_SCHEME.to_string(),
        Box::new(|path: &str| Arc::new(YamlFileProvider::new(path)) as Arc<dyn ConfigProvider>),
    );
    Mutex::new(m)
});

/// Registers a provider factory for a URL `scheme`.
///
/// The scheme includes the trailing `://`, e.g. `"http://"`. Replaces any
/// existing factory for that scheme. This is how additional config sources are
/// wired into the env-var path.
///
/// # Panics
///
/// Panics if the internal registry lock is poisoned (a prior panic while holding
/// it), which should not happen in normal operation.
pub fn register_provider(scheme: &str, factory: ProviderFactory) {
    REGISTRY
        .lock()
        .expect("config provider registry poisoned")
        .insert(scheme.to_string(), factory);
}

/// Splits a DSN into `(scheme_with_slashes, path)`. A DSN with no `scheme://`
/// prefix is treated as a bare path with the default `file://` scheme. Empty
/// input yields `None`.
fn parse_dsn(dsn: &str) -> Option<(String, String)> {
    let dsn = dsn.trim();
    if dsn.is_empty() {
        return None;
    }
    Some(dsn.find("://").map_or_else(
        || (DEFAULT_SCHEME.to_string(), dsn.to_string()),
        |idx| {
            let split = idx + 3; // include "://" in the scheme key
            (dsn[..split].to_string(), dsn[split..].to_string())
        },
    ))
}

/// Builds a provider from a `scheme://path` DSN using the registry, or `None` if
/// the DSN is empty or its scheme has no registered factory.
pub(crate) fn build_provider(dsn: &str) -> Option<Arc<dyn ConfigProvider>> {
    let (scheme, path) = parse_dsn(dsn)?;
    let reg = REGISTRY.lock().expect("config provider registry poisoned");
    reg.get(&scheme).map_or_else(
        || {
            warn!("No dynamic-config provider registered for scheme `{scheme}`");
            None
        },
        |factory| Some(factory(&path)),
    )
}

/// Builds a provider from [`CONFIG_URL_ENV`], if it is set and non-empty.
pub(crate) fn provider_from_env() -> Option<Arc<dyn ConfigProvider>> {
    let url = std::env::var(CONFIG_URL_ENV).ok()?;
    if url.trim().is_empty() {
        return None;
    }
    build_provider(&url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dsn_defaults_scheme_to_file() {
        assert_eq!(
            parse_dsn("/etc/aerospike/config.yaml"),
            Some(("file://".to_string(), "/etc/aerospike/config.yaml".to_string()))
        );
    }

    #[test]
    fn parse_dsn_keeps_explicit_scheme() {
        assert_eq!(
            parse_dsn("file:///tmp/c.yaml"),
            Some(("file://".to_string(), "/tmp/c.yaml".to_string()))
        );
        assert_eq!(
            parse_dsn("http://host/c.yaml"),
            Some(("http://".to_string(), "host/c.yaml".to_string()))
        );
    }

    #[test]
    fn parse_dsn_empty_is_none() {
        assert_eq!(parse_dsn("   "), None);
    }

    #[test]
    fn build_provider_resolves_file_scheme() {
        assert!(build_provider("/tmp/aerospike.yaml").is_some());
        assert!(build_provider("unknown://x").is_none());
    }
}

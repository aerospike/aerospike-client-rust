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

use async_trait::async_trait;

use super::ConfigDocument;
use crate::errors::Result;

/// A source of dynamic configuration.
///
/// The built-in [`YamlFileProvider`](super::YamlFileProvider) reads a local file,
/// but the trait is the extension point for other sources (HTTP endpoints, a
/// control-plane service, etc.) — implement it and register a factory with
/// [`register_provider`](super::register_provider) to make it reachable by URL
/// scheme.
///
/// A provider owns its source (path/URL/connection) and tracks whatever state it
/// needs to detect change between loads.
#[async_trait]
pub trait ConfigProvider: Send + Sync + std::fmt::Debug {
    /// Loads the latest configuration.
    ///
    /// Returns `Ok(None)` when the source is unchanged since the previous load (no
    /// work for the caller), `Ok(Some(doc))` with a fresh document otherwise.
    /// Transient/parse problems should be surfaced as `Err` so the watcher can log
    /// and retry on the next tick.
    async fn load(&self) -> Result<Option<ConfigDocument>>;
}

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

// `DynConfig` and its methods are crate-internal but live in a private submodule
// re-exported by `config`; `pub(crate)` is intentional, not redundant.
#![allow(clippy::redundant_pub_crate)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use hazarc::AtomicArc;

use super::{ConfigProvider, DynamicConfig};

/// Holds the live dynamic-configuration state for a [`Cluster`](crate::cluster::Cluster):
/// the provider it loads from and the latest `dynamic` section, swapped
/// atomically so the command hot path reads it lock-free.
///
/// The watcher loop and the static-config application live on `Cluster` (they
/// need cluster state); this type owns only what the command path touches.
#[derive(Debug)]
pub(crate) struct DynConfig {
    provider: Arc<dyn ConfigProvider>,
    dynamic: AtomicArc<DynamicConfig>,
    /// Whether the one-time static section has been applied yet.
    initialized: AtomicBool,
}

impl DynConfig {
    pub(crate) fn new(provider: Arc<dyn ConfigProvider>) -> Self {
        Self {
            provider,
            dynamic: AtomicArc::from(DynamicConfig::default()),
            initialized: AtomicBool::new(false),
        }
    }

    /// The configured provider.
    pub(crate) fn provider(&self) -> &Arc<dyn ConfigProvider> {
        &self.provider
    }

    /// Latest dynamic section (lock-free snapshot).
    pub(crate) fn dynamic(&self) -> Arc<DynamicConfig> {
        self.dynamic.load().clone()
    }

    /// Atomically replaces the dynamic section.
    pub(crate) fn store_dynamic(&self, dynamic: DynamicConfig) {
        self.dynamic.store(Arc::new(dynamic));
    }

    /// Returns whether the static section has been applied, marking it applied.
    /// Used to ensure static config is honored exactly once (at startup).
    pub(crate) fn mark_initialized(&self) -> bool {
        self.initialized.swap(true, Ordering::AcqRel)
    }
}

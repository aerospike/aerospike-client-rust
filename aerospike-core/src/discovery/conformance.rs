// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Seams for the shared conformance runner (`tests/conformance.rs`).
//!
//! The `aerospike-client-conformance` fixtures exercise address translation
//! and the seed refresh cycle as the cluster runs them, without a cluster.
//! Both live behind the `Cluster` type, so this module reaches them the way
//! Java's `ConformanceAdapter` reaches its package-private seams. Hidden:
//! kept reachable for the runner only, not API.

use std::sync::{Arc, Mutex};

use crate::cluster::seed_refresher::{Refresh, SeedRefresher};
use crate::discovery::{
    translate, AddressTranslator, Endpoint, SeedCandidateContext, SeedCandidateProvider,
    StaticMapAddressTranslator,
};
use crate::errors::Result;
use crate::locks::lock;
use crate::net::Host;
use crate::policy::ClientPolicy;

/// The tend interval the refresher is told, so every refresh is due.
const TEND_INTERVAL: u32 = 1000;

/// Translates `advertised` with the translator and miss mode the cluster
/// selects from `policy`: the configured translator, or `static-map` over
/// `ip_map` when none is set.
///
/// # Errors
///
/// A strict miss, as [`translate`] reports it.
pub fn translate_with_policy(policy: &ClientPolicy, advertised: &Endpoint) -> Result<Host> {
    let translator: Arc<dyn AddressTranslator> = policy
        .address_translator
        .clone()
        .unwrap_or_else(|| Arc::new(StaticMapAddressTranslator::new(policy.ip_map.clone())));
    translate(translator.as_ref(), policy.translation_on_miss, advertised)
}

/// A provider wrapper that keeps the inner provider's last answer, so a
/// refresh driven through the refresher can report what the provider
/// returned as well as the seed list that resulted.
struct Recording {
    inner: Arc<dyn SeedCandidateProvider>,
    last: Mutex<Option<Result<Vec<Host>>>>,
}

#[async_trait::async_trait]
impl SeedCandidateProvider for Recording {
    async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        let result = self.inner.refresh_seed_candidates(ctx).await;
        *lock(&self.last) = Some(result.clone());
        result
    }

    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    fn supports_deadline(&self) -> bool {
        true
    }

    fn refresh_timeout(&self) -> Option<std::time::Duration> {
        self.inner.refresh_timeout()
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    fn name(&self) -> String {
        self.inner.name()
    }
}

/// Drives a seed candidate provider through the client's own refresh and
/// merge code, one refresh per call.
///
/// [`initial`](Self::initial) is the refresh at client initialization, each
/// [`refresh`](Self::refresh) a periodic one. The tend deadline is lifted so
/// only the provider's own timeout bounds a refresh.
pub struct Refresher {
    refresher: SeedRefresher,
    recording: Arc<Recording>,
    seeds: Vec<Host>,
    tend_count: u64,
}

impl Refresher {
    /// A refresher over `provider` with `policy`'s merge policy and cluster
    /// name, and `static_seeds` as the configured seeds.
    #[must_use]
    pub fn new(
        policy: &ClientPolicy,
        provider: Arc<dyn SeedCandidateProvider>,
        static_seeds: Vec<Host>,
    ) -> Self {
        let mut policy = policy.clone();
        policy.discovery_refresh_interval = TEND_INTERVAL;
        policy.discovery_tend_refresh_deadline = u32::MAX;
        let recording = Arc::new(Recording {
            inner: provider,
            last: Mutex::new(None),
        });
        Refresher {
            refresher: SeedRefresher::new(recording.clone(), &policy, static_seeds),
            recording,
            seeds: Vec::new(),
            tend_count: 0,
        }
    }

    /// The refresh at client initialization: the provider's answer and the
    /// seed list it establishes.
    pub async fn initial(&mut self) -> (Result<Vec<Host>>, Vec<Host>) {
        let (hosts, error) = self.refresher.initial().await;
        let seeds = self.refresher.merge(&hosts, true);
        self.refresher.start(seeds.clone());
        self.seeds.clone_from(&seeds);
        let result = match error {
            Some(e) => Err(e),
            None => Ok(hosts),
        };
        (result, seeds)
    }

    /// One periodic refresh: the provider's answer and the seed list in
    /// effect afterwards, the previous one when the refresh published
    /// nothing.
    pub async fn refresh(&mut self) -> (Result<Vec<Host>>, Vec<Host>) {
        self.tend_count += 1;
        *lock(&self.recording.last) = None;
        if let Refresh::Publish(seeds) = self.refresher.tend(self.tend_count, TEND_INTERVAL).await
        {
            self.seeds = seeds;
        }
        let result = lock(&self.recording.last)
            .take()
            .unwrap_or_else(|| Ok(Vec::new()));
        (result, self.seeds.clone())
    }

    /// The seed list in effect.
    #[must_use]
    pub fn seeds(&self) -> &[Host] {
        &self.seeds
    }
}

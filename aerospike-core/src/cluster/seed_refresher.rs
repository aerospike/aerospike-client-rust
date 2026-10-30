// Copyright 2015-2026 Aerospike, Inc.
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

//! Periodic seed candidate refresh on the tend task.

#![allow(clippy::redundant_pub_crate)]

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aerospike_rt::time::Instant;

use crate::discovery::{SeedCandidateContext, SeedCandidateProvider, SeedMergePolicy};
use crate::errors::Error;
use crate::locks::lock;
use crate::net::Host;
use crate::policy::ClientPolicy;

/// Consecutive overruns after which the provider is disabled.
pub(crate) const MAX_OVERRUNS: u32 = 3;

/// Calls the seed candidate provider at construction and, when it needs
/// periodic refresh, from the tend task, merging each result into the seed
/// list per the merge policy. Measures every tend refresh against the deadline
/// and disables the provider after three consecutive overruns.
pub(crate) struct SeedRefresher {
    provider: Arc<dyn SeedCandidateProvider>,
    cluster_name: Option<String>,
    static_seeds: Vec<Host>,
    merge_policy: SeedMergePolicy,
    periodic: bool,
    refresh_interval: u32,
    tend_refresh_deadline: u32,
    /// The seed list established at construction, restored when the provider
    /// is disabled.
    init_seeds: Mutex<Vec<Host>>,
    overrun_count: AtomicU32,
    tripped: AtomicBool,
}

impl std::fmt::Debug for SeedRefresher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeedRefresher")
            .field("provider", &self.provider.name())
            .field("merge_policy", &self.merge_policy)
            .field("periodic", &self.periodic)
            .field("tripped", &self.is_tripped())
            .finish_non_exhaustive()
    }
}

/// What one tend refresh decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refresh {
    /// Not due, disabled, failed, overran or merged to nothing: keep the list.
    Keep,
    /// Publish this list.
    Publish(Vec<Host>),
}

impl SeedRefresher {
    pub(crate) fn new(
        provider: Arc<dyn SeedCandidateProvider>,
        policy: &ClientPolicy,
        static_seeds: Vec<Host>,
    ) -> Self {
        SeedRefresher {
            periodic: provider.needs_periodic_refresh(),
            provider,
            cluster_name: policy.cluster_name.clone(),
            static_seeds,
            merge_policy: policy.discovery_merge_policy,
            refresh_interval: policy.discovery_refresh_interval,
            tend_refresh_deadline: policy.discovery_tend_refresh_deadline,
            init_seeds: Mutex::new(Vec::new()),
            overrun_count: AtomicU32::new(0),
            tripped: AtomicBool::new(false),
        }
    }

    pub(crate) fn provider_name(&self) -> String {
        self.provider.name()
    }

    /// Combines the static seeds with `discovered` per the merge policy.
    pub(crate) fn merge(&self, discovered: &[Host], init: bool) -> Vec<Host> {
        self.merge_policy.merge(&self.static_seeds, discovered, init)
    }

    /// Records the seed list established at construction.
    pub(crate) fn start(&self, init_seeds: Vec<Host>) {
        *lock(&self.init_seeds) = init_seeds;
    }

    /// The provider's answer at construction: its hosts, or an empty list
    /// and the error when it failed (logged at warning level).
    pub(crate) async fn initial(&self) -> (Vec<Host>, Option<Error>) {
        let ctx = SeedCandidateContext::new(self.cluster_name.clone(), self.static_seeds.clone(), 0);
        match self.provider.refresh_seed_candidates(&ctx).await {
            Ok(hosts) => (hosts, None),
            Err(e) => {
                warn!(
                    "Seed candidate provider {} failed: {e}",
                    self.provider.name()
                );
                (Vec::new(), Some(e))
            }
        }
    }

    /// One tend iteration: refreshes when due and merges the result.
    pub(crate) async fn tend(&self, tend_count: u64, tend_interval: u32) -> Refresh {
        if !self.periodic || self.is_tripped() {
            return Refresh::Keep;
        }
        let every = u64::from((self.refresh_interval / tend_interval.max(1)).max(1));
        if tend_count % every != 0 {
            return Refresh::Keep;
        }

        let deadline = Duration::from_millis(u64::from(self.tend_refresh_deadline));
        let begin = Instant::now();
        let ctx = SeedCandidateContext::new(
            self.cluster_name.clone(),
            self.static_seeds.clone(),
            tend_count,
        );
        let result = aerospike_rt::timeout(deadline, self.provider.refresh_seed_candidates(&ctx)).await;
        let elapsed = begin.elapsed();

        if result.is_err() || elapsed > deadline {
            let count = self.overrun_count.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "Seed candidate refresh took {}ms, exceeding tend deadline {}ms. Consecutive overruns: {count}",
                elapsed.as_millis(),
                self.tend_refresh_deadline
            );
            if count >= MAX_OVERRUNS {
                self.tripped.store(true, Ordering::Relaxed);
                error!(
                    "Seed candidate provider exceeded tend deadline {count} consecutive times. \
                     Provider disabled for this client and initial seeds restored"
                );
                return Refresh::Publish(lock(&self.init_seeds).clone());
            }
            return Refresh::Keep;
        }
        self.overrun_count.store(0, Ordering::Relaxed);

        match result.expect("checked above") {
            Ok(hosts) => {
                let seeds = self.merge(&hosts, false);
                if seeds.is_empty() {
                    Refresh::Keep
                } else {
                    Refresh::Publish(seeds)
                }
            }
            Err(e) => {
                warn!("Seed candidate refresh failed: {e}");
                Refresh::Keep
            }
        }
    }

    /// Closes the provider. Call once, after the last `tend` has returned.
    pub(crate) async fn close(&self) {
        if let Err(e) = self.provider.close().await {
            warn!(
                "Seed candidate provider {} close failed: {e}",
                self.provider.name()
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn overrun_count(&self) -> u32 {
        self.overrun_count.load(Ordering::Relaxed)
    }

    pub(crate) fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::errors::Result;

    fn h(name: &str) -> Host {
        Host::new(name, 3000)
    }

    /// A provider whose every call is scripted: hosts, an error, or a sleep
    /// past the deadline.
    struct Scripted {
        calls: AtomicUsize,
        script: Vec<Step>,
        closed: AtomicUsize,
        contexts: Mutex<Vec<SeedCandidateContext>>,
    }

    #[derive(Clone)]
    enum Step {
        Hosts(Vec<Host>),
        Fail,
        Slow(u64),
    }

    #[async_trait::async_trait]
    impl SeedCandidateProvider for Scripted {
        async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
            lock(&self.contexts).push(ctx.clone());
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.script.get(n).cloned().unwrap_or(Step::Hosts(vec![])) {
                Step::Hosts(hosts) => Ok(hosts),
                Step::Fail => Err(Error::client_error("scripted failure")),
                Step::Slow(ms) => {
                    aerospike_rt::sleep(Duration::from_millis(ms)).await;
                    Ok(vec![h("late")])
                }
            }
        }
        fn needs_periodic_refresh(&self) -> bool {
            true
        }
        fn supports_deadline(&self) -> bool {
            true
        }
        async fn close(&self) -> Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn scripted(script: Vec<Step>) -> Arc<Scripted> {
        Arc::new(Scripted {
            calls: AtomicUsize::new(0),
            script,
            closed: AtomicUsize::new(0),
            contexts: Mutex::new(Vec::new()),
        })
    }

    fn policy(interval: u32, deadline: u32) -> ClientPolicy {
        ClientPolicy {
            tend_interval: 1000,
            discovery_refresh_interval: interval,
            discovery_tend_refresh_deadline: deadline,
            ..ClientPolicy::default()
        }
    }

    #[aerospike_macro::test]
    async fn calls_provider_once_per_refresh_interval_in_tends() {
        let provider = scripted(vec![Step::Hosts(vec![h("d1")]), Step::Hosts(vec![h("d2")]), Step::Hosts(vec![h("d3")])]);
        let r = SeedRefresher::new(provider.clone(), &policy(30_000, 100), vec![h("s1")]);
        r.start(vec![h("s1")]);
        for tend in 1..30 {
            assert_eq!(r.tend(tend, 1000).await, Refresh::Keep, "tend {tend}");
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(r.tend(30, 1000).await, Refresh::Publish(vec![h("s1"), h("d1")]));
        assert_eq!(r.tend(60, 1000).await, Refresh::Publish(vec![h("s1"), h("d2")]));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        let contexts = lock(&provider.contexts);
        assert_eq!(contexts[0].tend_iteration, 30);
        assert_eq!(contexts[0].configured_seeds, vec![h("s1")]);
        assert_eq!(contexts[1].tend_iteration, 60);
    }

    #[aerospike_macro::test]
    async fn a_failing_or_empty_refresh_keeps_the_list() {
        let provider = scripted(vec![Step::Fail, Step::Hosts(vec![])]);
        let p = ClientPolicy {
            discovery_merge_policy: SeedMergePolicy::Replace,
            ..policy(1000, 100)
        };
        let r = SeedRefresher::new(provider.clone(), &p, vec![h("s1")]);
        r.start(vec![h("s1")]);
        assert_eq!(r.tend(1, 1000).await, Refresh::Keep);
        assert_eq!(r.overrun_count(), 0);
        // REPLACE after init with an empty result: nothing to publish.
        assert_eq!(r.tend(2, 1000).await, Refresh::Keep);
    }

    #[aerospike_macro::test]
    async fn an_overrun_discards_the_result_and_three_trip_the_provider() {
        let provider = scripted(vec![
            Step::Slow(300),
            Step::Hosts(vec![h("d1")]),
            Step::Slow(300),
            Step::Slow(300),
            Step::Slow(300),
            Step::Hosts(vec![h("never")]),
        ]);
        let r = SeedRefresher::new(provider.clone(), &policy(1000, 100), vec![h("s1")]);
        r.start(vec![h("s1"), h("init")]);
        let started = Instant::now();
        assert_eq!(r.tend(1, 1000).await, Refresh::Keep);
        // The refresh is cancelled at the deadline, not waited out.
        assert!(started.elapsed() < Duration::from_millis(250), "{:?}", started.elapsed());
        assert_eq!(r.overrun_count(), 1);
        // A good refresh resets the count.
        assert_eq!(r.tend(2, 1000).await, Refresh::Publish(vec![h("s1"), h("d1")]));
        assert_eq!(r.overrun_count(), 0);
        assert_eq!(r.tend(3, 1000).await, Refresh::Keep);
        assert_eq!(r.tend(4, 1000).await, Refresh::Keep);
        assert!(!r.is_tripped());
        // The third consecutive overrun trips: the init list comes back.
        assert_eq!(r.tend(5, 1000).await, Refresh::Publish(vec![h("s1"), h("init")]));
        assert!(r.is_tripped());
        // Disabled for good: the provider is not called again.
        assert_eq!(r.tend(6, 1000).await, Refresh::Keep);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
        r.close().await;
        assert_eq!(provider.closed.load(Ordering::SeqCst), 1);
    }

    #[aerospike_macro::test]
    async fn merge_policies_apply_on_refresh() {
        for (policy_kind, expected) in [
            (SeedMergePolicy::Merge, vec![h("s1"), h("d1")]),
            (SeedMergePolicy::Replace, vec![h("d1")]),
            (SeedMergePolicy::DiscoveryOnly, vec![h("d1")]),
        ] {
            let provider = scripted(vec![Step::Hosts(vec![h("d1")])]);
            let p = ClientPolicy {
                discovery_merge_policy: policy_kind,
                ..policy(1000, 100)
            };
            let r = SeedRefresher::new(provider, &p, vec![h("s1")]);
            r.start(vec![h("s1")]);
            assert_eq!(r.tend(1, 1000).await, Refresh::Publish(expected), "{policy_kind:?}");
        }
    }

    #[aerospike_macro::test]
    async fn initial_failure_is_an_empty_result_with_the_error() {
        let provider = scripted(vec![Step::Fail]);
        let r = SeedRefresher::new(provider.clone(), &policy(1000, 100), vec![h("s1")]);
        let (hosts, err) = r.initial().await;
        assert!(hosts.is_empty());
        assert!(err.is_some());
        assert_eq!(lock(&provider.contexts)[0].tend_iteration, 0);
        assert_eq!(r.merge(&hosts, true), vec![h("s1")]);
        assert_eq!(r.provider_name(), provider.name());
    }

    #[aerospike_macro::test]
    async fn non_periodic_provider_does_no_tend_work() {
        let r = SeedRefresher::new(
            Arc::new(crate::discovery::StaticSeedCandidateProvider::new(vec![h("x")])),
            &policy(1000, 100),
            vec![h("s1")],
        );
        assert_eq!(r.tend(1, 1000).await, Refresh::Keep);
        assert_eq!(r.tend(1000, 1000).await, Refresh::Keep);
    }
}

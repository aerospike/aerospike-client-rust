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

//! Pluggable seed discovery and address translation.
//!
//! Two plugin points decide which addresses the client connects to:
//!
//! - A [`SeedCandidateProvider`] returns the seed hosts the client tries when
//!   it initializes the cluster and when it re-seeds because no node is
//!   active. The built-in providers are [`StaticSeedCandidateProvider`] (the
//!   hosts passed to [`Client::new`](crate::Client::new), the default),
//!   [`DnsSeedCandidateProvider`](dns::DnsSeedCandidateProvider) and, with the
//!   `discovery-aws` feature,
//!   [`AwsEc2TagSeedCandidateProvider`](aws::AwsEc2TagSeedCandidateProvider).
//!   [`ClientPolicy::discovery_merge_policy`](crate::ClientPolicy::discovery_merge_policy)
//!   combines the discovered seeds with the static ones.
//! - An [`AddressTranslator`] rewrites each address a server advertises into
//!   the address the client connects to. The built-in translators are
//!   [`StaticMapAddressTranslator`] (host to host, the default, built from
//!   [`ClientPolicy::ip_map`](crate::ClientPolicy::ip_map)),
//!   [`PortMapAddressTranslator`] (`host:port` or node name to `host:port`)
//!   and [`PassThroughAddressTranslator`] (every address unchanged). A miss
//!   is handled by
//!   [`ClientPolicy::translation_on_miss`](crate::ClientPolicy::translation_on_miss).
//!
//! Both are set on the [`ClientPolicy`](crate::ClientPolicy) in code, or
//! selected from the YAML configuration file under `static.client.discovery`
//! and `static.client.translation` (see the `dynamic-config` feature and
//! [`ClientPolicy::register_seed_candidate_provider`](crate::ClientPolicy::register_seed_candidate_provider)).
//!
//! # Threads and deadlines
//!
//! The client starts no task for discovery. A provider whose seeds can change
//! ([`needs_periodic_refresh`](SeedCandidateProvider::needs_periodic_refresh))
//! is called from the cluster tend task every
//! [`discovery_refresh_interval`](crate::ClientPolicy::discovery_refresh_interval)
//! and must finish within
//! [`discovery_tend_refresh_deadline`](crate::ClientPolicy::discovery_tend_refresh_deadline):
//! the refresh future is cancelled at the deadline, its result discarded, and
//! after three consecutive overruns the provider is disabled for the life of
//! the client and the seed list of initialization restored. A translator runs
//! on the tend task as well, in memory only: no I/O of any kind, including
//! name resolution.
//!
//! # Writing a provider
//!
//! ```rust,edition2021
//! use std::sync::{Arc, RwLock};
//! use aerospike::discovery::{SeedCandidateContext, SeedCandidateProvider};
//! use aerospike::{ClientPolicy, Host, Result};
//!
//! /// Seeds the application updates from its own service registry.
//! struct HostListProvider {
//!     hosts: RwLock<Vec<Host>>,
//! }
//!
//! #[async_trait::async_trait]
//! impl SeedCandidateProvider for HostListProvider {
//!     async fn refresh_seed_candidates(&self, _ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
//!         Ok(self.hosts.read().unwrap().clone())
//!     }
//!
//!     // The list changes, so the client re-reads it on the tend task...
//!     fn needs_periodic_refresh(&self) -> bool {
//!         true
//!     }
//!
//!     // ...which is allowed because the refresh does no I/O.
//!     fn supports_deadline(&self) -> bool {
//!         true
//!     }
//! }
//!
//! let provider = Arc::new(HostListProvider {
//!     hosts: RwLock::new(vec![Host::new("192.0.2.1", 3000)]),
//! });
//! let mut policy = ClientPolicy::default();
//! policy.seed_candidate_provider = Some(provider.clone());
//! ```
//!
//! # Writing a translator
//!
//! ```rust,edition2021
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use aerospike::discovery::{AddressTranslator, Endpoint, TranslationMissMode};
//! use aerospike::{ClientPolicy, Host};
//!
//! /// Maps an advertised `host:port` to the `host:port` to connect to.
//! struct NatTable(HashMap<String, Host>);
//!
//! impl AddressTranslator for NatTable {
//!     fn translate(&self, advertised: &Endpoint) -> Option<Host> {
//!         self.0.get(&format!("{}:{}", advertised.host, advertised.port)).cloned()
//!     }
//! }
//!
//! let mut table = HashMap::new();
//! table.insert("10.0.0.1:3000".to_string(), Host::new("lb.example.com", 30001));
//! let mut policy = ClientPolicy::default();
//! policy.address_translator = Some(Arc::new(NatTable(table)));
//! // A node whose advertised address is not in the table is not connected.
//! policy.translation_on_miss = TranslationMissMode::Strict;
//! ```

#[cfg(feature = "discovery-aws")]
pub mod aws;
// Test hook for the shared conformance fixtures (`tests/conformance.rs`):
// reaches the cluster's translation and seed-refresh seams without a
// cluster. Not API.
#[doc(hidden)]
pub mod conformance;
pub mod dns;

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::{Error, Result};
use crate::net::Host;

/// Name of the built-in static seed provider, as the YAML `provider` key
/// spells it.
pub const STATIC_PROVIDER: &str = "static";
/// Name of the built-in DNS seed provider.
pub const DNS_PROVIDER: &str = "dns";
/// Name of the built-in AWS EC2 tag seed provider.
pub const AWS_TAGS_PROVIDER: &str = "aws-tags";
/// The built-in seed provider names, which cannot be registered.
pub const BUILT_IN_PROVIDERS: [&str; 3] = [STATIC_PROVIDER, DNS_PROVIDER, AWS_TAGS_PROVIDER];

/// Name of the built-in static-map translator.
pub const STATIC_MAP_TRANSLATOR: &str = "static-map";
/// Name of the built-in port-map translator.
pub const PORT_MAP_TRANSLATOR: &str = "port-map";
/// Name of the built-in pass-through translator.
pub const PASS_THROUGH_TRANSLATOR: &str = "pass-through";
/// The built-in translator names, which cannot be registered.
pub const BUILT_IN_TRANSLATORS: [&str; 3] = [
    STATIC_MAP_TRANSLATOR,
    PORT_MAP_TRANSLATOR,
    PASS_THROUGH_TRANSLATOR,
];

/// What a [`SeedCandidateProvider`] is told about the call it is answering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedCandidateContext {
    /// Expected cluster name from
    /// [`ClientPolicy::cluster_name`](crate::ClientPolicy::cluster_name).
    pub cluster_name: Option<String>,
    /// The static seed hosts passed to the client constructor.
    pub configured_seeds: Vec<Host>,
    /// Cluster tend count when the provider is called: 0 at client
    /// initialization, then the number of completed tend iterations.
    /// Informational only.
    pub tend_iteration: u64,
}

impl SeedCandidateContext {
    /// Builds a context.
    #[must_use]
    pub fn new(
        cluster_name: Option<String>,
        configured_seeds: impl Into<Vec<Host>>,
        tend_iteration: u64,
    ) -> Self {
        SeedCandidateContext {
            cluster_name,
            configured_seeds: configured_seeds.into(),
            tend_iteration,
        }
    }
}

/// Source of the seed hosts the client tries when it initializes the cluster
/// and when it re-seeds because no node is active.
///
/// See the [module documentation](self) for the threading rules and an
/// example. Hosts returned are used as seeds, so they must carry the TLS name
/// when TLS is enabled; a missing TLS name is defaulted the same way as for
/// the configured seeds (the cluster name when set, otherwise the host name).
#[async_trait::async_trait]
pub trait SeedCandidateProvider: Send + Sync {
    /// Returns the current seed candidates.
    ///
    /// Called once at client initialization with
    /// [`tend_iteration`](SeedCandidateContext::tend_iteration) 0, before the
    /// cluster is seeded, and on each periodic refresh from the cluster tend
    /// task. An error at initialization is logged at warning level and the
    /// client continues with the seeds left by the merge policy; when none
    /// are left, construction fails with
    /// [`ClientResultCode::SeedDiscoveryFailed`](crate::ClientResultCode::SeedDiscoveryFailed).
    /// An error on a periodic refresh is logged at warning level and the
    /// previous seed list is kept.
    async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>>;

    /// Whether the candidates can change and must be refreshed periodically.
    /// A provider returning `false` is called once, at construction, and the
    /// client schedules no periodic work for it. Called once, at
    /// construction.
    fn needs_periodic_refresh(&self) -> bool;

    /// Whether [`refresh_seed_candidates`](Self::refresh_seed_candidates)
    /// honours a deadline: it either does no I/O or bounds all of its I/O so
    /// that it returns within a caller-supplied time limit. Only such a
    /// provider may need periodic refresh. Called once, at construction.
    fn supports_deadline(&self) -> bool;

    /// The longest one refresh can take, when the provider bounds its I/O by
    /// a configured timeout. Checked at client construction against
    /// [`ClientPolicy::discovery_tend_refresh_deadline`](crate::ClientPolicy::discovery_tend_refresh_deadline)
    /// for a provider that needs periodic refresh: a timeout above the
    /// deadline fails construction. `None` (the default) skips the check.
    fn refresh_timeout(&self) -> Option<Duration> {
        None
    }

    /// Releases the provider's resources. Called once after
    /// [`Client::close`](crate::Client::close), from the tend task as it
    /// exits, after the last refresh has returned. An error is logged at
    /// warning level. A provider shared by several clients is closed by each
    /// of them. The default does nothing.
    async fn close(&self) -> Result<()> {
        Ok(())
    }

    /// The name the client logs for this provider: the YAML name of a
    /// built-in provider, the type name otherwise.
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }
}

impl fmt::Debug for dyn SeedCandidateProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SeedCandidateProvider")
            .field(&self.name())
            .finish()
    }
}

/// Two providers are equal when they are the same object: a policy holding
/// one compares by identity, as it does for its TLS configuration.
impl PartialEq for dyn SeedCandidateProvider {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(self, other)
    }
}

/// Rewrites each address a server advertises into the address the client
/// connects to.
///
/// See the [module documentation](self) for the threading rules and an
/// example. The TLS name is never rewritten: the client always uses the
/// advertised endpoint's TLS name, whatever the returned host carries.
pub trait AddressTranslator: Send + Sync {
    /// Returns the host to connect to for `advertised`, or `None` when the
    /// translator has no mapping for it (a miss). `Some` is a hit, even when
    /// it equals the advertised host and port. The client applies
    /// [`ClientPolicy::translation_on_miss`](crate::ClientPolicy::translation_on_miss)
    /// to a miss.
    ///
    /// Runs on the cluster tend task and must be in memory only: no I/O of
    /// any kind, including name resolution.
    fn translate(&self, advertised: &Endpoint) -> Option<Host>;

    /// Notifies the translator that the cluster view changed, so it can drop
    /// cached state. The client keeps one counter per client instance,
    /// starting at 0, and adds one each time a peers refresh adds or removes
    /// a node or changes the address a node advertises; the view established
    /// at initialization is the starting view and is not notified. Runs on
    /// the tend task, in memory only. The default does nothing.
    fn on_cluster_view_changed(&self, view_change: u32) {
        let _ = view_change;
    }

    /// The name the client logs for this translator: the YAML name of a
    /// built-in translator, the type name otherwise.
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }
}

impl fmt::Debug for dyn AddressTranslator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AddressTranslator")
            .field(&self.name())
            .finish()
    }
}

/// Two translators are equal when they are the same object.
impl PartialEq for dyn AddressTranslator {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(self, other)
    }
}

/// Info command list an advertised address was returned by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceList {
    /// The standard lists: `peers-clear-std`, `peers-tls-std`,
    /// `service-clear-std`, `service-tls-std`.
    Standard,
    /// The alternate lists: `peers-clear-alt`, `peers-tls-alt`,
    /// `service-clear-alt`, `service-tls-alt`, set by the server's
    /// `alternate-access-address` and `alternate-access-port`. Used when
    /// [`ClientPolicy::use_services_alternate`](crate::ClientPolicy::use_services_alternate)
    /// is set.
    Alternate,
}

/// An address a server advertised, as handed to an [`AddressTranslator`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    /// Name of the node that advertised the address.
    pub node_name: String,
    /// TLS certificate name of the node, if any.
    pub tls_name: Option<String>,
    /// The advertised host name or IP address.
    pub host: String,
    /// The advertised port.
    pub port: u16,
    /// The info command list the address came from.
    pub source_list: SourceList,
}

impl Endpoint {
    /// Builds an endpoint.
    #[must_use]
    pub fn new(
        node_name: impl Into<String>,
        tls_name: Option<String>,
        host: impl Into<String>,
        port: u16,
        source_list: SourceList,
    ) -> Self {
        Endpoint {
            node_name: node_name.into(),
            tls_name,
            host: host.into(),
            port,
            source_list,
        }
    }

    /// The advertised address as a [`Host`] carrying the advertised TLS name.
    #[must_use]
    pub fn to_host(&self) -> Host {
        Host {
            name: self.host.clone(),
            tls_name: self.tls_name.clone(),
            port: self.port,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}:{} {:?}",
            self.node_name, self.host, self.port, self.source_list
        )
    }
}

/// What the client does when the address translator has no mapping for an
/// advertised endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TranslationMissMode {
    /// Connect to the advertised endpoint unchanged. YAML value:
    /// `pass_through`. The default.
    #[default]
    PassThrough,
    /// Do not connect to the advertised endpoint; the miss fails with
    /// [`ErrorKind::TranslationMiss`](crate::ErrorKind::TranslationMiss).
    /// A missed peer is logged at warning level and retried on the next
    /// tend. YAML value: `strict`.
    Strict,
}

impl TranslationMissMode {
    /// The YAML spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TranslationMissMode::PassThrough => "pass_through",
            TranslationMissMode::Strict => "strict",
        }
    }

    /// The YAML spellings, for error messages.
    pub const VALUES: [&'static str; 2] = ["pass_through", "strict"];
}

impl FromStr for TranslationMissMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "pass_through" => Ok(TranslationMissMode::PassThrough),
            "strict" => Ok(TranslationMissMode::Strict),
            other => Err(Error::invalid_argument(format!(
                "Invalid translation on_miss value '{other}': valid values: {}",
                Self::VALUES.join(", ")
            ))),
        }
    }
}

impl fmt::Display for TranslationMissMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How the static and the discovered seeds are combined into the seed list.
///
/// The static seeds are the hosts passed to the client constructor, the
/// discovered seeds the hosts the provider returns. Duplicate hosts (same
/// name and port) are removed, keeping the first occurrence. The policy
/// orders seed attempts only; it does not change node identity. Without a
/// configured provider, every policy yields the static seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SeedMergePolicy {
    /// Static seeds first, then discovered seeds. An empty discovery result
    /// leaves only the static seeds. For moving an existing deployment to
    /// discovery: the static seeds stay a known-good endpoint while discovery
    /// catches up. The default. YAML value: `merge`.
    #[default]
    Merge,
    /// At initialization, as [`Merge`](Self::Merge). Afterwards the discovered
    /// seeds of each applied refresh replace the list and the static seeds are
    /// no longer tried; an empty result keeps the previous list. For a new
    /// cloud deployment where the static seeds only bootstrap the client.
    /// YAML value: `replace`.
    Replace,
    /// Discovered seeds only; the static seeds when discovery returns none,
    /// at initialization or on a refresh. For static seeds that are an
    /// emergency fallback only. YAML value: `discovery_only`.
    DiscoveryOnly,
}

impl SeedMergePolicy {
    /// The YAML spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SeedMergePolicy::Merge => "merge",
            SeedMergePolicy::Replace => "replace",
            SeedMergePolicy::DiscoveryOnly => "discovery_only",
        }
    }

    /// The YAML spellings, for error messages.
    pub const VALUES: [&'static str; 3] = ["merge", "replace", "discovery_only"];

    /// Combines `static_seeds` and `discovered` seeds for this policy.
    /// `init` is true for the call at client initialization.
    #[must_use]
    pub fn merge(self, static_seeds: &[Host], discovered: &[Host], init: bool) -> Vec<Host> {
        match self {
            SeedMergePolicy::Replace if !init => distinct(&[discovered]),
            SeedMergePolicy::DiscoveryOnly => {
                if discovered.is_empty() {
                    distinct(&[static_seeds])
                } else {
                    distinct(&[discovered])
                }
            }
            SeedMergePolicy::Merge | SeedMergePolicy::Replace => {
                distinct(&[static_seeds, discovered])
            }
        }
    }
}

/// The hosts of `lists`, in order, without a second occurrence of the same
/// name and port (the TLS name does not take part, as in Java).
fn distinct(lists: &[&[Host]]) -> Vec<Host> {
    let mut out: Vec<Host> = Vec::new();
    for list in lists {
        for host in *list {
            if !out
                .iter()
                .any(|h| h.name == host.name && h.port == host.port)
            {
                out.push(host.clone());
            }
        }
    }
    out
}

impl FromStr for SeedMergePolicy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "merge" => Ok(SeedMergePolicy::Merge),
            "replace" => Ok(SeedMergePolicy::Replace),
            "discovery_only" => Ok(SeedMergePolicy::DiscoveryOnly),
            other => Err(Error::invalid_argument(format!(
                "Invalid discovery merge_policy value '{other}': valid values: {}",
                Self::VALUES.join(", ")
            ))),
        }
    }
}

impl fmt::Display for SeedMergePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Applies `translator` to `advertised` and the miss mode to its answer: the
/// host to connect to, always with the advertised TLS name.
///
/// # Errors
///
/// [`ErrorKind::TranslationMiss`](crate::ErrorKind::TranslationMiss) on a
/// miss under [`TranslationMissMode::Strict`].
pub fn translate(
    translator: &dyn AddressTranslator,
    on_miss: TranslationMissMode,
    advertised: &Endpoint,
) -> Result<Host> {
    match translator.translate(advertised) {
        Some(mut host) => {
            if host.tls_name != advertised.tls_name {
                host.tls_name.clone_from(&advertised.tls_name);
            }
            Ok(host)
        }
        None => match on_miss {
            TranslationMissMode::Strict => {
                Err(Error::translation_miss(advertised, &translator.name()))
            }
            TranslationMissMode::PassThrough => Ok(advertised.to_host()),
        },
    }
}

// ---------------------------------------------------------------------------
// Built-in providers and translators
// ---------------------------------------------------------------------------

/// Seed candidate provider over a fixed list of hosts.
///
/// The default, over the hosts passed to the client constructor, when
/// [`ClientPolicy::seed_candidate_provider`](crate::ClientPolicy::seed_candidate_provider)
/// is not set. Called once, at construction, and never on the tend task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticSeedCandidateProvider {
    hosts: Vec<Host>,
}

impl StaticSeedCandidateProvider {
    /// Builds a provider over `hosts`.
    #[must_use]
    pub fn new(hosts: impl Into<Vec<Host>>) -> Self {
        StaticSeedCandidateProvider {
            hosts: hosts.into(),
        }
    }

    /// The fixed hosts.
    #[must_use]
    pub fn hosts(&self) -> &[Host] {
        &self.hosts
    }
}

#[async_trait::async_trait]
impl SeedCandidateProvider for StaticSeedCandidateProvider {
    async fn refresh_seed_candidates(&self, _ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        Ok(self.hosts.clone())
    }

    /// `false`: the hosts never change.
    fn needs_periodic_refresh(&self) -> bool {
        false
    }

    /// `true`: the provider does no I/O.
    fn supports_deadline(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        STATIC_PROVIDER.to_string()
    }
}

/// Address translator mapping an advertised host name to another host name.
///
/// The port is kept. The default, over
/// [`ClientPolicy::ip_map`](crate::ClientPolicy::ip_map), when
/// [`ClientPolicy::address_translator`](crate::ClientPolicy::address_translator)
/// is not set. YAML: `provider: static-map` with `static_map`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticMapAddressTranslator {
    ip_map: Option<HashMap<String, String>>,
}

impl StaticMapAddressTranslator {
    /// Builds a translator over `ip_map`; `None` misses every endpoint.
    #[must_use]
    pub const fn new(ip_map: Option<HashMap<String, String>>) -> Self {
        StaticMapAddressTranslator { ip_map }
    }

    /// The map.
    #[must_use]
    pub const fn ip_map(&self) -> Option<&HashMap<String, String>> {
        self.ip_map.as_ref()
    }
}

impl AddressTranslator for StaticMapAddressTranslator {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        let mapped = self.ip_map.as_ref()?.get(&advertised.host)?;
        Some(Host {
            name: mapped.clone(),
            tls_name: advertised.tls_name.clone(),
            port: advertised.port,
        })
    }

    fn name(&self) -> String {
        STATIC_MAP_TRANSLATOR.to_string()
    }
}

/// Address translator returning every advertised address unchanged.
///
/// For servers that advertise a client-reachable address and port per node,
/// as with `use_services_alternate` and `alternate-access-port`. Never
/// misses. YAML: `provider: pass-through`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PassThroughAddressTranslator;

impl AddressTranslator for PassThroughAddressTranslator {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        Some(advertised.to_host())
    }

    fn name(&self) -> String {
        PASS_THROUGH_TRANSLATOR.to_string()
    }
}

/// Address translator mapping an advertised `host:port` or node name to a
/// `host:port`.
///
/// An IPv6 host in a key is written in brackets (`[2001:db8::1]:3000`). A
/// `host:port` key takes precedence over a node name key. YAML:
/// `provider: port-map` with `port_map`.
///
/// On AWS, a Network Load Balancer has at most 50 listeners, each serving one
/// port, so a layout giving every node its own port on one endpoint reaches
/// at most 50 nodes per endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortMapAddressTranslator {
    endpoints: HashMap<String, Host>,
    nodes: HashMap<String, Host>,
}

impl PortMapAddressTranslator {
    /// Builds a translator from `port_map`. Every value, and every key holding
    /// a colon, must be a `host:port` with the port in 1 to 65535.
    ///
    /// # Errors
    ///
    /// An invalid entry fails with a message naming it.
    pub fn new<K, V>(port_map: impl IntoIterator<Item = (K, V)>) -> Result<Self>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut endpoints = HashMap::new();
        let mut nodes = HashMap::new();
        for (key, value) in port_map {
            let (key, value) = (key.as_ref(), value.as_ref());
            if key.is_empty() {
                return Err(invalid_entry(key, value, "key is empty"));
            }
            let target = parse_host_port(key, value, value)?;
            if key.contains(':') {
                let advertised = parse_host_port(key, value, key)?;
                if endpoints
                    .insert(endpoint_key(&advertised.name, advertised.port), target)
                    .is_some()
                {
                    return Err(invalid_entry(key, value, "duplicates another key"));
                }
            } else {
                nodes.insert(key.to_string(), target);
            }
        }
        Ok(PortMapAddressTranslator { endpoints, nodes })
    }
}

fn endpoint_key(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn invalid_entry(key: &str, value: &str, reason: &str) -> Error {
    Error::invalid_argument(format!("Invalid port-map entry {key}: {value}: {reason}"))
}

/// Parses `address` (`host:port`, IPv6 host in brackets) for the entry
/// `key: value`.
fn parse_host_port(key: &str, value: &str, address: &str) -> Result<Host> {
    let Some(index) = address.rfind(':') else {
        return Err(invalid_entry(key, value, &format!("{address} is not host:port")));
    };
    let mut host = &address[..index];
    if let Some(inner) = host.strip_prefix('[') {
        host = inner.strip_suffix(']').ok_or_else(|| {
            invalid_entry(
                key,
                value,
                &format!("{address} has an invalid bracketed IPv6 host"),
            )
        })?;
    } else if host.contains(':') {
        return Err(invalid_entry(
            key,
            value,
            &format!("{address} has an IPv6 host that is not in brackets"),
        ));
    }
    if host.is_empty() {
        return Err(invalid_entry(key, value, &format!("{address} has an empty host")));
    }
    let port_str = &address[index + 1..];
    let port: i64 = port_str.parse().map_err(|_| {
        invalid_entry(
            key,
            value,
            &format!("{address} has an invalid port {port_str}"),
        )
    })?;
    if !(1..=65535).contains(&port) {
        return Err(invalid_entry(
            key,
            value,
            &format!("{address} has port {port} out of range 1-65535"),
        ));
    }
    Ok(Host::new(host, port as u16))
}

impl AddressTranslator for PortMapAddressTranslator {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        let target = self
            .endpoints
            .get(&endpoint_key(&advertised.host, advertised.port))
            .or_else(|| self.nodes.get(&advertised.node_name))?;
        Some(Host {
            name: target.name.clone(),
            tls_name: advertised.tls_name.clone(),
            port: target.port,
        })
    }

    fn name(&self) -> String {
        PORT_MAP_TRANSLATOR.to_string()
    }
}

/// A shared provider handle, as [`ClientPolicy`](crate::ClientPolicy) holds
/// it.
pub type SharedSeedCandidateProvider = Arc<dyn SeedCandidateProvider>;
/// A shared translator handle, as [`ClientPolicy`](crate::ClientPolicy) holds
/// it.
pub type SharedAddressTranslator = Arc<dyn AddressTranslator>;

#[cfg(test)]
mod tests {
    use super::*;

    fn h(name: &str, port: u16) -> Host {
        Host::new(name, port)
    }

    fn ep(node: &str, host: &str, port: u16) -> Endpoint {
        Endpoint::new(node, None, host, port, SourceList::Standard)
    }

    fn statics() -> Vec<Host> {
        vec![h("s1", 3000), h("s2", 3000)]
    }

    fn discovered() -> Vec<Host> {
        // The second one equals s2 by name and port but carries a TLS name:
        // dedup ignores the TLS name and keeps the first occurrence.
        vec![h("d1", 3000), Host::new_tls("s2", "tls", 3000), h("d2", 3000)]
    }

    #[test]
    fn merge_order() {
        let p = SeedMergePolicy::Merge;
        let expected = vec![h("s1", 3000), h("s2", 3000), h("d1", 3000), h("d2", 3000)];
        assert_eq!(p.merge(&statics(), &discovered(), true), expected);
        assert_eq!(p.merge(&statics(), &discovered(), false), expected);
        // The static copy of s2 wins, so no TLS name.
        assert_eq!(p.merge(&statics(), &discovered(), true)[1].tls_name, None);
        assert_eq!(p.merge(&statics(), &[], true), statics());
        assert_eq!(p.merge(&statics(), &[], false), statics());
    }

    #[test]
    fn replace_order() {
        let p = SeedMergePolicy::Replace;
        assert_eq!(
            p.merge(&statics(), &discovered(), true),
            vec![h("s1", 3000), h("s2", 3000), h("d1", 3000), h("d2", 3000)]
        );
        let after = p.merge(&statics(), &discovered(), false);
        assert_eq!(
            after,
            vec![h("d1", 3000), Host::new_tls("s2", "tls", 3000), h("d2", 3000)]
        );
        assert_eq!(after[1].tls_name.as_deref(), Some("tls"));
        assert_eq!(p.merge(&statics(), &[], true), statics());
        assert!(p.merge(&statics(), &[], false).is_empty());
    }

    #[test]
    fn discovery_only_order() {
        let p = SeedMergePolicy::DiscoveryOnly;
        let expected = vec![h("d1", 3000), Host::new_tls("s2", "tls", 3000), h("d2", 3000)];
        assert_eq!(p.merge(&statics(), &discovered(), true), expected);
        assert_eq!(p.merge(&statics(), &discovered(), false), expected);
        assert_eq!(p.merge(&statics(), &[], true), statics());
        assert_eq!(p.merge(&statics(), &[], false), statics());
    }

    #[test]
    fn policy_and_miss_mode_parse_their_yaml_names() {
        assert_eq!("merge".parse::<SeedMergePolicy>().unwrap(), SeedMergePolicy::Merge);
        assert_eq!(
            "replace".parse::<SeedMergePolicy>().unwrap(),
            SeedMergePolicy::Replace
        );
        assert_eq!(
            "discovery_only".parse::<SeedMergePolicy>().unwrap(),
            SeedMergePolicy::DiscoveryOnly
        );
        let err = "MERGE".parse::<SeedMergePolicy>().unwrap_err();
        assert!(err.to_string().contains("merge, replace, discovery_only"), "{err}");
        assert_eq!(
            "pass_through".parse::<TranslationMissMode>().unwrap(),
            TranslationMissMode::PassThrough
        );
        assert_eq!(
            "strict".parse::<TranslationMissMode>().unwrap(),
            TranslationMissMode::Strict
        );
        assert!("drop".parse::<TranslationMissMode>().is_err());
        assert_eq!(SeedMergePolicy::default(), SeedMergePolicy::Merge);
        assert_eq!(TranslationMissMode::default(), TranslationMissMode::PassThrough);
    }

    // ---- static-map ----

    #[test]
    fn static_map_hit_replaces_host_name_and_keeps_port_and_tls_name() {
        let map = HashMap::from([("10.0.0.1".to_string(), "192.168.1.1".to_string())]);
        let t = StaticMapAddressTranslator::new(Some(map));
        let advertised = Endpoint::new("n1", Some("tls1".into()), "10.0.0.1", 3100, SourceList::Standard);
        assert_eq!(
            t.translate(&advertised),
            Some(Host::new_tls("192.168.1.1", "tls1", 3100))
        );
        assert_eq!(t.translate(&ep("n1", "10.0.0.9", 3000)), None);
        assert_eq!(StaticMapAddressTranslator::new(None).translate(&ep("n1", "10.0.0.1", 3000)), None);
        assert_eq!(t.name(), "static-map");
    }

    // ---- pass-through ----

    #[test]
    fn pass_through_returns_the_advertised_endpoint_unchanged() {
        let t = PassThroughAddressTranslator;
        assert_eq!(t.translate(&ep("n1", "10.0.0.1", 3000)), Some(h("10.0.0.1", 3000)));
        assert_eq!(t.translate(&ep("n1", "2001:db8::1", 3000)), Some(h("2001:db8::1", 3000)));
        assert_eq!(t.translate(&ep("n1", "node.example.com", 3000)), Some(h("node.example.com", 3000)));
        let tls = Endpoint::new("n1", Some("other".into()), "10.0.0.1", 3000, SourceList::Alternate);
        assert_eq!(t.translate(&tls), Some(Host::new_tls("10.0.0.1", "other", 3000)));
        assert_eq!(t.name(), "pass-through");
    }

    // ---- port-map ----

    fn port_map(entries: &[(&str, &str)]) -> PortMapAddressTranslator {
        PortMapAddressTranslator::new(entries.iter().copied()).expect("valid port map")
    }

    #[test]
    fn port_map_host_port_key_hit_replaces_host_and_port() {
        let t = port_map(&[("10.0.0.1:3000", "lb.example.com:30001")]);
        let advertised = Endpoint::new("n1", Some("tls1".into()), "10.0.0.1", 3000, SourceList::Standard);
        assert_eq!(
            t.translate(&advertised),
            Some(Host::new_tls("lb.example.com", "tls1", 30001))
        );
        // A hit keeps a missing TLS name missing.
        assert_eq!(t.translate(&ep("n1", "10.0.0.1", 3000)), Some(h("lb.example.com", 30001)));
        // Another port misses.
        assert_eq!(t.translate(&ep("n1", "10.0.0.1", 3001)), None);
        assert_eq!(t.name(), "port-map");
    }

    #[test]
    fn port_map_node_name_key_matches_any_advertised_address_of_that_node() {
        let t = port_map(&[("BB9020011AC4202", "lb.example.com:30002")]);
        assert_eq!(
            t.translate(&ep("BB9020011AC4202", "10.0.0.1", 3000)),
            Some(h("lb.example.com", 30002))
        );
        assert_eq!(
            t.translate(&ep("BB9020011AC4202", "10.0.0.7", 4000)),
            Some(h("lb.example.com", 30002))
        );
        assert_eq!(t.translate(&ep("OTHER", "10.0.0.1", 3000)), None);
        assert_eq!(t.translate(&ep("", "10.0.0.1", 3000)), None);
    }

    #[test]
    fn port_map_ipv6_keys_and_values() {
        let t = port_map(&[
            ("[2001:db8::1]:3000", "lb.example.com:30002"),
            ("10.0.0.2:3000", "[2001:db8::9]:30003"),
            ("[2001:db8::3]:3000", "[2001:db8::4]:30004"),
        ]);
        assert_eq!(t.translate(&ep("n", "2001:db8::1", 3000)), Some(h("lb.example.com", 30002)));
        assert_eq!(t.translate(&ep("n", "10.0.0.2", 3000)), Some(h("2001:db8::9", 30003)));
        assert_eq!(t.translate(&ep("n", "2001:db8::3", 3000)), Some(h("2001:db8::4", 30004)));
    }

    #[test]
    fn port_map_host_port_key_takes_precedence_over_node_name() {
        let t = port_map(&[("10.0.0.1:3000", "lb:1"), ("BB9", "lb:2")]);
        assert_eq!(t.translate(&ep("BB9", "10.0.0.1", 3000)), Some(h("lb", 1)));
        assert_eq!(t.translate(&ep("BB9", "10.0.0.2", 3000)), Some(h("lb", 2)));
    }

    #[test]
    fn port_map_empty_map_misses_everything() {
        let t = port_map(&[]);
        assert_eq!(t.translate(&ep("n", "10.0.0.1", 3000)), None);
    }

    #[test]
    fn port_map_invalid_entries_are_rejected_naming_the_entry() {
        let cases: &[(&str, &str, &str)] = &[
            ("", "lb:1", "key is empty"),
            ("10.0.0.1:3000", "lb", "lb is not host:port"),
            ("10.0.0.1:3000", ":30001", ":30001 has an empty host"),
            ("10.0.0.1:3000", "lb:x", "lb:x has an invalid port x"),
            ("10.0.0.1:3000", "lb:0", "lb:0 has port 0 out of range 1-65535"),
            ("10.0.0.1:3000", "lb:65536", "lb:65536 has port 65536 out of range 1-65535"),
            ("2001:db8::1:3000", "lb:1", "has an IPv6 host that is not in brackets"),
            ("[2001:db8::1:3000", "lb:1", "has an invalid bracketed IPv6 host"),
            ("10.0.0.1:x", "lb:1", "10.0.0.1:x has an invalid port x"),
        ];
        for (key, value, reason) in cases {
            let err = PortMapAddressTranslator::new([(*key, *value)]).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(&format!("Invalid port-map entry {key}: {value}")), "{msg}");
            assert!(msg.contains(reason), "{msg} should contain {reason}");
        }
    }

    #[test]
    fn port_map_equivalent_host_port_keys_are_rejected() {
        let err = PortMapAddressTranslator::new([
            ("[2001:db8::1]:3000", "lb:1"),
            ("[2001:db8::1]:3000", "lb:2"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("duplicates another key"), "{err}");
    }

    #[test]
    fn port_map_port_range_bounds_are_accepted() {
        let t = port_map(&[("a:1", "lb:65535"), ("b:65535", "lb:1")]);
        assert_eq!(t.translate(&ep("n", "a", 1)), Some(h("lb", 65535)));
        assert_eq!(t.translate(&ep("n", "b", 65535)), Some(h("lb", 1)));
    }

    // ---- translate() with the miss mode ----

    #[test]
    fn translate_applies_the_miss_mode_and_keeps_the_advertised_tls_name() {
        struct Renamer;
        impl AddressTranslator for Renamer {
            fn translate(&self, advertised: &Endpoint) -> Option<Host> {
                (advertised.host == "hit").then(|| Host::new_tls("x", "changed", 9))
            }
        }
        let hit = Endpoint::new("n", Some("adv".into()), "hit", 1, SourceList::Standard);
        let miss = Endpoint::new("n", Some("adv".into()), "miss", 1, SourceList::Alternate);

        // The TLS name on the returned host is replaced by the advertised one.
        let got = translate(&Renamer, TranslationMissMode::Strict, &hit).unwrap();
        assert_eq!(got, Host::new_tls("x", "adv", 9));

        // Pass-through miss: the advertised endpoint unchanged.
        let got = translate(&Renamer, TranslationMissMode::PassThrough, &miss).unwrap();
        assert_eq!(got, Host::new_tls("miss", "adv", 1));

        // Strict miss: a typed error naming the translator and the list.
        let err = translate(&Renamer, TranslationMissMode::Strict, &miss).unwrap_err();
        assert!(matches!(err.kind(), crate::ErrorKind::TranslationMiss));
        assert_eq!(err.result_code(), -25);
        let msg = err.to_string();
        assert!(msg.contains("miss:1"), "{msg}");
        assert!(msg.contains("in the alternate list"), "{msg}");
        assert!(msg.contains("node n"), "{msg}");
        assert!(msg.contains(&Renamer.name()), "{msg}");
        let std_err = translate(&Renamer, TranslationMissMode::Strict, &ep("n", "miss", 1)).unwrap_err();
        assert!(std_err.to_string().contains("in the standard list"));
    }

    #[test]
    fn custom_translator_name_is_the_type_name_and_built_ins_use_yaml_names() {
        struct Custom;
        impl AddressTranslator for Custom {
            fn translate(&self, _: &Endpoint) -> Option<Host> {
                None
            }
        }
        assert!(Custom.name().ends_with("Custom"));
        let shared: SharedAddressTranslator = Arc::new(PassThroughAddressTranslator);
        assert_eq!(format!("{shared:?}"), "AddressTranslator(\"pass-through\")");
        let provider: SharedSeedCandidateProvider = Arc::new(StaticSeedCandidateProvider::new(vec![]));
        assert_eq!(format!("{provider:?}"), "SeedCandidateProvider(\"static\")");
        // Identity comparison.
        assert!(shared == shared.clone());
        let other: SharedAddressTranslator = Arc::new(PassThroughAddressTranslator);
        assert!(shared != other);
    }

    #[test]
    fn collision_error_names_both_nodes_and_the_translator() {
        let err = Error::translation_collision("BB9B", "BB9C", &h("127.0.0.1", 3000), "static-map");
        assert_eq!(err.result_code(), -26);
        assert!(matches!(err.kind(), crate::ErrorKind::TranslationCollision));
        let msg = err.to_string();
        for part in ["BB9B", "BB9C", "127.0.0.1:3000", "static-map"] {
            assert!(msg.contains(part), "{msg}");
        }
        let alt = Error::alternate_address_missing("BB9B", None);
        assert_eq!(alt.result_code(), -1);
        assert!(alt.to_string().contains("use_services_alternate"));
        assert!(Error::alternate_address_missing("BB9B", Some(&h("127.0.0.1", 1))).to_string().contains("127.0.0.1:1"));
    }
}

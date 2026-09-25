// Copyright 2015-2018 Aerospike, Inc.
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

use std::net::ToSocketAddrs;
use std::str;
use std::vec::Vec;

use crate::cluster::version_parser::{Version, VersionParser};
use crate::cluster::Cluster;
use crate::commands::Message;
use crate::errors::{Error, Result};
use crate::net::{Connection, Host};
use crate::policy::{AdminPolicy, ClientPolicy};
use crate::ToHosts;

// Validates a Database server node
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone)]
pub struct NodeValidator {
    pub name: String,
    pub aliases: Vec<Host>,
    pub address: String,
    pub client_policy: ClientPolicy,
    pub use_new_info: bool,
    pub version: Version,
    /// Cluster name the node reported in its `cluster-name` info response,
    /// normalized by [`normalize_cluster_name`]. Captured unconditionally so
    /// the discovered name is available even when
    /// `ClientPolicy::cluster_name` is unset and no validation runs.
    pub cluster_name: Option<String>,
    /// Session token from the TLS login, kept only when
    /// [`ClientPolicy::for_login_only`](crate::ClientPolicy::for_login_only)
    /// is active: the cleartext connections that follow authenticate with it
    /// instead of sending credentials.
    pub session: Option<crate::commands::admin_command::SessionInfo>,
    /// The node's TLS address, kept under `for_login_only` so the pool can
    /// log in again over TLS when the session expires.
    pub login_host: Option<Host>,
    /// Whether this validator was created for a seed host. When true,
    /// `validate_alias` queries `service-{tls,clear}-{std,alt}` and, if the
    /// seed isn't listed in the response, treats the seed as a load
    /// balancer and rewrites `aliases` to the first reachable real backend.
    pub detect_load_balancer: bool,
}

/// Normalizes a `cluster-name` info value: a server without a configured
/// cluster name answers `null` (or nothing), which is "no name", not a name.
pub(crate) fn normalize_cluster_name(raw: Option<&String>) -> Option<String> {
    raw.map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("null"))
        .map(str::to_owned)
}

// Generates a node validator
impl NodeValidator {
    pub fn new(client_policy: ClientPolicy) -> Self {
        NodeValidator {
            name: String::new(),
            aliases: vec![],
            address: String::new(),
            client_policy,
            use_new_info: true,
            version: Version::default(),
            cluster_name: None,
            session: None,
            login_host: None,
            detect_load_balancer: false,
        }
    }

    /// Construct a validator that performs load-balancer detection. Use
    /// this when validating user-supplied seed hosts; for peer hosts
    /// discovered through `services`/`peers`, use [`new`](Self::new).
    /// Under `seed_only_cluster` we treat the seed itself as the
    /// canonical service endpoint, so LB detection is suppressed.
    pub fn new_for_seed(client_policy: ClientPolicy) -> Self {
        let seed_only = client_policy.seed_only_cluster;
        let mut nv = Self::new(client_policy);
        nv.detect_load_balancer = !seed_only;
        nv
    }

    #[allow(clippy::option_if_let_else)]
    pub async fn validate_node(&mut self, cluster: &Cluster, host: &Host) -> Result<()> {
        self.resolve_aliases(host)
            .map_err(|e| e.chain_error("Failed to resolve host aliases"))?;

        let mut last_err = None;
        for alias in &self.aliases() {
            match self.validate_alias(cluster, alias).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    debug!("Alias {alias} failed: {err:?}");
                    last_err = Some(err);
                }
            }
        }

        match last_err {
            Some(err) => Err(err),
            None => unreachable!(),
        }
    }

    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.clone()
    }

    fn resolve_aliases(&mut self, host: &Host) -> Result<()> {
        self.aliases = (host.name.as_ref(), host.port)
            .to_socket_addrs()?
            .map(|addr| {
                Host::new_tls(
                    &addr.ip().to_string(),
                    &host.tls_name.clone().unwrap_or_default(),
                    addr.port(),
                )
            })
            .collect();

        debug!("Resolved aliases for host {}: {:?}", host, self.aliases);
        if self.aliases.is_empty() {
            Err(Error::connection(format!(
                "Failed to find addresses for {host}"
            )))
        } else {
            Ok(())
        }
    }

    async fn validate_alias(&mut self, cluster: &Cluster, alias: &Host) -> Result<()> {
        // `for_login_only` keeps exactly one connection encrypted: this one,
        // which carries the credential exchange. Java does the same in
        // `NodeValidator.validateAddress` before handing off to `SwitchClear`.
        let login_only = self.client_policy.login_only_active();
        let (mut conn, session) = if login_only {
            Connection::open_tls_login(alias, &self.client_policy, cluster.hashed_pass().as_ref())
                .await
                .map_err(Error::from)?
        } else {
            (
                Connection::new(alias, &self.client_policy, cluster.hashed_pass().as_ref()).await?,
                None,
            )
        };
        if login_only {
            self.session = session;
            self.login_host = Some(alias.clone());
            // The cleartext address is about to be read from the service
            // command, so there is nothing left for load-balancer detection
            // to resolve (Java disables it here for the same reason).
            self.detect_load_balancer = false;
        }
        let admin_policy = AdminPolicy {
            timeout: self.client_policy.timeout,
        };

        // Java skips LB detection for loopback seeds (the LB would never
        // sit on localhost) and only fires on the first validator pass —
        // we mirror that by gating the address command on
        // `detect_load_balancer && !alias.is_loopback()`.
        // Peer discovery is *not* mixed into this request: it goes through
        // the richer `peers-…` info command issued by `Node::refresh_peers`
        // after the seed Node is constructed (Java's `validatePeers →
        // Node.refreshPeers` flow).
        let address_command: Option<&'static str> =
            if self.detect_load_balancer && !host_is_loopback(alias) {
                Some(cluster.client_policy.load().service_string())
            } else {
                None
            };

        // Under `for_login_only` the node's non-TLS address is fetched in the
        // same round trip that validates it.
        let clear_command: Option<&'static str> = if login_only {
            Some(self.client_policy.clear_service_string())
        } else {
            None
        };

        let mut commands: Vec<&str> = vec!["node", "cluster-name", "build", "partition-generation"];
        if let Some(cmd) = address_command {
            commands.push(cmd);
        }
        if let Some(cmd) = clear_command {
            commands.push(cmd);
        }

        let info_map = Message::info(&admin_policy, &mut conn, &commands).await?;

        // Validation is a one-shot probe — close the socket now so we
        // don't rely on `Drop` running at a specific point later (matters
        // for debuggability and for parity with Java's try/finally pattern
        // in `NodeValidator.validateAddress`).
        conn.close();
        drop(conn);

        match info_map.get("node") {
            None => return Err(Error::invalid_node(String::from("Missing node name"))),
            Some(node_name) => self.name.clone_from(node_name),
        }

        // Reject nodes whose partition map hasn't been initialized yet
        // (`partition-generation == -1`). Mirrors Java's
        // `NodeValidator.validatePartitionGeneration`: a not-yet-ready
        // node would otherwise get admitted and serve a partial map.
        match info_map.get("partition-generation") {
            None => {
                return Err(Error::invalid_node(String::from(
                    "Missing partition-generation",
                )))
            }
            Some(gen_str) => {
                let gen = gen_str.parse::<i64>().map_err(|_| {
                    Error::invalid_node(format!(
                        "Node {} {} returned invalid partition-generation: {gen_str}",
                        self.name, alias
                    ))
                })?;
                if gen == -1 {
                    return Err(Error::invalid_node(format!(
                        "Node {} {} is not yet fully initialized",
                        self.name, alias
                    )));
                }
            }
        }

        // Discovery is separate from validation: the server's name is kept
        // whatever the policy says, so callers can select per-cluster
        // settings from it; the configured name, when present, is asserted
        // below exactly as before.
        self.cluster_name = normalize_cluster_name(info_map.get("cluster-name"));

        if let Some(ref cluster_name) = cluster.cluster_name() {
            match info_map.get("cluster-name") {
                None => return Err(Error::invalid_node(String::from("Missing cluster name"))),
                Some(info_name) if info_name == cluster_name => {}
                Some(info_name) => {
                    return Err(Error::invalid_node(format!(
                        "Cluster name mismatch: expected={cluster_name},
                                                         got={info_name}"
                    )))
                }
            }
        }

        self.address = alias.address();

        if let Some(build) = info_map.get("build") {
            let version = VersionParser::new(build).parse()?;
            // Mirror Java `setFeatures`: reject servers older than 4.9.0.3,
            // which is the minimum required for partition scans. Newer
            // features (query-show, batch-any, partition-query, app-id,
            // …) gracefully degrade — they're checked lazily through
            // `Version::supports_*()` at call time, so no separate feature
            // bitset is maintained.
            if !version.supports_partition_scan() {
                return Err(Error::invalid_node(format!(
                    "Node {} {} version {build} < 4.9.0.3. \
                     This client requires server version >= 4.9.0.3",
                    self.name, alias
                )));
            }
            self.version = version;
        }

        // Load-balancer detection: if this validator was created for a seed
        // and the seed isn't listed in the service response, the seed is
        // probably an LB. Replace `self.aliases` with the first reachable
        // real backend. Mirrors Java's `setAddress`.
        if let Some(addr_cmd) = address_command {
            if let Some(s) = info_map.get(addr_cmd) {
                if !s.trim().is_empty() {
                    self.maybe_swap_lb_address(cluster, alias, s).await;
                }
            }
        }

        // The TLS connection has done its one job (the login) and is already
        // closed. Move this node onto its cleartext address for good.
        if let Some(cmd) = clear_command {
            let raw = info_map.get(cmd).cloned().unwrap_or_default();
            self.switch_to_clear_address(&raw).await?;
        }

        Ok(())
    }

    /// Point this node at a non-TLS address (`for_login_only`).
    ///
    /// Mirrors Java's `SwitchClear`: walk the addresses the node reported,
    /// apply `ip_map`, and keep the first one that accepts a cleartext
    /// connection authenticated with the session token. Proving the token
    /// works before committing is the point — a clear address that cannot
    /// authenticate is no use, and finding out now beats finding out on the
    /// first command.
    async fn switch_to_clear_address(&mut self, raw: &str) -> Result<()> {
        let session = self.session.clone();
        for entry in raw.split(';') {
            if entry.trim().is_empty() {
                continue;
            }
            let parsed = match entry.to_hosts() {
                Ok(v) => v,
                Err(e) => {
                    debug!("for_login_only: cannot parse clear service entry `{entry}`: {e}");
                    continue;
                }
            };
            for mut host in parsed {
                // A cleartext endpoint has no TLS name to verify against.
                host.tls_name = None;
                if let Some(ref ip_map) = self.client_policy.ip_map {
                    if let Some(mapped) = ip_map.get(&host.name) {
                        host.name.clone_from(mapped);
                    }
                }
                for resolved in (host.name.as_str(), host.port)
                    .to_socket_addrs()
                    .into_iter()
                    .flatten()
                {
                    let candidate = Host::new(&resolved.ip().to_string(), resolved.port());
                    match Connection::open(
                        &candidate,
                        &self.client_policy,
                        None,
                        session.as_ref(),
                    )
                    .await
                    {
                        Ok((mut probe, _)) => {
                            probe.close();
                            drop(probe);
                            debug!(
                                "for_login_only: node {} switched to clear address {}",
                                self.name, candidate
                            );
                            self.address = candidate.address();
                            self.aliases = vec![candidate];
                            return Ok(());
                        }
                        Err(e) => {
                            debug!(
                                "for_login_only: clear address {candidate} unusable: {}",
                                Error::from(e)
                            );
                        }
                    }
                }
            }
        }

        Err(Error::invalid_node(format!(
            "for_login_only: node {} reported no usable non-TLS address (service response: `{raw}`)",
            self.name
        )))
    }

    async fn maybe_swap_lb_address(&mut self, cluster: &Cluster, seed: &Host, raw: &str) {
        // Parse the service response into Hosts, applying ip_map at the
        // same time so seed comparison happens on post-substitution values
        // (matches Java).
        let mut real_hosts: Vec<Host> = Vec::new();
        for entry in raw.split(';') {
            if entry.trim().is_empty() {
                continue;
            }
            let parsed = match entry.to_hosts() {
                Ok(v) => v,
                Err(e) => {
                    debug!("LB-detect: cannot parse service entry `{entry}`: {e}");
                    continue;
                }
            };
            for mut h in parsed {
                h.tls_name.clone_from(&seed.tls_name);
                if let Some(ref ip_map) = self.client_policy.ip_map {
                    if let Some(mapped) = ip_map.get(&h.name) {
                        h.name.clone_from(mapped);
                    }
                }
                real_hosts.push(h);
            }
        }

        // Seed found in the response — not an LB. Nothing to do.
        if real_hosts
            .iter()
            .any(|h| h.name == seed.name && h.port == seed.port)
        {
            return;
        }

        // Seed missing → LB. Find the first real host that accepts a
        // connection and rewrite `self.aliases` to point to it. We don't
        // recurse into a full re-validation: the cluster-level seed loop
        // will re-validate via `services()`/peer-discovery anyway.
        for candidate in &real_hosts {
            for resolved in (candidate.name.as_str(), candidate.port)
                .to_socket_addrs()
                .into_iter()
                .flatten()
            {
                let addr_str = resolved.ip().to_string();
                let real_alias = Host::new_tls(
                    &addr_str,
                    &candidate.tls_name.clone().unwrap_or_default(),
                    candidate.port,
                );
                if let Ok(mut probe) = Connection::new(
                    &real_alias,
                    &self.client_policy,
                    cluster.hashed_pass().as_ref(),
                )
                .await
                {
                    probe.close();
                    drop(probe);
                    info!("Seed {seed} is a load balancer; using real host {real_alias}");
                    self.aliases = vec![real_alias.clone()];
                    self.address = real_alias.address();
                    return;
                }
            }
        }

        info!(
            "Seed {seed} appears to be a load balancer but no real host was reachable; \
             using the original seed address"
        );
    }
}

/// `true` when `host.name` resolves to (or already is) a loopback address.
/// Used to skip load-balancer detection for loopback seeds since the LB
/// would never live on localhost.
fn host_is_loopback(host: &Host) -> bool {
    if let Ok(ip) = host.name.parse::<std::net::IpAddr>() {
        return ip.is_loopback();
    }
    matches!(host.name.as_str(), "localhost" | "::1")
}

#[cfg(test)]
mod cluster_name_tests {
    use super::normalize_cluster_name;

    #[test]
    fn normalize_treats_null_and_empty_as_no_name() {
        assert_eq!(normalize_cluster_name(None), None);
        assert_eq!(normalize_cluster_name(Some(&String::new())), None);
        assert_eq!(normalize_cluster_name(Some(&"  ".to_string())), None);
        assert_eq!(normalize_cluster_name(Some(&"null".to_string())), None);
        assert_eq!(normalize_cluster_name(Some(&"NULL".to_string())), None);
        assert_eq!(
            normalize_cluster_name(Some(&" production ".to_string())),
            Some("production".to_string())
        );
    }
}

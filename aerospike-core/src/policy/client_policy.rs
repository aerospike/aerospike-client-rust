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

use std::collections::HashMap;
use std::time::Duration;

use crate::commands::admin_command::AdminCommand;
use crate::errors::{Error, Result};

#[cfg(feature = "tls")]
use tokio_rustls::rustls::ClientConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Determines authentication mode.
pub enum AuthMode {
    /// No Authentication will be performed
    None,

    /// Uses internal authentication only when user/password defined. Hashed password is stored
    /// on the server. Do not send clear password. This is the default.
    Internal(String, String),

    /// Uses external authentication (like LDAP) when user/password defined. Specific external authentication is
    /// configured on server. If `TLSConfig` is defined, sends clear password on node login via TLS.
    /// Will return an error if `TLSConfig` is not defined.
    External(String, String),

    /// Uses external authentication (like LDAP) when user/password defined. Specific external
    /// authentication is configured on server. Sends the clear password on node login whether
    /// or not TLS is defined. This mode should only be used for testing purposes because it is
    /// not secure authentication.
    ExternalInsecure(String, String),

    /// Allows authentication and authorization based on a certificate. No user name or
    /// password needs to be configured. Requires TLS and a client certificate.
    /// Requires server version 5.7.0+
    PKI,
}

/// Minimum allowed value for [`ClientPolicy::tend_interval`], in milliseconds.
pub const TEND_INTERVAL_MIN_MS: u32 = 250;

/// `ClientPolicy` encapsulates parameters for client policy command.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
#[allow(clippy::struct_excessive_bools)] // a policy is a bag of flags
pub struct ClientPolicy {
    /// User authentication to cluster.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub auth_mode: AuthMode,

    /// TLS secure connection policy for TLS enabled servers.
    /// # Examples
    ///
    /// Using cert files to allow for client authentication.
    ///
    /// ```rust,edition2021,no_run
    /// # use rustls::RootCertStore;
    /// # use rustls::pki_types::CertificateDer;
    /// # use rustls::pki_types::PrivateKeyDer;
    /// # use rustls::pki_types::pem::PemObject;
    /// let mut root_store = RootCertStore {
    ///     roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    /// };
    ///
    /// root_store.add_parsable_certificates(
    ///     CertificateDer::pem_file_iter("tls_cacert_file")
    ///         .expect("Cannot open CA file")
    ///         .map(|result| result.unwrap()),
    /// );
    ///
    /// let client_ca = CertificateDer::from_pem_file("tls_cacert_file").expect("Cannot open CA file");
    /// let client_key = PrivateKeyDer::from_pem_file("tls_key_file").expect("Cannot open Key file");
    ///
    /// let tls_config = rustls::ClientConfig::builder()
    ///     .with_root_certificates(root_store)
    ///     .with_client_auth_cert(vec![client_ca], client_key)
    ///     .unwrap();
    /// ```
    ///
    /// Using cert files without enforcing client authentication.
    ///
    /// ```rust,edition2021,no_run
    /// # use rustls::RootCertStore;
    /// # use rustls::pki_types::CertificateDer;
    /// # use rustls::pki_types::pem::PemObject;
    /// let mut root_store = RootCertStore {
    ///     roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    /// };
    ///
    /// root_store.add_parsable_certificates(
    ///     CertificateDer::pem_file_iter("tls_cacert_file")
    ///         .expect("Cannot open CA file")
    ///         .map(|result| result.unwrap()),
    /// );
    ///
    /// let tls_config = rustls::ClientConfig::builder()
    ///     .with_root_certificates(root_store)
    ///     .with_no_client_auth();
    /// ```
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub tls_config: Option<ClientConfig>,

    /// Socket timeout in milliseconds for a single **info/admin command**:
    /// the `info` requests the tend loop issues every cycle (node refresh,
    /// peer discovery, `replicas` partition fetch, rack info, keep-alive
    /// probes) and the client's own info/admin calls
    /// ([`Client::info`](crate::Client::info), index and UDF management,
    /// `truncate`, security commands, …).
    ///
    /// The timeout applies per socket operation within such a command, not to
    /// a whole command or a whole tend cycle: a command that writes a request
    /// and reads a header plus a body allows this long for each of those
    /// steps. Lower it to detect an unresponsive node sooner; raise it for
    /// links with high latency or for `replicas` responses on clusters with
    /// very many namespaces.
    ///
    /// This value also serves as the fallback for
    /// [`connect_timeout`](field@Self::connect_timeout) when that field is
    /// `0`, so it transitively bounds establishing a connection too.
    ///
    /// # This does *not* bound cluster startup
    ///
    /// [`Client::new`](crate::Client::new) does not return until the
    /// partition table is fully formed and stable, and that wait is driven by
    /// the cluster's own convergence — not by this field. A healthy cluster
    /// stabilizes in one tend cycle, but each tend costs network round-trips
    /// and, on a security-enabled cluster, a `LOGIN` handshake per node, so
    /// how long it takes is a property of the cluster's size and
    /// configuration. Bounding
    /// it with a per-command timeout used to truncate convergence on larger
    /// or secured clusters and hand back a client that could not route
    /// anything; see [`ClientPolicy::fail_if_not_connected`](field@Self::fail_if_not_connected)
    /// for the knob that controls startup failure instead.
    ///
    /// Default: 1000
    pub timeout: u32,

    /// Timeout in milliseconds for establishing a connection to a server node:
    /// the TCP connect, TLS handshake, and login/authentication exchange.
    /// Applies everywhere the client opens a connection (seeding, tend,
    /// connection-pool growth, `min_conns_per_node` fill).
    ///
    /// `0` (the default) falls back to [`timeout`](field@Self::timeout).
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub connect_timeout: u32,

    /// Timeout in milliseconds for the login/authentication exchange
    /// (`LOGIN` or session-token `AUTHENTICATE`) performed on every freshly
    /// opened connection when the cluster has security enabled.
    ///
    /// `0` falls back to [`connect_timeout`](field@Self::connect_timeout)
    /// (and transitively to [`timeout`](field@Self::timeout)).
    ///
    /// Default: 5000
    pub login_timeout: u32,

    /// Connection idle timeout. Every time a connection is used, its idle
    /// deadline will be extended by this duration. When this deadline is reached,
    /// the connection will be closed and discarded from the connection pool.
    ///
    /// Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
    /// the server will stop automatically reaping based on socket idle timeouts.
    ///
    /// `0` disables the idle check entirely: pooled connections are never
    /// considered idle, so they are neither discarded nor kept alive by the
    /// tend-time reaper.
    ///
    /// Default: 0 (disabled).
    #[cfg_attr(
        feature = "dynamic-config",
        config(rename = "max_socket_idle", with = crate::config::secs_to_ms)
    )]
    pub idle_timeout: u32,

    /// Minimum number of connections allowed per server node.
    /// Preallocate min connections on client node creation.
    /// The client will periodically allocate new connections if count falls below min connections.
    ///
    /// Server proto-fd-idle-ms may also need to be increased substantially if min connections are defined.
    /// The proto-fd-idle-ms default directs the server to close connections that are idle for 60 seconds
    /// which can defeat the purpose of keeping connections in reserve for a future burst of activity.
    ///
    /// If server proto-fd-idle-ms is changed, client `ClientPolicy.idle_timeout` should also be
    /// changed to be a few seconds less than proto-fd-idle-ms.
    ///
    ///  Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
    ///  the server will stop automatically reaping based on socket idle timeouts.
    #[cfg_attr(
        feature = "dynamic-config",
        config(rename = "min_connections_per_node", startup)
    )]
    pub min_conns_per_node: usize,

    /// Maximum number of synchronous connections allowed per server node.
    ///
    /// Default: 100
    #[cfg_attr(
        feature = "dynamic-config",
        config(rename = "max_connections_per_node", startup)
    )]
    pub max_conns_per_node: usize,

    /// Number of connection pools used for each node. Machines with 8 CPU cores or less usually
    /// need only one connection pool per node. Machines with larger number of CPU cores may have
    /// their performance limited by contention for pooled connections. Contention for pooled
    /// connections can be reduced by creating multiple mini connection pools per node.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub conn_pools_per_node: u8,

    /// Cluster-wide cap on the number of connections that may be in the middle
    /// of being opened (TCP connect + TLS + login) at the same time. When a
    /// command finds its node's pool empty, the connection is opened by a
    /// background task while the command retries; this threshold bounds how
    /// many such opens can run concurrently across all nodes, protecting the
    /// cluster from a thundering herd after a cold start or mass disconnect.
    ///
    /// `0` (the default) means unlimited. Mirrors the Go client's
    /// `OpeningConnectionThreshold`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub opening_connection_threshold: usize,

    /// Whether [`Client::new`](crate::Client::new) fails when the cluster
    /// does not come up usable.
    ///
    /// With `true` (the default) construction returns an error unless the
    /// cluster is both reachable *and* routable: at least one seed connected,
    /// and every known node has contributed to the partition table. The error
    /// names every seed that failed, or reports that nodes were reachable but
    /// never produced a partition map.
    ///
    /// With `false`, construction succeeds regardless and the tend loop keeps
    /// trying in the background. Commands issued before the cluster becomes
    /// routable fail with `InvalidNamespace` ("partition map empty"), so this
    /// suits long-lived processes that must start before their database is
    /// up and can tolerate early failures.
    ///
    /// Note that this flag decides *whether* to fail, not *how long* to wait:
    /// the wait itself is bounded by the cluster's convergence, not by a
    /// timeout (see [`timeout`](field@Self::timeout)).
    ///
    /// Default: `true`
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub fail_if_not_connected: bool,

    /// Threshold at which the buffer attached to the connection will be shrunk by deallocating
    /// memory instead of just resetting the size of the underlying vec.
    /// Should be set to a value that covers as large a percentile of payload sizes as possible,
    /// while also being small enough not to occupy a significant amount of memory for the life
    /// of the connection pool.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub buffer_reclaim_threshold: usize,

    /// Use a tiered buffer pool for connection buffers that grow beyond
    /// `buffer_reclaim_threshold`. Instead of freeing an oversized buffer
    /// (and re-allocating on the next large command), the connection
    /// returns it to a bounded pool of power-of-two size classes shared
    /// by all of this client's connections, and checks buffers out of
    /// that pool when a command needs one. Each client (cluster) owns its
    /// own dedicated pool, sized by the `buffer_pool_*` fields below and
    /// freed when the client is dropped. Pooled buffers that go unused
    /// are aged out within about a minute (a victim-cache scheme driven
    /// by the cluster tend loop, mirroring the Go client's GC-backed
    /// `sync.Pool`). Set to `false` to restore the previous
    /// allocate/`shrink_to_fit` behavior.
    ///
    /// Default: true
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub use_buffer_pool: bool,

    /// Smallest buffer kept by this client's tiered buffer pool. Must be
    /// a power of two, at least 1024.
    ///
    /// Default: 8192 (8 KiB, matching the Go client's `MinBufferSize`)
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub buffer_pool_min_size: usize,

    /// Largest buffer kept by this client's tiered buffer pool; larger
    /// buffers are allocated fresh and never retained. Must be a power of
    /// two, >= `buffer_pool_min_size`.
    ///
    /// Default: 1048576 (1 MiB, matching the Go client's
    /// `PoolCutOffBufferSize`)
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub buffer_pool_max_size: usize,

    /// Retention budget per pool tier, in bytes: each power-of-two size
    /// class keeps at most `buffer_pool_tier_bytes / tier_size` buffers
    /// (clamped to between 2 and 64). Bounds the pool's worst-case
    /// footprint deterministically.
    ///
    /// Default: 4194304 (4 MiB per tier)
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub buffer_pool_tier_bytes: usize,

    /// Interval in milliseconds between cluster tends by the maintenance task.
    /// Minimum allowed value is [`TEND_INTERVAL_MIN_MS`] (250 ms); smaller values
    /// will be rejected by [`ClientPolicy::validate`].
    ///
    /// Default: 1000
    pub tend_interval: u32,

    /// Interval in milliseconds between dynamic-configuration reloads by the
    /// config watcher (see the `dynamic-config` feature). Sourced from the
    /// `static.client.config_interval` key (expressed in **seconds** in the
    /// config file). Effective minimum is 1000 ms. Has no effect unless a
    /// dynamic-config provider is active.
    ///
    /// Default: 1000
    #[cfg_attr(
        feature = "dynamic-config",
        config(rename = "config_interval", with = crate::config::secs_to_ms, startup)
    )]
    pub config_interval: u32,

    /// A IP translation table is used in cases where different clients
    /// use different server IP addresses. This may be necessary when
    /// using clients from both inside and outside a local area
    /// network. Default is no translation.
    /// The key is the IP address returned from friend info requests to other servers.
    /// The value is the real IP address used to connect to the server.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub ip_map: Option<HashMap<String, String>>,

    /// `UseServicesAlternate` determines if the client should use "services-alternate"
    /// instead of "services" in info request during cluster tending.
    /// "services-alternate" returns server configured external IP addresses that client
    /// uses to talk to nodes. "services-alternate" can be used in place of
    /// providing a client "ipMap".
    /// This feature is recommended instead of using the client-side `IpMap` above.
    ///
    /// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
    #[cfg_attr(feature = "dynamic-config", config(rename = "use_service_alternate"))]
    pub use_services_alternate: bool,

    /// Expected cluster name. If not `None`, server nodes must return this cluster name in order
    /// to join the client's view of the cluster. Should only be set when connecting to servers
    /// that support the "cluster-name" info command.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub cluster_name: Option<String>,

    /// Mark this client as belonging to a rack, and track server rack data. This field is useful when directing read commands to
    /// the server node that contains the key and exists on the same rack as the client.
    /// This serves to lower cloud provider costs when nodes are distributed across different
    /// racks/data centers.
    ///
    /// Racks are tried in **preference order**: node selection scans the
    /// replicas for a node on the first rack in the list, then the second,
    /// and so on, before falling back to any other active node.
    ///
    /// Replica.PreferRack and server rack configuration must
    /// also be set to enable this functionality.
    ///
    /// This one field carries both the switch and the preference list, so
    /// `None` is how rack awareness is turned off. **`Some(vec![])` is
    /// rejected** by validation.
    pub rack_ids: Option<Vec<usize>>,

    /// Application id is used to identify an application so that client operations can be correlated
    /// with server side metrics.
    #[cfg_attr(feature = "dynamic-config", config(rename = "app_id"))]
    pub application_id: Option<String>,

    /// Override the `client_id` in the `user_agent_id` payload sent to each node on connection
    /// validation. When `Some(client_id)`, `client_id` is used verbatim as the
    /// `user-agent-set:value=1,{client_id},{app_id}…` argument (still base64-encoded by the
    /// client before transmission). When `None`, the client falls back to
    /// the default `"rust-<version>"` format.
    /// This is meant to be used in clients that are using the rust client internally,
    /// and should not be used by third-party users.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub custom_client_id: Option<String>,

    /// Maximum number of errors (network errors + server-side `TIMEOUT`,
    /// `DEVICE_OVERLOAD`, `KEY_BUSY`) tolerated against a single node
    /// within one `error_rate_window`. Once the count exceeds this
    /// threshold the client trips a per-node circuit breaker and rejects
    /// further commands targeted at that node with `ResultCode::MaxErrorRate`
    /// until the next reset. Set to `0` to disable the breaker entirely.
    /// Defaults to 100.
    pub max_error_rate: usize,

    /// Number of cluster tend iterations after which each node's error
    /// counter is reset. Smaller values make the breaker more aggressive
    /// (tighter recovery), larger values make it more lenient. Defaults
    /// to 1.
    pub error_rate_window: usize,

    /// Restrict the cluster view to the seed addresses. When `true`:
    ///
    /// - Peer discovery is disabled — nodes returned by `peers` info
    ///   commands are ignored, even if other seeds advertise them.
    /// - Seed nodes are retained across connection failures (the tend
    ///   loop will not remove unresponsive seeds, and will re-seed
    ///   whenever the live node count drops below the seed count).
    /// - Load-balancer detection on seed validation is skipped — the
    ///   client treats the seed address as the canonical service
    ///   endpoint instead of resolving it to a backend node.
    ///
    /// Useful when the client sits behind a fixed VIP / proxy that
    /// fronts the cluster, or in tests that pin to a known seed list.
    /// Defaults to `false`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub seed_only_cluster: bool,
}

impl Default for ClientPolicy {
    fn default() -> ClientPolicy {
        ClientPolicy {
            auth_mode: AuthMode::None,
            timeout: 1_000,
            connect_timeout: 0,
            login_timeout: 5_000,
            idle_timeout: 0,
            min_conns_per_node: 0,
            max_conns_per_node: 100,
            conn_pools_per_node: 1,
            opening_connection_threshold: 0,
            fail_if_not_connected: true,
            tend_interval: 1000,
            config_interval: 1000,
            ip_map: None,
            use_services_alternate: false,
            cluster_name: None,
            buffer_reclaim_threshold: 65536,
            use_buffer_pool: true,
            buffer_pool_min_size: crate::net::buffer_pool::DEFAULT_MIN_POOLED_SIZE,
            buffer_pool_max_size: crate::net::buffer_pool::DEFAULT_MAX_POOLED_SIZE,
            buffer_pool_tier_bytes: crate::net::buffer_pool::DEFAULT_PER_TIER_BYTES,
            rack_ids: None,
            application_id: None,
            max_error_rate: 100,
            error_rate_window: 1,
            seed_only_cluster: false,
            custom_client_id: None,

            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }
}

impl ClientPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_conns_per_node > 0 && self.min_conns_per_node > self.max_conns_per_node {
            return Err(Error::client_error("minimum number of connections specified in the ClientPolicy is bigger than total connection pool size"));
        }

        // `rack_ids` is both the rack-awareness switch and the preference
        // list. An empty list turns the feature on with nothing to prefer: the
        // node-selection loop runs zero times, so no candidate and no fallback
        // is ever recorded, and every `Replica::PreferRack` read fails node
        // selection and is retried to exhaustion — surfacing as a client
        // timeout that says nothing about the misconfiguration. Refuse it here,
        // where the message can name the field.
        if self.rack_ids.as_ref().is_some_and(Vec::is_empty) {
            return Err(Error::invalid_argument(
                "ClientPolicy.rack_ids is Some([]): rack awareness is enabled with no preferred                  rack. Use None to disable rack awareness, or list at least one rack id",
            ));
        }

        if self.tend_interval < TEND_INTERVAL_MIN_MS {
            return Err(Error::client_error(format!(
                "Invalid tend_interval: {}. min: {}",
                self.tend_interval, TEND_INTERVAL_MIN_MS
            )));
        }

        // External authentication sends the clear password on login, so it
        // must ride over TLS. `ExternalInsecure` explicitly opts out of that
        // requirement (testing only).
        if matches!(self.auth_mode, AuthMode::External(_, _)) {
            #[cfg(feature = "tls")]
            let tls_enabled = self.tls_config.is_some();
            #[cfg(not(feature = "tls"))]
            let tls_enabled = false;

            if !tls_enabled {
                return Err(Error::client_error(
                    "TLS is required for AuthMode::External. Use AuthMode::ExternalInsecure \
                     to send external credentials without TLS (testing only)",
                ));
            }
        }

        // PKI authentication identifies the user by the client TLS
        // certificate, so it cannot work at all without a TLS config.
        if matches!(self.auth_mode, AuthMode::PKI) {
            #[cfg(feature = "tls")]
            let tls_enabled = self.tls_config.is_some();
            #[cfg(not(feature = "tls"))]
            let tls_enabled = false;

            if !tls_enabled {
                return Err(Error::client_error(
                    "TLS is required for AuthMode::PKI: the server identifies the user \
                     by the client TLS certificate",
                ));
            }
        }

        if self.use_buffer_pool {
            if !self.buffer_pool_min_size.is_power_of_two()
                || !self.buffer_pool_max_size.is_power_of_two()
            {
                return Err(Error::client_error(format!(
                    "buffer_pool_min_size ({}) and buffer_pool_max_size ({}) must be powers of two",
                    self.buffer_pool_min_size, self.buffer_pool_max_size
                )));
            }
            if self.buffer_pool_min_size < 1024
                || self.buffer_pool_min_size > self.buffer_pool_max_size
            {
                return Err(Error::client_error(format!(
                    "invalid buffer pool sizing: min {} must be >= 1024 and <= max {}",
                    self.buffer_pool_min_size, self.buffer_pool_max_size
                )));
            }
            if self.buffer_pool_tier_bytes == 0 {
                return Err(Error::client_error(
                    "buffer_pool_tier_bytes must be greater than 0",
                ));
            }
        }

        Ok(())
    }

    /// Whether rack awareness is in effect: configured *and* carrying at least
    /// one rack id.
    ///
    /// [`validate`](Self::validate) rejects `Some([])` at construction, but a
    /// dynamic-config reload can still merge an empty list into a live policy,
    /// and that path cannot return an error. Everything that acts on rack
    /// awareness goes through this predicate so an empty list degrades to
    /// "disabled" instead of breaking node selection.
    pub(crate) fn rack_aware(&self) -> bool {
        self.rack_ids.as_ref().is_some_and(|ids| !ids.is_empty())
    }

    pub(crate) fn application_id(&self) -> &str {
        if let Some(ref app_id) = self.application_id {
            if !app_id.is_empty() {
                return app_id;
            }
        }

        match self.auth_mode {
            crate::AuthMode::Internal(ref user, _)
            | crate::AuthMode::External(ref user, _)
            | crate::AuthMode::ExternalInsecure(ref user, _) => return user,
            _ => (),
        }

        "not-set"
    }

    pub(crate) fn timeout(&self) -> Duration {
        if self.timeout > 0 {
            Duration::from_millis(u64::from(self.timeout))
        } else {
            Duration::from_secs(30)
        }
    }

    /// Timeout for establishing a connection (TCP connect + TLS + auth).
    /// Falls back to [`timeout`](Self::timeout) when `connect_timeout` is `0`.
    pub(crate) fn connect_timeout(&self) -> Duration {
        if self.connect_timeout > 0 {
            Duration::from_millis(u64::from(self.connect_timeout))
        } else {
            self.timeout()
        }
    }

    /// Timeout for the login/authentication exchange on a fresh connection.
    /// Falls back to [`connect_timeout`](Self::connect_timeout) when
    /// `login_timeout` is `0`.
    pub(crate) fn login_timeout(&self) -> Duration {
        if self.login_timeout > 0 {
            Duration::from_millis(u64::from(self.login_timeout))
        } else {
            self.connect_timeout()
        }
    }

    /// Set username and password to use when authenticating to the cluster.
    pub fn set_auth_mode(&mut self, auth_mode: AuthMode) -> Result<()> {
        self.auth_mode = auth_mode;
        Ok(())
    }

    /// Return the hashed password for the auth mode.
    pub(crate) fn hashed_pass(&self) -> Option<String> {
        match self.auth_mode {
            AuthMode::External(_, ref password)
            | AuthMode::ExternalInsecure(_, ref password)
            | AuthMode::Internal(_, ref password) => {
                let password = AdminCommand::hash_password(password)
                    .expect("Unexpected error hashing the password");
                Some(password)
            }
            _ => None,
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) const fn peers_string(&self) -> &'static str {
        match (&self.tls_config, self.use_services_alternate) {
            (None, true) => "peers-clear-alt",
            (None, false) => "peers-clear-std",
            (Some(_), true) => "peers-tls-alt",
            (Some(_), false) => "peers-tls-std",
        }
    }

    #[cfg(not(feature = "tls"))]
    pub(crate) const fn peers_string(&self) -> &'static str {
        match self.use_services_alternate {
            true => "peers-clear-alt",
            false => "peers-clear-std",
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) const fn service_string(&self) -> &'static str {
        match (&self.tls_config, self.use_services_alternate) {
            (None, true) => "service-clear-alt",
            (None, false) => "service-clear-std",
            (Some(_), true) => "service-tls-alt",
            (Some(_), false) => "service-tls-std",
        }
    }

    #[cfg(not(feature = "tls"))]
    pub(crate) const fn service_string(&self) -> &'static str {
        match self.use_services_alternate {
            true => "service-clear-alt",
            false => "service-clear-std",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aerospike_rt::time::Duration;

    #[test]
    fn buffer_pool_sizing_is_validated() {
        let mut p = ClientPolicy::default();
        assert!(p.validate().is_ok());

        p.buffer_pool_min_size = 10_000; // not a power of two
        assert!(p.validate().is_err());

        p.buffer_pool_min_size = 2 * 1024 * 1024; // > max
        assert!(p.validate().is_err());

        p.buffer_pool_min_size = 512; // < 1024
        assert!(p.validate().is_err());

        p.buffer_pool_min_size = 8192;
        p.buffer_pool_tier_bytes = 0;
        assert!(p.validate().is_err());

        // Disabled pool skips the sizing checks entirely.
        p.use_buffer_pool = false;
        assert!(p.validate().is_ok());
    }

    #[test]
    fn connect_timeout_falls_back_to_timeout_when_zero() {
        let policy = ClientPolicy::default();
        assert_eq!(policy.connect_timeout, 0);
        assert_eq!(policy.connect_timeout(), policy.timeout());
    }

    #[test]
    fn connect_timeout_used_when_set() {
        let policy = ClientPolicy {
            connect_timeout: 1_500,
            timeout: 30_000,
            ..ClientPolicy::default()
        };
        assert_eq!(policy.connect_timeout(), Duration::from_millis(1_500));
        // The general timeout is unaffected.
        assert_eq!(policy.timeout(), Duration::from_millis(30_000));
    }

    #[test]
    fn login_timeout_fallback_chain() {
        // Explicit 0 → falls back to connect_timeout → falls back to timeout.
        let policy = ClientPolicy {
            login_timeout: 0,
            ..ClientPolicy::default()
        };
        assert_eq!(policy.login_timeout(), policy.timeout());

        // connect_timeout set, login_timeout 0 → connect_timeout wins.
        let policy = ClientPolicy {
            login_timeout: 0,
            connect_timeout: 1_500,
            ..ClientPolicy::default()
        };
        assert_eq!(policy.login_timeout(), Duration::from_millis(1_500));

        // login_timeout set → it wins over both.
        let policy = ClientPolicy {
            login_timeout: 700,
            connect_timeout: 1_500,
            ..ClientPolicy::default()
        };
        assert_eq!(policy.login_timeout(), Duration::from_millis(700));
        assert_eq!(policy.connect_timeout(), Duration::from_millis(1_500));
    }

    #[test]
    fn an_empty_rack_id_list_is_rejected() {
        // `Some([])` used to pass validation, enable rack-aware tend work, and
        // then fail every PreferRack read as a client timeout.
        let policy = ClientPolicy {
            rack_ids: Some(Vec::new()),
            ..ClientPolicy::default()
        };
        let err = policy.validate().unwrap_err();

        assert!(
            err.to_string().contains("rack_ids"),
            "the error must name the field: {err}"
        );
        assert_eq!(
            err.result_code(),
            i32::from(u8::from(crate::ResultCode::ParameterError)),
            "a bad policy value is an invalid argument: {err}"
        );

        // Both ways of not asking for an empty preference list stay valid.
        assert!(ClientPolicy {
            rack_ids: None,
            ..ClientPolicy::default()
        }
        .validate()
        .is_ok());
        assert!(ClientPolicy {
            rack_ids: Some(vec![0]),
            ..ClientPolicy::default()
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn rack_awareness_needs_a_rack_to_prefer() {
        // The predicate every rack-aware code path consults. An empty list is
        // unreachable through `Client::new`, but a dynamic-config reload can
        // still produce one, and that path cannot fail.
        let with = |rack_ids| ClientPolicy {
            rack_ids,
            ..ClientPolicy::default()
        };

        assert!(!with(None).rack_aware());
        assert!(!with(Some(Vec::new())).rack_aware());
        assert!(with(Some(vec![0])).rack_aware());
        assert!(with(Some(vec![2, 7])).rack_aware());
    }

    #[test]
    fn external_auth_requires_tls_but_insecure_does_not() {
        // External without a TLS config must be rejected at validation time.
        let policy = ClientPolicy {
            auth_mode: AuthMode::External("user".into(), "pass".into()),
            ..ClientPolicy::default()
        };
        let err = policy.validate().unwrap_err();
        assert!(
            err.to_string().contains("TLS is required"),
            "unexpected error: {err}"
        );

        // ExternalInsecure explicitly opts out of the TLS requirement.
        let policy = ClientPolicy {
            auth_mode: AuthMode::ExternalInsecure("user".into(), "pass".into()),
            ..ClientPolicy::default()
        };
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn pki_auth_requires_tls() {
        // PKI identifies the user by the client TLS certificate, so a
        // policy without a TLS config must be rejected at validation time.
        // Without the `tls` feature there is no way to supply a config, so
        // this branch rejects PKI unconditionally (same shape as the
        // External guard above).
        let policy = ClientPolicy {
            auth_mode: AuthMode::PKI,
            ..ClientPolicy::default()
        };
        let err = policy.validate().unwrap_err();
        assert!(
            err.to_string().contains("TLS is required"),
            "unexpected error: {err}"
        );

        // With a TLS config present, PKI passes validation.
        #[cfg(feature = "tls")]
        {
            use tokio_rustls::rustls::RootCertStore;

            let tls_config = ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();
            let policy = ClientPolicy {
                auth_mode: AuthMode::PKI,
                tls_config: Some(tls_config),
                ..ClientPolicy::default()
            };
            assert!(policy.validate().is_ok());
        }
    }

    #[test]
    fn external_insecure_uses_same_credentials_as_external() {
        // Both external modes hash the password for the CREDENTIAL field;
        // the login exchange itself is identical (USER + bcrypt CREDENTIAL +
        // CLEAR_PASSWORD) — only the TLS requirement differs.
        let external = ClientPolicy {
            auth_mode: AuthMode::External("user".into(), "pass".into()),
            ..ClientPolicy::default()
        };
        let insecure = ClientPolicy {
            auth_mode: AuthMode::ExternalInsecure("user".into(), "pass".into()),
            ..ClientPolicy::default()
        };
        let hashed = insecure.hashed_pass();
        assert!(hashed.is_some());
        assert_eq!(external.hashed_pass(), hashed);
        // The user name is reported as the application id for both modes.
        assert_eq!(insecure.application_id(), "user");
    }
}

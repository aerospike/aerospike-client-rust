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

use std::collections::{HashMap, HashSet};
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

    /// Allows authentication and authorization based on a certificate. No user name or
    /// password needs to be configured. Requires TLS and a client certificate.
    /// Requires server version 5.7.0+
    PKI,
}

/// `ClientPolicy` encapsulates parameters for client policy command.
#[derive(Debug, Clone)]
pub struct ClientPolicy {
    /// User authentication to cluster.
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
    pub tls_config: Option<ClientConfig>,

    /// Initial host connection timeout in milliseconds. The timeout when opening a connection
    /// to the server host for the first time.
    pub timeout: u32,

    /// Connection idle timeout. Every time a connection is used, its idle
    /// deadline will be extended by this duration. When this deadline is reached,
    /// the connection will be closed and discarded from the connection pool.
    ///
    /// Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
    /// the server will stop automatically reaping based on socket idle timeouts.
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
    pub min_conns_per_node: usize,

    /// Maximum number of synchronous connections allowed per server node.
    pub max_conns_per_node: usize,

    /// Number of connection pools used for each node. Machines with 8 CPU cores or less usually
    /// need only one connection pool per node. Machines with larger number of CPU cores may have
    /// their performance limited by contention for pooled connections. Contention for pooled
    /// connections can be reduced by creating multiple mini connection pools per node.
    pub conn_pools_per_node: u8,

    /// Throw exception if host connection fails during `addHost()`.
    pub fail_if_not_connected: bool,

    /// Threshold at which the buffer attached to the connection will be shrunk by deallocating
    /// memory instead of just resetting the size of the underlying vec.
    /// Should be set to a value that covers as large a percentile of payload sizes as possible,
    /// while also being small enough not to occupy a significant amount of memory for the life
    /// of the connection pool.
    pub buffer_reclaim_threshold: usize,

    /// `TendInterval` determines interval for checking for cluster state changes.
    /// Minimum possible interval is 10 Milliseconds.
    pub tend_interval: u32,

    /// A IP translation table is used in cases where different clients
    /// use different server IP addresses. This may be necessary when
    /// using clients from both inside and outside a local area
    /// network. Default is no translation.
    /// The key is the IP address returned from friend info requests to other servers.
    /// The value is the real IP address used to connect to the server.
    pub ip_map: Option<HashMap<String, String>>,

    /// `UseServicesAlternate` determines if the client should use "services-alternate"
    /// instead of "services" in info request during cluster tending.
    /// "services-alternate" returns server configured external IP addresses that client
    /// uses to talk to nodes. "services-alternate" can be used in place of
    /// providing a client "ipMap".
    /// This feature is recommended instead of using the client-side `IpMap` above.
    ///
    /// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
    pub use_services_alternate: bool,

    /// Expected cluster name. If not `None`, server nodes must return this cluster name in order
    /// to join the client's view of the cluster. Should only be set when connecting to servers
    /// that support the "cluster-name" info command.
    pub cluster_name: Option<String>,

    /// Mark this client as belonging to a rack, and track server rack data. This field is useful when directing read commands to
    /// the server node that contains the key and exists on the same rack as the client.
    /// This serves to lower cloud provider costs when nodes are distributed across different
    /// racks/data centers.
    ///
    /// Replica.PreferRack and server rack configuration must
    /// also be set to enable this functionality.
    pub rack_ids: Option<HashSet<usize>>,

    /// Application id is used to identify an application so that client operations can be correlated
    /// with server side metrics.
    pub application_id: Option<String>,

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
}

impl Default for ClientPolicy {
    fn default() -> ClientPolicy {
        ClientPolicy {
            auth_mode: AuthMode::None,
            timeout: 30_000,
            idle_timeout: 30_000,
            min_conns_per_node: 0,
            max_conns_per_node: 256,
            conn_pools_per_node: 1,
            fail_if_not_connected: true,
            tend_interval: 1000,
            ip_map: None,
            use_services_alternate: false,
            cluster_name: None,
            buffer_reclaim_threshold: 65536,
            rack_ids: None,
            application_id: None,
            max_error_rate: 100,
            error_rate_window: 1,

            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }
}

impl ClientPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_conns_per_node > 0 && self.min_conns_per_node > self.max_conns_per_node {
            return Err(Error::ClientError("minimum number of connections specified in the ClientPolicy is bigger than total connection pool size".into()));
        }

        Ok(())
    }

    pub(crate) fn application_id(&self) -> &str {
        if let Some(ref app_id) = self.application_id {
            if !app_id.is_empty() {
                return app_id;
            }
        }

        match self.auth_mode {
            crate::AuthMode::Internal(ref user, _) | crate::AuthMode::External(ref user, _) => {
                return user
            }
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

    /// Set username and password to use when authenticating to the cluster.
    pub fn set_auth_mode(&mut self, auth_mode: AuthMode) -> Result<()> {
        self.auth_mode = auth_mode;
        Ok(())
    }

    /// Return the hashed password for the auth mode.
    pub(crate) fn hashed_pass(&self) -> Option<String> {
        match self.auth_mode {
            AuthMode::External(_, ref password) | AuthMode::Internal(_, ref password) => {
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

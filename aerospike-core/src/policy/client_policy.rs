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
use crate::errors::Result;

#[cfg(feature = "tls")]
use tokio_rustls::rustls::ClientConfig;

#[derive(Debug, Clone, PartialEq)]
/// Determines authentication mode.
pub enum AuthMode {
    /// No Authentication will be performed
    None,

    /// Uses internal authentication only when user/password defined. Hashed password is stored
    /// on the server. Do not send clear password. This is the default.
    Internal(String, String),

    /// Uses external authentication (like LDAP) when user/password defined. Specific external authentication is
    /// configured on server.  If TLSConfig is defined, sends clear password on node login via TLS.
    /// Will return an error if TLSConfig is not defined.
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

    pub(crate) hashed_pass: Option<String>,

    /// TLS secure connection policy for TLS enabled servers.
    #[cfg(feature = "tls")]
    pub tls_config: Option<ClientConfig>,

    /// Initial host connection timeout in milliseconds.  The timeout when opening a connection
    /// to the server host for the first time.
    pub timeout: Option<Duration>,

    /// Connection idle timeout. Every time a connection is used, its idle
    /// deadline will be extended by this duration. When this deadline is reached,
    /// the connection will be closed and discarded from the connection pool.
    ///
    /// Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
    /// the server will stop automatically reaping based on socket idle timeouts.
    pub idle_timeout: Option<Duration>,

    /// Maximum number of synchronous connections allowed per server node.
    pub max_conns_per_node: usize,

    /// Number of connection pools used for each node. Machines with 8 CPU cores or less usually
    /// need only one connection pool per node. Machines with larger number of CPU cores may have
    /// their performance limited by contention for pooled connections. Contention for pooled
    /// connections can be reduced by creating multiple mini connection pools per node.
    pub conn_pools_per_node: usize,

    /// Throw exception if host connection fails during addHost().
    pub fail_if_not_connected: bool,

    /// Threshold at which the buffer attached to the connection will be shrunk by deallocating
    /// memory instead of just resetting the size of the underlying vec.
    /// Should be set to a value that covers as large a percentile of payload sizes as possible,
    /// while also being small enough not to occupy a significant amount of memory for the life
    /// of the connection pool.
    pub buffer_reclaim_threshold: usize,

    /// TendInterval determines interval for checking for cluster state changes.
    /// Minimum possible interval is 10 Milliseconds.
    pub tend_interval: Duration,

    /// A IP translation table is used in cases where different clients
    /// use different server IP addresses.  This may be necessary when
    /// using clients from both inside and outside a local area
    /// network. Default is no translation.
    /// The key is the IP address returned from friend info requests to other servers.
    /// The value is the real IP address used to connect to the server.
    pub ip_map: Option<HashMap<String, String>>,

    /// UseServicesAlternate determines if the client should use "services-alternate"
    /// instead of "services" in info request during cluster tending.
    /// "services-alternate" returns server configured external IP addresses that client
    /// uses to talk to nodes.  "services-alternate" can be used in place of
    /// providing a client "ipMap".
    /// This feature is recommended instead of using the client-side IpMap above.
    ///
    /// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
    pub use_services_alternate: bool,

    /// Expected cluster name. If not `None`, server nodes must return this cluster name in order
    /// to join the client's view of the cluster. Should only be set when connecting to servers
    /// that support the "cluster-name" info command.
    pub cluster_name: Option<String>,

    /// Mark this client as belonging to a rack, and track server rack data.  This field is useful when directing read commands to
    /// the server node that contains the key and exists on the same rack as the client.
    /// This serves to lower cloud provider costs when nodes are distributed across different
    /// racks/data centers.
    ///
    /// Replica.PreferRack and server rack configuration must
    /// also be set to enable this functionality.
    pub rack_ids: Option<HashSet<usize>>,
}

impl Default for ClientPolicy {
    fn default() -> ClientPolicy {
        ClientPolicy {
            auth_mode: AuthMode::None,
            hashed_pass: None,
            timeout: Some(Duration::new(30, 0)),
            idle_timeout: Some(Duration::new(30, 0)),
            max_conns_per_node: 256,
            conn_pools_per_node: 1,
            fail_if_not_connected: true,
            tend_interval: Duration::new(1, 0),
            ip_map: None,
            use_services_alternate: false,
            cluster_name: None,
            buffer_reclaim_threshold: 65536,
            rack_ids: None,

            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }
}

impl ClientPolicy {
    /// Set username and password to use when authenticating to the cluster.
    pub fn set_auth_mode(&mut self, auth_mode: AuthMode) -> Result<()> {
        match auth_mode {
            AuthMode::External(_, ref password) | AuthMode::Internal(_, ref password) => {
                let password = AdminCommand::hash_password(password)?;
                self.hashed_pass = Some(password);
            }
            _ => (),
        };
        self.auth_mode = auth_mode;
        Ok(())
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

// Copyright 2015-2017 Aerospike, Inc.
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

use std::time::Duration;
use std::collections::HashMap;

use errors::*;
use commands::admin_command::AdminCommand;

/// ClientPolicy encapsulates parameters for client policy command.
#[derive(Debug, Clone)]
pub struct ClientPolicy {
    /// User authentication to cluster. Leave empty for clusters running without restricted access.
    pub user_password: Option<(String, String)>,

    /// Initial host connection timeout in milliseconds.  The timeout when opening a connection
    /// to the server host for the first time.
    pub timeout: Option<Duration>,

    /// Connection idle timeout. Every time a connection is used, its idle
    /// deadline will be extended by this duration. When this deadline is reached,
    /// the connection will be closed and discarded from the connection pool.
    pub idle_timeout: Option<Duration>,

    /// Size of the Connection Queue cache.
    pub connection_pool_size_per_node: usize,

    /// Throw exception if host connection fails during addHost().
    pub fail_if_not_connected: bool,

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

    /// Size of the thread pool used in scan and query commands. These commands are often sent to
    /// multiple server nodes in parallel threads. A thread pool improves performance because
    /// threads do not have to be created/destroyed for each command.
    pub thread_pool_size: usize,
}

impl Default for ClientPolicy {
    fn default() -> ClientPolicy {
        ClientPolicy {
            user_password: None,
            timeout: Some(Duration::new(30, 0)),
            idle_timeout: Some(Duration::new(5, 0)),
            connection_pool_size_per_node: 256,
            fail_if_not_connected: true,
            tend_interval: Duration::new(1, 0),
            ip_map: None,
            use_services_alternate: false,
            thread_pool_size: 128,
        }
    }
}

impl ClientPolicy {
    /// Set username and password to use when authenticating to the cluster.
    pub fn set_user_password(&mut self, username: String, password: String) -> Result<()> {
        let password = AdminCommand::hash_password(&password)?;
        self.user_password = Some((username, password));
        Ok(())
    }
}

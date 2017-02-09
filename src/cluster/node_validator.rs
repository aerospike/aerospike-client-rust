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

use std::vec::Vec;
use std::net::ToSocketAddrs;
use std::str;

use errors::*;
use cluster::Cluster;
use commands::Message;
use net::{Host, Connection};
use policy::ClientPolicy;

// Validates a Database server node
#[derive(Clone)]
pub struct NodeValidator {
    pub name: String,
    pub aliases: Vec<Host>,
    pub address: String,
    pub client_policy: ClientPolicy,
    pub use_new_info: bool,
    pub supports_float: bool,
    pub supports_batch_index: bool,
    pub supports_replicas_all: bool,
    pub supports_geo: bool,
}

// Generates a node validator
impl NodeValidator {
    pub fn new(cluster: &Cluster) -> Self {
        NodeValidator {
            name: "".to_string(),
            aliases: vec![],
            address: "".to_string(),
            client_policy: cluster.client_policy().clone(),
            use_new_info: true,
            supports_float: false,
            supports_batch_index: false,
            supports_replicas_all: false,
            supports_geo: false,
        }
    }

    pub fn validate_node(&mut self, host: &Host) -> Result<()> {
        self.set_aliases(host).chain_err(|| "Failed to resolve host aliases")?;
        self.set_address().chain_err(|| "Failed to retrieve node address")?;
        Ok(())
    }

    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.to_vec()
    }

    fn set_aliases(&mut self, host: &Host) -> Result<()> {
        self.aliases = (host.name.as_ref(), host.port)
            .to_socket_addrs()?
            .map(|addr| Host::new(&addr.ip().to_string(), addr.port()))
            .collect();
        debug!("Resolved aliases for host {}: {:?}", host, self.aliases);
        Ok(())
    }

    fn set_address(&mut self) -> Result<()> {
        for alias in self.aliases.to_vec() {
            let mut conn = Connection::new_raw(&alias, &self.client_policy)?;
            conn.set_timeout(self.client_policy.timeout)?;

            let info_map = try!(Message::info(&mut conn, &["node", "features"]));

            if let Some(node_name) = info_map.get("node") {
                self.name = node_name.to_string();
                self.address = format!("{}:{}", alias.name, alias.port);

                if let Some(features) = info_map.get("features") {
                    self.set_features(features);
                }
            }
        }

        Ok(())
    }

    fn set_features(&mut self, features: &str) {
        let features = features.split(';');
        for feature in features {
            match feature {
                "float" => self.supports_float = true,
                "batch-index" => self.supports_batch_index = true,
                "replicas-all" => self.supports_replicas_all = true,
                "geo" => self.supports_geo = true,
                _ => (),
            }
        }
    }
}

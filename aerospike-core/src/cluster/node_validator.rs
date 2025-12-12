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
    pub services: Vec<Host>,
    pub address: String,
    pub client_policy: ClientPolicy,
    pub use_new_info: bool,
    pub version: Version,
}

// Generates a node validator
impl NodeValidator {
    pub fn new(client_policy: ClientPolicy) -> Self {
        NodeValidator {
            name: "".to_string(),
            services: vec![],
            aliases: vec![],
            address: "".to_string(),
            client_policy: client_policy,
            use_new_info: true,
            version: Version::default(),
        }
    }

    pub async fn validate_node(&mut self, cluster: &Cluster, host: &Host) -> Result<()> {
        self.resolve_aliases(host)
            .map_err(|e| e.chain_error("Failed to resolve host aliases"))?;

        let mut last_err = None;
        for alias in &self.aliases() {
            match self.validate_alias(cluster, alias).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    debug!("Alias {} failed: {:?}", alias, err);
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

    pub fn services(&self) -> Vec<Host> {
        self.services.clone()
    }

    fn resolve_aliases(&mut self, host: &Host) -> Result<()> {
        self.aliases = (host.name.as_ref(), host.port)
            .to_socket_addrs()?
            .map(|addr| {
                Host::new_tls(
                    &addr.ip().to_string(),
                    &host.tls_name.clone().unwrap_or("".into()),
                    addr.port(),
                )
            })
            .collect();

        debug!("Resolved aliases for host {}: {:?}", host, self.aliases);
        if self.aliases.is_empty() {
            Err(Error::Connection(format!("Failed to find addresses for {}", host)).into())
        } else {
            Ok(())
        }
    }

    async fn validate_alias(&mut self, cluster: &Cluster, alias: &Host) -> Result<()> {
        let mut conn = Connection::new(&alias, &self.client_policy).await?;
        let service_name = cluster.client_policy().await.service_string();
        let admin_policy = AdminPolicy {
            timeout: self.client_policy.timeout,
        };
        let info_map = Message::info(
            &admin_policy,
            &mut conn,
            &["node", "cluster-name", "build", service_name],
        )
        .await?;

        match info_map.get("node") {
            None => return Err(Error::InvalidNode(String::from("Missing node name"))),
            Some(node_name) => self.name = node_name.clone(),
        }

        if let Some(ref cluster_name) = cluster.cluster_name().await {
            match info_map.get("cluster-name") {
                None => return Err(Error::InvalidNode(String::from("Missing cluster name"))),
                Some(info_name) if info_name == cluster_name => {}
                Some(info_name) => {
                    return Err(Error::InvalidNode(format!(
                        "Cluster name mismatch: expected={},
                                                         got={}",
                        cluster_name, info_name
                    )))
                }
            }
        }

        self.address = alias.address();

        if let Some(build) = info_map.get("build") {
            let version = VersionParser::new(build).parse()?;
            self.version = version;
        }

        if let Some(peers) = info_map.get(service_name) {
            if peers.trim().len() > 0 {
                self.set_services(alias, peers);
            }
        }

        Ok(())
    }

    fn set_services(&mut self, alias: &Host, peers: &str) {
        let peers = peers.split(';');
        for peer in peers {
            match peer.to_hosts() {
                Err(e) => error!("Invalid host: {}, {}", peer, e),
                Ok(host) => {
                    let mut host: Vec<Host> = host
                        .into_iter()
                        .map(|h| Host {
                            tls_name: alias.tls_name.clone(),
                            ..h
                        })
                        .collect();
                    self.services.append(&mut host);
                }
            };
        }
    }
}

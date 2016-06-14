// Copyright 2013-2016 Aerospike, Inc.
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

use std::ffi::{CString, CStr};

use std::rc::Rc;
use std::cell::RefCell;
use std::error::Error;
use std::time::Duration;
use std::vec::Vec;
use std::sync::{Arc, Mutex};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs, TcpStream};
use std::str::FromStr;
use std::str;

use net::{Host, Connection};
use Cluster;
use error::AerospikeResult;
use command::info_command::Message;

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct NodeValidator<'a> {
    pub name: String,
    pub aliases: Vec<Host>,
    pub address: String,
    pub use_new_info: bool, // = true
    pub cluster: &'a Cluster,

    pub supports_float: bool,
    pub supports_batch_index: bool,
    pub supports_replicas_all: bool,
    pub supports_geo: bool,
}

fn socket_addr_to_string(s: &SocketAddr) -> String {
    unsafe {
        let ptr: *const _ = ::std::mem::transmute(&s.ip());
        str::from_utf8(CStr::from_ptr(ptr).to_bytes()).unwrap().into()
    }
}


// Generates a node validator
impl<'a> NodeValidator<'a> {
    pub fn new(cluster: &'a Cluster, host: &Host) -> AerospikeResult<Self> {
        let timeout = cluster.client_policy().timeout;
        let mut nv = NodeValidator {
            use_new_info: true,
            cluster: cluster,

            name: "".to_string(),
            aliases: vec![],
            address: "".to_string(),

            supports_float: false,
            supports_batch_index: false,
            supports_replicas_all: false,
            supports_geo: false,
        };

        try!(nv.set_aliases(host));
        try!(nv.set_address(timeout));

        Ok(nv)
    }

    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.to_vec()
    }

    fn set_aliases(&mut self, host: &Host) -> AerospikeResult<()> {
        let ip_parsed: Result<IpAddr, _> = FromStr::from_str(&host.name);
        match ip_parsed {
            Ok(_) => self.aliases = vec![Host::new(host.name.clone(), host.port)],
            _ => {
                let sa_parsed: Result<SocketAddr, _> = FromStr::from_str(&host.name);
                let addrs = try!(sa_parsed);
                let mut aliases: Vec<Host> = vec![];
                for addr in try!(addrs.to_socket_addrs()) {
                    aliases.push(Host::new(format!("{:?}", socket_addr_to_string(&addr)),
                                           host.port));
                }
                self.aliases = aliases;
            }
        }

        debug!("Node Validator has {} nodes.", self.aliases.len());
        Ok(())
    }

    fn set_address(&mut self, timeout: Option<Duration>) -> AerospikeResult<()> {
        for alias in self.aliases.to_vec() {
            let mut conn = try!(Connection::new_raw(&alias));
            try!(conn.set_timeout(timeout));

            // TODO: authenticate
            let info_map = try!(Message::info(&mut conn,
                                              &["node", "build", "features"]));

            if let Some(node_name) = info_map.get("node") {
                self.name = node_name.to_string();
                self.address = format!("{}:{}", alias.name, alias.port);

                if let Some(features) = info_map.get("features") {
                    try!(self.set_features(features.to_string()));
                }
            }
        }

        Ok(())
    }

    fn set_features(&mut self, features: String) -> AerospikeResult<()> {
        let features = features.split(";");
        for feature in features {
            match feature {
                "float" => self.supports_float = true,
                "batch-index" => self.supports_batch_index = true,
                "replicas-all" => self.supports_replicas_all = true,
                "geo" => self.supports_geo = true,
                _ => (),
            }
        }

        Ok(())
    }
}

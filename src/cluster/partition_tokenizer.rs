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

extern crate rustc_serialize;

use std::str;
use std::collections::HashMap;
use std::vec::Vec;
use std::sync::{Arc, RwLock};

use rustc_serialize::base64::FromBase64;

use errors::*;
use cluster::Node;
use net::Connection;
use command::info_command::Message;
use cluster::node;

const REPLICAS_NAME: &'static str = "replicas-master";

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct PartitionTokenizer {
    buffer: Vec<u8>,
    length: usize,
    offset: usize,
}

impl PartitionTokenizer {
    pub fn new(conn: &mut Connection) -> Result<Self> {
        let info_map = try!(Message::info(conn, &vec![REPLICAS_NAME]));
        if let Some(buf) = info_map.get(REPLICAS_NAME) {
            return Ok(PartitionTokenizer {
                length: info_map.len(),
                buffer: buf.as_bytes().to_owned(),
                offset: 0,
            });
        }
        bail!(ErrorKind::BadResponse("Missing replicas info".to_string()));
    }

    pub fn update_partition(&self,
                            nmap: Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>>,
                            node: Arc<Node>)
                            -> Result<HashMap<String, Vec<Arc<Node>>>> {

        let mut amap = nmap.read().unwrap().clone();

        // <ns>:<base64-encoded partition map>;<ns>:<base64-encoded partition map>; ...
        let part_str = try!(str::from_utf8(&self.buffer));
        let mut parts = part_str.trim_right().split(|c| c == ':' || c == ';');
        loop {
            match (parts.next(), parts.next()) {
                (Some(ns), Some(part)) => {
                    let ns = ns.to_string();
                    let restore_buffer = try!(part.from_base64());

                    if !amap.contains_key(&ns) {
                        let mut node_array: Vec<Arc<Node>> = Vec::with_capacity(node::PARTITIONS);
                        for _ in 0..node::PARTITIONS {
                            node_array.push(node.clone());
                        }
                        amap.insert(ns, node_array);
                    } else {
                        let mut node_array: &mut Vec<Arc<Node>> = amap.entry(ns).or_insert(vec![]);
                        for i in 0..node::PARTITIONS {
                            if restore_buffer[i >> 3] & (0x80 >> (i & 7) as u8) != 0 {
                                node_array[i] = node.clone();
                            }
                        }
                    }
                }
                (None, None) => break,
                _ => bail!(ErrorKind::BadResponse("Error parsing partition info".to_string()))
            }
        }

        Ok(amap)
    }
}

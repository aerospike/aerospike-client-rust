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
use std::str;
use std::sync::Arc;
use std::vec::Vec;

use crate::cluster::node;
use crate::cluster::Node;
use crate::commands::Message;
use crate::errors::{ErrorKind, Result};
use crate::net::Connection;

const REPLICAS_NAME: &str = "replicas-master";

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct PartitionTokenizer {
    buffer: Vec<u8>,
}

impl PartitionTokenizer {
    pub async fn new(conn: &mut Connection) -> Result<Self> {
        let info_map = Message::info(conn, &[REPLICAS_NAME]).await?;
        if let Some(buf) = info_map.get(REPLICAS_NAME) {
            return Ok(PartitionTokenizer {
                buffer: buf.as_bytes().to_owned(),
            });
        }
        bail!(ErrorKind::BadResponse("Missing replicas info".to_string()))
    }

    pub fn update_partition(
        &self,
        nmap: &mut HashMap<String, [Option<Arc<Node>>; node::PARTITIONS]>,
        node: Arc<Node>,
    ) -> Result<()> {
        // <ns>:<base64-encoded partition map>;<ns>:<base64-encoded partition map>; ...
        let part_str = str::from_utf8(&self.buffer)?;
        for part in part_str.trim_end().split(';') {
            match part.split_once(':') {
                Some((ns, part)) => {
                    let restore_buffer = base64::decode(part)?;
                    let entry = nmap.entry(ns.to_string()).or_insert_with(||[(); node::PARTITIONS].map(|_|None));
                    for (idx, item) in entry.iter_mut().enumerate() {
                        if restore_buffer[idx >> 3] & (0x80 >> (idx & 7) as u8) != 0 {
                            *item = Some(node.clone());
                        }
                    }
                }
                _ => bail!(ErrorKind::BadResponse(
                    "Error parsing partition info".to_string()
                )),
            }
        }

        Ok(())
    }
}

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

use std::str;
use std::sync::Arc;
use std::vec::Vec;

use crate::cluster::node;
use crate::cluster::Node;
use crate::commands::Message;
use crate::errors::{ErrorKind, Result};
use crate::net::Connection;

use super::{PartitionTable, PartitionForNamespace};

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct PartitionTokenizer {
    buffer: Vec<u8>,
    request_type: RequestedReplicas,
}

#[derive(Debug, Clone)]
enum RequestedReplicas {
    ReplicasMaster, // Ancient
    ReplicasAll, // Old
    Replicas, // Modern,
}

impl RequestedReplicas {
    const fn command(&self) -> &'static str {
        match self {
            RequestedReplicas::ReplicasMaster => "replicas-master",
            RequestedReplicas::ReplicasAll => "replicas-all",
            RequestedReplicas::Replicas => "replicas",
        }
    }
}

impl PartitionTokenizer {
    pub async fn new(conn: &mut Connection, node: &Arc<Node>) -> Result<Self> {
        let request_type = match (node.features().supports_replicas, node.features().supports_replicas_all) {
            (true, _) => RequestedReplicas::Replicas,
            (false, true) => RequestedReplicas::ReplicasAll,
            (false, false) => RequestedReplicas::ReplicasMaster,
        };

        let command = request_type.command();
        let info_map = Message::info(conn, &[command, node::PARTITION_GENERATION]).await?;
        if let Some(buf) = info_map.get(command) {
            return Ok(PartitionTokenizer {
                buffer: buf.as_bytes().to_owned(),
                request_type,
            });
        }

        // We re-update the partitions right now (in case its changed since it was last polled)
        node.update_partitions(&info_map)?;

        bail!(ErrorKind::BadResponse("Missing replicas info".to_string()))
    }

    pub fn update_partition(
        &self,
        nmap: &mut PartitionTable,
        node: &Arc<Node>,
    ) -> Result<()> {
        // <ns>:<base64-encoded partition map>;<ns>:<base64-encoded partition map>; ...
        let part_str = str::from_utf8(&self.buffer)?;
        for part in part_str.trim_end().split(';') {
            match part.split_once(':') {
                Some((ns, info)) => {
                    let mut info_section = info.split(',');
                    let reigime = if matches!(self.request_type, RequestedReplicas::Replicas) {
                        info_section
                        .next()
                        .ok_or_else(||ErrorKind::BadResponse("Missing reigime".to_string()))?
                        .parse()
                        .map_err(|err|ErrorKind::BadResponse(format!("Invalid reigime: {err}")))?
                    } else {
                        0
                    };

                    let n_replicas = if matches!(self.request_type, RequestedReplicas::Replicas | RequestedReplicas::ReplicasAll) {
                        info_section
                        .next()
                        .ok_or_else(||ErrorKind::BadResponse("Missing replicas count".to_string()))?
                        .parse()
                        .map_err(|err|ErrorKind::BadResponse(format!("Invalid replicas count: {err}")))?
                    } else {
                        1
                    };

                    let entry = nmap.entry(ns.to_string()).or_insert_with(PartitionForNamespace::default);

                    if entry.replicas != n_replicas && reigime >= entry.nodes.iter().map(|(r, _)|*r).max().unwrap_or_default() {
                        let wanted_size = n_replicas * node::PARTITIONS;
                        entry.nodes.resize_with(wanted_size, ||(0, None));
                        entry.replicas = n_replicas;
                    }

                    for (section, replica) in info_section.zip(entry.nodes.chunks_mut(node::PARTITIONS)) {
                        let restore_buffer = base64::decode(section)?;
                        for (idx, (this_reigimes, item)) in replica.iter_mut().enumerate() {
                            if restore_buffer[idx >> 3] & (0x80 >> (idx & 7) as u8) != 0 && reigime >= *this_reigimes {
                                *item = Some(node.clone());
                                *this_reigimes = reigime;
                            }
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

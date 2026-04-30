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

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::cluster::node;
use crate::cluster::Node;
use crate::errors::{Error, Result};

use super::PartitionTable;

/// Info commands required to populate a `PartitionTokenizer`. Exposed so
/// callers can issue them over their own connection (e.g. the long-lived
/// tend connection on a Node) and pass the resulting map to
/// [`from_info_map`](PartitionTokenizer::from_info_map).
pub const PARTITION_INFO_COMMANDS: &[&str] = &["replicas", node::PARTITION_GENERATION];

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct PartitionTokenizer {
    buffer: Vec<u8>,
    /// `partition-generation` reported alongside `replicas` in the same
    /// info round-trip. Committed onto the node by [`update_partition`]
    /// after the map has been successfully refreshed (Java's
    /// `partitionGeneration = parser.getGeneration()`).
    generation: isize,
}

impl PartitionTokenizer {
    /// Build a tokenizer from a previously-fetched info map. The caller
    /// is expected to have requested [`PARTITION_INFO_COMMANDS`] over a
    /// connection of its choosing — typically a Node's long-lived tend
    /// socket — and to hand the response in here.
    pub fn from_info_map(info_map: &HashMap<String, String>) -> Result<Self> {
        let generation = match info_map.get(node::PARTITION_GENERATION) {
            Some(s) => s.parse::<isize>().map_err(|err| {
                Error::BadResponse(format!("Invalid partition-generation: {err}"))
            })?,
            None => {
                return Err(Error::BadResponse(
                    "Missing partition-generation".to_string(),
                ))
            }
        };

        match info_map.get("replicas") {
            Some(buf) => Ok(PartitionTokenizer {
                buffer: buf.as_bytes().to_owned(),
                generation,
            }),
            None => Err(Error::BadResponse("Missing replicas info".to_string())),
        }
    }

    /// Convenience wrapper for callers that just want to fetch + parse on
    /// the node's tend connection.
    pub async fn from_node(node: &Arc<Node>, policy: &crate::AdminPolicy) -> Result<Self> {
        let info_map = node.tend_info(policy, PARTITION_INFO_COMMANDS).await?;
        Self::from_info_map(&info_map)
    }

    pub fn update_partition(&self, nmap: &mut PartitionTable, node: &Arc<Node>) -> Result<()> {
        // <ns>:<base64-encoded partition map>;<ns>:<base64-encoded partition map>; ...
        let part_str = str::from_utf8(&self.buffer)?;
        // Log a regime regression at most once per `update_partition` call
        // so a flood of stale slots doesn't spam the log. Mirrors Java's
        // `regimeError` flag in `PartitionParser`.
        let mut regime_error_logged = false;
        for part in part_str.trim_end().split(';') {
            match part.split_once(':') {
                Some((ns, info)) => {
                    let mut info_section = info.split(',');
                    let reigime = info_section
                        .next()
                        .ok_or_else(|| Error::BadResponse("Missing regime".to_string()))?
                        .parse()
                        .map_err(|err| Error::BadResponse(format!("Invalid regime: {err}")))?;

                    let n_replicas = info_section
                        .next()
                        .ok_or_else(|| Error::BadResponse("Missing replicas count".to_string()))?
                        .parse()
                        .map_err(|err| {
                            Error::BadResponse(format!("Invalid replicas count: {err}"))
                        })?;

                    let entry = nmap.entry(ns.to_string()).or_default();
                    entry.sc_mode = reigime != 0;

                    // Replica-count change → resize unconditionally. Java's
                    // PartitionParser does the same. The previous version
                    // gated the resize behind a monotonic-regime check,
                    // which could leave the array sized for the old replica
                    // count after a quick-restart that resets regime to 0.
                    if entry.replicas != n_replicas {
                        let wanted_size = n_replicas * node::PARTITIONS;
                        entry.nodes.resize_with(wanted_size, || (0, None));
                        entry.replicas = n_replicas;
                    }

                    for (section, replica) in
                        info_section.zip(entry.nodes.chunks_mut(node::PARTITIONS))
                    {
                        let restore_buffer = BASE64.decode(section)?;
                        for (idx, (this_reigimes, item)) in replica.iter_mut().enumerate() {
                            if restore_buffer[idx >> 3] & (0x80 >> (idx & 7) as u8) == 0 {
                                continue;
                            }
                            if reigime < *this_reigimes {
                                // Stale regime: don't overwrite. Log once
                                // per call to surface split-brain regimes
                                // without flooding.
                                if !regime_error_logged {
                                    info!(
                                        "{node} regime({reigime}) < old regime({})",
                                        *this_reigimes
                                    );
                                    regime_error_logged = true;
                                }
                                continue;
                            }
                            // If this slot was previously mapped to a
                            // different node, force that node to refresh
                            // its partition map next tend so it picks up
                            // the ownership transfer instead of returning
                            // a stale view. Mirrors Java's
                            // `nodeOld.partitionGeneration = -1`.
                            if let Some(prev) = item.as_ref() {
                                if !Arc::ptr_eq(prev, node) {
                                    prev.set_partition_generation(-1);
                                }
                            }
                            *item = Some(node.clone());
                            *this_reigimes = reigime;
                        }
                    }
                }
                _ => {
                    return Err(Error::BadResponse(
                        "Error parsing partition info".to_string(),
                    ))
                }
            }
        }

        // Commit the generation only after the map has been parsed
        // successfully — mirrors Java `partitionGeneration = parser.getGeneration()`
        // at the bottom of `Node.refreshPartitions`. Without this, every
        // tend would re-fetch the partition map indefinitely because
        // `verify_partition_generation` keeps comparing against the stale
        // value.
        node.set_partition_generation(self.generation);

        Ok(())
    }
}

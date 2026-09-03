// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::Node;

use std::sync::Arc;

/// Representation of the status of a scan/query  for a specific partition.
#[derive(Debug)]
pub struct PartitionStatus {
    /// Record's bval.
    pub bval: Option<u64>,
    /// Partition Id.
    pub id: u16,
    /// Should the partition be retried?
    pub retry: bool,
    /// Digest of the key to retry from.
    pub digest: Option<[u8; 20]>,

    /// Partition's corresponding node.
    pub node: Option<Arc<Node>>,

    /// Partition map's corresponding sequence.
    pub sequence: Option<u64>,

    /// Round marker for the delivery watermark below. Bumped when the
    /// partition is assigned for a (re)query round, so entries a consumer
    /// drains from an earlier round can no longer move the cursor.
    pub(crate) epoch: u32,
    /// Records of this partition handed to the record channel this round,
    /// in digest order — the stamp source.
    pub(crate) delivered: u32,
    /// Longest contiguous prefix of `delivered` the consumers have taken
    /// out. Only this prefix's tail digest is a safe resume point.
    pub(crate) consumed: u32,
    /// Consumed-out-of-order stamps waiting for the gap below them to close:
    /// `(seq, digest, bval)`, sorted by `seq`. Empty whenever a single
    /// consumer drains the stream in order.
    pub(crate) pending: Vec<(u32, [u8; 20], Option<u64>)>,
}

impl PartitionStatus {
    pub(crate) const fn new(partition_id: usize) -> Self {
        PartitionStatus {
            bval: None,
            id: partition_id as u16,
            retry: true,
            digest: None,

            node: None,
            sequence: None,

            epoch: 0,
            delivered: 0,
            consumed: 0,
            pending: Vec::new(),
        }
    }

    /// Opens a new delivery round: entries stamped in earlier rounds become
    /// stale and stop moving the cursor.
    pub(crate) fn begin_delivery_round(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.delivered = 0;
        self.consumed = 0;
        self.pending.clear();
    }

    pub(crate) const fn set_digest(&mut self, digest: Option<[u8; 20]>) {
        self.digest = digest;
    }

    pub(crate) const fn reset_sequence(&mut self) {
        self.sequence = None;
    }

    pub(crate) fn reset_node(&mut self) {
        self.node = None;
    }
}

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

use crate::query::PartitionStatus;
use crate::Node;

use parking_lot::{Mutex, MutexGuard};

use std::sync::Arc;

/// The partitions of one node's share of a scan/query.
///
/// `parts_full` and `parts_partial` hold *indices* into `parts`, the status
/// array shared with the owning
/// [`PartitionFilter`](crate::query::PartitionFilter). Holding an
/// `Arc<Mutex<PartitionStatus>>` per entry instead meant one heap allocation
/// per partition — 4096 of them for a full scan — where an index costs two
/// bytes and the array is allocated once.
#[derive(Debug)]
pub struct NodePartitions {
    pub(crate) node: Arc<Node>,
    pub(crate) parts: Arc<Vec<Mutex<PartitionStatus>>>,
    pub(crate) parts_full: Vec<u16>,
    pub(crate) parts_partial: Vec<u16>,
    pub(crate) record_count: u64,
    pub(crate) record_max: u64,
    pub(crate) disallowed_count: u64,
    pub(crate) parts_unavailable: u64,
}

impl NodePartitions {
    pub fn new(node: Arc<Node>, capacity: usize, parts: Arc<Vec<Mutex<PartitionStatus>>>) -> Self {
        NodePartitions {
            node,
            parts,
            parts_full: Vec::with_capacity(capacity),
            parts_partial: Vec::with_capacity(capacity),
            record_count: 0,
            record_max: 0,
            disallowed_count: 0,
            parts_unavailable: 0,
        }
    }

    /// Files partition `index` under "full" or "partial" depending on whether a
    /// resume digest was carried over from an earlier scan/query.
    pub fn add_partition(&mut self, index: u16) {
        let has_digest = self.parts[index as usize].lock().digest.is_some();

        if has_digest {
            self.parts_partial.push(index);
        } else {
            self.parts_full.push(index);
        }
    }

    /// Locks and reads one of this node's partition statuses by index.
    pub(crate) fn status(&self, index: u16) -> MutexGuard<'_, PartitionStatus> {
        self.parts[index as usize].lock()
    }
}

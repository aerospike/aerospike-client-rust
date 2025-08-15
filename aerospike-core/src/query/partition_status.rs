// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
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

    pub(crate) node: Option<Arc<Node>>,
    pub(crate) sequence: Option<u64>,
}

impl PartitionStatus {
    pub(crate) fn new(partition_id: usize) -> Self {
        PartitionStatus {
            bval: None,
            id: partition_id as u16,
            retry: true,
            digest: None,

            node: None,
            sequence: None,
        }
    }

    pub(crate) fn set_digest(&mut self, digest: Option<[u8; 20]>) {
        self.digest = digest;
    }

    pub(crate) fn reset_sequence(&mut self) {
        self.sequence = None;
    }

    pub(crate) fn reset_node(&mut self) {
        self.node = None;
    }
}

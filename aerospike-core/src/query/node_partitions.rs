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

use crate::query::SemanticSync;
use crate::query::PartitionStatus;
use crate::Node;

use std::sync::Arc;

#[derive(Debug)]
pub struct NodePartitions {
    pub(crate) node: Arc<Node>,
    pub(crate) parts_full: Vec<Arc<SemanticSync<PartitionStatus>>>,
    pub(crate) parts_partial: Vec<Arc<SemanticSync<PartitionStatus>>>,
    pub(crate) record_count: u64,
    pub(crate) record_max: u64,
    pub(crate) disallowed_count: u64,
    pub(crate) parts_unavailable: u64,
}

impl NodePartitions {
    pub fn new(node: Arc<Node>, capacity: usize) -> Self {
        NodePartitions {
            node,
            parts_full: Vec::with_capacity(capacity),
            parts_partial: Vec::with_capacity(capacity),
            record_count: 0,
            record_max: 0,
            disallowed_count: 0,
            parts_unavailable: 0,
        }
    }

    pub fn add_partition(&mut self, part: Arc<SemanticSync<PartitionStatus>>) {
        let digest = part.digest;

        match digest {
            None => self.parts_full.push(part),
            Some(_) => self.parts_partial.push(part),
        }
    }
}

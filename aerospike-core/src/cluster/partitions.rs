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

use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::cluster::node;
use crate::policy::Replica;
use crate::Key;
use crate::Node;

// Validates a Database server node
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Partitions {
    pub replicas: Vac<Vec<Arc<Node>>>,
    pub sc_mode: bool,
    regimes: Vec<usize>,
}

impl Partitions {
    fn new(partition_count: usize, replica_count: usize, cp_mode: bool) -> Self {
        let mut replicas: Vec<Vec<Option<Arc<Node>>>> = Vec::new_capacity(replica_count);
        for _ in 0..replica_count {
            let mut p = Vec<Arc<Node>>::new_capacity(partition_count);
            p.resize(partition_count, None);
            replicas.push(p);
        }

    let regimes = Vec::new_capacity(partition_count);
    regimes.resize(partition_count, 0);

        Partitions{
            replicas: replicas,
            sc_mode:   cpMode,
            regimes:  regimes,
        }
    }

    fn set_replica_count(&mut self, replica_count: usize) {
        if self.replicas.length < replica_count {
            let i = self.replicas.length();

            // Extend the size
                let mut p = Vec<Arc<Node>>::new_capacity(_PARTITIONS);
                self.replicas.resize(replica_count, p);
        } else {
            // Reduce the size
            self.replicas.resize(replica_count);
        }
}
    }

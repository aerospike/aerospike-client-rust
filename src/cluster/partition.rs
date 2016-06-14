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

use std::rc::Rc;
use std::cell::RefCell;
use std::error::Error;
use std::time::Duration;
use std::vec::Vec;
use std::sync::Arc;
use net::Host;
use std::io::Cursor;
use std::fmt;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use Cluster;
use cluster::node;
use common::Key;
use Value;
use error::AerospikeResult;

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct Partition<'a> {
    pub namespace: &'a str,
    pub partition_id: usize,
}

impl<'a> Partition<'a> {
    pub fn new(namespace: &'a str, partition_id: usize) -> Self {
        Partition {
            namespace: namespace,
            partition_id: partition_id,
        }
    }

    pub fn new_by_key(key: &Key<'a>) -> Self {
        let mut rdr = Cursor::new(&key.digest[0..4]);

        Partition {
            namespace: key.namespace,

            // CAN'T USE MOD directly - mod will give negative numbers.
            // First AND makes positive and negative correctly, then mod.
            // For any x, y : x % 2^y = x & (2^y - 1); the second method is twice as fast
            partition_id: rdr.read_u32::<LittleEndian>().unwrap() as usize & (node::PARTITIONS - 1),
        }
    }
}


impl<'a> PartialEq for Partition<'a> {
    fn eq(&self, other: &Partition) -> bool {
        self.namespace == other.namespace && self.partition_id == other.partition_id
    }
}

impl<'a> fmt::Display for Partition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        format!("Partition ({}: {})", self.namespace, self.partition_id).fmt(f)
    }
}

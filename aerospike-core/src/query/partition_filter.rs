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

use crate::cluster::node;
use crate::query::PartitionStatus;
use crate::Key;

use aerospike_rt::Mutex;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// `PartitionFilter` is used in scan/queries. This filter is also used as a cursor.
///
/// If a previous scan/query returned all records specified by a `PartitionFilter` instance, a
/// future scan/query using the same `PartitionFilter` instance will only return new records added
/// after the last record read (in digest order) in each partition in the previous scan/query.
#[derive(Debug)]
pub struct PartitionFilter {
    /// Beginning partition
    pub begin: usize,
    /// Number of partitions from the beginning partition to include.
    pub count: usize,
    /// Digest of a Key to scan/query
    pub digest: Option<[u8; 20]>,

    /// status of the partitions
    pub partitions: Option<Vec<Arc<Mutex<PartitionStatus>>>>,

    /// Is partition completely scanned/queried.
    pub done: AtomicBool,

    /// Should the partition be retried.
    pub retry: AtomicBool,
}

impl PartitionFilter {
    pub(crate) const fn new(begin: usize, count: usize) -> Self {
        PartitionFilter {
            begin,
            count,
            digest: None,

            partitions: None,
            done: AtomicBool::new(false),
            retry: AtomicBool::new(false),
        }
    }

    /// Creates a partition filter that
    /// reads all the partitions.
    pub const fn all() -> Self {
        Self::new(0, node::PARTITIONS)
    }

    /// `NewPartitionFilterById` creates a partition filter by partition id.
    /// Partition id is between 0 - 4095
    pub const fn by_id(partition_id: usize) -> Self {
        Self::new(partition_id, 1)
    }

    /// `NewPartitionFilterByRange` creates a partition filter by partition range.
    /// begin partition id is between 0 - 4095
    /// count is the number of partitions, in the range of 1 - 4096 inclusive.
    pub const fn by_range(begin: usize, count: usize) -> Self {
        Self::new(begin, count)
    }

    /// Returns records after the key's digest in the partition containing the digest.
    /// Records in all other partitions are not included. The digest is used to determine
    /// order and this is not the same as userKey order.
    //
    /// This method only works for scan or query with nil filter (primary index query).
    /// This method does not work for a secondary index query because the digest alone
    /// is not sufficient to determine a cursor in a secondary index query.   
    pub fn by_key(key: &Key) -> Self {
        PartitionFilter {
            begin: key.partition_id(),
            count: 1,
            digest: Some(key.digest),

            partitions: None,
            done: AtomicBool::new(false),
            retry: AtomicBool::new(false),
        }
    }

    /// Returns true if all specified data has been read.
    pub fn done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    pub(crate) fn set_partitions(&mut self, partitions: Vec<Arc<Mutex<PartitionStatus>>>) {
        self.partitions = Some(partitions);
    }

    pub(crate) async fn reset_partition_status(&mut self) {
        if let Some(ref mut partitions) = self.partitions {
            // Reset replica sequence and last node used.
            for part in partitions.iter_mut() {
                let mut part = part.lock().await;
                part.reset_sequence();
                part.reset_node();
            }
        }
    }
}

impl Default for PartitionFilter {
    fn default() -> Self {
        Self::all()
    }
}

impl Clone for PartitionFilter {
    fn clone(&self) -> Self {
        Self {
            begin: self.begin,
            count: self.count,
            digest: self.digest,

            partitions: self.partitions.clone(),
            done: AtomicBool::new(self.done.load(Ordering::Relaxed)),
            retry: AtomicBool::new(self.retry.load(Ordering::Relaxed)),
        }
    }
}

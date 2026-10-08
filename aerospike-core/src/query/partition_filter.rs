// Copyright 2015-2026 Aerospike, Inc.
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

use parking_lot::Mutex;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(feature = "serialization")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

    /// Status of each partition in `[begin, begin + count)`, indexed by
    /// offset from `begin`.
    ///
    /// One heap allocation for the whole range, shared by `Arc`. The per-node
    /// groupings refer to entries by index rather than holding an `Arc` each,
    /// because a
    /// full-partition scan otherwise allocated 4096 separate
    /// `Arc<Mutex<PartitionStatus>>` blocks — about 819 KB and 4096
    /// allocations per query, before a single record was read.
    ///
    /// The lock is a `parking_lot::Mutex`: every critical section is a few
    /// field assignments with no `.await` inside, so an async mutex bought
    /// nothing, and unlike `std::sync::Mutex` this one neither allocates on
    /// first lock nor costs more than a byte per partition.
    ///
    /// Hidden: reachable for language bindings that rebuild cursors; not API.
    #[doc(hidden)]
    pub partitions: Option<Arc<Vec<Mutex<PartitionStatus>>>>,

    /// Is partition completely scanned/queried.
    pub(crate) done: AtomicBool,

    /// Should the partition be retried.
    pub(crate) retry: AtomicBool,
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

    pub(crate) fn set_partitions(&mut self, partitions: Arc<Vec<Mutex<PartitionStatus>>>) {
        self.partitions = Some(partitions);
    }

    pub(crate) fn reset_partition_status(&self) {
        if let Some(ref partitions) = self.partitions {
            // Reset replica sequence and last node used.
            for part in partitions.iter() {
                let mut part = part.lock();
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

/// A clone is an independent cursor: the per-partition progress is copied,
/// so two queries resumed from a filter and its clone do not share state.
impl Clone for PartitionFilter {
    fn clone(&self) -> Self {
        let partitions = self.partitions.as_ref().map(|parts| {
            Arc::new(
                parts
                    .iter()
                    .map(|part| Mutex::new(part.lock().clone()))
                    .collect(),
            )
        });
        Self {
            begin: self.begin,
            count: self.count,
            digest: self.digest,
            partitions,
            done: AtomicBool::new(self.done.load(Ordering::Relaxed)),
            retry: AtomicBool::new(self.retry.load(Ordering::Relaxed)),
        }
    }
}

/// Serde form of a [`PartitionFilter`]: the range and each partition's resume
/// point — id, retry flag, bval and last digest, the fields the Go client
/// persists too — without the transient node and sequence state.
#[cfg(feature = "serialization")]
#[derive(Serialize, Deserialize)]
struct FilterRepr {
    begin: usize,
    count: usize,
    digest: Option<[u8; 20]>,
    done: bool,
    /// One entry per partition of the range, in order; empty for a filter no
    /// query has run yet.
    partitions: Vec<PartitionRepr>,
}

#[cfg(feature = "serialization")]
#[derive(Serialize, Deserialize)]
struct PartitionRepr {
    id: u16,
    retry: bool,
    bval: Option<u64>,
    digest: Option<[u8; 20]>,
}

/// A filter serializes as its resume state, so a paginated query can hand
/// its cursor to another process and continue there: deserialize it, pass it
/// to the same query (namespace, set and statement), and the records resume
/// after the last digest read in each partition. Take the snapshot once the
/// stream is exhausted, where
/// [`RecordStream::partition_filter`](crate::query::RecordStream::partition_filter)
/// hands the filter back.
#[cfg(feature = "serialization")]
impl Serialize for PartitionFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let partitions = self.partitions.as_ref().map_or_else(Vec::new, |parts| {
            parts
                .iter()
                .map(|part| {
                    let part = part.lock();
                    PartitionRepr {
                        id: part.id,
                        retry: part.retry,
                        bval: part.bval,
                        digest: part.digest,
                    }
                })
                .collect()
        });
        FilterRepr {
            begin: self.begin,
            count: self.count,
            digest: self.digest,
            done: self.done(),
            partitions,
        }
        .serialize(serializer)
    }
}

/// Rebuilds a filter from its serialized resume state. The range must lie
/// within `0..4096` and the entries, when present, must cover exactly that
/// range in order — anything else is a deserialization error rather than a
/// filter that skips or repeats partitions.
#[cfg(feature = "serialization")]
impl<'de> Deserialize<'de> for PartitionFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;

        let FilterRepr {
            begin,
            count,
            digest,
            done,
            partitions,
        } = FilterRepr::deserialize(deserializer)?;

        if count == 0 || begin >= node::PARTITIONS || begin + count > node::PARTITIONS {
            return Err(D::Error::custom(format!(
                "invalid partition range ({begin},{count})"
            )));
        }
        if !partitions.is_empty() && partitions.len() != count {
            return Err(D::Error::custom(format!(
                "{} partition entries for a range of {count}",
                partitions.len()
            )));
        }
        if let Some((i, entry)) = partitions
            .iter()
            .enumerate()
            .find(|(i, entry)| usize::from(entry.id) != begin + i)
        {
            return Err(D::Error::custom(format!(
                "entry {i} is partition {}, expected {}",
                entry.id,
                begin + i
            )));
        }

        let retry = partitions.is_empty() || partitions.iter().any(|entry| entry.retry);
        let partitions = if partitions.is_empty() {
            None
        } else {
            Some(Arc::new(
                partitions
                    .into_iter()
                    .map(|entry| {
                        let mut status = PartitionStatus::new(usize::from(entry.id));
                        status.retry = entry.retry;
                        status.bval = entry.bval;
                        status.digest = entry.digest;
                        Mutex::new(status)
                    })
                    .collect(),
            ))
        };

        Ok(PartitionFilter {
            begin,
            count,
            digest,
            partitions,
            done: AtomicBool::new(done),
            retry: AtomicBool::new(retry),
        })
    }
}

#[cfg(all(test, feature = "serialization"))]
mod tests {
    use std::sync::atomic::Ordering;

    use super::PartitionFilter;
    use crate::query::PartitionTracker;

    fn round_trip(filter: &PartitionFilter) -> PartitionFilter {
        let json = serde_json::to_string(filter).expect("serialize");
        serde_json::from_str(&json).expect("deserialize")
    }

    #[test]
    fn fresh_filter_round_trips() {
        let filter = PartitionFilter::by_range(10, 5);
        let json = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            json,
            r#"{"begin":10,"count":5,"digest":null,"done":false,"partitions":[]}"#
        );

        let rebuilt = round_trip(&filter);
        assert_eq!(
            (rebuilt.begin, rebuilt.count, rebuilt.digest),
            (10, 5, None)
        );
        assert!(
            rebuilt.partitions.is_none(),
            "no query ran, nothing to resume from"
        );
        assert!(!rebuilt.done());
        assert!(rebuilt.retry.load(Ordering::Relaxed));
    }

    #[test]
    fn resume_state_survives_a_round_trip() {
        let mut filter = PartitionFilter::by_range(0, 4);
        filter.set_partitions(PartitionTracker::init_partitions(0, 4, None));
        {
            let parts = filter.partitions.as_ref().unwrap();
            let mut finished = parts[2].lock();
            finished.retry = false;
            finished.digest = Some([7; 20]);
            finished.bval = Some(9);
        }
        filter.done.store(true, Ordering::Relaxed);

        let rebuilt = round_trip(&filter);
        assert!(rebuilt.done());
        assert!(
            rebuilt.retry.load(Ordering::Relaxed),
            "three partitions still want a retry"
        );
        {
            let parts = rebuilt
                .partitions
                .as_ref()
                .expect("entries were carried over");
            assert_eq!(parts.len(), 4);
            assert!(parts[0].lock().retry);
            let finished = parts[2].lock();
            assert_eq!(
                (finished.id, finished.retry, finished.bval, finished.digest),
                (2, false, Some(9), Some([7; 20]))
            );
            assert!(finished.node.is_none(), "transient state is not persisted");
            drop(finished);
        }
        assert_eq!(
            serde_json::to_string(&rebuilt).unwrap(),
            serde_json::to_string(&filter).unwrap()
        );
    }

    /// The serialized form is a contract with every cursor an application
    /// has already stored: field names, order and encodings. This fixture
    /// is the `v1` schema, fully populated, and it is frozen — it must load
    /// forever, and the current code must keep producing it byte for byte
    /// for the same filter. A change that fails these tests breaks stored
    /// cursors. The only acceptable evolution is adding a field the
    /// deserializer treats as optional, with a `v2` fixture added alongside
    /// this one, never replacing it.
    const FIXTURE_V1: &str = r#"{"begin":3,"count":4,"digest":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171],"done":false,"partitions":[{"id":3,"retry":true,"bval":null,"digest":null},{"id":4,"retry":false,"bval":42,"digest":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]},{"id":5,"retry":true,"bval":18446744073709551615,"digest":[255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]},{"id":6,"retry":false,"bval":null,"digest":[0,255,0,255,0,255,0,255,0,255,0,255,0,255,0,255,0,255,0,255]}]}"#;

    /// One partition's persisted state: id, retry, bval, digest.
    type Entry = (u16, bool, Option<u64>, Option<[u8; 20]>);

    fn reference_filter() -> PartitionFilter {
        let mut filter = PartitionFilter::by_range(3, 4);
        filter.digest = Some([0xAB; 20]);
        filter.set_partitions(PartitionTracker::init_partitions(3, 4, None));
        {
            let parts = filter.partitions.as_ref().unwrap();
            let set = |i: usize, retry: bool, bval: Option<u64>, digest: Option<[u8; 20]>| {
                let mut part = parts[i].lock();
                part.retry = retry;
                part.bval = bval;
                part.digest = digest;
            };
            set(0, true, None, None);
            set(
                1,
                false,
                Some(42),
                Some(core::array::from_fn(|i| i as u8 + 1)),
            );
            set(2, true, Some(u64::MAX), Some([0xFF; 20]));
            set(
                3,
                false,
                None,
                Some(core::array::from_fn(|i| if i % 2 == 0 { 0 } else { 255 })),
            );
        }
        filter
    }

    /// Backward compatibility: a v1 cursor always loads, with every field
    /// landing where it did when it was written.
    #[test]
    fn a_v1_cursor_always_deserializes() {
        let loaded: PartitionFilter =
            serde_json::from_str(FIXTURE_V1).expect("the v1 fixture must load");
        assert_eq!((loaded.begin, loaded.count), (3, 4));
        assert_eq!(loaded.digest, Some([0xAB; 20]));
        assert!(!loaded.done());
        assert!(loaded.retry.load(Ordering::Relaxed));

        let parts = loaded.partitions.as_ref().expect("four entries");
        let snapshot: Vec<Entry> = parts
            .iter()
            .map(|p| {
                let p = p.lock();
                (p.id, p.retry, p.bval, p.digest)
            })
            .collect();
        assert_eq!(
            snapshot,
            vec![
                (3, true, None, None),
                (
                    4,
                    false,
                    Some(42),
                    Some(core::array::from_fn(|i| i as u8 + 1))
                ),
                (5, true, Some(u64::MAX), Some([0xFF; 20])),
                (
                    6,
                    false,
                    None,
                    Some(core::array::from_fn(|i| if i % 2 == 0 { 0 } else { 255 }))
                ),
            ]
        );
    }

    /// Forward compatibility, producer side: today's code still writes the
    /// v1 form byte for byte, so a cursor it stores is readable by any
    /// client that understands v1. Field names, order and encodings are
    /// all pinned here, which also covers non-self-describing formats that
    /// depend on field order and count.
    #[test]
    fn the_current_code_still_writes_the_v1_form() {
        assert_eq!(
            serde_json::to_string(&reference_filter()).unwrap(),
            FIXTURE_V1
        );
        let reloaded: PartitionFilter = serde_json::from_str(FIXTURE_V1).unwrap();
        assert_eq!(
            serde_json::to_string(&reloaded).unwrap(),
            FIXTURE_V1,
            "load then store is the identity"
        );
    }

    /// Forward compatibility, consumer side: a cursor written by a newer
    /// client that added fields, at either level, still loads here, so a
    /// rolling upgrade can run in either direction.
    #[test]
    fn fields_from_a_newer_client_are_ignored() {
        let newer = FIXTURE_V1
            .replacen(
                "\"begin\":3,",
                "\"schema\":2,\"begin\":3,\"owner\":\"job-7\",",
                1,
            )
            .replacen("\"id\":4,", "\"id\":4,\"generation\":9,", 1);
        assert_ne!(newer, FIXTURE_V1);
        let loaded: PartitionFilter =
            serde_json::from_str(&newer).expect("unknown fields are skipped");
        assert_eq!(serde_json::to_string(&loaded).unwrap(), FIXTURE_V1);
    }

    #[test]
    fn a_cursor_that_does_not_cover_its_range_is_rejected() {
        let entry = |id: u16| format!(r#"{{"id":{id},"retry":true,"bval":null,"digest":null}}"#);
        let json = |begin: usize, count: usize, entries: &[u16]| {
            let entries: Vec<String> = entries.iter().map(|&id| entry(id)).collect();
            format!(
                r#"{{"begin":{begin},"count":{count},"digest":null,"done":false,"partitions":[{}]}}"#,
                entries.join(",")
            )
        };
        let parse = |s: String| serde_json::from_str::<PartitionFilter>(&s);

        assert!(parse(json(5, 3, &[5, 6])).is_err(), "two entries for three");
        assert!(parse(json(5, 3, &[5, 7, 6])).is_err(), "out of order");
        assert!(parse(json(5, 3, &[5, 6, 7])).is_ok());
        assert!(
            parse(json(4096, 1, &[])).is_err(),
            "begin past the last partition"
        );
        assert!(parse(json(4000, 97, &[])).is_err(), "range past the end");
        assert!(parse(json(0, 0, &[])).is_err(), "empty range");
    }
}

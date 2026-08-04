// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Internal Top-K merge engine.
//!
//! Each node hands this merger its own already-bounded (`<= k`) result
//! buffer, accumulated by `Client::execute_top_k_query`
//! (`aerospike-core/src/client.rs`) via `StreamCommand::top_k_buffer`;
//! `TopKMerger::merge` combines those buffers into the final global Top-K:
//! comparator-sort, dedup by digest (generation-first, then rank), truncate
//! to `limit`.
//!
//! No shared mutable state is mutated concurrently here — each node's buffer
//! is produced independently, and `merge` runs once, sequentially, after
//! every node's buffer is available (once the whole job completes with no
//! errors — see `Client::execute_top_k_query`'s doc comment for the
//! whole-job-retry rationale).

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::query::order_by::{Order, OrderBy, OrderByFlags, OrderByType};
use crate::Record;
use crate::Value;

/// The extracted, comparable form of a record's order-by bin value. `Nil`
/// represents a missing bin or a bin whose value doesn't match `OrderByType`
/// (design doc contract: NIL sorts after every non-NIL value, in both
/// directions).
#[derive(Debug, Clone, PartialEq)]
enum RankValue {
    Integer(i64),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Nil,
}

impl RankValue {
    fn extract(record: &Record, order_by: &OrderBy) -> Self {
        let Some(value) = record.bins.get(&order_by.bin_name) else {
            return RankValue::Nil;
        };

        match (order_by.order_type, value) {
            (OrderByType::Integer, Value::Int(i)) => RankValue::Integer(*i),
            (OrderByType::Double, Value::Float(f)) => RankValue::Double(f64::from(f)),
            (OrderByType::String, Value::String(s)) => {
                if order_by.flags == OrderByFlags::CaseInsensitive {
                    RankValue::String(s.to_lowercase())
                } else {
                    RankValue::String(s.clone())
                }
            }
            (OrderByType::Bytes, Value::Blob(b)) => RankValue::Bytes(b.clone()),
            // Type mismatch (e.g. declared Integer but the bin holds a String) is
            // treated the same as a missing bin: NIL.
            _ => RankValue::Nil,
        }
    }

    /// Ordering ignoring `Nil`/`Nil` and `Nil`/non-`Nil` cases, which the
    /// caller (`rank_cmp`) handles first so this only needs to compare two
    /// same-variant, non-`Nil` values.
    fn cmp_non_nil(&self, other: &Self) -> Ordering {
        match (self, other) {
            (RankValue::Integer(a), RankValue::Integer(b)) => a.cmp(b),
            (RankValue::Double(a), RankValue::Double(b)) => a.total_cmp(b),
            (RankValue::String(a), RankValue::String(b)) => a.cmp(b),
            (RankValue::Bytes(a), RankValue::Bytes(b)) => a.cmp(b),
            // Unreachable in practice: `extract` always produces a `RankValue`
            // variant matching `order_by.order_type`, so two `RankValue`s
            // derived from the same `OrderBy` are always the same variant.
            _ => Ordering::Equal,
        }
    }
}

/// Compares two candidates' rank, per the design doc / server `cf_topk`
/// contract: NIL sorts after every non-NIL value in *both* directions; ties
/// (including NIL/NIL) are equal here and broken by digest ascending by the
/// caller. Returns an ordering where `Less` means "ranks better" (i.e. this
/// is the ordering a min-heap-of-the-worst or a `sort_by` would use to put
/// the best candidates first).
fn rank_cmp(a: &RankValue, b: &RankValue, direction: Order) -> Ordering {
    match (a, b) {
        (RankValue::Nil, RankValue::Nil) => Ordering::Equal,
        (RankValue::Nil, _) => Ordering::Greater,
        (_, RankValue::Nil) => Ordering::Less,
        (a, b) => {
            let cmp = a.cmp_non_nil(b);
            match direction {
                Order::Asc => cmp,
                Order::Desc => cmp.reverse(),
            }
        }
    }
}

/// Digest tie-break: ascending, per the design doc / server contract.
fn digest_cmp(a: &Record, b: &Record) -> Ordering {
    let da = a.key.as_ref().map(|k| k.digest);
    let db = b.key.as_ref().map(|k| k.digest);
    da.cmp(&db)
}

/// Merges per-node Top-K result buffers into the final global Top-K.
pub(crate) struct TopKMerger {
    order_by: OrderBy,
    limit: usize,
}

impl TopKMerger {
    pub(crate) const fn new(order_by: OrderBy, limit: usize) -> Self {
        TopKMerger { order_by, limit }
    }

    /// Full best-first comparator: rank, then digest ascending on ties.
    fn compare(&self, a: &Record, b: &Record) -> Ordering {
        let ra = RankValue::extract(a, &self.order_by);
        let rb = RankValue::extract(b, &self.order_by);
        rank_cmp(&ra, &rb, self.order_by.direction).then_with(|| digest_cmp(a, b))
    }

    /// Merge every node's already-bounded result buffer into the final
    /// global Top-K: concatenate, dedup by digest (generation-first, then
    /// rank — per the server's `cf_topk` contract; this differs from the
    /// Java client's rank-only dedup rule), sort best-first, and truncate
    /// to `limit`.
    pub(crate) fn merge(&self, per_node_results: Vec<Vec<Record>>) -> Vec<Record> {
        let mut by_digest: HashMap<[u8; 20], Record> = HashMap::new();

        for record in per_node_results.into_iter().flatten() {
            let Some(digest) = record.key.as_ref().map(|k| k.digest) else {
                // No digest to dedup on (shouldn't happen on a real query
                // stream, where every record carries its key) — keep it,
                // keyed by a value that can never collide with a real digest
                // dedup entry, so it still participates in the final sort.
                by_digest.insert(fallback_key(&record), record);
                continue;
            };

            match by_digest.get(&digest) {
                None => {
                    by_digest.insert(digest, record);
                }
                Some(existing) => {
                    if self.should_replace(existing, &record) {
                        by_digest.insert(digest, record);
                    }
                }
            }
        }

        let mut merged: Vec<Record> = by_digest.into_values().collect();
        merged.sort_by(|a, b| self.compare(a, b));
        merged.truncate(self.limit);
        merged
    }

    /// Generation-first, then rank: the server's authoritative dedup rule
    /// (`cf_topk.h`: "Duplicate digests keep the highest generation, then
    /// the better-ranked entry ... if generations tie").
    fn should_replace(&self, existing: &Record, candidate: &Record) -> bool {
        match candidate.generation.cmp(&existing.generation) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => self.compare(candidate, existing) == Ordering::Less,
        }
    }
}

/// Synthesizes a unique map key for a record with no digest, so it doesn't
/// collide with (or get silently dropped by) real digest-keyed entries. Only
/// reachable if a caller feeds this merger records that didn't come from a
/// normal query stream.
fn fallback_key(record: &Record) -> [u8; 20] {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    record.generation.hash(&mut hasher);
    for (k, v) in &record.bins {
        k.hash(&mut hasher);
        format!("{v}").hash(&mut hasher);
    }
    let h = hasher.finish().to_le_bytes();
    let mut digest = [0xffu8; 20];
    digest[..8].copy_from_slice(&h);
    digest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexMap, Key};

    fn key_with_digest(byte: u8) -> Key {
        Key {
            namespace: "ns".to_string(),
            set_name: "set".to_string(),
            user_key: None,
            digest: [byte; 20],
        }
    }

    fn record(digest_byte: u8, generation: u32, bin: Option<(&str, Value)>) -> Record {
        let mut bins = IndexMap::new();
        if let Some((name, value)) = bin {
            bins.insert(name.to_string(), value);
        }
        Record::new(
            Some(key_with_digest(digest_byte)),
            bins,
            None,
            generation,
            0,
        )
    }

    fn order_by(direction: Order) -> OrderBy {
        OrderBy {
            bin_name: "score".to_string(),
            order_type: OrderByType::Integer,
            direction,
            flags: OrderByFlags::None,
        }
    }

    #[test]
    fn nil_ranks_last_in_both_directions() {
        for direction in [Order::Asc, Order::Desc] {
            let merger = TopKMerger::new(order_by(direction), 10);
            let with_value = record(1, 1, Some(("score", Value::Int(5))));
            let missing_bin = record(2, 1, None);
            let wrong_type = record(3, 1, Some(("score", Value::String("oops".into()))));

            let merged = merger.merge(vec![vec![with_value.clone(), missing_bin, wrong_type]]);
            assert_eq!(merged.len(), 3);
            assert_eq!(merged[0].key.as_ref().unwrap().digest, with_value.key.unwrap().digest, "non-NIL must sort before NIL for direction {direction:?}");
        }
    }

    #[test]
    fn descending_orders_largest_first() {
        let merger = TopKMerger::new(order_by(Order::Desc), 10);
        let low = record(1, 1, Some(("score", Value::Int(1))));
        let high = record(2, 1, Some(("score", Value::Int(9))));
        let mid = record(3, 1, Some(("score", Value::Int(5))));

        let merged = merger.merge(vec![vec![low, high.clone(), mid]]);
        assert_eq!(merged[0].key.as_ref().unwrap().digest, high.key.unwrap().digest);
        assert_eq!(merged[0].bins["score"], Value::Int(9));
        assert_eq!(merged[2].bins["score"], Value::Int(1));
    }

    #[test]
    fn ascending_orders_smallest_first() {
        let merger = TopKMerger::new(order_by(Order::Asc), 10);
        let low = record(1, 1, Some(("score", Value::Int(1))));
        let high = record(2, 1, Some(("score", Value::Int(9))));

        let merged = merger.merge(vec![vec![high, low.clone()]]);
        assert_eq!(merged[0].key.as_ref().unwrap().digest, low.key.unwrap().digest);
    }

    #[test]
    fn ties_break_by_digest_ascending() {
        let merger = TopKMerger::new(order_by(Order::Desc), 10);
        let a = record(9, 1, Some(("score", Value::Int(5))));
        let b = record(1, 1, Some(("score", Value::Int(5))));

        let merged = merger.merge(vec![vec![a, b.clone()]]);
        assert_eq!(merged[0].key.as_ref().unwrap().digest, b.key.unwrap().digest);
    }

    #[test]
    fn dedup_prefers_higher_generation_even_if_worse_ranked() {
        let merger = TopKMerger::new(order_by(Order::Desc), 10);
        // Same digest, two "reads" of the same record: an older, better-ranked
        // read (gen 1, score 9) and a newer, worse-ranked read (gen 2, score 1).
        // The server's generation-first rule must keep the newer one.
        let stale_but_better = record(1, 1, Some(("score", Value::Int(9))));
        let fresh_but_worse = record(1, 2, Some(("score", Value::Int(1))));

        let merged = merger.merge(vec![vec![stale_but_better], vec![fresh_but_worse]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].generation, 2);
        assert_eq!(merged[0].bins["score"], Value::Int(1));
    }

    #[test]
    fn dedup_falls_back_to_rank_when_generation_ties() {
        let merger = TopKMerger::new(order_by(Order::Desc), 10);
        let worse = record(1, 1, Some(("score", Value::Int(1))));
        let better = record(1, 1, Some(("score", Value::Int(9))));

        let merged = merger.merge(vec![vec![worse], vec![better]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].bins["score"], Value::Int(9));
    }

    #[test]
    fn case_insensitive_string_comparison() {
        let order_by = OrderBy {
            bin_name: "name".to_string(),
            order_type: OrderByType::String,
            direction: Order::Asc,
            flags: OrderByFlags::CaseInsensitive,
        };
        let merger = TopKMerger::new(order_by, 10);
        let upper = record(1, 1, Some(("name", Value::String("Banana".into()))));
        let lower = record(2, 1, Some(("name", Value::String("apple".into()))));

        let merged = merger.merge(vec![vec![upper, lower.clone()]]);
        assert_eq!(merged[0].key.as_ref().unwrap().digest, lower.key.unwrap().digest);
    }

    #[test]
    fn bytes_compare_lexicographically() {
        let order_by = OrderBy {
            bin_name: "b".to_string(),
            order_type: OrderByType::Bytes,
            direction: Order::Asc,
            flags: OrderByFlags::None,
        };
        let merger = TopKMerger::new(order_by, 10);
        let a = record(1, 1, Some(("b", Value::Blob(vec![2, 0]))));
        let b = record(2, 1, Some(("b", Value::Blob(vec![1, 0, 0]))));

        let merged = merger.merge(vec![vec![a, b.clone()]]);
        assert_eq!(merged[0].key.as_ref().unwrap().digest, b.key.unwrap().digest);
    }

    #[test]
    fn truncates_to_limit_across_multiple_node_buffers() {
        let merger = TopKMerger::new(order_by(Order::Desc), 2);
        let records: Vec<Record> = (0..5)
            .map(|i| record(i, 1, Some(("score", Value::Int(i as i64)))))
            .collect();
        // Split across two "node buffers" to mirror real per-node input.
        let (first, second) = records.split_at(3);
        let merged = merger.merge(vec![first.to_vec(), second.to_vec()]);

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].bins["score"], Value::Int(4));
        assert_eq!(merged[1].bins["score"], Value::Int(3));
    }

    #[test]
    fn heap_eviction_boundary_k_minus_1_k_k_plus_1() {
        for (n, k) in [(4usize, 5usize), (5, 5), (6, 5)] {
            let merger = TopKMerger::new(order_by(Order::Desc), k);
            let records: Vec<Record> = (0..n)
                .map(|i| record(i as u8, 1, Some(("score", Value::Int(i as i64)))))
                .collect();
            let merged = merger.merge(vec![records]);
            assert_eq!(merged.len(), n.min(k));
            // Best-first: highest score first.
            for pair in merged.windows(2) {
                assert!(pair[0].bins["score"] >= pair[1].bins["score"]);
            }
        }
    }

    // Concurrency-adjacent regression test (the Java client's own test
    // suite doesn't cover this): simulate multiple "node tasks"
    // independently observing the *same* digest at different
    // generations/values, arriving in every possible order, and assert the
    // outcome is always the correct, order-independent generation-first pick
    // -- not just "doesn't panic under load".
    #[test]
    fn dedup_is_order_independent_for_racing_duplicate_digests() {
        let merger = TopKMerger::new(order_by(Order::Desc), 10);
        let candidates = [
            record(7, 1, Some(("score", Value::Int(100)))), // gen 1, best rank
            record(7, 3, Some(("score", Value::Int(1)))),   // gen 3, worst rank -> must win
            record(7, 2, Some(("score", Value::Int(50)))),  // gen 2, mid rank
        ];

        // Every permutation of "arrival order" (as if fed by racing node
        // tasks / partition re-scans) must converge on the same answer.
        use itertools_like_permutations::permutations;
        for perm in permutations(&candidates) {
            let buffers: Vec<Vec<Record>> = perm.into_iter().map(|r| vec![r]).collect();
            let merged = merger.merge(buffers);
            assert_eq!(merged.len(), 1);
            assert_eq!(merged[0].generation, 3, "must always keep the highest generation regardless of arrival order");
            assert_eq!(merged[0].bins["score"], Value::Int(1));
        }
    }

    // Minimal permutation helper so this test doesn't need an extra dev-dependency.
    mod itertools_like_permutations {
        use crate::Record;

        pub(super) fn permutations(items: &[Record]) -> Vec<Vec<Record>> {
            fn helper(remaining: Vec<Record>, acc: &mut Vec<Record>, out: &mut Vec<Vec<Record>>) {
                if remaining.is_empty() {
                    out.push(acc.clone());
                    return;
                }
                for i in 0..remaining.len() {
                    let mut rest = remaining.clone();
                    let item = rest.remove(i);
                    acc.push(item);
                    helper(rest, acc, out);
                    acc.pop();
                }
            }
            let mut out = Vec::new();
            helper(items.to_vec(), &mut Vec::new(), &mut out);
            out
        }
    }
}

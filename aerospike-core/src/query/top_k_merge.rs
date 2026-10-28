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

//! Internal bounded Top-K reducer.
//!
//! One accumulator is shared by every node command of a Top-K query. It keeps
//! the best `k` records by the statement's order keys, deduplicates a record
//! that two nodes both returned (a partition that moved mid-query) by keeping
//! the later-updated copy, and hands the survivors out best-first.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use crate::query::order_by::{Order, OrderBy, OrderByFlags, OrderByType};
use crate::Record;
use crate::Value;

/// Comparable form of one order key's value in a record.
#[derive(Debug, Clone, PartialEq)]
enum RankValue {
    Integer(i64),
    Double(f64),
    String(Vec<u8>),
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
                    RankValue::String(
                        s.as_bytes()
                            .iter()
                            .map(u8::to_ascii_lowercase)
                            .collect(),
                    )
                } else {
                    RankValue::String(s.as_bytes().to_vec())
                }
            }
            (OrderByType::Bytes, Value::Blob(b)) => RankValue::Bytes(b.clone()),
            // Missing or mismatched values rank as NIL.
            _ => RankValue::Nil,
        }
    }

    /// Compares two non-NIL values in natural order.
    // The exact comparison is the point: it is what makes -0.0 equal to 0.0.
    #[allow(clippy::float_cmp)]
    fn cmp_non_nil(&self, other: &Self) -> Ordering {
        match (self, other) {
            (RankValue::Integer(a), RankValue::Integer(b)) => a.cmp(b),
            // NaN sorts after every other double; -0.0 and 0.0 are equal, so
            // the digest decides between them, as it does for any tie.
            (RankValue::Double(a), RankValue::Double(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => {
                    if a == b {
                        Ordering::Equal
                    } else {
                        a.total_cmp(b)
                    }
                }
            },
            (RankValue::String(a), RankValue::String(b))
            | (RankValue::Bytes(a), RankValue::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

/// Compares one key. `Less` means the first value ranks better. NIL ranks last
/// whatever the direction.
fn key_cmp(a: &RankValue, b: &RankValue, direction: Order) -> Ordering {
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

/// A record's order key: one rank per order-by clause, compared in order.
#[derive(Debug, Clone)]
struct Rank(Vec<RankValue>);

impl Rank {
    fn extract(record: &Record, keys: &[OrderBy]) -> Self {
        Rank(keys.iter().map(|key| RankValue::extract(record, key)).collect())
    }
}

/// Compares two ranks key by key. `Less` means the first rank is better.
fn rank_cmp(a: &Rank, b: &Rank, keys: &[OrderBy]) -> Ordering {
    keys.iter()
        .zip(a.0.iter().zip(b.0.iter()))
        .map(|(key, (x, y))| key_cmp(x, y, key.direction))
        .find(|cmp| *cmp != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

/// Best-first order: the rank, then the digest ascending.
fn best_cmp(a: (&Rank, &[u8; 20]), b: (&Rank, &[u8; 20]), keys: &[OrderBy]) -> Ordering {
    rank_cmp(a.0, b.0, keys).then_with(|| a.1.cmp(b.1))
}

/// Bounded Top-K accumulator.
pub struct TopKAccumulator {
    keys: Arc<[OrderBy]>,
    limit: usize,
    by_digest: HashMap<[u8; 20], Candidate>,
    /// The heap's greatest element is the next record to evict.
    worst_first: BinaryHeap<HeapKey>,
}

impl TopKAccumulator {
    pub fn new(keys: Vec<OrderBy>, limit: usize) -> Self {
        TopKAccumulator {
            keys: keys.into(),
            limit,
            by_digest: HashMap::new(),
            worst_first: BinaryHeap::new(),
        }
    }

    /// Offers one query response record with the last-update time its row
    /// carried (0 when the server sent none), retaining only the best `k`.
    pub fn accept(&mut self, record: Record, last_update_time: u64) {
        let digest = record
            .key
            .as_ref()
            .map_or_else(|| fallback_key(&record), |key| key.digest);
        let candidate = Candidate {
            rank: Rank::extract(&record, &self.keys),
            record,
            last_update_time,
        };

        if let Some(existing) = self.by_digest.get(&digest) {
            if !self.replaces(&candidate, existing) {
                return;
            }
            self.by_digest.insert(digest, candidate);
            self.rebuild_heap();
            return;
        }

        if self.by_digest.len() >= self.limit {
            // Would be evicted at once.
            if let Some(worst) = self.worst_first.peek() {
                if best_cmp(
                    (&candidate.rank, &digest),
                    (&worst.rank, &worst.digest),
                    &self.keys,
                ) != Ordering::Less
                {
                    return;
                }
            }
        }

        self.worst_first.push(HeapKey {
            rank: candidate.rank.clone(),
            digest,
            keys: self.keys.clone(),
        });
        self.by_digest.insert(digest, candidate);

        if self.by_digest.len() > self.limit {
            let worst = self
                .worst_first
                .pop()
                .expect("Top-K heap is non-empty when its map exceeds its limit");
            self.by_digest.remove(&worst.digest);
        }
    }

    /// Forgets every retained record, for a retried round.
    pub fn clear(&mut self) {
        self.by_digest.clear();
        self.worst_first.clear();
    }

    /// Drains retained records in best-first order.
    pub fn take_results(&mut self) -> Vec<Record> {
        self.worst_first.clear();
        let keys = self.keys.clone();
        let mut entries: Vec<(Rank, [u8; 20], Record)> = std::mem::take(&mut self.by_digest)
            .into_iter()
            .map(|(digest, candidate)| (candidate.rank, digest, candidate.record))
            .collect();
        entries.sort_by(|a, b| best_cmp((&a.0, &a.1), (&b.0, &b.1), &keys));
        entries.into_iter().map(|(_, _, record)| record).collect()
    }

    /// A duplicate digest keeps the later-updated copy; with the same
    /// last-update time, the better-ranked one.
    fn replaces(&self, candidate: &Candidate, existing: &Candidate) -> bool {
        if candidate.last_update_time != existing.last_update_time {
            return candidate.last_update_time > existing.last_update_time;
        }
        rank_cmp(&candidate.rank, &existing.rank, &self.keys) == Ordering::Less
    }

    fn rebuild_heap(&mut self) {
        let keys = self.keys.clone();
        self.worst_first = self
            .by_digest
            .iter()
            .map(|(digest, candidate)| HeapKey {
                rank: candidate.rank.clone(),
                digest: *digest,
                keys: keys.clone(),
            })
            .collect();
    }
}

/// A retained record, its pre-extracted order key and its last-update time.
struct Candidate {
    record: Record,
    rank: Rank,
    last_update_time: u64,
}

/// Heap element used to select the next record to evict.
struct HeapKey {
    rank: Rank,
    digest: [u8; 20],
    keys: Arc<[OrderBy]>,
}

impl HeapKey {
    /// Compares candidates in best-first order.
    fn best_cmp(&self, other: &Self) -> Ordering {
        best_cmp(
            (&self.rank, &self.digest),
            (&other.rank, &other.digest),
            &self.keys,
        )
    }
}

impl PartialEq for HeapKey {
    fn eq(&self, other: &Self) -> bool {
        self.best_cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapKey {}

impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.best_cmp(other)
    }
}

/// Synthesizes a map key for a record without a digest.
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

    fn record(digest_byte: u8, bins: &[(&str, Value)]) -> Record {
        let mut map = IndexMap::new();
        for (name, value) in bins {
            map.insert((*name).to_string(), value.clone());
        }
        Record::new(Some(key_with_digest(digest_byte)), map, None, 1, 0)
    }

    fn score(digest_byte: u8, score: i64) -> Record {
        record(digest_byte, &[("score", Value::Int(score))])
    }

    fn key(bin: &str, order_type: OrderByType, direction: Order, flags: OrderByFlags) -> OrderBy {
        OrderBy {
            bin_name: bin.to_string(),
            order_type,
            direction,
            flags,
        }
    }

    fn by_score(direction: Order) -> Vec<OrderBy> {
        vec![key("score", OrderByType::Integer, direction, OrderByFlags::None)]
    }

    /// Merges records that all carry the same last-update time.
    fn merge(keys: Vec<OrderBy>, limit: usize, records: Vec<Record>) -> Vec<Record> {
        merge_with_lut(keys, limit, records.into_iter().map(|r| (r, 0)).collect())
    }

    fn merge_with_lut(keys: Vec<OrderBy>, limit: usize, records: Vec<(Record, u64)>) -> Vec<Record> {
        let mut accumulator = TopKAccumulator::new(keys, limit);
        for (record, lut) in records {
            accumulator.accept(record, lut);
        }
        accumulator.take_results()
    }

    fn digest_of(record: &Record) -> u8 {
        record.key.as_ref().unwrap().digest[0]
    }

    fn digests(records: &[Record]) -> Vec<u8> {
        records.iter().map(digest_of).collect()
    }

    #[test]
    fn nil_ranks_last_in_both_directions() {
        for direction in [Order::Asc, Order::Desc] {
            let with_value = score(1, 5);
            let missing_bin = record(2, &[]);
            let wrong_type = record(3, &[("score", Value::String("oops".into()))]);

            let merged = merge(by_score(direction), 10, vec![missing_bin, wrong_type, with_value]);
            assert_eq!(merged.len(), 3);
            assert_eq!(
                digest_of(&merged[0]),
                1,
                "non-NIL must sort before NIL for direction {direction:?}"
            );
        }
    }

    #[test]
    fn descending_orders_largest_first() {
        let merged = merge(by_score(Order::Desc), 10, vec![score(1, 1), score(2, 9), score(3, 5)]);
        assert_eq!(digests(&merged), vec![2, 3, 1]);
    }

    #[test]
    fn ascending_orders_smallest_first() {
        let merged = merge(by_score(Order::Asc), 10, vec![score(2, 9), score(1, 1)]);
        assert_eq!(digests(&merged), vec![1, 2]);
    }

    #[test]
    fn ties_break_by_digest_ascending() {
        let merged = merge(by_score(Order::Desc), 10, vec![score(9, 5), score(1, 5)]);
        assert_eq!(digests(&merged), vec![1, 9]);
    }

    #[test]
    fn dedup_keeps_the_later_updated_copy_even_if_worse_ranked() {
        let merged = merge_with_lut(
            by_score(Order::Desc),
            10,
            vec![(score(1, 9), 100), (score(1, 1), 200)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].bins["score"], Value::Int(1));
    }

    #[test]
    fn dedup_ignores_an_earlier_updated_copy() {
        let merged = merge_with_lut(
            by_score(Order::Desc),
            10,
            vec![(score(1, 1), 200), (score(1, 9), 100)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].bins["score"], Value::Int(1));
    }

    #[test]
    fn dedup_falls_back_to_rank_when_last_update_time_ties() {
        let merged = merge(by_score(Order::Desc), 10, vec![score(1, 1), score(1, 9)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].bins["score"], Value::Int(9));

        let merged = merge(by_score(Order::Desc), 10, vec![score(1, 9), score(1, 1)]);
        assert_eq!(merged[0].bins["score"], Value::Int(9));
    }

    #[test]
    fn dedup_is_order_independent_for_racing_duplicate_digests() {
        let candidates = [
            (score(7, 100), 1), // oldest, best rank
            (score(7, 1), 3),   // newest, worst rank -> must win
            (score(7, 50), 2),
        ];
        for perm in permutations(&candidates) {
            let merged = merge_with_lut(by_score(Order::Desc), 10, perm);
            assert_eq!(merged.len(), 1);
            assert_eq!(
                merged[0].bins["score"],
                Value::Int(1),
                "the latest copy must win whatever the arrival order"
            );
        }
    }

    #[test]
    fn a_candidate_that_cannot_place_is_skipped_without_disturbing_the_heap() {
        let mut accumulator = TopKAccumulator::new(by_score(Order::Desc), 2);
        accumulator.accept(score(1, 10), 0);
        accumulator.accept(score(2, 20), 0);
        accumulator.accept(score(3, 5), 0); // worse than both: skipped
        assert_eq!(accumulator.by_digest.len(), 2);
        assert!(!accumulator.by_digest.contains_key(&[3; 20]));
        accumulator.accept(score(4, 15), 0); // displaces 10
        assert_eq!(digests(&accumulator.take_results()), vec![2, 4]);
    }

    #[test]
    fn clear_forgets_everything() {
        let mut accumulator = TopKAccumulator::new(by_score(Order::Desc), 5);
        accumulator.accept(score(1, 10), 0);
        accumulator.clear();
        assert!(accumulator.take_results().is_empty());
        accumulator.accept(score(2, 1), 0);
        assert_eq!(digests(&accumulator.take_results()), vec![2]);
    }

    #[test]
    fn second_key_breaks_ties_in_its_own_direction() {
        let keys = vec![
            key("score", OrderByType::Integer, Order::Desc, OrderByFlags::None),
            key("name", OrderByType::String, Order::Asc, OrderByFlags::None),
        ];
        let rec = |d: u8, s: i64, n: &str| {
            record(d, &[("score", Value::Int(s)), ("name", Value::String(n.into()))])
        };
        let merged = merge(
            keys,
            10,
            vec![rec(1, 5, "zed"), rec(2, 5, "amy"), rec(3, 7, "zed"), rec(4, 5, "amy")],
        );
        // 7 first; then the 5s by name ascending, then digest.
        assert_eq!(digests(&merged), vec![3, 2, 4, 1]);
    }

    #[test]
    fn second_key_nil_ranks_last_among_equal_first_keys() {
        let keys = vec![
            key("score", OrderByType::Integer, Order::Asc, OrderByFlags::None),
            key("name", OrderByType::String, Order::Desc, OrderByFlags::None),
        ];
        let merged = merge(
            keys,
            10,
            vec![
                score(1, 5),
                record(2, &[("score", Value::Int(5)), ("name", Value::String("a".into()))]),
            ],
        );
        assert_eq!(digests(&merged), vec![2, 1]);
    }

    #[test]
    fn case_insensitive_string_comparison() {
        let keys = vec![key(
            "name",
            OrderByType::String,
            Order::Asc,
            OrderByFlags::CaseInsensitive,
        )];
        let upper = record(1, &[("name", Value::String("Banana".into()))]);
        let lower = record(2, &[("name", Value::String("apple".into()))]);
        let merged = merge(keys, 10, vec![upper, lower]);
        assert_eq!(digests(&merged), vec![2, 1]);
    }

    #[test]
    fn case_insensitive_string_comparison_folds_ascii_only() {
        let keys = vec![key(
            "name",
            OrderByType::String,
            Order::Asc,
            OrderByFlags::CaseInsensitive,
        )];
        // ASCII-only folding retains UTF-8 byte ordering: "Ä" < "ä".
        let upper = record(9, &[("name", Value::String("Ä".into()))]);
        let lower = record(1, &[("name", Value::String("ä".into()))]);
        let merged = merge(keys, 10, vec![lower, upper]);
        assert_eq!(digests(&merged), vec![9, 1]);
    }

    #[test]
    fn nan_sorts_after_every_finite_double_in_natural_order() {
        let keys = |direction| vec![key("score", OrderByType::Double, direction, OrderByFlags::None)];
        let finite = || record(1, &[("score", Value::from(5.0))]);
        let negative_nan = || {
            record(
                2,
                &[("score", Value::from(f64::from_bits(0xfff8_0000_0000_0000)))],
            )
        };

        let merged = merge(keys(Order::Asc), 10, vec![negative_nan(), finite()]);
        assert_eq!(digests(&merged), vec![1, 2]);
        // NaN is the natural maximum, so descending puts it first.
        let merged = merge(keys(Order::Desc), 10, vec![finite(), negative_nan()]);
        assert_eq!(digests(&merged), vec![2, 1]);
    }

    #[test]
    fn negative_zero_equals_zero_and_the_digest_decides() {
        let keys = vec![key("score", OrderByType::Double, Order::Asc, OrderByFlags::None)];
        let positive = record(9, &[("score", Value::from(0.0))]);
        let negative = record(1, &[("score", Value::from(-0.0))]);
        let merged = merge(keys.clone(), 10, vec![positive, negative]);
        assert_eq!(digests(&merged), vec![1, 9]);

        let positive = record(1, &[("score", Value::from(0.0))]);
        let negative = record(9, &[("score", Value::from(-0.0))]);
        let merged = merge(keys, 10, vec![negative, positive]);
        assert_eq!(digests(&merged), vec![1, 9]);
    }

    #[test]
    fn bytes_compare_lexicographically() {
        let keys = vec![key("b", OrderByType::Bytes, Order::Asc, OrderByFlags::None)];
        let a = record(1, &[("b", Value::Blob(vec![2, 0]))]);
        let b = record(2, &[("b", Value::Blob(vec![1, 0, 0]))]);
        let merged = merge(keys, 10, vec![a, b]);
        assert_eq!(digests(&merged), vec![2, 1]);
    }

    #[test]
    fn truncates_to_limit_across_arrivals() {
        let records: Vec<Record> = (0..5).map(|i| score(i, i64::from(i))).collect();
        let merged = merge(by_score(Order::Desc), 2, records);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].bins["score"], Value::Int(4));
        assert_eq!(merged[1].bins["score"], Value::Int(3));
    }

    #[test]
    fn heap_eviction_boundary_k_minus_1_k_k_plus_1() {
        for (n, k) in [(4usize, 5usize), (5, 5), (6, 5)] {
            let records: Vec<Record> = (0..n).map(|i| score(i as u8, i as i64)).collect();
            let merged = merge(by_score(Order::Desc), k, records);
            assert_eq!(merged.len(), n.min(k));
            for pair in merged.windows(2) {
                assert!(pair[0].bins["score"] >= pair[1].bins["score"]);
            }
        }
    }

    fn permutations(items: &[(Record, u64)]) -> Vec<Vec<(Record, u64)>> {
        fn helper(
            remaining: Vec<(Record, u64)>,
            acc: &mut Vec<(Record, u64)>,
            out: &mut Vec<Vec<(Record, u64)>>,
        ) {
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

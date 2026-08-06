// Copyright 2026 Aerospike, Inc.
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

//! The k best of everything seen, in O(k) memory.
//!
//! This is the whole trick behind both aggregations, and the reason it works on a namespace larger than memory:
//! a bounded max-heap keeps the *worst* of the k best at the top, so every record after the first k costs one
//! comparison and, at most, one push and one pop. The accumulator never grows.
//!
//! # One rule, no special cases
//!
//! **Larger is always worse.** `Metric::score` inverts the similarity metrics before anything reaches this
//! module, so the heap, the merge, the ordering and the client-side combine share one comparison and none of
//! them knows which metric produced the number. That inversion lives in exactly one function.
//!
//! # Why merging is exact
//!
//! `merge` keeps the k best of two heaps. That operation is associative and commutative, so it does not matter
//! how the records were divided between threads, nodes, or dumps: **combining per-thread heaps gives the same k
//! as one heap over everything.** It is what makes a distributed exact top-k possible without any node seeing
//! more than k results — and it also means an accumulator that *dumps* stays correct, because the client merges
//! the dumped heaps the same way.

use std::collections::BinaryHeap;

use aerospike_udf_sdk_native::{partial, Blob};

partial! {
    /// One record in the running: what identifies it, and how good it is.
    ///
    /// `score` is arranged so larger is worse — see the module note. `digest` is always present and always 20
    /// bytes; `key` carries the primary key **only if the client stored one**, which most do not, so it is
    /// usually empty. Both are here because a caller needs to fetch the winners afterwards, and a digest is
    /// enough to do that.
    ///
    /// Deliberately **not** the record's bins. A heap of k×(vector + bins) is the thing this design exists to
    /// avoid; the caller batch-reads the k winners, which is how top-k works everywhere else too.
    pub struct Hit {
        pub score: f64,
        pub digest: Blob,
        pub key: String,
    }
}

impl Eq for Hit {}

impl Ord for Hit {
    /// By score, then by digest.
    ///
    /// **The digest tiebreak is load-bearing.** Two records at the same distance are otherwise ordered by
    /// whichever thread happened to see one first, so a top-k over ties would differ run to run and no test
    /// could pin it. Comparing digests makes the answer a function of the data alone.
    ///
    /// `total_cmp` rather than `partial_cmp`: `f64` is not `Ord` because of NaN, and a NaN distance — which a
    /// vector of NaNs will produce — must sort somewhere definite rather than poisoning the heap.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.digest.cmp(&other.digest))
    }
}

impl PartialOrd for Hit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The k best, and nothing else.
#[derive(Debug, Default)]
pub struct BestK {
    /// A max-heap, so the *worst* kept hit is on top and is the one a new candidate has to beat.
    heap: BinaryHeap<Hit>,
    k: usize,
}

impl BestK {
    pub fn new(k: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(k.min(1024)),
            k,
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// The worst score currently kept, once the heap is full.
    ///
    /// A caller can use this to skip work: a candidate that cannot beat it does not need its identity built.
    pub fn threshold(&self) -> Option<f64> {
        if self.heap.len() < self.k {
            None
        } else {
            self.heap.peek().map(|h| h.score)
        }
    }

    /// Offer a candidate. Kept only if it is better than the worst of the k.
    pub fn offer(&mut self, hit: Hit) {
        if self.k == 0 {
            return;
        }

        if self.heap.len() < self.k {
            self.heap.push(hit);
            return;
        }

        // The comparison that makes this O(1) per record after the first k.
        if let Some(worst) = self.heap.peek() {
            if hit < *worst {
                self.heap.pop();
                self.heap.push(hit);
            }
        }
    }

    /// Fold another heap in, keeping the k best of both.
    ///
    /// Associative and commutative, which is what makes the distributed answer exact — see the module note.
    pub fn absorb(&mut self, other: impl IntoIterator<Item = Hit>) {
        for hit in other {
            self.offer(hit);
        }
    }

    /// Best first.
    pub fn sorted(&self) -> Vec<Hit> {
        let mut out: Vec<Hit> = self.heap.iter().cloned().collect();

        out.sort();

        out
    }

    /// The hits as they sit, for serializing.
    pub fn hits(&self) -> Vec<Hit> {
        self.heap.iter().cloned().collect()
    }

    /// Roughly what this is costing, for the operator's `agg-memory-limit`.
    ///
    /// Bounded by construction, so this never triggers a dump for a sane `k` — and if it ever did, the answer
    /// would still be correct, because merging dumped heaps is the same operation as merging per-thread ones.
    pub fn heap_used(&self) -> usize {
        self.heap.len() * (core::mem::size_of::<Hit>() + 20)
    }

    pub fn replace_all(&mut self, hits: Vec<Hit>, k: usize) {
        self.k = k;
        self.heap.clear();

        for hit in hits {
            self.offer(hit);
        }
    }
}

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

//! Vector search as a native UDF: distances, exact top-k, and bounded ordering.
//!
//! ```text
//! topk("embedding", <query blob>, "cosine", 10)     the 10 nearest, exactly, over any number of records
//! orderby("price", "asc", 20)                       the 20 cheapest
//! vec_distance("embedding", <query blob>, "dot")    one record, one number
//! ```
//!
//! # Why this example exists
//!
//! **I predicted this would be the first case where the UDF is the bottleneck, and the benchmark says I was
//! wrong.** The reasoning was that a 768-dimension cosine costs ~300 ns scalar against ~75 ns with SIMD, which
//! would make the arithmetic twice the cost of reading the record. Measured over 50,000 records: the whole
//! top-k costs **460 ns per record**, and swapping the SIMD kernel for a plain scalar loop moves it by about
//! **100 ns** — so the distance is a *quarter* of the work, not double it. Reading a 3 KB blob record is most
//! of the rest, which is the part I had not accounted for: the record pipeline scales with record size too.
//!
//! `simsimd` is still worth having — 1.2x is real and it costs 120 KB of resident code, a tenth of what the
//! JSONPath example's dependencies cost — but it is a modest win, not a decisive one, and LLVM autovectorising
//! the obvious loop is most of the reason.
//!
//! **What is decisive is not the kernel at all.** Computing the top-k in the server takes 23 ms; fetching all
//! 50,000 vectors and sorting them on the client takes 2,182 ms. **96x**, because the server ships 10 hits and
//! the alternative ships 146 MB. That is the number this example is really about, and it would have been just as
//! true with the scalar loop.
//!
//! **Top-k is also the shape the per-thread accumulator was built for.** The state is a bounded heap of `k`, so
//! one query thread sends `k` hits however many records it read — and `k = 100` costs the same as `k = 10`. Lua
//! finalizes per partition slice, so the same query from one node would send 4096 heaps.
//!
//! # Exactness
//!
//! This is **not** approximate nearest neighbour. Every record is measured. What makes that affordable is that
//! nothing is *kept*: the accumulator holds k hits, not n. Merging two heaps keeps the k best of both, and that
//! operation is associative and commutative — so the client combining one heap per thread per node gets exactly
//! the same k as a single heap over the whole namespace would.
//!
//! An approximate index is a different tool for a different job; see Aerospike Vector Search. This is the
//! brute-force answer, and brute force is the right answer more often than people expect — at 200,000 records it
//! costs less than the query that returns them.

pub mod heap;
pub mod vec;

#[cfg(test)]
mod tests;

use std::ffi::CStr;

use aerospike_udf_sdk_native::{
    module, udfs, Aggregation, Args, AsPartial, Blob, Ctx, Emit, Error, Fold, Record, Result, Value,
};

use heap::{BestK, Hit};
use vec::{AlignedBuf, DType, Measurer, Metric, Skipped};

/// The identity of a record, for a hit.
fn identify(rec: &Record) -> (Blob, String) {
    let digest = rec
        .digest()
        .map(|d| Blob(d.to_vec()))
        .unwrap_or_default();

    // The primary key only if the client asked for it to be stored, which most do not. Empty otherwise, rather
    // than absent — a fixed shape is easier for a caller than an optional field.
    let key = match rec.key() {
        Ok(v) => match v.as_str() {
            Some(s) => s.to_string(),
            None => match v.as_i64() {
                Some(i) => i.to_string(),
                None => String::new(),
            },
        },
        Err(_) => String::new(),
    };

    (digest, key)
}

udfs! {
    /// The distance from this record's vector to `query`.
    ///
    /// `query` is a blob of packed little-endian `f32`. Answers nil when the record has no vector or one of a
    /// different dimensionality — a miss, not an error, for the same reason a JSONPath miss is.
    fn vec_distance(
        rec: &Record,
        bin: &str,
        query: &[u8],
        metric: &str,
        dtype: Option<&str>,
    ) -> Result<Option<f64>> {
        // Resolved per call here, because a record UDF measures one record - there is nothing to amortise. The
        // aggregations below resolve it once per query thread instead, which is the case that matters.
        let m = Measurer::new(
            Metric::parse(metric)?,
            DType::parse(dtype.unwrap_or("f32"))?,
            query,
        )?;

        let mut scratch = AlignedBuf::default();

        Ok(m.distance(rec, bin, &mut scratch)?.ok())
    }

    /// How many dimensions this record's vector has, or nil if it has none.
    fn vec_dim(rec: &Record, bin: &str, dtype: Option<&str>) -> Result<Option<i64>> {
        // The element type has to be declared here too: a 3072-byte blob is 768 f32s or 384 f64s, and nothing in
        // the data says which.
        let t = DType::parse(dtype.unwrap_or("f32"))?;

        Ok(vec::dimension(rec, bin, t)?.map(|d| d as i64))
    }

    /// The L2 norm of this record's vector.
    ///
    /// Useful for checking whether embeddings were normalized before they were stored, which decides whether
    /// `dot` and `cosine` rank the same way.
    fn vec_norm(rec: &Record, bin: &str, dtype: Option<&str>) -> Result<Option<f64>> {
        let t = DType::parse(dtype.unwrap_or("f32"))?;

        let Some(dim) = vec::dimension(rec, bin, t)? else {
            return Ok(None);
        };

        // The squared distance to the origin *is* the squared norm, so the same kernel answers it.
        let zero = vec![0u8; dim * t.size()];
        let m = Measurer::new(Metric::SqEuclidean, t, &zero)?;
        let mut scratch = AlignedBuf::default();

        Ok(m.distance(rec, bin, &mut scratch)?.ok().map(f64::sqrt))
    }
}

//==========================================================
// topk — exact nearest neighbours, in O(k).
//

/// The k nearest records to a query vector.
///
/// Written by hand rather than through `partial!` because a `BinaryHeap` is not a value: the state below holds
/// the heap plus the configuration it was built from, and only the heap crosses the wire.
pub struct TopK {
    /// **The whole point of this field.** The metric and the element type are resolved to one function pointer
    /// when the aggregation is configured - once per query thread - so `accumulate` makes a single indirect call
    /// with a target that never changes. No `match` on the metric, none on the element type, nothing decided per
    /// record. `None` only before `configure` has run.
    measurer: Option<Measurer>,

    bin: String,
    k: usize,

    best: BestK,

    /// Reused for every record, so the unaligned path costs a copy and never an allocation.
    scratch: AlignedBuf,

    /// **Counted, not ignored and not fatal.** A namespace with a stray 512-dimension vector among 768s is a
    /// data problem the caller needs to hear about; failing the whole scan over it would be worse, and dropping
    /// it silently would make a top-k that ignored a fifth of the data look exactly like a correct one.
    skipped_dim: i64,
    skipped_none: i64,
}

impl Default for TopK {
    fn default() -> Self {
        Self {
            measurer: None,
            bin: String::new(),
            k: 10,
            best: BestK::new(10),
            scratch: AlignedBuf::default(),
            skipped_dim: 0,
            skipped_none: 0,
        }
    }
}

impl AsPartial for TopK {
    /// The heap and the two counters. Not the query vector — that comes back from `configure` on the other side,
    /// because a partial carries what was *accumulated*.
    fn to_partial<'c>(&self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(3)?;
        let hits = self.best.hits();
        let list = ctx.list(hits.len() as u32)?;

        for hit in &hits {
            let entry = ctx.map(3)?;

            entry.put_str_f64("score", hit.score)?;
            entry.put(&Value::string("digest"), &Value::blob(&hit.digest))?;
            entry.put_str_str("key", &hit.key)?;
            list.push_map(&entry)?;
        }

        map.put_str_list("hits", &list)?;
        map.put_str_int("skipped_dim", self.skipped_dim)?;
        map.put_str_int("skipped_none", self.skipped_none)?;

        // Whether the zero-copy path is actually being taken, which is otherwise invisible from outside. Reported
        // rather than assumed because the assumption has been wrong: see the alignment note in vec.rs.
        let (aligned, copied) = self
            .measurer
            .as_ref()
            .map(Measurer::path_counts)
            .unwrap_or((0, 0));

        map.put_str_int("zero_copy", aligned as i64)?;
        map.put_str_int("copied", copied as i64)?;

        Ok(map.as_value())
    }

    fn from_partial(&mut self, partial: Value<'_>) -> Result<()> {
        let map = partial
            .as_map()
            .ok_or_else(|| Error::msg("a topk partial must be a map"))?;

        self.skipped_dim = map.get_str("skipped_dim").as_i64().unwrap_or(0);
        self.skipped_none = map.get_str("skipped_none").as_i64().unwrap_or(0);

        let Some(list) = map.get_str("hits").as_list() else {
            return Ok(());
        };

        // Collected first: `for_each` answers only whether to keep going, so nothing fallible can happen inside
        // it.
        let mut entries = Vec::with_capacity(list.len() as usize);

        list.for_each(|_, item| {
            entries.push(item);

            true
        });

        let mut hits = Vec::with_capacity(entries.len());

        for entry in entries {
            let Some(m) = entry.as_map() else { continue };

            hits.push(Hit {
                score: m.get_str("score").as_f64().unwrap_or(f64::MAX),
                digest: Blob(m.get_str("digest").as_bytes().unwrap_or_default().to_vec()),
                key: m.get_str("key").as_str().unwrap_or_default().to_string(),
            });
        }

        self.best.replace_all(hits, self.k);

        Ok(())
    }
}

impl Aggregation for TopK {
    const NAME: &'static CStr = c"topk";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("argument 1 must be the vector bin name"))?
            .to_string();

        let query = args
            .get(1)
            .as_bytes()
            .ok_or_else(|| Error::msg("argument 2 must be the query vector, as a packed blob"))?;

        let metric = Metric::parse(args.get(2).as_str().unwrap_or("cosine"))?;

        self.k = args.get(3).as_i64().unwrap_or(10).clamp(1, 10_000) as usize;

        // Defaults to f32 so an existing four-argument call keeps working, and because it is what an embedding
        // pipeline stores.
        let dtype = DType::parse(args.get(4).as_str().unwrap_or("f32"))?;

        // **Everything constant for this query is resolved here**, including a copy of the query into an aligned
        // buffer - an argument is borrowed for the length of this call and the accumulator outlives it.
        self.measurer = Some(Measurer::new(metric, dtype, query)?);
        self.best = BestK::new(self.k);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let measurer = self
            .measurer
            .as_ref()
            .ok_or_else(|| Error::msg("topk was not configured"))?;

        // One length compare, one alignment compare, one indirect call.
        let measured = measurer.distance(rec, &self.bin, &mut self.scratch)?;

        let distance = match measured {
            Ok(d) => d,
            Err(Skipped::WrongDimension) => {
                self.skipped_dim += 1;

                return Ok(Fold::Continue);
            }
            Err(Skipped::NoVector) => {
                self.skipped_none += 1;

                return Ok(Fold::Continue);
            }
        };

        let score = measurer.metric().score(distance);

        // The cheap rejection: once the heap is full, a candidate that cannot beat the worst kept hit does not
        // need its digest or key read at all. On a large scan almost every record takes this branch.
        if let Some(worst) = self.best.threshold() {
            if score >= worst {
                return Ok(Fold::Continue);
            }
        }

        let (digest, key) = identify(rec);

        self.best.offer(Hit { score, digest, key });

        // Never full: the heap is bounded by k, which is the entire point.
        Ok(Fold::Continue)
    }

    fn heap_used(&self) -> usize {
        self.best.heap_used()
    }

    fn merge(&mut self, other: &Self) {
        self.best.absorb(other.best.hits());
        self.skipped_dim += other.skipped_dim;
        self.skipped_none += other.skipped_none;
    }

    /// The answer: the hits best-first, with the distances converted back to what the caller asked about, plus
    /// what was skipped.
    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let sorted = self.best.sorted();
        let list = ctx.list(sorted.len() as u32)?;

        for hit in &sorted {
            let entry = ctx.map(3)?;

            // Back to the number the metric means - a `dot` similarity was negated on the way in so the heap
            // could have one comparison, and reporting that negative would be a lie.
            // Back to the number the metric means. A `dot` similarity was negated on the way in so the heap
            // could have one comparison, and reporting that negative would be a lie.
            let metric = self.measurer.as_ref().map(Measurer::metric).unwrap_or_default();

            entry.put_str_f64("distance", metric.unscore(hit.score))?;
            entry.put(&Value::string("digest"), &Value::blob(&hit.digest))?;

            if !hit.key.is_empty() {
                entry.put_str_str("key", &hit.key)?;
            }

            list.push_map(&entry)?;
        }

        let out = ctx.map(5)?;

        out.put_str_list("hits", &list)?;
        out.put_str_int("skipped_dim", self.skipped_dim)?;
        out.put_str_int("skipped_none", self.skipped_none)?;

        Ok(out.as_value())
    }
}

//==========================================================
// orderby — the same machinery, a scalar key.
//

/// The first `limit` records by a bin's value.
///
/// **A limit is required.** Ordering without one means holding every record, which is a collecting aggregation
/// plus a client-side merge of sorted runs — real work, for no advantage over scanning and sorting client-side.
/// Refusing it is more honest than half-implementing it.
pub struct OrderBy {
    bin: String,
    ascending: bool,
    limit: usize,

    best: BestK,
    skipped: i64,
}

impl Default for OrderBy {
    fn default() -> Self {
        Self {
            bin: String::new(),
            ascending: true,
            limit: 10,
            best: BestK::new(10),
            skipped: 0,
        }
    }
}

impl AsPartial for OrderBy {
    fn to_partial<'c>(&self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(2)?;
        let hits = self.best.hits();
        let list = ctx.list(hits.len() as u32)?;

        for hit in &hits {
            let entry = ctx.map(3)?;

            entry.put_str_f64("score", hit.score)?;
            entry.put(&Value::string("digest"), &Value::blob(&hit.digest))?;
            entry.put_str_str("key", &hit.key)?;
            list.push_map(&entry)?;
        }

        map.put_str_list("hits", &list)?;
        map.put_str_int("skipped", self.skipped)?;

        Ok(map.as_value())
    }

    fn from_partial(&mut self, partial: Value<'_>) -> Result<()> {
        let map = partial
            .as_map()
            .ok_or_else(|| Error::msg("an orderby partial must be a map"))?;

        self.skipped = map.get_str("skipped").as_i64().unwrap_or(0);

        let Some(list) = map.get_str("hits").as_list() else {
            return Ok(());
        };

        let mut entries = Vec::with_capacity(list.len() as usize);

        list.for_each(|_, item| {
            entries.push(item);

            true
        });

        let mut hits = Vec::with_capacity(entries.len());

        for entry in entries {
            let Some(m) = entry.as_map() else { continue };

            hits.push(Hit {
                score: m.get_str("score").as_f64().unwrap_or(f64::MAX),
                digest: Blob(m.get_str("digest").as_bytes().unwrap_or_default().to_vec()),
                key: m.get_str("key").as_str().unwrap_or_default().to_string(),
            });
        }

        self.best.replace_all(hits, self.limit);

        Ok(())
    }
}

impl Aggregation for OrderBy {
    const NAME: &'static CStr = c"orderby";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("argument 1 must be the bin to order by"))?
            .to_string();

        self.ascending = match args.get(1).as_str().unwrap_or("asc") {
            "asc" | "ascending" => true,
            "desc" | "descending" => false,
            other => {
                return Err(Error::msg(format!(
                    "direction must be asc or desc, got {other:?}"
                )))
            }
        };

        self.limit = args.get(2).as_i64().unwrap_or(10).clamp(1, 100_000) as usize;
        self.best = BestK::new(self.limit);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let value = rec.get(&self.bin)?;

        // Numbers only. A string ordering would need the strings in the heap, which unbounds the state that the
        // whole design depends on being bounded - so it is refused by omission and counted.
        let Some(n) = value.as_f64().or_else(|| value.as_i64().map(|i| i as f64)) else {
            self.skipped += 1;

            return Ok(Fold::Continue);
        };

        // The same inversion trick the metrics use: the heap only ever knows that larger is worse.
        let score = if self.ascending { n } else { -n };

        if let Some(worst) = self.best.threshold() {
            if score >= worst {
                return Ok(Fold::Continue);
            }
        }

        let (digest, key) = identify(rec);

        self.best.offer(Hit { score, digest, key });

        Ok(Fold::Continue)
    }

    fn heap_used(&self) -> usize {
        self.best.heap_used()
    }

    fn merge(&mut self, other: &Self) {
        self.best.absorb(other.best.hits());
        self.skipped += other.skipped;
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let sorted = self.best.sorted();
        let list = ctx.list(sorted.len() as u32)?;

        for hit in &sorted {
            let entry = ctx.map(3)?;

            entry.put_str_f64("value", if self.ascending { hit.score } else { -hit.score })?;
            entry.put(&Value::string("digest"), &Value::blob(&hit.digest))?;

            if !hit.key.is_empty() {
                entry.put_str_str("key", &hit.key)?;
            }

            list.push_map(&entry)?;
        }

        let out = ctx.map(2)?;

        out.put_str_list("rows", &list)?;
        out.put_str_int("skipped", self.skipped)?;

        Ok(out.as_value())
    }
}

//==========================================================
// Stage probes — where the per-record time actually goes.
//
// Each of these does strictly less work than the next, and every one is a **separate aggregation with its own
// name** rather than one function with a mode argument. That is deliberate: a `match` on a mode field would put a
// branch in the very loop being measured, and at ~300 ns a record the measurement deserves better than that.
//
// The deltas between them attribute the time:
//
//   probe_count           query orchestration + the record pipeline, with no UDF work at all
//   probe_read   - count  reading the blob bin (`rec_get_bytes`)
//   probe_copy   - read   an unconditional memcpy into scratch
//   probe_dist   - read   aligning and measuring, which is what `topk` does minus the heap
//   topk         - dist   the bounded heap
//

macro_rules! probes {
    ($($name:ident = $udf:expr => |$me:ident, $rec:ident| $body:block),* $(,)?) => {
        $(
            pub struct $name {
                bin: String,
                seen: i64,
                bytes: i64,
                measurer: Option<Measurer>,
                scratch: AlignedBuf,
            }

            impl Default for $name {
                fn default() -> Self {
                    Self {
                        bin: String::new(),
                        seen: 0,
                        bytes: 0,
                        measurer: None,
                        scratch: AlignedBuf::default(),
                    }
                }
            }

            impl AsPartial for $name {
                fn to_partial<'c>(&self, ctx: &'c Ctx) -> Result<Value<'c>> {
                    let map = ctx.map(2)?;

                    map.put_str_int("seen", self.seen)?;
                    map.put_str_int("bytes", self.bytes)?;

                    Ok(map.as_value())
                }

                fn from_partial(&mut self, partial: Value<'_>) -> Result<()> {
                    let map = partial
                        .as_map()
                        .ok_or_else(|| Error::msg("a probe partial must be a map"))?;

                    self.seen = map.get_str("seen").as_i64().unwrap_or(0);
                    self.bytes = map.get_str("bytes").as_i64().unwrap_or(0);

                    Ok(())
                }
            }

            impl Aggregation for $name {
                const NAME: &'static CStr = $udf;

                fn configure(&mut self, args: Args<'_>) -> Result<()> {
                    self.bin = args.get(0).as_str().unwrap_or("v").to_string();

                    // Built even where it is unused, so that every probe pays the same setup cost and the deltas
                    // are only about the per-record work.
                    if let Some(q) = args.get(1).as_bytes() {
                        self.measurer = Some(Measurer::new(
                            Metric::parse(args.get(2).as_str().unwrap_or("cosine"))?,
                            DType::parse(args.get(3).as_str().unwrap_or("f32"))?,
                            q,
                        )?);
                    }

                    Ok(())
                }

                fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
                    let $me = self;
                    let $rec = rec;

                    $body

                    Ok(Fold::Continue)
                }

                fn merge(&mut self, other: &Self) {
                    self.seen += other.seen;
                    self.bytes += other.bytes;
                }

                fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
                    self.to_partial(ctx)
                }
            }
        )*
    };
}

probes! {
    // Nothing but the increment. Whatever this costs is the query and the record pipeline.
    ProbeCount = c"probe_count" => |me, _rec| {
        me.seen += 1;
    },

    // One host call to read the blob, and a use of the result so nothing is optimised away.
    ProbeRead = c"probe_read" => |me, rec| {
        me.seen += 1;

        if let Some(bytes) = rec.get_bytes(&me.bin)? {
            me.bytes += bytes.len() as i64;
        }
    },

    // The same read, then an unconditional copy - the cost of the fallback path, whether or not it is taken.
    ProbeCopy = c"probe_copy" => |me, rec| {
        me.seen += 1;

        if let Some(bytes) = rec.get_bytes(&me.bin)? {
            let copied = me.scratch.fill(bytes);

            me.bytes += copied.len() as i64;
        }
    },

    // Read, align or copy, and measure - `topk` without the heap.
    ProbeDist = c"probe_dist" => |me, rec| {
        me.seen += 1;

        if let Some(m) = me.measurer.as_ref() {
            if let Ok(d) = m.measure_bytes_at(rec, &me.bin, &mut me.scratch)? {
                // Accumulated so the distance cannot be dead code.
                me.bytes += d as i64;
            }
        }
    },
}

module! {
    name: "vector",
    version: "1.0.0",
    functions: [vec_distance, vec_dim, vec_norm],
    aggregations: [TopK, OrderBy, ProbeCount, ProbeRead, ProbeCopy, ProbeDist],
}

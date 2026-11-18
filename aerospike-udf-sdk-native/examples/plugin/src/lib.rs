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

//! An example native UDF module, in Rust.
//!
//! Deliberately the **same functions** as the C example in the server tree's `modules/mod-native/examples`,
//! so the two can be compared line for line — and so the client suite can run against either one under the
//! same module name. What the comparison shows is that the Rust version does the type checking the C version
//! has to write out by hand, and does it before the body is entered.
//!
//! ```sh
//! cargo build --release
//! # then point the server's config at target/release/libaerospike_udf_example_native.so
//! ```

use std::ffi::CStr;

use aerospike_udf_sdk_native::{
    module, partial, udfs, Aggregation, Args, Ctx, Emit, Error, Fold, List, Record, Result, Value,
};

udfs! {
    /// Add `by` to bin "n" and return the new value. `by` defaults to 1.
    ///
    /// The shape of every native record UDF: read, modify, stage, **save**. `save` is what makes it real —
    /// without it this returns a plausible number and changes nothing.
    fn increment(rec: &Record, by: Option<i64>) -> Result<i64> {
        let next = rec.get_int("n")?.unwrap_or(0) + by.unwrap_or(1);

        rec.set_int("n", next)?;
        rec.save()?;

        Ok(next)
    }

    /// Sum every argument that was supplied.
    ///
    /// `Option<i64>` for each: an argument the client left out arrives as `None`, so one signature covers
    /// every arity up to four without the body checking anything. A truly variadic UDF would take the whole
    /// argument list instead - this is the shape that keeps the types.
    fn add_all(
        rec: &Record,
        a: Option<i64>,
        b: Option<i64>,
        c: Option<i64>,
        d: Option<i64>,
    ) -> Result<i64> {
        let _ = rec;

        Ok([a, b, c, d].into_iter().flatten().sum())
    }

    /// Sum a list argument, in bulk.
    ///
    /// One host call per 256 elements rather than one per element — the difference that decides whether a
    /// collection-heavy UDF beats Lua or loses to it.
    fn sum_args(rec: &Record, values: List<'_>) -> Result<i64> {
        let _ = rec;

        values.sum_ints()
    }

    /// Total bin "l", a list of integers.
    fn sum_list(rec: &Record) -> Result<i64> {
        let list = rec
            .get("l")?
            .as_list()
            .ok_or_else(|| Error::msg("bin 'l' is not a list"))?;

        list.sum_ints()
    }

    /// Hand back whatever came in, whatever type it is.
    ///
    /// `Option<Value>` on both sides: the argument may be left out, and nil is a legitimate answer.
    fn echo<'a>(rec: &'a Record, value: Option<Value<'a>>) -> Result<Option<Value<'a>>> {
        let _ = rec;

        Ok(value)
    }

    /// Read a bin's bytes and write them straight back to another bin.
    ///
    /// **A probe for blob-subtype fidelity, not a useful operation.** A blob particle carries a subtype - `HLL`,
    /// `VECTOR`, or one of the legacy per-language tags - and this ABI flattens a value to a type, a length and a
    /// pointer, with no room for it. So the bytes survive a round trip and the subtype may not, which would turn an
    /// HLL bin into an ordinary blob and make every later HLL operation on it fail.
    ///
    /// Whether that actually happens is what this exists to establish. Lua does not have the problem, because it
    /// hands a UDF the `as_bytes` object itself and never flattens it.
    fn copy_bin(rec: &Record, from: &str, to: &str) -> Result<i64> {
        let bytes = rec
            .get_bytes(from)?
            .ok_or_else(|| Error::msg(format!("bin '{from}' is absent or not byte-shaped")))?
            .to_vec();

        let len = bytes.len() as i64;

        rec.set_blob(to, &bytes)?;
        rec.save()?;

        Ok(len)
    }

    /// A map of the record's own metadata, built on the way out.
    fn describe<'a>(rec: &'a Record) -> Result<Value<'a>> {
        let map = rec.ctx().map(4)?;

        map.put_str_int("gen", i64::from(rec.generation()))?;
        map.put_str_int("ttl", i64::from(rec.ttl()))?;
        map.put_str_int("bins", i64::from(rec.num_bins()))?;

        if let Some(set) = rec.set_name() {
            map.put_str_str("set", set)?;
        }

        Ok(map.as_value())
    }

    /// A list holding a map holding a list, to prove structures come back whole.
    fn nested<'a>(rec: &'a Record) -> Result<Value<'a>> {
        let ctx = rec.ctx();

        let inner = ctx.list(3)?;

        inner.push_int(1)?;
        inner.push_int(2)?;
        inner.push_int(3)?;

        let map = ctx.map(1)?;

        map.put_str_list("inner", &inner)?;

        let outer = ctx.list(2)?;

        outer.push_int(7)?;
        outer.push_map(&map)?;

        Ok(outer.as_value())
    }

    /// A UDF that reports failure, so the error path can be seen from a client.
    fn always_fails(rec: &Record) -> Result<()> {
        let _ = rec;

        Err(Error::msg("this function always fails, on purpose"))
    }

    /// A UDF that panics, to show that a panic is an error rather than a dead server.
    fn always_panics(rec: &Record) -> Result<i64> {
        let _ = rec;

        panic!("this function always panics, on purpose")
    }
}

//==========================================================
// Aggregations.
//

// The four shapes, in the order the design describes them: reduce, reduce-with-an-answer, filter, collect.

partial! {
    /// Sum one integer bin. **Reducing** — fixed size whatever the record count, so it never dumps.
    ///
    /// `partial!` writes the serialization, which is the only reason there is no code here about the wire.
    /// Note the state is plain fields, so the host's zeroed allocation is all of it and an aggregation over any
    /// number of records allocates **nothing**.
    pub struct Sum {
        total: i64,
        bin: String,
    }
}

impl Aggregation for Sum {
    const NAME: &'static CStr = c"sum";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        // A `String` rather than the fixed `BinName` below, because this one is allocated once per accumulator
        // - once per query thread - and not per record. `Stats` keeps the POD version for the comparison.
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
            .to_string();

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        // One host call per record, and nothing else. This is the entire per-record cost.
        if let Some(v) = rec.get_int(&self.bin)? {
            self.total += v;
        }

        // Never full: nothing here grows.
        Ok(Fold::Continue)
    }

    fn merge(&mut self, other: &Self) {
        self.total += other.total;
    }

    fn finalize<'c>(&mut self, _ctx: &'c Ctx) -> Result<Value<'c>> {
        Ok(Value::int(self.total))
    }
}

partial! {
    /// Count, sum, min and max of one integer bin — and a **mean**, which is the point.
    ///
    /// A mean could never have gone on the wire: one of these arrives per query thread per node, and means do
    /// not add. It is safe here only because `finalize` is not what gets merged — `to_partial` is, and
    /// `partial!` wrote that from the field list. This is the whole argument for splitting the two, in one
    /// aggregation.
    pub struct Stats {
        count: i64,
        sum: i64,
        min: i64,
        max: i64,
        seen: bool,
        bin: String,
    }
}

impl Aggregation for Stats {
    const NAME: &'static CStr = c"stats";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
            .to_string();

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let Some(v) = rec.get_int(&self.bin)? else {
            return Ok(Fold::Continue);
        };

        self.count += 1;
        self.sum += v;

        if !self.seen {
            self.seen = true;
            self.min = v;
            self.max = v;
        } else {
            self.min = self.min.min(v);
            self.max = self.max.max(v);
        }

        Ok(Fold::Continue)
    }

    fn merge(&mut self, other: &Self) {
        if !other.seen {
            return;
        }

        self.count += other.count;
        self.sum += other.sum;

        if !self.seen {
            self.seen = true;
            self.min = other.min;
            self.max = other.max;
        } else {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(5)?;

        map.put_str_int("count", self.count)?;
        map.put_str_int("sum", self.sum)?;

        if self.seen {
            map.put_str_int("min", self.min)?;
            map.put_str_int("max", self.max)?;
            map.put_str_f64("mean", self.sum as f64 / self.count as f64)?;
        }

        Ok(map.as_value())
    }
}

partial! {
    /// `Stats`, but dumped every `chunk` records — **a measurement instrument, not a useful aggregation.**
    ///
    /// It exists to separate two things the headline aggregation number conflates. A native aggregation is
    /// faster than Lua both because there is no engine *and* because it keeps one accumulator per query thread
    /// where Lua finalizes per partition slice — 4 partials against 4096. Those are different savings, and
    /// running this against `stats` on the same data with the same engine isolates the second one: identical
    /// per-record work, chosen number of partials.
    pub struct StatsChunked {
        count: i64,
        sum: i64,
        chunk: i64,
        bin: String,
    }
}

impl Aggregation for StatsChunked {
    const NAME: &'static CStr = c"stats_chunked";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
            .to_string();

        // Zero means "never dump", which makes this identical to `stats` and is the control case.
        self.chunk = args.get(1).as_i64().unwrap_or(0);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        if let Some(v) = rec.get_int(&self.bin)? {
            self.count += 1;
            self.sum += v;
        }

        Ok(if self.chunk > 0 && self.count >= self.chunk {
            Fold::Full
        } else {
            Fold::Continue
        })
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.sum += other.sum;
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(2)?;

        map.put_str_int("count", self.count)?;
        map.put_str_int("sum", self.sum)?;

        Ok(map.as_value())
    }
}

partial! {
    /// Return every record whose bin is at least a threshold. **The cheapest shape this ABI can express.**
    ///
    /// Zero state, zero allocations, no partials: matching records go straight to the response, and the client
    /// reads them exactly as it would a plain query's. One host call to read the bin, one to pass the record
    /// through.
    ///
    /// A unit struct, so `state_size` is 0 — the host still hands out a byte so that the pointer is real, and
    /// nothing is ever written through it.
    pub struct Over;
}

impl Aggregation for Over {
    const NAME: &'static CStr = c"over";

    fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold> {
        // The threshold comes from the record's own bin rather than an argument, because a unit struct has
        // nowhere to keep one - which is exactly the trade a zero-state aggregation makes. `Collect` below
        // shows the other side of it.
        if rec.get_int("v")?.unwrap_or(i64::MIN) >= rec.get_int("floor")?.unwrap_or(0) {
            // Straight to the response. Nothing built, nothing copied, nothing accumulated.
            out.record(rec)?;
        }

        // Never full: nothing is being held.
        Ok(Fold::Continue)
    }

    fn finalize<'c>(&mut self, _ctx: &'c Ctx) -> Result<Value<'c>> {
        // Nothing was accumulated, so there is no answer to compute. Required rather than defaulted on
        // purpose: a *reducing* aggregation that forgot to write this would otherwise answer nil and look like
        // it worked, and that mistake is much more likely than this one.
        Ok(Value::nil())
    }
}

partial! {
    /// Collect one bin's values, dumping every `chunk` of them. **The shape that makes dumping mandatory.**
    ///
    /// Output grows with the input, so it cannot be held to the end of a query over a billion records. It
    /// answers [`Fold::Full`] once it has `chunk` values and the host writes them out, resets it and re-runs
    /// `configure` — which is why `chunk` and `bin` survive being dumped.
    pub struct Collect {
        pub values: Vec<i64>,
        chunk: i64,
        bin: String,
    }
}

impl Aggregation for Collect {
    const NAME: &'static CStr = c"collect";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
            .to_string();

        self.chunk = args.get(1).as_i64().unwrap_or(1000).max(1);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let Some(v) = rec.get_int(&self.bin)? else {
            return Ok(Fold::Continue);
        };

        self.values.push(v);

        Ok(if self.values.len() as i64 >= self.chunk {
            Fold::Full
        } else {
            Fold::Continue
        })
    }

    fn heap_used(&self) -> usize {
        // What an operator's `agg-memory-limit` watches. Reporting it is what lets the server bound this
        // accumulator without trusting the plugin's own `chunk`.
        self.values.len() * core::mem::size_of::<i64>()
    }

    fn merge(&mut self, other: &Self) {
        self.values.extend_from_slice(&other.values);
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let list = ctx.list(self.values.len() as u32)?;

        for v in &self.values {
            list.push_int(*v)?;
        }

        Ok(list.as_value())
    }
}

module! {
    name: "stats-rs",
    version: "1.0.0",
    functions: [
        increment, add_all, sum_args, sum_list, echo, copy_bin, describe, nested, always_fails,
        always_panics
    ],
    aggregations: [Sum, Stats, StatsChunked, Over, Collect],
}

//==========================================================
// The client half.
//
// These run the aggregations above *in a client process*, through the SDK's local host, with no server
// involved. That is the point: the code being exercised here is the code the server dlopen's, not a
// client-side copy of it - so a bug in `merge` or `finalize` shows up in one place rather than diverging
// between two.
//

#[cfg(test)]
mod tests {
    use super::*;

    use aerospike_core::Value as CoreValue;
    use aerospike_udf_sdk_native::client::Combiner;

    fn map(pairs: &[(&str, CoreValue)]) -> CoreValue {
        let mut m = std::collections::HashMap::new();

        for (k, v) in pairs {
            m.insert(CoreValue::from(*k), v.clone());
        }

        CoreValue::HashMap(m)
    }

    /// Several nodes' `Stats` partials, combined by `Stats::merge`, answered by `Stats::finalize`.
    ///
    /// The assertion that matters is the **mean**: it is not in any partial, cannot be derived from one alone,
    /// and is correct only if every partial was merged before `finalize` ran.
    #[test]
    fn a_client_combines_stats_partials_with_the_plugins_own_code() {
        let args = [CoreValue::from("v")];
        let mut combiner = Combiner::<Stats>::new(&args).unwrap();

        // Three query threads' worth: 1..=10 split unevenly, as partitions would split it.
        for partial in [
            map(&[
                ("count", CoreValue::from(4)),
                ("sum", CoreValue::from(10)),
                ("min", CoreValue::from(1)),
                ("max", CoreValue::from(4)),
                ("seen", CoreValue::from(1)),
                ("bin", CoreValue::from("v")),
            ]),
            map(&[
                ("count", CoreValue::from(3)),
                ("sum", CoreValue::from(18)),
                ("min", CoreValue::from(5)),
                ("max", CoreValue::from(7)),
                ("seen", CoreValue::from(1)),
                ("bin", CoreValue::from("v")),
            ]),
            map(&[
                ("count", CoreValue::from(3)),
                ("sum", CoreValue::from(27)),
                ("min", CoreValue::from(8)),
                ("max", CoreValue::from(10)),
                ("seen", CoreValue::from(1)),
                ("bin", CoreValue::from("v")),
            ]),
        ] {
            combiner.absorb(&partial).unwrap();
        }

        let answer = combiner.finish().unwrap();

        let CoreValue::HashMap(m) = &answer else {
            panic!("wanted a map, got {answer:?}");
        };

        assert_eq!(m.get(&CoreValue::from("count")), Some(&CoreValue::from(10)));
        assert_eq!(m.get(&CoreValue::from("sum")), Some(&CoreValue::from(55)));
        assert_eq!(
            m.get(&CoreValue::from("min")),
            Some(&CoreValue::from(1)),
            "min across every partial, not within one"
        );
        assert_eq!(m.get(&CoreValue::from("max")), Some(&CoreValue::from(10)));
        assert_eq!(
            m.get(&CoreValue::from("mean")),
            Some(&CoreValue::from(5.5)),
            "the mean is computed on the client, from fully merged data - it was never on the wire"
        );
    }

    /// A `Sum` partial is a bare integer, so this checks the macro handles a partial that is not a map at all…
    /// which it does not, and should not: `partial!` always writes a map, so `Sum`'s wire form is
    /// `{total, bin}`. Worth pinning, because "a sum's partial is an integer" is the obvious wrong guess.
    #[test]
    fn a_sum_partial_is_a_map_of_its_fields_not_a_bare_integer() {
        let args = [CoreValue::from("v")];
        let combiner = Combiner::<Sum>::new(&args).unwrap();

        let read = combiner
            .read(&map(&[
                ("total", CoreValue::from(42)),
                ("bin", CoreValue::from("v")),
            ]))
            .unwrap();

        assert_eq!(read.total, 42);

        // And a bare integer is refused rather than silently read as zero.
        assert!(
            combiner.read(&CoreValue::from(42)).is_err(),
            "a partial that is not this accumulator's map must not be accepted"
        );
    }

    /// A collecting aggregation's partials are its output, in chunks — so they are read one at a time rather
    /// than merged, which is the entire reason it dumped them.
    #[test]
    fn a_client_reads_collect_chunks_one_at_a_time() {
        let args = [CoreValue::from("v"), CoreValue::from(3)];
        let combiner = Combiner::<Collect>::new(&args).unwrap();

        let chunk = |values: &[i64]| {
            map(&[
                (
                    "values",
                    CoreValue::List(values.iter().map(|v| CoreValue::from(*v)).collect()),
                ),
                ("chunk", CoreValue::from(3)),
                ("bin", CoreValue::from("v")),
            ])
        };

        let mut seen = Vec::new();

        for partial in [chunk(&[1, 2, 3]), chunk(&[4, 5, 6]), chunk(&[7])] {
            let held = combiner.read(&partial).unwrap();

            assert!(
                held.values.len() <= 3,
                "never more than one chunk in memory, which is the property dumping bought"
            );

            seen.extend(held.values.iter().copied());
        }

        assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    /// `configure` runs on the client too, so a field set from the arguments is populated before a partial is
    /// read into it — which is what lets a partial carry only the accumulated part.
    #[test]
    fn configure_runs_client_side() {
        let combiner = Combiner::<Collect>::new(&[CoreValue::from("score"), CoreValue::from(7)]).unwrap();

        // Nothing absorbed: these came from `configure`, not from a partial.
        assert_eq!(combiner.accumulator().chunk, 7);
        assert_eq!(combiner.accumulator().bin, "score");
    }
}

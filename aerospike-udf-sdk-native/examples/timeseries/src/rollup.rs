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

//! Ten thousand series into one, server-side.
//!
//! ```text
//! ts_rollup("cpu")                    sum every record's ring, bucket by bucket
//! ts_rollup("cpu", "max")             the worst any one series reported, per bucket
//! ts_rollup("cpu", "sum", 200)        and downsampled to 200 points before it leaves
//! ```
//!
//! # Why the alignment is free
//!
//! A bucket index is `t / step` anchored to the Unix epoch, so bucket 29,453,760 means the same minute in every record
//! in the cluster. Rings that agree on `step_ms` therefore align by **index**, with no timestamp matching, no
//! interpolation and nothing to sort. That is the property the epoch-anchored indexing was chosen for, and it is what
//! makes this aggregation a few dozen lines rather than a join.
//!
//! Rings that *disagree* on shape are refused and counted, not resampled. Resampling would produce a plausible number
//! from series that were never comparable.
//!
//! # The accumulator keeps a count ring, and the stored rings do not
//!
//! [`crate::config::Agg`] has no `Mean`, because a mean needs a count per slot and that would double every **stored**
//! ring on every record forever. An accumulator is transient and there is one per query thread, so it can afford the
//! second array — and mean-across-records is the question people actually ask of a fleet. So `sum`, `count` and `mean`
//! all come back, and the asymmetry is the point rather than an inconsistency.
//!
//! # What it costs
//!
//! O(records × slots): every record's live window is walked once. A 1,440-slot ring over 10,000 records is 14.4 million
//! slot visits, which is nothing; a 65,536-slot ring over the same records is 655 million, which is not. The cost is
//! inherent to aligning whole rings rather than a shortcoming to optimise away, so it is written down here.

use aerospike_udf_sdk_native::{
    partial, Aggregation, Args, Blob, Ctx, Emit, Error, Fold, Record, Result, Value,
};
use core::ffi::CStr;

use crate::config::{self, Agg, Config, Kind, Resolution, Shape};
use crate::ring::{self, Num, RingView, NO_BUCKET};

/// `res_code` when no archive was named.
const RES_NONE: i64 = -1;

partial! {
    /// One accumulator per query thread: a ring of combined values and a parallel ring of contributor counts.
    pub struct Rollup {
        bin: String,
        cfg_bin: String,
        /// How values from different records combine. An [`Agg`] code.
        op: i64,
        /// Resolution to downsample to in `finalize`. 0 means the ring's own.
        max_points: i64,
        step_ms: i64,
        slots: i64,
        /// The archive that was **adopted**, as a [`Resolution`] code, or [`RES_NONE`].
        ///
        /// `finalize` needs it to know whether its bucket indices are multiples of a step or calendar months. A partial
        /// that adopted the monthly archive and one that adopted the hourly one are not comparable, so this joins the
        /// shape that `matches` tests.
        res_code: i64,
        /// The archive the caller **asked** for, or [`RES_NONE`] for the finest.
        want_res: i64,
        /// A [`Kind`] code, adopted from the first ring that was readable.
        kind: i64,
        last_bucket: i64,
        /// `slots * 8` bytes of combined values. Meaningless where `counts` is zero.
        values: Blob,
        /// `slots * 4` bytes of `u32`: how many records contributed to each bucket.
        ///
        /// This doubles as the validity marker, which is why `values` needs no `NaN`/`i64::MIN` sentinel.
        counts: Blob,
        /// Whether the caller pinned the shape. See `configure`.
        declared: bool,
        records: i64,
        samples: i64,
        skipped_absent: i64,
        skipped_shape: i64,
        skipped_unreadable: i64,
        /// Samples older than the accumulator's window by the time they arrived.
        dropped_old: i64,
        saturated: bool,
    }
}

impl Rollup {
    fn agg(&self) -> Agg {
        Agg::from_u8(self.op as u8).unwrap_or(Agg::Sum)
    }

    fn kind(&self) -> Kind {
        Kind::from_u8(self.kind as u8).unwrap_or(Kind::F64)
    }

    fn adopted(&self) -> bool {
        self.slots > 0
    }

    /// The archive this accumulator holds, if it holds one.
    fn resolution(&self) -> Option<Resolution> {
        if self.res_code < 0 {
            return None;
        }

        Resolution::from_u8(self.res_code as u8).ok()
    }

    /// The first millisecond of a bucket.
    ///
    /// **Not `bucket * step_ms`.** A month archive's bucket 673 is a calendar month, and multiplying it by an
    /// approximate step would put the label weeks away from the data. The approximation is fine for the *width* a chart
    /// axis reports; it is not fine for a boundary.
    fn bucket_start_ms(&self, bucket: i64) -> i64 {
        match self.resolution() {
            Some(r) => r.bucket_start_ms(bucket),
            None => bucket.saturating_mul(self.step_ms),
        }
    }

    /// Take the shape of the first readable ring. Everything after it has to match.
    fn adopt(&mut self, shape: &Shape) {
        let slots = shape.slots().max(0) as usize;

        self.step_ms = shape.approx_step_ms();
        self.slots = shape.slots();
        self.kind = i64::from(shape.kind().as_u8());
        self.res_code = shape.resolution().map_or(RES_NONE, |r| i64::from(r.as_u8()));
        self.last_bucket = NO_BUCKET;
        self.values = Blob(vec![0u8; slots * 8]);
        self.counts = Blob(vec![0u8; slots * 4]);
    }

    fn matches(&self, shape: &Shape) -> bool {
        self.step_ms == shape.approx_step_ms()
            && self.slots == shape.slots()
            && self.kind == i64::from(shape.kind().as_u8())
            && self.res_code == shape.resolution().map_or(RES_NONE, |r| i64::from(r.as_u8()))
    }

    /// Normalise the two arrays, because a partial arrives from another process.
    ///
    /// A `Blob` field is whatever the wire carried. Every access below indexes by slot, so the lengths are fixed here
    /// once rather than checked in five places — the same reasoning as the summary example's HyperLogLog registers,
    /// and with the per-record scan kept out of the hot path this time.
    fn normalise(&mut self) {
        let slots = self.slots.max(0) as usize;

        if self.values.0.len() != slots * 8 {
            self.values.0.resize(slots * 8, 0);
        }

        if self.counts.0.len() != slots * 4 {
            self.counts.0.resize(slots * 4, 0);
        }
    }

    fn slot_of(&self, bucket: i64) -> usize {
        bucket.rem_euclid(self.slots) as usize
    }

    fn count_at(&self, slot: usize) -> u32 {
        config::le_u32(&self.counts.0, slot * 4)
    }

    fn set_count(&mut self, slot: usize, n: u32) {
        self.counts.0[slot * 4..slot * 4 + 4].copy_from_slice(&n.to_le_bytes());
    }

    fn value_at(&self, slot: usize) -> Option<Num> {
        if self.count_at(slot) == 0 {
            return None;
        }

        let raw = config::le_i64(&self.values.0, slot * 8);

        Some(match self.kind() {
            Kind::F64 => Num::F(f64::from_bits(raw as u64)),
            Kind::I64 => Num::I(raw),
        })
    }

    fn set_value(&mut self, slot: usize, v: Num) {
        let raw = match v {
            Num::I(i) => i,
            // The accumulator carries its own parallel count ring, so a mean arriving from an archive is folded to its
            // value here and the count is tracked alongside as it is for every other kind.
            other => other.as_f64().to_bits() as i64,
        };

        self.values.0[slot * 8..slot * 8 + 8].copy_from_slice(&raw.to_le_bytes());
    }

    fn clear(&mut self, slot: usize) {
        self.set_count(slot, 0);
        self.values.0[slot * 8..slot * 8 + 8].fill(0);
    }

    /// Move the window forward to `bucket`, retiring everything it passed.
    ///
    /// The target bucket is cleared **too**, unlike a ring append: [`Self::add_at`] accumulates rather than overwrites,
    /// so a stale slot left in place would have a retired bucket's total added to a new one's.
    fn advance_to(&mut self, bucket: i64) {
        if self.last_bucket == NO_BUCKET {
            self.last_bucket = bucket;

            return;
        }

        if bucket <= self.last_bucket {
            return;
        }

        let gap = bucket - self.last_bucket;
        let to_clear = gap.min(self.slots);

        for k in 0..to_clear {
            let slot = self.slot_of(bucket - k);

            self.clear(slot);
        }

        self.last_bucket = bucket;
    }

    fn in_window(&self, bucket: i64) -> bool {
        self.last_bucket != NO_BUCKET
            && bucket <= self.last_bucket
            && bucket > self.last_bucket - self.slots
    }

    /// Fold one bucket's value in, carrying how many records it already represents.
    fn add_at(&mut self, bucket: i64, v: Num, contributors: u32) {
        if !self.in_window(bucket) {
            self.dropped_old += 1;

            return;
        }

        let slot = self.slot_of(bucket);
        let existing = self.value_at(slot);
        let (combined, over) = ring::combine(self.agg(), existing, Some(v));

        self.saturated |= over;

        if let Some(value) = combined {
            self.set_value(slot, value);
            self.set_count(slot, self.count_at(slot).saturating_add(contributors));
        }
    }
}

impl Aggregation for Rollup {
    const NAME: &'static CStr = c"ts_rollup";

    /// `ts_rollup(bin, [op], [max_points], [cfg_bin], [step_ms], [slots], [res])`
    ///
    /// `bin` is the series' **base** name; with archives, `res` picks which one to roll up and the finest is the default.
    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the ring's bin name"))?
            .to_string();

        let op = match args.get(1).as_str() {
            Some(name) => Agg::parse(name)?,
            None => Agg::Sum,
        };

        // `count` and `first` do not mean anything across records - there is no order between records, so "first"
        // would be whichever partition answered first, and the count comes back regardless.
        if matches!(op, Agg::Count | Agg::First) {
            return Err(Error::msg(
                "roll up with sum, min, max or last - 'count' is always reported anyway, and \
                 'first' has no meaning across records because they have no order",
            ));
        }

        self.op = i64::from(op.as_u8());
        self.max_points = args.get(2).as_i64().unwrap_or(0).max(0);
        self.cfg_bin = args
            .get(3)
            .as_str()
            .unwrap_or(config::DEFAULT_BIN)
            .to_string();

        // **Pinning the shape is how a rollup becomes deterministic**, and the reason it is offered.
        //
        // Without it the accumulator takes the shape of the first record it happens to read, and there is no single
        // "first" across query threads: a set holding three series stepped at 60s and one at 1s could adopt either,
        // per thread, and then discard the rest by shape. A tutorial run did exactly that - it adopted the one odd
        // record and reported a confident `result=ok` built from a single series.
        //
        // Declare `step_ms` and `slots` and every record that does not match is skipped and counted instead, whichever
        // order they arrive in.
        let (step_ms, slots) = (args.get(4).as_i64().unwrap_or(0), args.get(5).as_i64().unwrap_or(0));

        // The archive to read, kept separately from `res_code`: that field ends up holding what was *adopted*, and
        // before the first record there is nothing adopted yet.
        self.want_res = match args.get(6).as_str() {
            Some(name) => i64::from(Resolution::parse(name)?.as_u8()),
            None => RES_NONE,
        };
        self.res_code = RES_NONE;

        if step_ms > 0 && slots > 0 {
            if slots > config::MAX_SLOTS {
                return Err(Error::msg(format!(
                    "slots must be at most {}, got {slots}",
                    config::MAX_SLOTS
                )));
            }

            self.step_ms = step_ms;
            self.slots = slots;
            self.kind = i64::from(Kind::F64.as_u8());
            self.last_bucket = NO_BUCKET;
            self.values = Blob(vec![0u8; slots as usize * 8]);
            self.counts = Blob(vec![0u8; slots as usize * 4]);
            self.declared = true;
        } else if step_ms > 0 || slots > 0 {
            return Err(Error::msg(
                "declare both step_ms and slots, or neither - half a shape cannot be matched against",
            ));
        }

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let Some(cfg_bytes) = rec.get_bytes(&self.cfg_bin)? else {
            self.skipped_absent += 1;

            return Ok(Fold::Continue);
        };

        // A record whose config or ring will not decode is **skipped and counted**, not fatal. One malformed record
        // out of ten thousand should not lose the other 9,999 answers - and the count means it is not lost either.
        let Ok(cfg) = Config::decode(cfg_bytes) else {
            self.skipped_unreadable += 1;

            return Ok(Fold::Continue);
        };

        // Of the canonical encoding, matching `load_config` - see the note there.
        let fingerprint = cfg.fingerprint_of();

        // Which ring of this record to read. A record whose config has no such archive is a shape mismatch, not a fatal
        // error: a set can hold series with different archive tables and the answer is still about the ones that match.
        let want = if self.want_res < 0 {
            None
        } else {
            match Resolution::from_u8(self.want_res as u8) {
                Ok(r) => Some(r),
                Err(_) => None,
            }
        };

        let shape = match (cfg.has_archives(), want) {
            (false, None) => Shape::base(&cfg, fingerprint),
            (true, None) => match Shape::archive(&cfg, fingerprint, 0) {
                Ok(s) => s,
                Err(_) => {
                    self.skipped_shape += 1;

                    return Ok(Fold::Continue);
                }
            },
            (_, Some(r)) => match Shape::at_resolution(&cfg, fingerprint, r) {
                Ok(s) => s,
                Err(_) => {
                    self.skipped_shape += 1;

                    return Ok(Fold::Continue);
                }
            },
        };

        let bin = shape.bin_name(&self.bin);

        let Some(ring_bytes) = rec.get_bytes(&bin)? else {
            self.skipped_absent += 1;

            return Ok(Fold::Continue);
        };

        let Ok(ring) = RingView::open(ring_bytes, &shape) else {
            self.skipped_unreadable += 1;

            return Ok(Fold::Continue);
        };

        if !self.adopted() {
            self.adopt(&shape);
        } else {
            self.normalise();

            // A declared shape fixes step and slots but says nothing about the sample width or which archive, so the
            // first matching record still supplies those.
            if self.declared
                && self.step_ms == shape.approx_step_ms()
                && self.slots == shape.slots()
                && self.records == 0
            {
                self.kind = i64::from(shape.kind().as_u8());
                self.res_code = shape.resolution().map_or(RES_NONE, |r| i64::from(r.as_u8()));
            }

            if !self.matches(&shape) {
                self.skipped_shape += 1;

                return Ok(Fold::Continue);
            }
        }

        let Some((first, last)) = ring.window() else {
            // Initialised but never appended to. It counts as a record seen; it just has nothing to say.
            self.records += 1;

            return Ok(Fold::Continue);
        };

        self.advance_to(last);

        for bucket in first..=last {
            if let Some(v) = ring.at(bucket) {
                self.add_at(bucket, v, 1);
                self.samples += 1;
            }
        }

        self.records += 1;

        Ok(Fold::Continue)
    }

    fn merge(&mut self, other: &Self) {
        if !other.adopted() {
            // **The counters still have to come across.** A partial that skipped every record it saw never adopted a
            // shape, and returning here without taking its counts meant a query where nothing matched reported
            // `skipped_shape = 0` — which reads as "nothing was skipped" rather than "three records were refused". The
            // whole point of these counters is that a dropped record is not silently dropped.
            self.skipped_absent += other.skipped_absent;
            self.skipped_shape += other.skipped_shape;
            self.skipped_unreadable += other.skipped_unreadable;
            self.dropped_old += other.dropped_old;

            return;
        }

        if !self.adopted() {
            let Ok(kind) = Kind::from_u8(other.kind as u8) else { return };

            self.step_ms = other.step_ms;
            self.slots = other.slots;
            self.kind = i64::from(kind.as_u8());
            self.res_code = other.res_code;
            self.last_bucket = NO_BUCKET;
            self.values = Blob(vec![0u8; other.slots.max(0) as usize * 8]);
            self.counts = Blob(vec![0u8; other.slots.max(0) as usize * 4]);

            if self.bin.is_empty() {
                self.bin = other.bin.clone();
            }

            if self.op == 0 {
                self.op = other.op;
            }

            if self.max_points == 0 {
                self.max_points = other.max_points;
            }
        }

        self.normalise();

        if self.step_ms != other.step_ms
            || self.slots != other.slots
            || self.kind != other.kind
            || self.res_code != other.res_code
        {
            // Two partials of different shapes cannot be combined into one true answer. **Keep the one that saw more
            // records** rather than whichever arrived first, so the answer is a property of the data instead of the
            // order partitions were scanned in - and count the loser's records as skipped either way, so `finalize`
            // can refuse to call the result plain `ok`.
            if !self.declared && other.records > self.records {
                let mine = self.records;
                let carried = (
                    self.skipped_absent,
                    self.skipped_shape,
                    self.skipped_unreadable,
                    self.dropped_old,
                );

                *self = other.clone();

                self.skipped_absent += carried.0;
                self.skipped_shape += carried.1 + mine;
                self.skipped_unreadable += carried.2;
                self.dropped_old += carried.3;

                return;
            }

            self.skipped_shape += other.records.max(1) + other.skipped_shape;

            return;
        }

        self.advance_to(other.last_bucket);

        if other.last_bucket != NO_BUCKET {
            for bucket in (other.last_bucket - other.slots + 1)..=other.last_bucket {
                let slot = other.slot_of(bucket);
                let contributors = other.count_at(slot);

                if contributors == 0 {
                    continue;
                }

                if let Some(v) = other.value_at(slot) {
                    self.add_at(bucket, v, contributors);
                }
            }
        }

        self.records += other.records;
        self.samples += other.samples;
        self.skipped_absent += other.skipped_absent;
        self.skipped_shape += other.skipped_shape;
        self.skipped_unreadable += other.skipped_unreadable;
        self.dropped_old += other.dropped_old;
        self.saturated |= other.saturated;
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        self.normalise();

        let map = ctx.map(20)?;

        map.put_str_str("bin", &self.bin)?;
        map.put_str_str("op", self.agg().name())?;
        map.put_str_int("records", self.records)?;
        map.put_str_int("samples", self.samples)?;
        map.put_str_int("skipped_absent", self.skipped_absent)?;
        map.put_str_int("skipped_shape", self.skipped_shape)?;
        map.put_str_int("skipped_unreadable", self.skipped_unreadable)?;

        if self.dropped_old > 0 {
            map.put_str_int("dropped_old", self.dropped_old)?;
        }

        if self.saturated {
            map.put_str_int("saturated", 1)?;
        }

        if !self.adopted() || self.last_bucket == NO_BUCKET {
            map.put_str_int("points", 0)?;
            map.put_str_str("result", "empty")?;

            return Ok(map.as_value());
        }

        map.put_str_int("step_ms", self.step_ms)?;
        map.put_str_int("slots", self.slots)?;
        map.put_str_str("kind", self.kind().name())?;

        if let Some(r) = self.resolution() {
            map.put_str_str("resolution", r.name())?;
            map.put_str_str("archive_bin", &format!("{}~{}", self.bin, r.suffix()))?;
        }

        let last = self.last_bucket;
        let first = last - self.slots + 1;
        let span = self.slots;

        let factor = if self.max_points >= 1 && span > self.max_points {
            (span + self.max_points - 1) / self.max_points
        } else {
            1
        };

        let points = (span + factor - 1) / factor;
        let down = self.agg().downsample_with();

        let values = ctx.list(points as u32)?;
        let counts = ctx.list(points as u32)?;
        let mut known = 0i64;

        for p in 0..points {
            let group_start = first + p * factor;
            let group_end = (group_start + factor - 1).min(last);
            let mut acc: Option<Num> = None;
            let mut contributors: u32 = 0;

            for b in group_start..=group_end {
                let slot = self.slot_of(b);

                if let Some(v) = self.value_at(slot) {
                    acc = ring::combine(down, acc, Some(v)).0;
                    contributors = contributors.saturating_add(self.count_at(slot));
                }
            }

            match acc {
                None => {
                    values.push(&Value::nil())?;
                    counts.push_int(0)?;
                }
                Some(Num::I(v)) => {
                    values.push_int(v)?;
                    counts.push_int(i64::from(contributors))?;
                    known += 1;
                }
                // A mean reports as its average, like everywhere else.
                Some(other) => {
                    values.push_f64(other.as_f64())?;
                    counts.push_int(i64::from(contributors))?;
                    known += 1;
                }
            }
        }

        map.put_str_int("from_ms", self.bucket_start_ms(first))?;
        map.put_str_int("to_ms", self.bucket_start_ms(last))?;
        map.put_str_int("bucket_ms", factor.saturating_mul(self.step_ms))?;
        map.put_str_int("points", points)?;
        map.put_str_int("known", known)?;
        map.put_str_int("unknown", points - known)?;
        map.put_str_list("values", &values)?;
        map.put_str_list("counts", &counts)?;
        map.put_str_int("shape_declared", i64::from(self.declared))?;

        // **Not `ok`.** Records were dropped for disagreeing with a shape nobody chose, so this answer describes some
        // subset of the set and a caller testing `result == "ok"` should not get it. Declaring the shape, or narrowing
        // the query, turns it back into `ok`.
        map.put_str_str(
            "result",
            if self.skipped_shape > 0 && !self.declared {
                "shape_conflict"
            } else {
                "ok"
            },
        )?;

        Ok(map.as_value())
    }
}

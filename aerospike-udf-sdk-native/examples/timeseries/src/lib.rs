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

//! A round-robin time series in a blob bin, with its shape in another.
//!
//! ```text
//! ts_init("cpu", 60000, 1440)                       one day at one-minute resolution
//! ts_init("bytes", 1000, 3600, "kind=i64,mode=counter,agg=sum,wrap=32")
//! ts_init("cpu", 0, 0, "archives=default")          seven archives: one hour of seconds to 25 years of years
//! ts_append("cpu", 1767225600000, 42.5)             one sample, into every archive it has
//! ts_append_all(bins, ts, values)                   every metric in the record, one commit
//! ts_append_many("cpu", times, values)              a batch, one borrow of each bin for all of it
//! ts_report("cpu", nil, nil, 200)                   the last day in 200 points, downsampled here
//! ts_report("cpu", three_years_ago)                 picks the archive that reaches back that far
//! ts_stats("cpu")                                   count/sum/min/max/first/last, no list at all
//! ts_info("cpu")                                    which rings exist and how far back each goes
//! ```
//!
//! # Archives
//!
//! A single ring is capped at 65,536 slots, so one second of resolution reaches 18 hours and a year is unreachable.
//! [`config::default_archives`] keeps seven rings at coarsening resolution — `cpu~s` through `cpu~y` — reaching 25 years
//! in 60.8 KiB, about sixteen metrics to a record.
//!
//! Consolidation needs **no accumulator**: a bucket is `floor(t / step)` anchored to the epoch, so an hourly archive's
//! buckets are exactly 3,600 of the second archive's wide and aligned to them for free. Every archive is therefore fed
//! the *raw* sample and its own `agg` does the rest — which also makes a coarse mean the true mean of the samples rather
//! than RRDtool's mean of means.
//!
//! A series is either a single ring or a hierarchy, never both: with archives the base bin holds nothing, because the
//! finest archive already is the ring it would hold.
//!
//! # Why a blob rather than a list
//!
//! A CDT list of 1,440 doubles is msgpack: appending walks and rewrites the packed form, and reading unpacks all of it
//! to hand back something addressed by index. A ring in a blob is `slots × 8` bytes at computable offsets — one
//! `memcpy` in, one out, and the sample for a timestamp is found by arithmetic rather than by traversal.
//!
//! The shape lives in a second bin, [`config`], so a client can read what a series looks like without pulling the
//! series. Both bins are written inside one UDF invocation, under the record lock, so they cannot be observed out of
//! step.
//!
//! # What this costs, honestly
//!
//! An append is O(1) in slots *touched* and O(slots) in bytes *moved*. The ring is modified **in place**, through
//! `Record::with_bytes_mut`, which removed two of the four copies a read-modify-write used to pay and made a 512 KB
//! ring's append 2.1× faster; the two that remain are the floor. [`ts_append_many`] is still the recommended path for
//! anything above a trickle, because it borrows the bin once for the whole batch. See [`ring`] for the measurements.
//!
//! # Nothing is silently dropped
//!
//! Every append reports where it landed — `placed`, `combined`, `backfilled` — or why it did not: `too_old`,
//! `backfill_refused`, `would_wipe`. A counter reading additionally reports `baseline`, `delta`, `wrapped` or `reset`,
//! because a counter that restarted is a fact about the data and not a number to smooth over.

#![allow(clippy::needless_range_loop)]

use core::ffi::CStr;

use aerospike_udf_sdk_native::{
    module, udfs, Error, Kind as ValueKind, List, Map, Record, Result, Value,
};

pub mod calendar;
pub mod config;
pub mod ring;
pub mod rollup;

#[cfg(test)]
mod tests;

use config::{Agg, Archive, Config, Kind, Mode, Resolution, Shape};
use ring::{Num, Outcome, RingMut, RingView};

/// Wall clock in milliseconds, for `created_ms` when a caller does not supply one.
///
/// Every entry point takes an explicit `now_ms` where it matters, so tests never depend on this.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse the `key=value,key=value` options string.
///
/// **An unknown key is an error.** `agg=maxx` or `kidn=i64` is a typo, and a typo that is ignored produces a series
/// with a shape the caller did not ask for and will not notice until the numbers are wrong.
pub fn apply_opts(cfg: &mut Config, opts: &str) -> Result<()> {
    // `archives` and `xff` are gathered and applied after the loop, so `xff=50,archives=default` and
    // `archives=default,xff=50` mean the same thing. An options string whose meaning depends on the order of its keys
    // is a trap, and this one has no reason to be ordered.
    let mut archives: Option<Vec<Archive>> = None;
    let mut xff: Option<u8> = None;

    for part in opts.split(',') {
        let part = part.trim();

        if part.is_empty() {
            continue;
        }

        let Some((key, value)) = part.split_once('=') else {
            return Err(Error::msg(format!(
                "option '{part}' is not key=value - try 'agg=max,kind=i64'"
            )));
        };

        let (key, value) = (key.trim(), value.trim());

        match key {
            "agg" => cfg.agg = Agg::parse(value)?,
            "kind" => cfg.kind = Kind::parse(value)?,
            "mode" => cfg.mode = Mode::parse(value)?,
            "backfill" => cfg.allow_backfill = parse_bool(key, value)?,
            "wipe" => cfg.allow_wipe = parse_bool(key, value)?,
            "wrap" => {
                cfg.wrap_bits = value
                    .parse::<u8>()
                    .map_err(|_| Error::msg(format!("wrap must be 0, 32 or 64, got '{value}'")))?
            }
            "archives" => {
                archives = match value {
                    "default" | "all" => Some(config::default_archives().to_vec()),
                    "none" | "" => Some(Vec::new()),
                    spec => Some(Archive::parse_list(spec)?),
                }
            }
            "heartbeat" => {
                cfg.heartbeat_ms = value.parse::<i64>().map_err(|_| {
                    Error::msg(format!("heartbeat must be milliseconds, got '{value}'"))
                })?
            }
            "xff" => {
                xff = Some(value.parse::<u8>().map_err(|_| {
                    Error::msg(format!("xff must be a percentage from 0 to 100, got '{value}'"))
                })?)
            }
            other => {
                return Err(Error::msg(format!(
                    "unknown option '{other}' - one of agg, kind, mode, backfill, wipe, wrap, \
                     archives, heartbeat, xff"
                )))
            }
        }
    }

    if let Some(mut set) = archives {
        if let Some(pct) = xff {
            for a in &mut set {
                a.xff_pct = pct;
            }
        }

        cfg.set_archives(&set)?;
    } else if xff.is_some() {
        return Err(Error::msg(
            "xff describes an archive's density, so it needs archives= to apply to",
        ));
    }

    cfg.validate()
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    match value {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(Error::msg(format!("{key} must be true or false, got '{other}'"))),
    }
}

fn cfg_bin_name(given: Option<&str>) -> &str {
    given.unwrap_or(config::DEFAULT_BIN)
}

/// Read the config bin, or say clearly that the series was never initialised.
fn load_config(rec: &Record, cfg_bin: &str) -> Result<(Config, u32)> {
    let bytes = rec.get_bytes(cfg_bin)?.ok_or_else(|| {
        Error::msg(format!(
            "no time-series config in bin '{cfg_bin}' - call ts_init first"
        ))
    })?;

    let cfg = Config::decode(bytes)?;

    // **The fingerprint is of the config's canonical encoding, not of the bin's bytes.**
    //
    // This used to hash the first 40 bytes, which was the whole config when there was only one - and silently became
    // *the header only* when the archive table was appended, so every ring in an archived series failed to validate
    // against the config it had just been built from. Re-encoding is also robust to a bin that decodes correctly but
    // carries trailing bytes: two bins that mean the same config get the same fingerprint.
    let fingerprint = cfg.fingerprint_of();

    Ok((cfg, fingerprint))
}

/// The shape of one ring of a series, given the resolution a caller asked for.
///
/// **A series is either a single ring or a hierarchy, never both.** With archives the base bin holds nothing: the
/// finest archive *is* the ring the base bin used to be, and keeping a copy of it under the base name would store every
/// one-second sample twice — 28 KiB per metric, forever, to say the same thing.
fn shape_of(cfg: &Config, cfg_fp: u32, res: Option<Resolution>) -> Result<Shape> {
    match (cfg.has_archives(), res) {
        (false, None) => Ok(Shape::base(cfg, cfg_fp)),
        (false, Some(r)) => Err(Error::msg(format!(
            "this series keeps a single ring, so it has no {} archive - \
             initialise it with 'archives=default' to get the hierarchy",
            r.name()
        ))),
        // Finest first, so archive 0 is the closest analogue of the single ring.
        (true, None) => Shape::archive(cfg, cfg_fp, 0),
        (true, Some(r)) => Shape::at_resolution(cfg, cfg_fp, r),
    }
}

fn parse_res(res: Option<&str>) -> Result<Option<Resolution>> {
    res.map(Resolution::parse).transpose()
}

/// Borrow one ring for reading, by shape.
fn open_ring<'r>(rec: &'r Record, base: &str, shape: &Shape) -> Result<RingView<'r>> {
    let bin = shape.bin_name(base);
    let bytes = rec.get_bytes(&bin)?.ok_or_else(|| {
        Error::msg(format!("no ring in bin '{bin}' - call ts_init for it"))
    })?;

    RingView::open(bytes, shape)
}

/// Which archive answers a range, and the reason, so a report can say why it chose.
///
/// **The finest archive that reaches back far enough.** Coarser archives exist to cover longer ranges, not to save
/// work: downsampling inside the UDF costs a few thousand additions, so there is never a reason to answer from a
/// coarser archive than the data allows. If nothing reaches back far enough the coarsest answers, and says so — a
/// truncated answer with an explanation beats an empty one.
///
/// Reach is measured **from now**, not across the requested width. An archive retains the last `span` of time relative
/// to its newest bucket, so a one-hour archive cannot answer "the hour before last" however narrow that window is.
/// Returns the index, why, and **whether the chosen archive actually reaches back that far** — a returned fact rather
/// than something a caller re-derives by matching on the message, which is how a reason string quietly becomes an API.
fn select_archive(cfg: &Config, from_ms: Option<i64>, now_ms: i64) -> (usize, &'static str, bool) {
    let n = cfg.archives().len();

    if n <= 1 {
        return (0, "the only archive", true);
    }

    let Some(from) = from_ms else {
        return (0, "no start was given, so the finest archive", true);
    };

    let age = now_ms.saturating_sub(from).max(0);

    for (i, a) in cfg.archives().iter().enumerate() {
        if a.approx_span_ms() >= age {
            return (i, "the finest archive that reaches back that far", true);
        }
    }

    (n - 1, "no archive reaches back that far, so the coarsest", false)
}

/// Turn an appended value into the ring's own width, refusing a lossy conversion.
///
/// An `i64` ring fed a double, or an `f64` ring fed an integer, is a caller mistake worth reporting: the first would
/// truncate and the second is accepted, because widening an integer that the caller chose to store as a double is what
/// they asked for. The asymmetry is deliberate and it is the direction that cannot lose information.
fn num_of(value: &Value<'_>, kind: Kind) -> Result<Num> {
    match kind {
        Kind::I64 => match value.kind() {
            ValueKind::Int => Ok(Num::I(value.as_i64().unwrap_or(0))),
            other => Err(Error::msg(format!(
                "this ring stores i64 and {other:?} was appended - pass an integer, \
                 because truncating a double here would lose data silently"
            ))),
        },
        Kind::F64 => match value.kind() {
            ValueKind::Double => finite(value.as_f64().unwrap_or(0.0)),
            ValueKind::Int => Ok(Num::F(value.as_i64().unwrap_or(0) as f64)),
            other => Err(Error::msg(format!(
                "this ring stores f64 and {other:?} was appended"
            ))),
        },
    }
}

/// Refuse a sample that is not a finite number.
///
/// **`NaN` is the unknown marker.** Storing one would report `accepted = 1` and leave a gap — a write the caller was told
/// succeeded and that reads back as "nothing was ever here". An infinity is worse than useless in the other direction: it
/// is a real value as far as every aggregation is concerned, so one of them poisons a `sum` or a `mean` bucket
/// permanently, and no later sample can bring it back.
///
/// Both are refused rather than silently converted, for the same reason a double offered to an integer ring is refused:
/// the caller has more information about what went wrong than this function does.
fn finite(v: f64) -> Result<Num> {
    if v.is_finite() {
        return Ok(Num::F(v));
    }

    Err(Error::msg(format!(
        "a sample has to be a finite number, and {v} is not - NaN is how this ring marks a slot \
         unknown, so storing one would report a successful write of a gap"
    )))
}

/// Write a sample into a result map, as an integer for an `I64` ring so it stays exact.
fn put_num(map: &Map<'_>, key: &str, value: Num) -> Result<()> {
    match value {
        Num::I(v) => map.put_str_int(key, v),
        // A mean reports as its average - the sum and count are storage, not the answer.
        other => {
            let v = other.as_f64();

            if v.is_finite() {
                map.put_str_f64(key, v)
            } else {
                Ok(())
            }
        }
    }
}

fn push_num(list: &List<'_>, value: Option<Num>) -> Result<()> {
    match value {
        // Unknown is **nil, not zero**. A slot nothing was written to and a slot holding 0.0 are different facts, and
        // a series that cannot tell them apart makes every gap look like an outage of value zero.
        None => list.push(&Value::nil()),
        Some(Num::I(v)) => list.push_int(v),
        // A mean reports as its average. An empty mean cannot reach here - `read_slot` returns None for a zero count.
        Some(other) => list.push_f64(other.as_f64()),
    }
}

/// Describe the shape on any result map, so a caller never has to remember what they configured.
fn put_shape(map: &Map<'_>, shape: &Shape) -> Result<()> {
    let cfg = shape.config();

    // For a base ring this is the step the caller chose; for an archive it is the resolution's width, approximate for a
    // month or a year because a chart axis needs a number and a calendar month has no single one.
    map.put_str_int("step_ms", shape.approx_step_ms())?;
    map.put_str_int("slots", shape.slots())?;
    map.put_str_str("agg", shape.agg().name())?;
    map.put_str_str("kind", cfg.kind.name())?;
    map.put_str_str("mode", cfg.mode.name())?;

    if let Some(r) = shape.resolution() {
        map.put_str_str("resolution", r.name())?;

        if shape.xff_pct() > 0 {
            map.put_str_int("xff", i64::from(shape.xff_pct()))?;
        }
    }

    Ok(())
}

/// What one archive did with one sample.
struct Landed {
    resolution: Option<Resolution>,
    applied: ring::Applied,
    /// The first millisecond of the bucket it landed in — calendar-correct for a month or a year, which is why it is
    /// carried out rather than recomputed as `bucket * step` by the caller.
    bucket_ms: i64,
    last_bucket: i64,
    resets: u32,
    saturated: bool,
}

/// Append one sample to **every ring of a series** — the single ring, or all seven archives.
///
/// # Why every archive sees the raw sample
///
/// RRDtool consolidates: a coarse archive is fed the fine archive's already-averaged values, so a yearly mean is a mean
/// of monthly means, which is biased whenever sample density varies. Here each archive buckets the *raw* sample by its
/// own resolution, and because a bucket is `floor(t / step)` anchored to the epoch, an hourly bucket contains exactly
/// the 3,600 second-buckets it should with no accumulator and no alignment step. So a coarse mean is the true mean of
/// the samples — better than RRDtool, and it falls out of the indexing rather than costing anything.
///
/// Each archive is a separate bin, so each is a separate in-place borrow; there is one `rec.write()` for all of them.
fn append_everywhere(
    rec: &mut Record,
    base: &str,
    cfg: &Config,
    cfg_fp: u32,
    ts_ms: i64,
    sample: Num,
) -> Result<Vec<Landed>> {
    let count = if cfg.has_archives() { cfg.archives().len() } else { 1 };
    let mut out = Vec::with_capacity(count);

    for i in 0..count {
        let shape = if cfg.has_archives() {
            Shape::archive(cfg, cfg_fp, i)?
        } else {
            Shape::base(cfg, cfg_fp)
        };

        let bin = shape.bin_name(base);
        let landed = rec
            .with_bytes_mut(&bin, |bytes| -> Result<_> {
                let mut ring = RingMut::open(bytes, &shape)?;
                let applied = ring.append(ts_ms, sample)?;

                Ok(Landed {
                    resolution: shape.resolution(),
                    bucket_ms: shape.bucket_start_ms(applied.bucket),
                    applied,
                    last_bucket: ring.last_bucket(),
                    resets: ring.resets(),
                    saturated: ring.saturated(),
                })
            })?
            .ok_or_else(|| Error::msg(format!("no ring in bin '{bin}' - call ts_init for it")))??;

        out.push(landed);
    }

    Ok(out)
}

udfs! {
    /// Create a series: the config bin if it is absent, and the ring or rings for `bin`.
    ///
    /// `ts_init(bin, step_ms, slots, [opts], [cfg_bin], [now_ms])`
    ///
    /// `opts` is `key=value` pairs: `agg`, `kind`, `mode`, `backfill`, `wipe`, `wrap`, `archives`, `heartbeat`, `xff`.
    /// An unknown key is an error rather than a shrug — see [`apply_opts`].
    ///
    /// # One ring, or an RRDtool-style hierarchy
    ///
    /// Without `archives`, `bin` holds one ring of `slots` buckets `step_ms` wide, and nothing below applies.
    ///
    /// With `archives=default`, `bin` holds **nothing** and seven rings appear beside it — `cpu~s`, `cpu~m`, `cpu~h`,
    /// `cpu~d`, `cpu~w`, `cpu~mo`, `cpu~y` — covering one hour of seconds through 25 years of years in 60.8 KiB. The
    /// base bin is left empty deliberately: the finest archive already *is* the ring it would hold.
    ///
    /// `step_ms` and `slots` are then taken **from the finest archive** rather than from the arguments, because with a
    /// hierarchy they have no separate meaning and two numbers that disagree with the table beside them are a bug
    /// waiting to be found. They still appear in the config, so `ts_rollup` can pin a shape.
    ///
    /// An explicit set is `archives=s:3600:last;m:1440;h:336:mean:50` — `resolution:slots[:agg[:xff]]`, semicolon
    /// separated, `mean` and `xff=0` by default.
    ///
    /// # It will not quietly destroy a series
    ///
    /// If the config bin already exists with the **same** shape, any missing rings are created and the ones already
    /// there are left alone. If it exists with a **different** shape, this fails: every ring in the record was built
    /// against the old one, and rewriting it would leave them all decodable and all meaning something else. Drop the
    /// bins or use a different config bin.
    fn ts_init<'a>(
        rec: &'a Record,
        bin: &'a str,
        step_ms: i64,
        slots: i64,
        opts: Option<&'a str>,
        cfg_bin: Option<&'a str>,
        now_ms: Option<i64>,
    ) -> Result<Value<'a>> {
        let cfg_bin = cfg_bin_name(cfg_bin);
        let created = now_ms.unwrap_or_else(self::now_ms);

        let mut wanted = Config::new(step_ms, slots, created)?;

        if let Some(opts) = opts {
            apply_opts(&mut wanted, opts)?;
        }

        if wanted.has_archives() {
            let finest = wanted.archives()[0];

            // A calendar-only hierarchy has no finest *step*, so the caller's stands. Nothing places a sample against
            // it in that case - every archive buckets through the calendar.
            if let Some(step) = finest.resolution.step_ms() {
                wanted.step_ms = step;
            }

            wanted.slots = finest.slots;
            wanted.validate()?;

            // Checked now rather than discovered at the first append, with six of seven rings already written.
            config::check_base_name(bin, wanted.archives())?;
        }

        // An existing config wins on `created_ms`, so comparing shapes does not trip over a timestamp.
        let existing = rec.get_bytes(cfg_bin)?.map(Config::decode).transpose()?;

        let cfg = match existing {
            None => wanted,
            Some(found) => {
                let mut comparable = wanted;

                comparable.created_ms = found.created_ms;

                if comparable != found {
                    let shape = format!(
                        "step {} slots {} {}/{}/{}",
                        found.step_ms,
                        found.slots,
                        found.agg.name(),
                        found.kind.name(),
                        found.mode.name()
                    );
                    // The archive list only when there is one. "archives: a single ring and no archives" is what
                    // appending it unconditionally produced, and a message that says nothing twice reads as a bug.
                    let shape = if found.has_archives() {
                        format!("{shape}, archives: {}", found.archive_names())
                    } else {
                        shape
                    };

                    return Err(Error::msg(format!(
                        "bin '{cfg_bin}' already describes a different series ({shape}); \
                         every ring in this record was built against it, so changing it would \
                         silently reinterpret them all"
                    )));
                }

                found
            }
        };

        let encoded = cfg.encode();
        let fingerprint = Config::fingerprint(&encoded);
        let rings = if cfg.has_archives() {
            cfg.archives().len()
        } else {
            1
        };

        let map = rec.ctx().map(12)?;
        let table = rec.ctx().list(rings as u32)?;
        let mut made = 0i64;
        let mut found = 0i64;
        let mut total_bytes = 0i64;

        for i in 0..rings {
            let shape = if cfg.has_archives() {
                Shape::archive(&cfg, fingerprint, i)?
            } else {
                Shape::base(&cfg, fingerprint)
            };

            let name = shape.bin_name(bin);
            // Deliberately not idempotent-by-overwrite: re-running init must never be a way to erase a ring.
            let exists = rec.get_bytes(&name)?.is_some();

            if exists {
                found += 1;
            } else {
                rec.set_blob(&name, &ring::create(&shape))?;
                made += 1;
            }

            total_bytes += shape.ring_len() as i64;

            let entry = rec.ctx().map(8)?;

            entry.put_str_int("index", i as i64)?;
            entry.put_str_str("bin", &name)?;
            entry.put_str_int("slots", shape.slots())?;
            entry.put_str_str("agg", shape.agg().name())?;
            entry.put_str_int("bytes", shape.ring_len() as i64)?;
            entry.put_str_int("span_ms", shape.approx_span_ms())?;
            entry.put_str_str("result", if exists { "exists" } else { "created" })?;

            if let Some(r) = shape.resolution() {
                entry.put_str_str("resolution", r.name())?;
            }

            table.push_map(&entry)?;
        }

        let wrote_config = existing.is_none();

        if wrote_config {
            rec.set_blob(cfg_bin, &encoded)?;
        }

        if wrote_config || made > 0 {
            rec.write()?;
        }

        put_shape(&map, &Shape::base(&cfg, fingerprint))?;
        map.put_str_str("bin", bin)?;
        map.put_str_str("config_bin", cfg_bin)?;
        map.put_str_int("archives", cfg.archives().len() as i64)?;
        map.put_str_int("rings_created", made)?;
        map.put_str_int("rings_existing", found)?;
        map.put_str_int("ring_bytes", total_bytes)?;
        map.put_str_list("table", &table)?;

        if cfg.heartbeat_ms > 0 {
            map.put_str_int("heartbeat_ms", cfg.heartbeat_ms)?;
        }

        map.put_str_str(
            "result",
            match (made, found) {
                (0, _) => "exists",
                (_, 0) => "created",
                // Some rings were there and some were not: a set that was interrupted, or widened.
                _ => "completed",
            },
        )?;

        Ok(map.as_value())
    }

    /// Append one sample — to the single ring, or to every archive of the hierarchy.
    ///
    /// `ts_append(bin, ts_ms, value, [cfg_bin])`
    ///
    /// The result always says what happened. A rejected append leaves the ring byte-identical, so a wrong timestamp is
    /// recoverable rather than destructive.
    ///
    /// With archives, `result` is the **finest** archive's outcome — the closest analogue of the single ring — and
    /// `per_archive` gives all seven. One sample normally reads `placed` on the second archive and `combined` on every
    /// coarser one, which is consolidation happening rather than anything to act on.
    fn ts_append<'a>(
        rec: &'a mut Record,
        bin: &'a str,
        ts_ms: i64,
        value: Value<'a>,
        cfg_bin: Option<&'a str>,
    ) -> Result<Value<'a>> {
        // Read the shape first: it is a different bin, so it does not alias the rings, and the mutable borrows below
        // need `&mut rec` to themselves.
        let (cfg, fingerprint) = load_config(rec, cfg_bin_name(cfg_bin))?;
        let sample = num_of(&value, cfg.kind)?;

        // **Each ring is modified where it lies.** `with_bytes_mut` hands out the bin's own bytes, so this pays neither
        // the copy out nor the copy back - see `Record::with_bytes_mut` for which two of the four copies remain.
        let landed = append_everywhere(rec, bin, &cfg, fingerprint, ts_ms, sample)?;
        let first = &landed[0];
        let applied = first.applied;
        let map = rec.ctx().map(16)?;
        let accepted = landed.iter().filter(|l| l.applied.outcome.accepted()).count() as i64;
        let cleared: i64 = landed.iter().map(|l| l.applied.cleared).sum();
        let saturated = landed.iter().any(|l| l.saturated);

        map.put_str_str("result", applied.outcome.name())?;
        map.put_str_int("bucket", applied.bucket)?;
        map.put_str_int("bucket_ms", first.bucket_ms)?;
        map.put_str_int("cleared", cleared)?;

        if let Some(event) = applied.counter {
            map.put_str_str("counter", event.name())?;
        }

        if let Some(stored) = applied.stored {
            put_num(&map, "stored", stored)?;
        }

        if cfg.has_archives() {
            let per = rec.ctx().map(landed.len() as u32)?;

            for l in &landed {
                let name = l.resolution.map_or("ring", Resolution::name);

                per.put_str_str(name, l.applied.outcome.name())?;
            }

            map.put_str_int("archives", landed.len() as i64)?;
            map.put_str_int("archives_accepted", accepted)?;
            map.put_str_int("archives_refused", landed.len() as i64 - accepted)?;
            map.put_str_map("per_archive", &per)?;
        }

        if accepted == 0 {
            // Nothing was staged anywhere, so nothing is written. Every ring on disk is untouched.
            map.put_str_int("accepted", 0)?;

            return Ok(map.as_value());
        }

        map.put_str_int("accepted", 1)?;
        map.put_str_int("last_bucket", first.last_bucket)?;

        if first.resets > 0 {
            map.put_str_int("counter_resets", i64::from(first.resets))?;
        }

        if saturated {
            map.put_str_int("saturated", 1)?;
        }

        // No `set_blob`: the bytes are already the bins'. Handing out the mutable pointer marked each changed, so this
        // commits what was written in place - **one write for every archive**, not one per archive.
        rec.write()?;

        Ok(map.as_value())
    }

    /// Append one sample to each of several metrics — **one record write for the lot**.
    ///
    /// `ts_append_all(bins, ts_ms, values, [cfg_bin])`
    ///
    /// # Why this exists
    ///
    /// One config serves every series in a record, so sixteen metrics sampled together are sixteen `ts_append` calls
    /// that each read the config, walk their archives and commit the record. With the default archive set that is 16 × 7
    /// = 112 in-place ring writes spread over sixteen record commits, and the commit is the expensive part.
    ///
    /// This reads the config once and commits once. The rings touched are the same ones; what disappears is fifteen
    /// round trips and fifteen record writes.
    fn ts_append_all<'a>(
        rec: &'a mut Record,
        bins: List<'a>,
        ts_ms: i64,
        values: List<'a>,
        cfg_bin: Option<&'a str>,
    ) -> Result<Value<'a>> {
        let (cfg, fingerprint) = load_config(rec, cfg_bin_name(cfg_bin))?;
        let n = bins.len() as usize;

        if values.len() as usize != n {
            return Err(Error::msg(format!(
                "bins has {n} entries and values has {} - they have to line up",
                values.len()
            )));
        }

        // Both lists are pulled out before any ring is borrowed, because the borrow wants the record to itself.
        let mut names: Vec<String> = Vec::with_capacity(n);

        bins.for_each(|_, v| {
            names.push(v.as_str().unwrap_or_default().to_string());

            true
        });

        let mut samples: Vec<Result<Num>> = Vec::with_capacity(n);

        values.for_each(|_, v| {
            samples.push(num_of(&v, cfg.kind));

            true
        });

        if names.len() != n || samples.len() != n {
            return Err(Error::msg("a list changed length while it was being read"));
        }

        let mut accepted = 0i64;
        let mut refused = 0i64;
        // Gathered plainly and turned into host values afterwards: a `Map` borrows the record's context, and the
        // in-place ring writes below want the record to themselves.
        let mut outcomes: Vec<(&str, &'static str)> = Vec::with_capacity(n);

        for (name, sample) in names.iter().zip(samples) {
            if name.is_empty() {
                return Err(Error::msg("a bin name in the list is not a string"));
            }

            let landed = append_everywhere(rec, name, &cfg, fingerprint, ts_ms, sample?)?;
            let ok = landed.iter().filter(|l| l.applied.outcome.accepted()).count() as i64;

            accepted += ok;
            refused += landed.len() as i64 - ok;

            // The finest ring's outcome, which is the one a caller acts on; `ts_append` on that metric gives the rest.
            outcomes.push((name, landed[0].applied.outcome.name()));
        }

        let map = rec.ctx().map(8)?;
        let per = rec.ctx().map(n as u32)?;

        for (name, outcome) in outcomes {
            per.put_str_str(name, outcome)?;
        }

        map.put_str_int("metrics", n as i64)?;
        map.put_str_int("rings_accepted", accepted)?;
        map.put_str_int("rings_refused", refused)?;
        map.put_str_map("per_metric", &per)?;

        if accepted > 0 {
            // **One commit for every metric and every archive.**
            rec.write()?;
        }

        map.put_str_str(
            "result",
            match (accepted, refused) {
                (0, 0) => "empty",
                (0, _) => "refused",
                (_, 0) => "ok",
                _ => "partial",
            },
        )?;

        Ok(map.as_value())
    }

    /// Append a batch: **one ring copy for the whole list** instead of one per sample.
    ///
    /// `ts_append_many(bin, times, values, [cfg_bin])`
    ///
    /// This is the reason the example exists. A ring is copied out of its particle and back in on every write, so N
    /// separate `ts_append` calls copy it 2N times; this copies it twice and touches N slots in between. The lists
    /// come across in one bulk host call each rather than element by element.
    ///
    /// Samples are applied in the order given, which matters: they compose through the configured aggregation, and
    /// `last` depends on order by definition. Per-outcome counts come back, so a batch that was half rejected says so.
    ///
    /// With archives this borrows **each** archive's bin once for the whole batch, so a 1,000-sample batch into the
    /// default set is seven borrows rather than seven thousand. The reported counts are the **finest** archive's: a batch
    /// spanning ten minutes is `placed` there and mostly `combined` on the hour archive, and the second number describes
    /// consolidation rather than the batch.
    fn ts_append_many<'a>(
        rec: &'a mut Record,
        bin: &'a str,
        times: List<'a>,
        values: List<'a>,
        cfg_bin: Option<&'a str>,
    ) -> Result<Value<'a>> {
        let (cfg, fingerprint) = load_config(rec, cfg_bin_name(cfg_bin))?;

        let n = times.len() as usize;

        if values.len() as usize != n {
            return Err(Error::msg(format!(
                "times has {n} entries and values has {} - they have to line up",
                values.len()
            )));
        }

        // One host call per list rather than 2n. `ints_into`/`doubles_into` fill a slice in one crossing.
        let mut ts = vec![0i64; n];
        let got_ts = times.ints_into(0, &mut ts)?;

        // Values are pulled out before the ring is borrowed, because both want the record.
        let ints = match cfg.kind {
            Kind::I64 => {
                let mut vs = vec![0i64; n];
                let got = values.ints_into(0, &mut vs)?;

                vs.truncate(got_ts.min(got));

                Some(vs)
            }
            Kind::F64 => None,
        };

        let floats = match cfg.kind {
            Kind::F64 => {
                let mut vs = vec![0.0f64; n];
                let got = values.doubles_into(0, &mut vs)?;

                vs.truncate(got_ts.min(got));

                // The bulk path does not go through `num_of`, so it needs the same finite check - and the whole batch is
                // checked **before** anything is written, because a NaN discovered halfway through would leave the
                // samples before it applied and the ones after it not.
                if let Some(i) = vs.iter().position(|v| !v.is_finite()) {
                    return Err(Error::msg(format!(
                        "sample {i} of this batch is {}, and a sample has to be a finite number - \
                         NaN is how a ring marks a slot unknown",
                        vs[i]
                    )));
                }

                Some(vs)
            }
            Kind::I64 => None,
        };

        // **One borrow of each bin's bytes for the whole batch.** N separate `ts_append` calls would each copy the ring
        // out and back; this touches N slots inside one in-place borrow per archive and copies nothing.
        let rings = if cfg.has_archives() { cfg.archives().len() } else { 1 };
        let mut counts = [0i64; 6];
        let mut cleared = 0i64;
        let mut last_bucket = ring::NO_BUCKET;
        let mut new_resets = 0u32;
        let mut saturated = false;

        for r in 0..rings {
            let shape = if cfg.has_archives() {
                Shape::archive(&cfg, fingerprint, r)?
            } else {
                Shape::base(&cfg, fingerprint)
            };

            let name = shape.bin_name(bin);
            let outcome = rec
                .with_bytes_mut(&name, |bytes| -> Result<_> {
                    let mut ring = RingMut::open(bytes, &shape)?;
                    let resets_before = ring.resets();
                    let mut counts = [0i64; 6];
                    let mut cleared = 0i64;

                    if let Some(vs) = &ints {
                        for (i, &v) in vs.iter().enumerate() {
                            let applied = ring.append(ts[i], Num::I(v))?;

                            counts[outcome_index(applied.outcome)] += 1;
                            cleared += applied.cleared;
                        }
                    }

                    if let Some(vs) = &floats {
                        for (i, &v) in vs.iter().enumerate() {
                            let applied = ring.append(ts[i], Num::F(v))?;

                            counts[outcome_index(applied.outcome)] += 1;
                            cleared += applied.cleared;
                        }
                    }

                    Ok((
                        counts,
                        cleared,
                        ring.last_bucket(),
                        ring.resets().saturating_sub(resets_before),
                        ring.saturated(),
                    ))
                })?
                .ok_or_else(|| Error::msg(format!("no ring in bin '{name}' - call ts_init for it")))??;

            // The finest archive's counts are the reported ones: a batch spanning ten minutes is `placed` there and
            // mostly `combined` on the hour archive, and the second number describes consolidation rather than the batch.
            if r == 0 {
                counts = outcome.0;
                last_bucket = outcome.2;
                new_resets = outcome.3;
            }

            cleared += outcome.1;
            saturated |= outcome.4;
        }

        let map = rec.ctx().map(14)?;
        let accepted: i64 = counts[0] + counts[1] + counts[2];

        if cfg.has_archives() {
            map.put_str_int("archives", rings as i64)?;
        }

        map.put_str_int("offered", n as i64)?;
        map.put_str_int("accepted", accepted)?;
        map.put_str_int("placed", counts[0])?;
        map.put_str_int("combined", counts[1])?;
        map.put_str_int("backfilled", counts[2])?;
        map.put_str_int("too_old", counts[3])?;
        map.put_str_int("backfill_refused", counts[4])?;
        map.put_str_int("would_wipe", counts[5])?;
        map.put_str_int("cleared", cleared)?;
        map.put_str_int("last_bucket", last_bucket)?;

        if new_resets > 0 {
            map.put_str_int("counter_resets", i64::from(new_resets))?;
        }

        if saturated {
            map.put_str_int("saturated", 1)?;
        }

        // One write for the batch. The bytes are already the bin's, so there is nothing to set - taking the mutable
        // borrow marked it changed.
        if accepted > 0 {
            rec.write()?;
        }

        Ok(map.as_value())
    }

    /// Read the series out, downsampled **here** rather than on the client.
    ///
    /// `ts_report(bin, [from_ms], [to_ms], [max_points], [cfg_bin], [res], [now_ms])`
    ///
    /// With `max_points`, adjacent buckets are combined with the configured aggregation until the answer fits, and
    /// `bucket_ms` says what resolution came back. A 65,536-slot ring answered in 200 points ships 200 numbers instead
    /// of 65,536 — the whole argument for computing next to the data.
    ///
    /// Timestamps are **implied**, not sent: value `i` covers `from_ms + i * bucket_ms`. That halves the wire against
    /// a list of pairs and is exactly reconstructible.
    ///
    /// # Choosing an archive
    ///
    /// With a hierarchy and no `res`, this picks the **finest archive that still reaches back to `from_ms`** and says
    /// which and why in `resolution` and `archive_chosen_because`. Asking for the last ten minutes answers from the
    /// per-second archive; asking for the last three years answers from the monthly one. `res` — `s`, `m`, `h`, `d`,
    /// `w`, `mo`, `y` — overrides that and fails if the series has no such archive, rather than quietly substituting a
    /// neighbour.
    ///
    /// If nothing reaches far enough the coarsest archive answers and `result` is `truncated`, not `ok`: the numbers are
    /// real but the window is not the one that was asked for, and a caller checking `result == "ok"` should notice.
    fn ts_report<'a>(
        rec: &'a Record,
        bin: &'a str,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        max_points: Option<i64>,
        cfg_bin: Option<&'a str>,
        res: Option<&'a str>,
        now_ms: Option<i64>,
    ) -> Result<Value<'a>> {
        let (cfg, fingerprint) = load_config(rec, cfg_bin_name(cfg_bin))?;
        let asked = parse_res(res)?;

        // Selection happens before anything is read, from the archive descriptors alone - so choosing costs no ring
        // reads at all, and the choice is a property of the config rather than of what happens to be in the rings.
        let (shape, because, truncated) = match (cfg.has_archives(), asked) {
            (true, None) => {
                let now = now_ms.unwrap_or_else(self::now_ms);
                let (index, why, reached) = select_archive(&cfg, from_ms, now);

                (Shape::archive(&cfg, fingerprint, index)?, why, !reached)
            }
            (_, chosen) => (shape_of(&cfg, fingerprint, chosen)?, "asked for by name", false),
        };

        let ring = open_ring(rec, bin, &shape)?;
        let map = rec.ctx().map(20)?;

        put_shape(&map, &shape)?;
        map.put_str_str("bin", bin)?;
        map.put_str_int("appends", ring.appends())?;

        if cfg.has_archives() {
            map.put_str_str("archive_bin", &shape.bin_name(bin))?;
            map.put_str_str("archive_chosen_because", because)?;
        }

        if ring.resets() > 0 {
            map.put_str_int("counter_resets", i64::from(ring.resets()))?;
        }

        if ring.saturated() {
            map.put_str_int("saturated", 1)?;
        }

        let Some((first, last)) = ring.window() else {
            map.put_str_int("points", 0)?;
            map.put_str_int("known", 0)?;
            map.put_str_str("result", "empty")?;

            return Ok(map.as_value());
        };

        let from_bucket = from_ms.map_or(first, |t| ring.bucket_of(t)).max(first);
        let to_bucket = to_ms.map_or(last, |t| ring.bucket_of(t)).min(last);

        map.put_str_int("last_ms", ring.bucket_start_ms(last))?;

        if from_bucket > to_bucket {
            map.put_str_int("points", 0)?;
            map.put_str_int("known", 0)?;
            map.put_str_str("result", "no_overlap")?;

            return Ok(map.as_value());
        }

        let span = to_bucket - from_bucket + 1;

        // The factor is chosen so `points <= max_points`; without one the resolution is the ring's own. Bounded either
        // way, because `span <= slots <= MAX_SLOTS`.
        let factor = match max_points {
            Some(m) if m >= 1 && span > m => (span + m - 1) / m,
            _ => 1,
        };

        let points = (span + factor - 1) / factor;
        let down = shape.agg().downsample_with();
        let list = rec.ctx().list(points as u32)?;
        let mut known = 0i64;
        let mut suppressed = 0i64;

        for p in 0..points {
            let group_start = from_bucket + p * factor;
            let group_end = (group_start + factor - 1).min(to_bucket);
            let mut acc: Option<Num> = None;

            for b in group_start..=group_end {
                let (next, _) = ring::combine(down, acc, ring.at(b));

                acc = next;

                // Counted separately, because "no data" and "data too sparse to trust" are different facts and a caller
                // tuning `xff` needs to see the second one happening.
                if ring.at(b).is_none() && ring.at_unfiltered(b).is_some() {
                    suppressed += 1;
                }
            }

            if acc.is_some() {
                known += 1;
            }

            push_num(&list, acc)?;
        }

        map.put_str_int("from_ms", ring.bucket_start_ms(from_bucket))?;
        map.put_str_int("to_ms", ring.bucket_start_ms(to_bucket))?;
        map.put_str_int("bucket_ms", factor.saturating_mul(shape.approx_step_ms()))?;
        map.put_str_int("points", points)?;
        map.put_str_int("known", known)?;
        map.put_str_int("unknown", points - known)?;

        if suppressed > 0 {
            map.put_str_int("xff_suppressed", suppressed)?;
        }

        map.put_str_str("downsampled_with", down.name())?;
        map.put_str_list("values", &list)?;
        map.put_str_str("result", if truncated { "truncated" } else { "ok" })?;

        Ok(map.as_value())
    }

    /// Summarise a window without returning it.
    ///
    /// `ts_stats(bin, [from_ms], [to_ms], [cfg_bin], [res], [now_ms])`
    ///
    /// The cheapest possible read: count, sum, min, max, first, last and their timestamps, and **no list at all**. An
    /// `I64` ring's sum stays in `i64` and saturates visibly rather than wrapping, for the same reason the summary
    /// example keeps an integer accumulator.
    ///
    /// Archive selection works exactly as it does for `ts_report`.
    fn ts_stats<'a>(
        rec: &'a Record,
        bin: &'a str,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        cfg_bin: Option<&'a str>,
        res: Option<&'a str>,
        now_ms: Option<i64>,
    ) -> Result<Value<'a>> {
        let (cfg, fingerprint) = load_config(rec, cfg_bin_name(cfg_bin))?;
        let asked = parse_res(res)?;

        let shape = match (cfg.has_archives(), asked) {
            (true, None) => {
                let now = now_ms.unwrap_or_else(self::now_ms);
                let (index, _, _) = select_archive(&cfg, from_ms, now);

                Shape::archive(&cfg, fingerprint, index)?
            }
            (_, chosen) => shape_of(&cfg, fingerprint, chosen)?,
        };

        let ring = open_ring(rec, bin, &shape)?;
        let map = rec.ctx().map(20)?;

        put_shape(&map, &shape)?;
        map.put_str_str("bin", bin)?;

        if cfg.has_archives() {
            map.put_str_str("archive_bin", &shape.bin_name(bin))?;
        }

        let Some((first, last)) = ring.window() else {
            map.put_str_int("count", 0)?;
            map.put_str_str("result", "empty")?;

            return Ok(map.as_value());
        };

        let from_bucket = from_ms.map_or(first, |t| ring.bucket_of(t)).max(first);
        let to_bucket = to_ms.map_or(last, |t| ring.bucket_of(t)).min(last);

        if from_bucket > to_bucket {
            map.put_str_int("count", 0)?;
            map.put_str_str("result", "no_overlap")?;

            return Ok(map.as_value());
        }

        let mut count = 0i64;
        let mut sum: Option<Num> = None;
        let mut min: Option<Num> = None;
        let mut max: Option<Num> = None;
        let mut firsts: Option<(i64, Num)> = None;
        let mut lasts: Option<(i64, Num)> = None;
        let mut saturated = false;

        for b in from_bucket..=to_bucket {
            let Some(v) = ring.at(b) else { continue };

            count += 1;

            let (next, over) = ring::combine(Agg::Sum, sum, Some(v));

            sum = next;
            saturated |= over;

            min = ring::combine(Agg::Min, min, Some(v)).0;
            max = ring::combine(Agg::Max, max, Some(v)).0;

            if firsts.is_none() {
                firsts = Some((b, v));
            }

            lasts = Some((b, v));
        }

        map.put_str_int("from_ms", ring.bucket_start_ms(from_bucket))?;
        map.put_str_int("to_ms", ring.bucket_start_ms(to_bucket))?;
        map.put_str_int("buckets", to_bucket - from_bucket + 1)?;
        map.put_str_int("count", count)?;
        map.put_str_int("unknown", (to_bucket - from_bucket + 1) - count)?;

        if let Some(v) = sum {
            put_num(&map, "sum", v)?;

            if count > 0 {
                map.put_str_f64("mean", v.as_f64() / count as f64)?;
            }
        }

        if let Some(v) = min {
            put_num(&map, "min", v)?;
        }

        if let Some(v) = max {
            put_num(&map, "max", v)?;
        }

        if let Some((b, v)) = firsts {
            put_num(&map, "first", v)?;
            map.put_str_int("first_ms", ring.bucket_start_ms(b))?;
        }

        if let Some((b, v)) = lasts {
            put_num(&map, "last", v)?;
            map.put_str_int("last_ms", ring.bucket_start_ms(b))?;
        }

        if saturated || ring.saturated() {
            map.put_str_int("saturated", 1)?;
        }

        map.put_str_str("result", "ok")?;

        Ok(map.as_value())
    }

    /// What a series actually looks like: its shape, and every archive with the window it currently holds.
    ///
    /// `ts_info(bin, [cfg_bin])`
    ///
    /// This is the answer to "which bin do I read, and how far back does it go" — the index-to-resolution mapping, the
    /// bin name, the retention, and for each ring whether it has anything in it yet. Reads through the zero-copy path,
    /// so asking costs no particle materialisation even though it touches every ring.
    fn ts_info<'a>(rec: &'a mut Record, bin: &'a str, cfg_bin: Option<&'a str>) -> Result<Value<'a>> {
        let cfg_bin_name = cfg_bin_name(cfg_bin).to_string();
        let (cfg, fingerprint) = load_config(rec, &cfg_bin_name)?;
        let rings = if cfg.has_archives() { cfg.archives().len() } else { 1 };
        let mut total_bytes = 0i64;
        let mut present = 0i64;

        /// One ring's facts, gathered before any host value is built.
        struct Row {
            shape: Shape,
            bin: String,
            state: &'static str,
            error: Option<String>,
            appends: i64,
            resets: u32,
            saturated: bool,
            window: Option<(i64, i64)>,
        }

        // The zero-copy read borrows the record mutably, and a host `Map` borrows it immutably, so the two cannot be
        // interleaved. Gathering first is not a workaround: it is the borrow checker pointing out that building results
        // while mutating the record is what would need justifying.
        let mut rows: Vec<Row> = Vec::with_capacity(rings);

        for i in 0..rings {
            let shape = if cfg.has_archives() {
                Shape::archive(&cfg, fingerprint, i)?
            } else {
                Shape::base(&cfg, fingerprint)
            };

            let name = shape.bin_name(bin);

            total_bytes += shape.ring_len() as i64;

            // Read-only, through the reference path: no particle is materialised for any of these.
            let found = rec.with_bytes_ref(&name, |bytes| {
                RingView::open(bytes, &shape)
                    .map(|ring| (ring.window(), ring.appends(), ring.resets(), ring.saturated()))
            })?;

            let mut row = Row {
                shape,
                bin: name,
                state: "missing",
                error: None,
                appends: 0,
                resets: 0,
                saturated: false,
                window: None,
            };

            match found {
                None => {}
                // One unreadable ring should not hide the other six, so it is reported in place.
                Some(Err(e)) => {
                    row.state = "unreadable";
                    row.error = Some(format!("{e}"));
                }
                Some(Ok((window, appends, resets, saturated))) => {
                    present += 1;

                    row.state = if window.is_some() { "ok" } else { "empty" };
                    row.appends = appends;
                    row.resets = resets;
                    row.saturated = saturated;
                    row.window = window;
                }
            }

            rows.push(row);
        }

        let table = rec.ctx().list(rings as u32)?;

        for (i, row) in rows.iter().enumerate() {
            let shape = &row.shape;
            let entry = rec.ctx().map(14)?;

            entry.put_str_int("index", i as i64)?;
            entry.put_str_str("bin", &row.bin)?;
            entry.put_str_int("slots", shape.slots())?;
            entry.put_str_str("agg", shape.agg().name())?;
            entry.put_str_int("bytes", shape.ring_len() as i64)?;
            entry.put_str_int("span_ms", shape.approx_span_ms())?;
            entry.put_str_int("step_ms", shape.approx_step_ms())?;
            entry.put_str_str("state", row.state)?;

            if let Some(r) = shape.resolution() {
                entry.put_str_str("resolution", r.name())?;
            }

            if shape.xff_pct() > 0 {
                entry.put_str_int("xff", i64::from(shape.xff_pct()))?;
                entry.put_str_int(
                    "xff_needs_samples",
                    shape.expected_samples() * i64::from(shape.xff_pct()) / 100,
                )?;
            }

            if let Some(e) = &row.error {
                entry.put_str_str("error", e)?;
            }

            if row.state == "ok" || row.state == "empty" {
                entry.put_str_int("appends", row.appends)?;
            }

            if row.resets > 0 {
                entry.put_str_int("counter_resets", i64::from(row.resets))?;
            }

            if row.saturated {
                entry.put_str_int("saturated", 1)?;
            }

            if let Some((first, last)) = row.window {
                entry.put_str_int("from_ms", shape.bucket_start_ms(first))?;
                entry.put_str_int("to_ms", shape.bucket_start_ms(last))?;
            }

            table.push_map(&entry)?;
        }

        let map = rec.ctx().map(14)?;

        put_shape(&map, &Shape::base(&cfg, fingerprint))?;
        map.put_str_str("bin", bin)?;
        map.put_str_str("config_bin", &cfg_bin_name)?;
        map.put_str_int("created_ms", cfg.created_ms)?;
        map.put_str_int("archives", cfg.archives().len() as i64)?;
        map.put_str_int("rings_present", present)?;
        map.put_str_int("ring_bytes", total_bytes)?;
        map.put_str_int("allow_backfill", i64::from(cfg.allow_backfill))?;
        map.put_str_int("allow_wipe", i64::from(cfg.allow_wipe))?;

        if cfg.wrap_bits > 0 {
            map.put_str_int("wrap_bits", i64::from(cfg.wrap_bits))?;
        }

        if cfg.heartbeat_ms > 0 {
            map.put_str_int("heartbeat_ms", cfg.heartbeat_ms)?;
        }

        map.put_str_list("table", &table)?;
        map.put_str_str("result", "ok")?;

        Ok(map.as_value())
    }
}

udfs! {
    /// Read `n` blob bins the copying way and reduce each: `get_bytes` materialises the particle into an as_val first.
    ///
    /// Paired with `ts_probe_ref`, which does **identical arithmetic** over the same bytes and differs only in how it
    /// obtained them.
    ///
    /// # Why many bins rather than one bin many times
    ///
    /// `get_bytes` caches the as_val it materialises, so a second read of the *same* bin is free: the copy happens
    /// once per UDF call, not once per call to `get_bytes`. Looping over one bin would therefore measure one copy
    /// amortised over the loop, which is to say nothing.
    ///
    /// Reading `n` distinct bins gives `n` copies inside a single round trip, so the ~65 µs the query and dispatch
    /// cost is divided across all of them instead of dwarfing one. A first attempt compared a single 512 KB read and
    /// produced 1.81x, 0.75x and 1.12x across three sizes - one of them *negative*, which is impossible when the only
    /// difference is a removed memcpy, and a clear sign the round trip was swamping the effect.
    fn ts_probe_copy<'a>(rec: &'a Record, prefix: &'a str, n: i64) -> Result<i64> {
        let mut total = 0i64;

        for i in 0..n.max(1) {
            let bin = format!("{prefix}{i}");
            let bytes = rec
                .get_bytes(&bin)?
                .ok_or_else(|| Error::msg(format!("no bin '{bin}'")))?;

            total = total.wrapping_add(count_known(bytes));
        }

        Ok(total)
    }

    /// The same reads with no copy at all.
    fn ts_probe_ref<'a>(rec: &'a mut Record, prefix: &'a str, n: i64) -> Result<i64> {
        let mut total = 0i64;

        for i in 0..n.max(1) {
            let bin = format!("{prefix}{i}");

            match rec.with_bytes_ref(&bin, count_known)? {
                Some(c) => total = total.wrapping_add(c),
                // Say so rather than quietly reporting the slow path's number as the fast one's - that would make the
                // whole benchmark a measurement of nothing.
                None => {
                    return Err(Error::msg(format!(
                        "the zero-copy path did not apply to bin '{bin}'"
                    )))
                }
            }
        }

        Ok(total)
    }

    /// Sum a **list** bin through the native ABI.
    ///
    /// `rec.get` on a list bin makes the host call `as_bin_particle_to_asval`, which deserialises the whole packed
    /// list into an `as_arraylist` - one heap allocation per element plus the list itself. `doubles_into` then does a
    /// bulk copy out of that. So the allocation count is O(elements) before this function sees a single number.
    ///
    /// Paired with `ts_probe_blob_sum`, which sums the same values held as a blob and allocates nothing.
    fn ts_probe_list_sum<'a>(rec: &'a Record, bin: &'a str) -> Result<i64> {
        let list = rec
            .get(bin)?
            .as_list()
            .ok_or_else(|| Error::msg("that bin does not hold a list"))?;

        let n = list.len() as usize;
        let mut values = vec![0.0f64; n];
        let got = list.doubles_into(0, &mut values)?;
        let mut total = 0.0f64;

        for &v in &values[..got] {
            total += v;
        }

        Ok(total as i64)
    }

    /// The same sum over the same values, held as a blob and read with no copy and no allocation.
    fn ts_probe_blob_sum<'a>(rec: &'a mut Record, bin: &'a str) -> Result<i64> {
        let total = rec
            .with_bytes_ref(bin, |bytes| {
                let mut total = 0.0f64;

                for chunk in bytes.chunks_exact(8) {
                    let mut b = [0u8; 8];

                    b.copy_from_slice(chunk);
                    total += f64::from_le_bytes(b);
                }

                total
            })?
            .ok_or_else(|| Error::msg("the zero-copy path did not apply"))?;

        Ok(total as i64)
    }

    /// Sum a list bin through the **packed cursor** — no deserialisation, no allocation.
    ///
    /// The third point of the comparison: `ts_probe_list_sum` pays one heap allocation per element,
    /// `ts_probe_blob_sum` is the zero-allocation floor with the values held as a blob, and this reads the *same list
    /// bin* the first one reads while allocating as little as the second.
    fn ts_probe_list_cursor<'a>(rec: &'a Record, bin: &'a str) -> Result<i64> {
        let n = rec
            .list_len(bin)?
            .ok_or_else(|| Error::msg("the packed cursor did not apply to that bin"))? as usize;

        let mut values = vec![0.0f64; n];
        let got = rec
            .list_doubles(bin, 0, &mut values)?
            .ok_or_else(|| Error::msg("the packed cursor did not apply to that bin"))?;

        let mut total = 0.0f64;

        for &v in &values[..got] {
            total += v;
        }

        Ok(total as i64)
    }

    /// Look `k` string keys up in a map bin the as_val way: the host deserialises every entry first.
    fn ts_probe_map_sum<'a>(rec: &'a Record, bin: &'a str, k: i64) -> Result<i64> {
        let map = rec
            .get(bin)?
            .as_map()
            .ok_or_else(|| Error::msg("that bin does not hold a map"))?;

        let mut total = 0i64;

        for i in 0..k.max(1) {
            let key = format!("k{i}");

            total += map.get_str(&key).as_i64().unwrap_or(0);
        }

        Ok(total)
    }

    /// The same lookups through the **packed cursor** — nothing deserialised, nothing allocated.
    fn ts_probe_map_cursor<'a>(rec: &'a Record, bin: &'a str, k: i64) -> Result<i64> {
        let mut total = 0i64;

        for i in 0..k.max(1) {
            let key = format!("k{i}");

            match rec.map_get_str(bin, &key)? {
                Some(v) => total += v.as_i64().unwrap_or(0),
                None => return Err(Error::msg(format!("the packed cursor missed key '{key}'"))),
            }
        }

        Ok(total)
    }

    /// Entry count through the cursor, so a test can check it against what the client wrote.
    fn ts_probe_map_len<'a>(rec: &'a Record, bin: &'a str) -> Result<i64> {
        rec.map_len(bin)?
            .map(i64::from)
            .ok_or_else(|| Error::msg("the packed cursor did not apply to that bin"))
    }

    /// Build an `n`-element list result: one as_val per element, then serialised into the response.
    ///
    /// Paired with `ts_probe_build_none`, which runs the same loop and the same arithmetic but keeps the numbers in a
    /// local and returns a scalar. The difference is the cost of *constructing* the result - which is the part an arena
    /// would target.
    fn ts_probe_build_list<'a>(rec: &'a Record, n: i64) -> Result<Value<'a>> {
        let n = n.max(0) as u32;
        let list = rec.ctx().list(n)?;

        for i in 0..n {
            list.push_int(i64::from(i))?;
        }

        Ok(list.as_value())
    }

    /// The same loop, building nothing.
    fn ts_probe_build_none<'a>(rec: &'a Record, n: i64) -> Result<i64> {
        let _ = rec;

        let mut total = 0i64;

        for i in 0..n.max(0) {
            total = total.wrapping_add(i);
        }

        Ok(total)
    }

    /// Write an `n`-element list to a bin the ordinary way: one as_val per element, staged, committed.
    fn ts_probe_wlist_asval<'a>(rec: &'a Record, bin: &'a str, n: i64) -> Result<i64> {
        let n = n.max(0) as u32;
        let list = rec.ctx().list(n)?;

        for i in 0..n {
            list.push_int(i64::from(i))?;
        }

        rec.set(bin, &list.as_value())?;
        rec.write()?;

        Ok(i64::from(n))
    }

    /// The same write through the **packed writer** — no intermediate values, one pass, applied immediately.
    fn ts_probe_wlist_packed<'a>(rec: &'a mut Record, bin: &'a str, n: i64) -> Result<i64> {
        let n = n.max(0) as usize;
        let values: Vec<i64> = (0..n as i64).collect();

        rec.set_list_ints(bin, &values)?;

        Ok(n as i64)
    }

    /// Walk a nested document with the **cursor**, summing every integer at any depth.
    ///
    /// Paired with `ts_probe_tree_asval`, which sums the same integers through `rec.get` - so the two must agree, and
    /// the difference between them is the deserialisation the cursor skips.
    fn ts_probe_tree_cursor<'a>(rec: &'a Record, bin: &'a str) -> Result<i64> {
        let cursor = rec
            .cursor(bin)?
            .ok_or_else(|| Error::msg("the cursor did not apply to that bin"))?;

        Ok(cursor_sum(cursor, 0))
    }

    /// The same sum through the as_val tree.
    fn ts_probe_tree_asval<'a>(rec: &'a Record, bin: &'a str) -> Result<i64> {
        Ok(value_sum(&rec.get(bin)?))
    }

    /// Increment an integer bin the staged way: read, add, stage, commit.
    fn ts_probe_incr_staged<'a>(rec: &'a Record, bin: &'a str, delta: i64) -> Result<i64> {
        let now = rec.get_int(bin)?.unwrap_or(0) + delta;

        rec.set_int(bin, now)?;
        rec.write()?;

        Ok(now)
    }

    /// The same increment as one immediate op.
    fn ts_probe_incr_op<'a>(rec: &'a mut Record, bin: &'a str, delta: i64) -> Result<i64> {
        rec.op_incr(bin, delta)?;

        Ok(delta)
    }

    /// Append to a blob bin the staged way: read it all, copy, append, stage, commit.
    fn ts_probe_append_staged<'a>(rec: &'a Record, bin: &'a str, n: i64) -> Result<i64> {
        let mut buf = rec.get_bytes(bin)?.unwrap_or(&[]).to_vec();

        buf.extend(core::iter::repeat(0xABu8).take(n.max(0) as usize));

        let len = buf.len() as i64;

        rec.set_blob(bin, &buf)?;
        rec.write()?;

        Ok(len)
    }

    /// The same append as one immediate op - the existing value is never read.
    fn ts_probe_append_op<'a>(rec: &'a mut Record, bin: &'a str, n: i64) -> Result<i64> {
        let tail = vec![0xABu8; n.max(0) as usize];

        rec.op_append_blob(bin, &tail)?;

        Ok(n)
    }
}

/// Count the slots holding a value. The same walk `ts_stats` does, so the probes measure a realistic read.
fn count_known(bytes: &[u8]) -> i64 {
    if bytes.len() <= ring::HEADER {
        return 0;
    }

    let mut n = 0i64;

    for chunk in bytes[ring::HEADER..].chunks_exact(8) {
        let mut b = [0u8; 8];

        b.copy_from_slice(chunk);

        if !f64::from_le_bytes(b).is_nan() {
            n += 1;
        }
    }

    n
}


/// Sum every integer a cursor can reach, descending into nested containers.
///
/// Depth-limited rather than trusting the document: a cursor borrows bytes another process wrote, and a hostile or
/// corrupt one could nest far enough to exhaust the stack. The same reasoning as clamping a HyperLogLog register.
fn cursor_sum(mut cursor: aerospike_udf_sdk_native::PackedCursor<'_>, depth: u32) -> i64 {
    if depth > 32 {
        return 0;
    }

    let mut total = 0i64;

    while let Some((value, child)) = cursor.next() {
        match child {
            Some(nested) => total = total.wrapping_add(cursor_sum(nested, depth + 1)),
            None => {
                if let Some(v) = value.as_i64() {
                    total = total.wrapping_add(v);
                }
            }
        }
    }

    total
}

/// The same walk over a materialised value.
fn value_sum(value: &Value<'_>) -> i64 {
    match value.kind() {
        ValueKind::Int => value.as_i64().unwrap_or(0),
        ValueKind::List => {
            let mut total = 0i64;

            if let Some(list) = value.as_list() {
                list.for_each(|_, item| {
                    total = total.wrapping_add(value_sum(&item));

                    true
                });
            }

            total
        }
        ValueKind::Map => {
            let mut total = 0i64;

            if let Some(map) = value.as_map() {
                map.for_each(|k, v| {
                    total = total.wrapping_add(value_sum(&k)).wrapping_add(value_sum(&v));

                    true
                });
            }

            total
        }
        _ => 0,
    }
}

fn outcome_index(outcome: Outcome) -> usize {
    match outcome {
        Outcome::Placed => 0,
        Outcome::Combined => 1,
        Outcome::Backfilled => 2,
        Outcome::TooOld => 3,
        Outcome::BackfillRefused => 4,
        Outcome::WouldWipe => 5,
    }
}

module! {
    name: "timeseries",
    version: "1.0.0",
    functions: [
        ts_init, ts_append, ts_append_all, ts_append_many, ts_report, ts_stats, ts_info,
        ts_probe_copy, ts_probe_ref, ts_probe_incr_staged, ts_probe_incr_op,
        ts_probe_append_staged, ts_probe_append_op,
        ts_probe_list_sum, ts_probe_blob_sum, ts_probe_list_cursor,
        ts_probe_map_sum, ts_probe_map_cursor, ts_probe_map_len,
        ts_probe_build_list, ts_probe_build_none,
        ts_probe_wlist_asval, ts_probe_wlist_packed,
        ts_probe_tree_cursor, ts_probe_tree_asval,
    ],
    aggregations: [rollup::Rollup],
}

/// Re-exported so a client building a `Combiner` can name it without reaching into the module.
pub use rollup::Rollup;

const _: () = {
    // A ring's header has to stay a multiple of eight, or slot offsets stop being clean multiples of the sample width.
    assert!(ring::HEADER % 8 == 0);
    assert!(config::LEN % 8 == 0);
};

/// The name the aggregation registers under, for callers building a `Statement`.
pub const ROLLUP_NAME: &CStr = c"ts_rollup";

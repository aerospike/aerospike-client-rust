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

//! The series shape — in its own bin, in forty fixed bytes.
//!
//! # Why not a map bin
//!
//! Because a map is msgpack, and msgpack is parsed. Every read of a `{step: …, slots: …}` map walks a type byte, a
//! length, a key string and a value for each entry, allocates, and hands back something that has to be looked up by
//! name. This is eight fields at known offsets: reading `step_ms` is `i64::from_le_bytes` at offset 8, and the whole
//! config is one 40-byte `memcpy` that the host already did.
//!
//! # Why its own bin rather than a header on the ring
//!
//! Because a client can then read the shape **without pulling the ring**. `operate()` fetching one 40-byte bin is a
//! different transaction from fetching half a megabyte, and discovering what a series looks like should not cost the
//! series. Inside a UDF the whole record is resident either way, so the benefit is entirely on the client side — worth
//! saying plainly rather than implying the split buys something it does not.
//!
//! One config serves **every ring in the record**, so `cpu`, `mem` and `net` can share a shape while each keeps its
//! own position. A record is a series, or a few parallel ones sampled together.
//!
//! # Why both bins are safe to keep in step
//!
//! A UDF runs under the record lock, so the config read and the ring write are one transaction. Nothing can observe a
//! half-updated pair, and nothing can slip between them.
//!
//! What *can* happen is a client rewriting the config bin directly, which would silently reinterpret every existing
//! ring. So a ring stores a fingerprint of the config it was built against and refuses to open against a different
//! one — see [`Config::fingerprint`].

use aerospike_udf_sdk_native::{Error, Result};

/// Marks the bin as a config rather than something else that happens to be 40 bytes.
pub const MAGIC: [u8; 4] = *b"RRC1";

/// The version this build writes. A `version = 1` config is still read, and means "no archives".
pub const VERSION: u8 = 2;

/// The version that had no archive table.
pub const VERSION_V1: u8 = 1;

/// Bytes per archive descriptor, appended after the fixed header in a v2 config.
pub const ARCHIVE_LEN: usize = 16;

/// The most archives one config may describe. Seven is the full set of resolutions; the cap exists because
/// `n_archives` arrives from a bin.
pub const MAX_ARCHIVES: usize = 7;

/// The encoded length. Fixed, so every offset below is a constant.
pub const LEN: usize = 40;

/// Where the config lives unless a caller says otherwise.
pub const DEFAULT_BIN: &str = "rrcfg";

/// The largest ring this will build: 65,536 slots is 512 KB of samples plus a header.
///
/// A ceiling rather than a suggestion, because `slots` arrives as an argument. An unclamped one used to be able to
/// drive an allocation straight off a caller's integer — the same shape of bug as the unclamped `k` in the summary
/// example's `top_values`. The number itself is chosen against a 1 MB write block: a ring has to leave room for the
/// rest of the record.
pub const MAX_SLOTS: i64 = 65_536;

/// How two samples landing in the same step combine.
///
/// Every one of these falls out of a single "is this slot unknown?" check, so the set costs one match arm each. There
/// is deliberately no `Mean`: it needs a count *per slot*, which doubles the stored ring. `Sum` alongside a second
/// ring configured `Count` gives the same answer with the cost visible in the record rather than hidden in the layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    /// The newest sample wins. The right default for a gauge.
    Last,
    /// The first sample in the step wins and later ones are ignored.
    First,
    Min,
    Max,
    /// Adds. The right choice for something already a delta, and for a counter.
    Sum,
    /// Counts samples and ignores their values.
    Count,
    /// The arithmetic mean of the samples in the bucket.
    ///
    /// **The one aggregation that needs a wider slot**: a mean cannot be folded into a single running number, so a
    /// `Mean` slot is sixteen bytes - a running `sum` and a `count` - where every other aggregation is eight.
    ///
    /// It was deliberately absent while there was only one ring, because doubling the *stored* ring to carry a count
    /// was not worth it. Archives change that: a coarse archive is small, RRDtool's default consolidation is AVERAGE,
    /// and the count doubles as the input to [`Archive::xff`].
    Mean,
}

impl Agg {
    pub fn as_u8(self) -> u8 {
        match self {
            Agg::Last => 0,
            Agg::First => 1,
            Agg::Min => 2,
            Agg::Max => 3,
            Agg::Sum => 4,
            Agg::Count => 5,
            Agg::Mean => 6,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Agg::Last,
            1 => Agg::First,
            2 => Agg::Min,
            3 => Agg::Max,
            4 => Agg::Sum,
            5 => Agg::Count,
            6 => Agg::Mean,
            other => return Err(Error::msg(format!("unknown aggregation code {other}"))),
        })
    }

    /// Parse the name a caller passes to `ts_init`.
    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name {
            "last" => Agg::Last,
            "first" => Agg::First,
            "min" => Agg::Min,
            "max" => Agg::Max,
            "sum" => Agg::Sum,
            "count" => Agg::Count,
            "mean" | "avg" | "average" => Agg::Mean,
            other => {
                return Err(Error::msg(format!(
                    "unknown aggregation '{other}' - one of last, first, min, max, sum, count"
                )))
            }
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Agg::Last => "last",
            Agg::First => "first",
            Agg::Min => "min",
            Agg::Max => "max",
            Agg::Sum => "sum",
            Agg::Count => "count",
            Agg::Mean => "mean",
        }
    }

    /// How to combine two *already aggregated* buckets, which is not always the same operation.
    ///
    /// Downsampling a `Count` series means **summing** the counts, not counting them again: two buckets holding 3 and
    /// 4 samples become one holding 7. Every other mode combines with itself.
    pub fn downsample_with(self) -> Self {
        match self {
            Agg::Count => Agg::Sum,
            // A mean of means is not the mean, but a `Mean` slot carries its count - so combining two of them adds
            // both sums and both counts and stays exact. `combine` handles that; this only says which rule applies.
            other => other,
        }
    }

    /// Bytes one slot needs. Sixteen for a mean, eight for everything else.
    pub fn slot_width(self) -> usize {
        match self {
            Agg::Mean => 16,
            _ => 8,
        }
    }
}

/// The width of one archive's buckets.
///
/// **An enum rather than a `step_ms`, because two of these are not durations.** Second through week are fixed multiples
/// of a millisecond and bucket by division; month and year are calendar units and bucket through [`crate::calendar`].
/// Carrying them as a step in milliseconds would mean approximating a month as 30 days, which drifts through the
/// calendar until a bucket labelled March covers most of April.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Resolution {
    Second,
    Minute,
    Hour,
    Day,
    /// Monday-based. Epoch weeks start on a Thursday, which is almost never what anyone means.
    Week,
    /// A calendar month, 28 to 31 days.
    Month,
    /// A calendar year, 365 or 366 days.
    Year,
}

impl Resolution {
    /// Every resolution, finest first. The default archive set is exactly this.
    pub const ALL: [Resolution; 7] = [
        Resolution::Second,
        Resolution::Minute,
        Resolution::Hour,
        Resolution::Day,
        Resolution::Week,
        Resolution::Month,
        Resolution::Year,
    ];

    pub fn as_u8(self) -> u8 {
        match self {
            Resolution::Second => 0,
            Resolution::Minute => 1,
            Resolution::Hour => 2,
            Resolution::Day => 3,
            Resolution::Week => 4,
            Resolution::Month => 5,
            Resolution::Year => 6,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Resolution::Second,
            1 => Resolution::Minute,
            2 => Resolution::Hour,
            3 => Resolution::Day,
            4 => Resolution::Week,
            5 => Resolution::Month,
            6 => Resolution::Year,
            other => return Err(Error::msg(format!("unknown resolution code {other}"))),
        })
    }

    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name {
            "s" | "sec" | "second" => Resolution::Second,
            "m" | "min" | "minute" => Resolution::Minute,
            "h" | "hour" => Resolution::Hour,
            "d" | "day" => Resolution::Day,
            "w" | "week" => Resolution::Week,
            "mo" | "month" => Resolution::Month,
            "y" | "year" => Resolution::Year,
            other => {
                return Err(Error::msg(format!(
                    "unknown resolution '{other}' - one of second, minute, hour, day, week, month, year"
                )))
            }
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Resolution::Second => "second",
            Resolution::Minute => "minute",
            Resolution::Hour => "hour",
            Resolution::Day => "day",
            Resolution::Week => "week",
            Resolution::Month => "month",
            Resolution::Year => "year",
        }
    }

    /// The bin-name suffix for this resolution's archive: `cpu~h` is the hourly archive of `cpu`.
    ///
    /// **Deliberately the resolution rather than the archive's index.** An index would be shorter, and it would also
    /// mean `cpu~2` names the hourly archive in one record and the daily one in a record whose config declares fewer
    /// archives — the same bin name meaning two things. A resolution names itself.
    ///
    /// Every one of these is a string [`Resolution::parse`] already accepts, so the two directions cannot drift into
    /// separate tables.
    pub fn suffix(self) -> &'static str {
        match self {
            Resolution::Second => "s",
            Resolution::Minute => "m",
            Resolution::Hour => "h",
            Resolution::Day => "d",
            Resolution::Week => "w",
            // Not "m": that is the minute, and a bin name is not the place to discover a collision.
            Resolution::Month => "mo",
            Resolution::Year => "y",
        }
    }

    /// Milliseconds one bucket spans, or `None` for the calendar units.
    ///
    /// `None` is the whole reason this is an enum: a caller that wants a duration for a month has to be told there
    /// isn't one, rather than handed 30 days.
    pub fn step_ms(self) -> Option<i64> {
        use crate::calendar::*;

        Some(match self {
            Resolution::Second => MS_PER_SEC,
            Resolution::Minute => MS_PER_MIN,
            Resolution::Hour => MS_PER_HOUR,
            Resolution::Day => MS_PER_DAY,
            Resolution::Week => MS_PER_WEEK,
            Resolution::Month | Resolution::Year => return None,
        })
    }

    /// The bucket a timestamp falls in.
    pub fn bucket_of(self, ms: i64) -> i64 {
        use crate::calendar;

        match self {
            Resolution::Week => calendar::week_bucket(ms),
            Resolution::Month => calendar::month_bucket(ms),
            Resolution::Year => calendar::year_bucket(ms),
            // Floored, so a pre-1970 timestamp does not land in the epoch's own bucket.
            other => ms.div_euclid(other.step_ms().unwrap_or(1)),
        }
    }

    /// The first millisecond of a bucket.
    pub fn bucket_start_ms(self, bucket: i64) -> i64 {
        use crate::calendar;

        match self {
            Resolution::Week => calendar::week_bucket_start_ms(bucket),
            Resolution::Month => calendar::month_bucket_start_ms(bucket),
            Resolution::Year => calendar::year_bucket_start_ms(bucket),
            other => bucket.saturating_mul(other.step_ms().unwrap_or(1)),
        }
    }

    /// Roughly how long `slots` of this resolution covers, for reporting and for archive selection.
    ///
    /// Approximate for month and year on purpose - it answers "does this archive reach back far enough", where 30.44
    /// days a month is fine. Nothing *stores* against this number.
    pub fn approx_span_ms(self, slots: i64) -> i64 {
        use crate::calendar::*;

        let each = match self {
            Resolution::Month => 2_629_746_000, // 30.44 days
            Resolution::Year => 31_556_952_000, // 365.24 days
            other => other.step_ms().unwrap_or(MS_PER_SEC),
        };

        each.saturating_mul(slots)
    }
}

/// How a sample is stored in a slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Eight bytes of IEEE double. `NaN` means unknown, which is free and unambiguous.
    F64,
    /// Eight bytes of signed integer. `i64::MIN` means unknown.
    ///
    /// Here because an integer counter laundered through an `f64` loses its low bits above 2⁵³ — the defect the
    /// summary example's two-accumulator sum exists to avoid. A byte counter belongs in an `i64` ring.
    I64,
}

impl Kind {
    pub fn as_u8(self) -> u8 {
        match self {
            Kind::F64 => 0,
            Kind::I64 => 1,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Kind::F64,
            1 => Kind::I64,
            other => return Err(Error::msg(format!("unknown sample kind code {other}"))),
        })
    }

    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name {
            "f64" | "double" | "float" => Kind::F64,
            "i64" | "int" | "integer" => Kind::I64,
            other => {
                return Err(Error::msg(format!("unknown sample kind '{other}' - f64 or i64")))
            }
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::F64 => "f64",
            Kind::I64 => "i64",
        }
    }
}

/// What an appended number *means*.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The number is the value. Store it.
    Gauge,
    /// The number is a running total, and the slot stores the **difference** from the previous reading.
    ///
    /// So the first append of a counter stores *unknown*: one reading is not a rate. A decrease means the counter
    /// reset or wrapped, and which of those it was cannot be known from the numbers alone — see
    /// [`Config::wrap_bits`].
    Counter,
}

impl Mode {
    pub fn as_u8(self) -> u8 {
        match self {
            Mode::Gauge => 0,
            Mode::Counter => 1,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Mode::Gauge,
            1 => Mode::Counter,
            other => return Err(Error::msg(format!("unknown mode code {other}"))),
        })
    }

    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name {
            "gauge" => Mode::Gauge,
            "counter" => Mode::Counter,
            other => return Err(Error::msg(format!("unknown mode '{other}' - gauge or counter"))),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Mode::Gauge => "gauge",
            Mode::Counter => "counter",
        }
    }
}

pub const FLAG_ALLOW_BACKFILL: u8 = 1 << 0;

/// Permission to perform an append that erases the entire window. Off by default — see [`Config::allow_wipe`].
pub const FLAG_ALLOW_WIPE: u8 = 1 << 1;

/// One archive: a resolution, how many buckets of it to keep, and how samples inside a bucket combine.
///
/// **The archive hierarchy is what makes long retention possible at all.** A single ring is capped at `MAX_SLOTS`, so a
/// one-second series reaches 18 hours and a one-minute series 45 days — a year is simply unreachable. Seven archives at
/// coarsening resolution reach 25 years in 60 KiB.
///
/// # Consolidation needs no accumulator here
///
/// RRDtool keeps a consolidated-data-point accumulator per archive, because its archives do not self-align. Ours do: a
/// bucket is derived from the timestamp and the resolution alone, so an archive stepped at one hour has buckets exactly
/// 3,600 of the second archive's wide and aligned to them, for free. Consolidation is therefore just *appending the
/// same sample to every archive* — each one's own `agg` does the rest, with no extra state.
///
/// Better than RRDtool's in one respect: because every archive sees the raw samples, a coarse mean is the true mean of
/// the samples rather than a mean of means, which is biased whenever sample density varies.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Archive {
    pub resolution: Resolution,
    pub agg: Agg,
    /// Buckets kept. `1..=MAX_SLOTS`.
    pub slots: i64,
    /// RRDtool's xfiles factor, as a percentage of a bucket's samples that must be known for it to report a value.
    ///
    /// Zero means report whatever is there. Above zero, a bucket whose sample count falls below this share of what the
    /// resolution *could* have held reads as unknown — so a yearly average built from two readings during an outage
    /// does not look as authoritative as one built from a year of them.
    pub xff_pct: u8,
}

impl Archive {
    /// A placeholder for the unused tail of [`Config::archive_table`]. Never validated, never read.
    pub const EMPTY: Self = Self {
        resolution: Resolution::Second,
        agg: Agg::Last,
        slots: 0,
        xff_pct: 0,
    };

    pub fn new(resolution: Resolution, agg: Agg, slots: i64) -> Self {
        Self { resolution, agg, slots, xff_pct: 0 }
    }

    /// Bytes this archive's ring occupies, header included.
    pub fn ring_len(&self) -> usize {
        crate::ring::HEADER + (self.slots.max(0) as usize) * self.agg.slot_width()
    }

    /// Roughly how far back this archive reaches.
    pub fn approx_span_ms(&self) -> i64 {
        self.resolution.approx_span_ms(self.slots)
    }

    pub fn validate(&self) -> Result<()> {
        if self.slots < 1 || self.slots > MAX_SLOTS {
            return Err(Error::msg(format!(
                "archive {} slots must be between 1 and {MAX_SLOTS}, got {}",
                self.resolution.name(),
                self.slots
            )));
        }

        if self.xff_pct > 100 {
            return Err(Error::msg(format!(
                "archive {} xff must be a percentage, got {}",
                self.resolution.name(),
                self.xff_pct
            )));
        }

        Ok(())
    }

    fn encode_into(&self, out: &mut [u8]) {
        out[0] = self.resolution.as_u8();
        out[1] = self.agg.as_u8();
        out[2] = self.xff_pct;
        out[3] = 0; // reserved
        out[4..12].copy_from_slice(&self.slots.to_le_bytes());
        // 12..16 reserved
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let a = Self {
            resolution: Resolution::from_u8(bytes[0])?,
            agg: Agg::from_u8(bytes[1])?,
            xff_pct: bytes[2],
            slots: le_i64(bytes, 4),
        };

        a.validate()?;

        Ok(a)
    }

    /// Parse one `resolution:slots[:agg[:xff]]` descriptor, as `ts_init`'s `archives=` option carries it.
    ///
    /// `agg` defaults to `mean`, which is RRDtool's default consolidation and the right one for every archive coarser
    /// than the sampling interval. `xff` defaults to 0, meaning report whatever is there.
    pub fn parse(spec: &str) -> Result<Self> {
        let mut parts = spec.split(':');

        let resolution = match parts.next().map(str::trim) {
            Some(r) if !r.is_empty() => Resolution::parse(r)?,
            _ => {
                return Err(Error::msg(format!(
                    "archive '{spec}' needs a resolution - try 'h:336:mean'"
                )))
            }
        };

        let slots = match parts.next().map(str::trim) {
            Some(s) if !s.is_empty() => s.parse::<i64>().map_err(|_| {
                Error::msg(format!("archive '{spec}' slot count '{s}' is not a number"))
            })?,
            _ => {
                return Err(Error::msg(format!(
                    "archive '{spec}' needs a slot count - try '{}:336'",
                    resolution.suffix()
                )))
            }
        };

        let agg = match parts.next().map(str::trim) {
            Some(a) if !a.is_empty() => Agg::parse(a)?,
            _ => Agg::Mean,
        };

        let xff_pct = match parts.next().map(str::trim) {
            Some(x) if !x.is_empty() => x
                .parse::<u8>()
                .map_err(|_| Error::msg(format!("archive '{spec}' xff '{x}' is not a percentage")))?,
            _ => 0,
        };

        if parts.next().is_some() {
            return Err(Error::msg(format!(
                "archive '{spec}' has more than the four fields resolution:slots:agg:xff"
            )));
        }

        let archive = Self { resolution, agg, slots, xff_pct };

        archive.validate()?;

        Ok(archive)
    }

    /// Parse a whole comma-free archive list: `s:3600:last;m:1440;h:336`.
    ///
    /// **Semicolons, not commas**, because the surrounding options string is already comma-separated. A nested comma
    /// would need quoting or escaping, and an options parser that needs escaping is one people get wrong.
    pub fn parse_list(spec: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();

        for part in spec.split(';') {
            let part = part.trim();

            if part.is_empty() {
                continue;
            }

            out.push(Self::parse(part)?);
        }

        if out.is_empty() {
            return Err(Error::msg(format!(
                "archives '{spec}' describes none - use 'default', 'none', or 'h:336;d:100'"
            )));
        }

        Ok(out)
    }
}

/// The longest a bin name may be. An Aerospike limit, not this example's.
pub const BIN_NAME_LIMIT: usize = 15;

/// Separates a series' base name from its archive's resolution: `cpu` becomes `cpu~h`.
pub const ARCHIVE_SEP: char = '~';

/// The bin an archive's ring lives in.
///
/// One bin per archive rather than one bin holding all of them, because the alternative is worse in both directions: a
/// client wanting the last hour would pull all 60 KiB, and an append would rewrite all of it rather than the 28 KiB the
/// second archive actually occupies.
pub fn archive_bin(base: &str, resolution: Resolution) -> String {
    format!("{base}{ARCHIVE_SEP}{}", resolution.suffix())
}

/// Whether every archive bin a set would need fits Aerospike's bin-name limit.
///
/// Checked at `ts_init` rather than discovered at the first append, when six of seven rings already exist.
pub fn check_base_name(base: &str, archives: &[Archive]) -> Result<()> {
    if base.is_empty() {
        return Err(Error::msg("a series needs a bin name"));
    }

    if base.contains(ARCHIVE_SEP) {
        return Err(Error::msg(format!(
            "a base bin name cannot contain '{ARCHIVE_SEP}' - that is what separates it from its archive"
        )));
    }

    for a in archives {
        let name = archive_bin(base, a.resolution);

        if name.len() > BIN_NAME_LIMIT {
            return Err(Error::msg(format!(
                "archive bin '{name}' is {} characters and a bin name may be {BIN_NAME_LIMIT} - \
                 with archives the base name has to be {} at most",
                name.len(),
                BIN_NAME_LIMIT - (name.len() - base.len())
            )));
        }
    }

    Ok(())
}

/// The default archive set: every resolution, sized so many metrics fit one record.
///
/// | resolution | slots | covers | agg | bytes |
/// |---|---|---|---|---|
/// | second | 3,600 | 1 hour | last | 28,856 |
/// | minute | 1,440 | 1 day | mean | 23,096 |
/// | hour | 336 | 14 days | mean | 5,432 |
/// | day | 100 | ~3 months | mean | 1,656 |
/// | week | 104 | 2 years | mean | 1,720 |
/// | month | 60 | 5 years | mean | 1,016 |
/// | year | 25 | 25 years | mean | 456 |
/// | | | | | **62,232** |
///
/// **60.8 KiB per metric**, so roughly sixteen metrics share a record under a 1 MB write block.
///
/// The second archive uses `last` rather than `mean` deliberately: a one-second bucket normally holds one sample, where
/// the two are the same number, and `mean` would double the largest archive to gain nothing. Sample faster than once a
/// second and `mean` there becomes worth its 28 KiB.
pub fn default_archives() -> [Archive; MAX_ARCHIVES] {
    [
        Archive::new(Resolution::Second, Agg::Last, 3_600),
        Archive::new(Resolution::Minute, Agg::Mean, 1_440),
        Archive::new(Resolution::Hour, Agg::Mean, 336),
        Archive::new(Resolution::Day, Agg::Mean, 100),
        Archive::new(Resolution::Week, Agg::Mean, 104),
        Archive::new(Resolution::Month, Agg::Mean, 60),
        Archive::new(Resolution::Year, Agg::Mean, 25),
    ]
}

/// The shape of a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Config {
    pub agg: Agg,
    pub kind: Kind,
    pub mode: Mode,
    /// Milliseconds one slot covers. Strictly positive.
    pub step_ms: i64,
    /// How many slots. `1..=MAX_SLOTS`.
    pub slots: i64,
    pub created_ms: i64,
    /// Whether a sample for an *older* live bucket may be merged in after the fact.
    pub allow_backfill: bool,
    /// Whether an append whose gap reaches `slots` — which erases every slot — is permitted.
    ///
    /// **Off by default, and that is the interesting default.** A timestamp in seconds fed to a series stepped in
    /// milliseconds, or the reverse, produces exactly this: one append that wipes the history. Rejecting it leaves
    /// the data intact and the caller able to fix the timestamp; performing it and mentioning it in the result leaves
    /// nothing to fix. A series genuinely sampled less often than its own window is misconfigured, but it can say so
    /// by setting this.
    pub allow_wipe: bool,
    /// Counter width for wraparound reconstruction: 0 (none), 32, or 64.
    ///
    /// A decrease in a counter is either a reset or a wrap, and the numbers alone cannot say which. With a width
    /// declared, a decrease where **both** readings fit that width is treated as a wrap and the delta reconstructed;
    /// anything else is a reset and stores unknown. Ignored for `F64`, where there is no defined wrap.
    pub wrap_bits: u8,
    /// The archive hierarchy, finest first. Read through [`Config::archives`].
    ///
    /// **A fixed array rather than a `Vec`**, for two reasons: `Config` stays `Copy`, which `RingView` relies on to be
    /// `Copy` itself, and a plugin allocates nothing to read a shape. Seven is the whole set of resolutions, so there is
    /// no growth to accommodate.
    pub archive_table: [Archive; MAX_ARCHIVES],
    /// How many of [`Config::archive_table`] are in use. Zero is the single-ring behaviour a v1 config describes.
    pub n_archives: u8,
    /// Longest gap a counter may span before a reading becomes a new baseline instead of a delta. 0 means unlimited.
    ///
    /// RRDtool calls this the heartbeat, and adopting it closes a real hole: without it a counter reading arriving three
    /// hours after the previous one has its whole delta charged to a single bucket, as though that were one interval's
    /// traffic. Defaults to 0 so existing series keep their behaviour exactly.
    pub heartbeat_ms: i64,
}

impl Config {
    /// A gauge with the given step and capacity, `last` wins, `f64` samples.
    pub fn new(step_ms: i64, slots: i64, created_ms: i64) -> Result<Self> {
        let cfg = Self {
            agg: Agg::Last,
            kind: Kind::F64,
            mode: Mode::Gauge,
            step_ms,
            slots,
            created_ms,
            allow_backfill: true,
            allow_wipe: false,
            wrap_bits: 0,
            archive_table: [Archive::EMPTY; MAX_ARCHIVES],
            n_archives: 0,
            heartbeat_ms: 0,
        };

        cfg.validate()?;

        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.step_ms <= 0 {
            return Err(Error::msg("step_ms must be positive"));
        }

        if self.slots < 1 || self.slots > MAX_SLOTS {
            return Err(Error::msg(format!(
                "slots must be between 1 and {MAX_SLOTS}, got {}",
                self.slots
            )));
        }

        // The ring's byte length is `slots * 8`, and it is computed from this. Checking the product rather than
        // trusting the bound above keeps the arithmetic honest if MAX_SLOTS ever moves.
        if self.slots.checked_mul(self.agg.slot_width() as i64).is_none() {
            return Err(Error::msg("slots * slot width overflows"));
        }

        if self.n_archives as usize > MAX_ARCHIVES {
            return Err(Error::msg(format!(
                "at most {MAX_ARCHIVES} archives, got {}",
                self.n_archives
            )));
        }

        // Finest first, and each resolution at most once. Out of order would make archive selection pick the wrong one,
        // and a duplicate would mean two rings competing for the same bin name.
        for pair in self.archives().windows(2) {
            if pair[0].resolution >= pair[1].resolution {
                return Err(Error::msg(format!(
                    "archives must be ordered finest first with no repeats - {} came before {}",
                    pair[0].resolution.name(),
                    pair[1].resolution.name()
                )));
            }
        }

        for a in self.archives() {
            a.validate()?;
        }

        if self.heartbeat_ms < 0 {
            return Err(Error::msg("heartbeat_ms cannot be negative"));
        }

        if !matches!(self.wrap_bits, 0 | 32 | 64) {
            return Err(Error::msg(format!(
                "wrap_bits must be 0, 32 or 64, got {}",
                self.wrap_bits
            )));
        }

        Ok(())
    }

    /// The bytes that go in the bin.
    /// The bytes that go in the bin.
    ///
    /// Variable length now: the fixed header, then one descriptor per archive. A v1 reader would see `version = 2` and
    /// refuse rather than misread the table as padding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![0u8; LEN + self.archives().len() * ARCHIVE_LEN];

        out[0..4].copy_from_slice(&MAGIC);
        out[4] = VERSION;
        out[5] = self.agg.as_u8();
        out[6] = self.kind.as_u8();
        out[7] = self.mode.as_u8();
        out[8..16].copy_from_slice(&self.step_ms.to_le_bytes());
        out[16..24].copy_from_slice(&self.slots.to_le_bytes());
        out[24..32].copy_from_slice(&self.created_ms.to_le_bytes());

        let mut flags = 0u8;

        if self.allow_backfill {
            flags |= FLAG_ALLOW_BACKFILL;
        }

        if self.allow_wipe {
            flags |= FLAG_ALLOW_WIPE;
        }

        out[32] = flags;
        out[33] = self.wrap_bits;
        out[34] = self.n_archives;
        // 35 reserved. A u32 of milliseconds caps a heartbeat at 49 days, which is far past useful.
        out[36..40].copy_from_slice(&(self.heartbeat_ms.clamp(0, u32::MAX as i64) as u32).to_le_bytes());

        for (i, a) in self.archives().iter().enumerate() {
            let at = LEN + i * ARCHIVE_LEN;

            a.encode_into(&mut out[at..at + ARCHIVE_LEN]);
        }

        out
    }

    /// Read a config back, refusing anything that is not one.
    ///
    /// Every field is validated rather than trusted. This is bytes from a bin, which is to say bytes another process
    /// wrote, and the summary example's HyperLogLog registers are the standing reminder of what that means.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < LEN {
            return Err(Error::msg(format!(
                "a config bin is {LEN} bytes, this one is {}",
                bytes.len()
            )));
        }

        if bytes[0..4] != MAGIC {
            return Err(Error::msg(
                "that bin does not hold a time-series config - wrong magic",
            ));
        }

        if bytes[4] != VERSION && bytes[4] != VERSION_V1 {
            return Err(Error::msg(format!(
                "config layout version {}, this build understands {VERSION_V1} and {VERSION}",
                bytes[4]
            )));
        }

        let mut cfg = Self {
            agg: Agg::from_u8(bytes[5])?,
            kind: Kind::from_u8(bytes[6])?,
            mode: Mode::from_u8(bytes[7])?,
            step_ms: le_i64(bytes, 8),
            slots: le_i64(bytes, 16),
            created_ms: le_i64(bytes, 24),
            allow_backfill: bytes[32] & FLAG_ALLOW_BACKFILL != 0,
            allow_wipe: bytes[32] & FLAG_ALLOW_WIPE != 0,
            wrap_bits: bytes[33],
            archive_table: [Archive::EMPTY; MAX_ARCHIVES],
            n_archives: 0,
            heartbeat_ms: 0,
        };

        // A v1 config has no table and no heartbeat - those bytes were reserved and are not guaranteed zero, so they are
        // read only when the version says they mean something.
        if bytes[4] == VERSION {
            let n = bytes[34] as usize;

            if n > MAX_ARCHIVES {
                return Err(Error::msg(format!(
                    "config claims {n} archives, at most {MAX_ARCHIVES} are possible"
                )));
            }

            let need = LEN + n * ARCHIVE_LEN;

            if bytes.len() < need {
                return Err(Error::msg(format!(
                    "config claims {n} archives, which needs {need} bytes, but the bin holds {}",
                    bytes.len()
                )));
            }

            cfg.heartbeat_ms = i64::from(le_u32(bytes, 36));

            for i in 0..n {
                let at = LEN + i * ARCHIVE_LEN;

                cfg.archive_table[i] = Archive::decode(&bytes[at..at + ARCHIVE_LEN])?;
            }

            cfg.n_archives = n as u8;
        }

        cfg.validate()?;

        Ok(cfg)
    }

    /// The archives in use, finest first.
    pub fn archives(&self) -> &[Archive] {
        &self.archive_table[..(self.n_archives as usize).min(MAX_ARCHIVES)]
    }

    /// Replace the archive set. Keeps the table and the count from drifting apart.
    pub fn set_archives(&mut self, archives: &[Archive]) -> Result<()> {
        if archives.len() > MAX_ARCHIVES {
            return Err(Error::msg(format!(
                "at most {MAX_ARCHIVES} archives, got {}",
                archives.len()
            )));
        }

        self.archive_table = [Archive::EMPTY; MAX_ARCHIVES];

        for (slot, a) in self.archive_table.iter_mut().zip(archives) {
            *slot = *a;
        }

        self.n_archives = archives.len() as u8;

        self.validate()
    }

    /// The byte length of a ring built against this config.
    pub fn ring_len(&self) -> usize {
        crate::ring::HEADER + (self.slots as usize) * self.agg.slot_width()
    }

    /// A fingerprint of the encoded config, stored in every ring built against it.
    ///
    /// **This is what makes editing the config bin an error rather than a silent reinterpretation.** Change the step
    /// from 60s to 1s without rebuilding the rings and every slot still holds a number, still decodes, and now means
    /// something else entirely. A ring that carries the fingerprint of the config it was built against can refuse.
    ///
    /// FNV-1a, 32-bit: four lines, no dependency, and collision resistance is not the property needed here — this
    /// guards against a config that was *edited*, not one that was chosen adversarially to collide.
    pub fn fingerprint(bytes: &[u8]) -> u32 {
        let mut hash: u32 = 0x811c_9dc5;

        for &b in bytes {
            hash ^= u32::from(b);
            hash = hash.wrapping_mul(0x0100_0193);
        }

        hash
    }

    pub fn fingerprint_of(&self) -> u32 {
        Self::fingerprint(&self.encode())
    }

    /// Whether this series stores an archive hierarchy rather than a single ring.
    ///
    /// The two are exclusive: a series with archives keeps **no** ring in its base bin, because the finest archive
    /// already is that ring. Declaring both would store the same samples twice — for the default set, 28 KiB of
    /// one-second samples in duplicate, per metric, forever.
    pub fn has_archives(&self) -> bool {
        self.n_archives > 0
    }

    /// The bucket width of the finest archive, which is what a coarse bucket's sample count is measured against.
    ///
    /// This is the denominator of [`Archive::xff`]: an hourly bucket built from one-second samples could hold 3,600 of
    /// them, and `xff` asks what share of that it actually holds. Falls back to `step_ms` for a series with no
    /// archives, where the ring's own step is the only interval there is.
    pub fn finest_step_ms(&self) -> i64 {
        match self.archives().first() {
            // `approx_span_ms(1)` rather than `step_ms()` so a hierarchy whose finest archive is a calendar unit still
            // has a denominator. Approximate is right here: xff is a judgement about density, not a stored quantity.
            Some(a) => a.resolution.step_ms().unwrap_or_else(|| a.resolution.approx_span_ms(1)),
            None => self.step_ms,
        }
    }

    /// The archive at a resolution, and its index.
    pub fn archive_at(&self, resolution: Resolution) -> Option<(usize, Archive)> {
        self.archives()
            .iter()
            .position(|a| a.resolution == resolution)
            .map(|i| (i, self.archive_table[i]))
    }
}

/// Everything one ring's bytes have to be interpreted against.
///
/// # Why this exists rather than passing a `Config`
///
/// A `Config` describes a **series**, and a series with archives has seven rings whose slot counts, aggregations and
/// bucket widths all differ. Handing a ring the config alone was fine while there was one of them and became a way to
/// read the hourly archive as though it were the per-second one — same magic, same fingerprint, and every byte would
/// decode. `Shape` names one ring: the config it belongs to, which archive of it it is, and the fingerprint it must
/// match.
///
/// `Copy`, because [`crate::ring::RingView`] is, and it is a handful of scalars.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Shape {
    cfg: Config,
    /// `None` for the single ring a series without archives keeps in its base bin.
    archive: Option<Archive>,
    cfg_fp: u32,
}

impl Shape {
    /// The single ring of a series with no archive hierarchy.
    pub fn base(cfg: &Config, cfg_fp: u32) -> Self {
        Self { cfg: *cfg, archive: None, cfg_fp }
    }

    /// The ring of one archive, by index.
    pub fn archive(cfg: &Config, cfg_fp: u32, index: usize) -> Result<Self> {
        let archive = *cfg.archives().get(index).ok_or_else(|| {
            Error::msg(format!(
                "this series has {} archives, so there is no archive {index}",
                cfg.archives().len()
            ))
        })?;

        Ok(Self { cfg: *cfg, archive: Some(archive), cfg_fp })
    }

    /// The ring of one archive, by resolution.
    pub fn at_resolution(cfg: &Config, cfg_fp: u32, resolution: Resolution) -> Result<Self> {
        let (_, archive) = cfg.archive_at(resolution).ok_or_else(|| {
            Error::msg(format!(
                "this series has no {} archive - it keeps {}",
                resolution.name(),
                cfg.archive_names()
            ))
        })?;

        Ok(Self { cfg: *cfg, archive: Some(archive), cfg_fp })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn fingerprint(&self) -> u32 {
        self.cfg_fp
    }

    pub fn archive_descriptor(&self) -> Option<Archive> {
        self.archive
    }

    /// The resolution this ring buckets by, or `None` for a base ring bucketing by `step_ms`.
    pub fn resolution(&self) -> Option<Resolution> {
        self.archive.map(|a| a.resolution)
    }

    pub fn agg(&self) -> Agg {
        self.archive.map_or(self.cfg.agg, |a| a.agg)
    }

    pub fn slots(&self) -> i64 {
        self.archive.map_or(self.cfg.slots, |a| a.slots)
    }

    pub fn xff_pct(&self) -> u8 {
        self.archive.map_or(0, |a| a.xff_pct)
    }

    pub fn kind(&self) -> Kind {
        self.cfg.kind
    }

    pub fn mode(&self) -> Mode {
        self.cfg.mode
    }

    pub fn allow_backfill(&self) -> bool {
        self.cfg.allow_backfill
    }

    pub fn allow_wipe(&self) -> bool {
        self.cfg.allow_wipe
    }

    pub fn wrap_bits(&self) -> u8 {
        self.cfg.wrap_bits
    }

    pub fn heartbeat_ms(&self) -> i64 {
        self.cfg.heartbeat_ms
    }

    pub fn slot_width(&self) -> usize {
        self.agg().slot_width()
    }

    pub fn ring_len(&self) -> usize {
        crate::ring::HEADER + (self.slots().max(0) as usize) * self.slot_width()
    }

    /// Milliseconds one bucket spans, or `None` for a calendar resolution that has no fixed width.
    pub fn step_ms(&self) -> Option<i64> {
        match self.archive {
            Some(a) => a.resolution.step_ms(),
            None => Some(self.cfg.step_ms),
        }
    }

    /// A bucket width to *report*, where an approximation is acceptable and `None` is not.
    ///
    /// Used for the `bucket_ms` a report carries, so a client can label an axis. Never used to place a sample: that
    /// goes through [`Shape::bucket_of`], which uses the calendar.
    pub fn approx_step_ms(&self) -> i64 {
        match self.archive {
            Some(a) => a.resolution.approx_span_ms(1),
            None => self.cfg.step_ms,
        }
    }

    pub fn bucket_of(&self, ts_ms: i64) -> i64 {
        match self.archive {
            Some(a) => a.resolution.bucket_of(ts_ms),
            // Floored, so a pre-1970 timestamp does not land in the epoch's own bucket.
            None => ts_ms.div_euclid(self.cfg.step_ms),
        }
    }

    pub fn bucket_start_ms(&self, bucket: i64) -> i64 {
        match self.archive {
            Some(a) => a.resolution.bucket_start_ms(bucket),
            None => bucket.saturating_mul(self.cfg.step_ms),
        }
    }

    /// How far back this ring reaches, approximately.
    pub fn approx_span_ms(&self) -> i64 {
        self.approx_step_ms().saturating_mul(self.slots())
    }

    /// How many samples a bucket of this ring could hold, at the finest archive's rate.
    ///
    /// The denominator of the `xff` test. One for the finest archive itself, 60 for a minute archive over per-second
    /// samples, and about 31.5 million for a year archive over the same.
    pub fn expected_samples(&self) -> i64 {
        let finest = self.cfg.finest_step_ms().max(1);

        (self.approx_step_ms() / finest).max(1)
    }

    /// Whether a bucket holding `samples` readings passes this archive's `xff`.
    ///
    /// `xff = 0` passes everything, which is why it is the default: an archive that suppresses buckets is a choice, not
    /// a surprise.
    pub fn passes_xff(&self, samples: i64) -> bool {
        let xff = i64::from(self.xff_pct());

        if xff == 0 {
            return true;
        }

        samples.saturating_mul(100) >= xff.saturating_mul(self.expected_samples())
    }

    /// The bin this ring lives in, given the series' base name.
    pub fn bin_name(&self, base: &str) -> String {
        match self.archive {
            Some(a) => archive_bin(base, a.resolution),
            None => base.to_string(),
        }
    }
}

impl Config {
    /// The resolutions this series keeps, for an error message that tells the caller what is available.
    pub fn archive_names(&self) -> String {
        if !self.has_archives() {
            return "none".to_string();
        }

        self.archives()
            .iter()
            .map(|a| a.resolution.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// A little-endian `i64` at a known offset.
///
/// Little-endian everywhere and explicitly, so a ring written on one architecture reads on another. The caller has
/// already checked the length, which is what makes the slice indexing sound.
pub(crate) fn le_i64(bytes: &[u8], off: usize) -> i64 {
    let mut buf = [0u8; 8];

    buf.copy_from_slice(&bytes[off..off + 8]);

    i64::from_le_bytes(buf)
}

pub(crate) fn le_u32(bytes: &[u8], off: usize) -> u32 {
    let mut buf = [0u8; 4];

    buf.copy_from_slice(&bytes[off..off + 4]);

    u32::from_le_bytes(buf)
}

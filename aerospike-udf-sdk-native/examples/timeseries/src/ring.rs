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

//! The ring: a 56-byte header and `slots × 8` (or × 16) bytes of samples, in one blob bin.
//!
//! # A ring says what it is
//!
//! The header carries its **resolution**, its **aggregation** and its **xff**, so a ring is interpreted by its own bytes
//! rather than by whichever config a caller happened to pass. That matters once a series has seven archives: they share
//! one config bin and one fingerprint, so before the resolution was stored, opening the hourly archive against the
//! minute archive's shape produced a ring where the magic matched, the fingerprint matched, and every slot decoded to a
//! number an hour away from where it belonged.
//!
//! Length alone does not catch it either — two archives can have the same slot count and the same aggregation, and then
//! only the resolution tells them apart.
//!
//! # The slot for a timestamp needs no stored epoch
//!
//! A timestamp's **bucket** is `t / step`, floored, and its slot is `bucket mod slots`. Buckets are anchored to the
//! Unix epoch, which every timestamp already is, so nothing has to remember where the series started. What has to be
//! remembered is `last_bucket`, the newest bucket written — because a slot is only live if its bucket is inside
//! `(last_bucket − slots, last_bucket]`, and outside that window the bytes are a retired bucket's leftovers.
//!
//! Division is `div_euclid` and the modulus is `rem_euclid`, not `/` and `%`. For a pre-1970 timestamp `-1 / 60000`
//! truncates toward zero and gives bucket 0, putting a negative time in the same bucket as the epoch itself;
//! `div_euclid` floors and gives −1. Timestamps before 1970 are rare and silently wrong is worse than rare.
//!
//! # The ring is never 8-byte aligned, and that cannot be fixed
//!
//! A blob particle is one type byte followed by a 4-byte length, packed, so its data begins **five** bytes in: the
//! pointer the host hands back is always `≡ 1 (mod 4)`. Padding at write time does not help — the reader would have to
//! know how much padding to skip, and computing that from the pointer it happens to get is exactly the thing that is
//! not reproducible.
//!
//! So every sample goes through `from_le_bytes` and `to_le_bytes`. **Never** a cast to `&[f64]`: that is undefined
//! behaviour in Rust regardless of the fact that aarch64 and x86-64 both tolerate unaligned loads in hardware. The
//! byte-array route compiles to the same single unaligned load, and being explicitly little-endian also makes a ring
//! written on one architecture readable on another.
//!
//! # Appending is still O(slots) in bytes moved, but half of that went away
//!
//! An append used to copy the ring **four** times: the host materialised the particle into a private value, this code
//! copied that into a `Vec`, `set_blob` copied it back into a new value, and the write copied that into the new
//! particle. [`RingMut`] borrows the bin's own bytes through `Record::with_bytes_mut` and removes the middle two.
//!
//! Measured, one append against ring size — same server, same code path, only the copying changed:
//!
//! | slots | ring bytes | four copies | two copies |
//! |---|---|---|---|
//! | 64 | 552 B | 71 µs | 66 µs |
//! | 1,024 | 8 KB | 77 µs | 74 µs |
//! | 8,192 | 64 KB | 97 µs | 84 µs |
//! | **65,536** | **512 KB** | **390 µs** | **187 µs** |
//!
//! **2.1× on a 512 KB ring, and nothing measurable below about 64 KB** — under that the round trip dominates and the
//! copies are noise. Two copies remain and they are the floor rather than an oversight: the private copy is what makes
//! mutating safe, and building the new particle is what makes it durable.
//!
//! So append is still O(1) in slots *touched* and O(slots) in bytes *moved*, just with a smaller constant.
//! [`crate::ts_append_many`] remains the answer for throughput, because it borrows once for the whole batch.

use crate::config::{le_i64, le_u32, Agg, Kind, Mode, Resolution, Shape};
use aerospike_udf_sdk_native::{Error, Result};

pub const MAGIC: [u8; 4] = *b"RRD1";

/// The layout this build writes.
///
/// **Version 2 added the archive fields**, which are what make a ring self-describing: its resolution, its aggregation
/// and its `xff` are in its own header rather than inferred from whichever config the caller passed. A v1 ring is
/// refused rather than read, because every one of those was written before archives existed and there is no rule that
/// recovers the header a v2 reader wants from bytes v1 never stored.
pub const VERSION: u8 = 2;

/// Header length. A multiple of eight, so slot offsets stay clean arithmetic even though the absolute address cannot.
pub const HEADER: usize = 56;

/// `last_bucket` when nothing has ever been written.
pub const NO_BUCKET: i64 = i64::MIN;

/// The unknown marker for an `I64` ring. `NaN` serves for `F64`.
pub const UNKNOWN_I64: i64 = i64::MIN;

/// The resolution byte of a base ring — one that buckets by `step_ms` rather than by a calendar-aware resolution.
///
/// A sentinel rather than a flag bit, because it occupies the field it describes: there is no state where a ring has
/// both a resolution and a raw step.
pub const RES_NONE: u8 = 0xFF;

const O_KIND: usize = 5;
const O_MODE: usize = 6;
const O_FLAGS: usize = 7;
const O_CFG_FP: usize = 8;
const O_RESETS: usize = 12;
const O_LAST_BUCKET: usize = 16;
const O_LAST_RAW: usize = 24;
const O_APPENDS: usize = 32;
const O_RES: usize = 40;
const O_AGG: usize = 41;
const O_XFF: usize = 42;
// 43 and 44..48 reserved.
const O_LAST_RAW_MS: usize = 48;

/// Set when an `I64` slot's `Sum` had to saturate rather than wrap.
pub const FLAG_SATURATED: u8 = 1 << 0;

/// Set once a counter has a previous reading to subtract from.
pub const FLAG_HAS_RAW: u8 = 1 << 1;

/// A sample, in whichever of the two widths the ring stores.
///
/// Two variants rather than everything as `f64`, so an `I64` ring's arithmetic — min, max, sum, count — stays exact.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Num {
    F(f64),
    I(i64),
    /// A running mean: the sum of the samples and how many there were.
    ///
    /// **Both halves have to be stored**, which is why a `Mean` slot is sixteen bytes. Keeping only the mean would make
    /// consolidation wrong: averaging two buckets' averages is not the average of their samples unless both held the
    /// same number of them, and across archives they never do.
    Mean { sum: f64, count: i64 },
}

impl Num {
    pub fn as_f64(self) -> f64 {
        match self {
            Num::F(v) => v,
            Num::I(v) => v as f64,
            // The mean itself. A count of zero is a slot nothing was added to, and reports as unknown rather than as a
            // division by zero.
            Num::Mean { sum, count } => {
                if count > 0 {
                    sum / count as f64
                } else {
                    f64::NAN
                }
            }
        }
    }

    /// How many samples are behind this value. One for a plain sample; the running count for a mean.
    ///
    /// This is what an archive's `xff` tests: a coarse bucket built from two samples out of three thousand is not
    /// worth the same as one built from three thousand.
    pub fn samples(self) -> i64 {
        match self {
            Num::Mean { count, .. } => count,
            _ => 1,
        }
    }

    pub fn kind(self) -> Kind {
        match self {
            Num::I(_) => Kind::I64,
            // A mean is a float statistic even over integer samples - the average of two integers is rarely one.
            Num::F(_) | Num::Mean { .. } => Kind::F64,
        }
    }
}

/// Where an append landed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A newer bucket than any before it. Older slots may have been retired to get there.
    Placed,
    /// The newest bucket already had a sample, and the two were combined.
    Combined,
    /// An older bucket, still inside the window.
    Backfilled,
    /// Older than the window. Nothing was changed.
    TooOld,
    /// Inside the window, but the config does not allow backfill. Nothing was changed.
    BackfillRefused,
    /// The gap reaches `slots`, so this append would erase every slot, and `allow_wipe` is off.
    ///
    /// Nothing was changed, which is the point: the data is still there to be salvaged once the timestamp is fixed.
    WouldWipe,
}

impl Outcome {
    pub fn accepted(self) -> bool {
        matches!(self, Outcome::Placed | Outcome::Combined | Outcome::Backfilled)
    }

    pub fn name(self) -> &'static str {
        match self {
            Outcome::Placed => "placed",
            Outcome::Combined => "combined",
            Outcome::Backfilled => "backfilled",
            Outcome::TooOld => "too_old",
            Outcome::BackfillRefused => "backfill_refused",
            Outcome::WouldWipe => "would_wipe",
        }
    }
}

/// What a counter reading turned into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterEvent {
    /// The first reading. Recorded as the baseline; the slot stores unknown, because one reading is not a rate.
    Baseline,
    /// A normal increase.
    Delta,
    /// A decrease reconstructed as a wraparound, because both readings fit the declared width.
    Wrapped,
    /// A decrease that is not a declared wraparound. The slot stores unknown and `resets` is incremented.
    Reset,
    /// The previous reading is older than the heartbeat allows, so this one becomes a new baseline.
    ///
    /// **RRDtool's heartbeat, and it closes a real hole.** Without it, a counter reading arriving three hours after the
    /// previous one has its entire delta charged to a single bucket, as though three hours of traffic had happened in
    /// one interval — a spike in the graph at exactly the moment the data is least trustworthy. Treating the reading as
    /// a baseline says "the gap is unknown" instead of inventing a number for it.
    Stale,
}

impl CounterEvent {
    pub fn name(self) -> &'static str {
        match self {
            CounterEvent::Baseline => "baseline",
            CounterEvent::Delta => "delta",
            CounterEvent::Wrapped => "wrapped",
            CounterEvent::Reset => "reset",
            CounterEvent::Stale => "stale",
        }
    }
}

/// The full result of one append, because "it worked" is not enough to act on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Applied {
    pub outcome: Outcome,
    /// Slots retired to reach a newer bucket.
    pub cleared: i64,
    /// Present only for a counter ring.
    pub counter: Option<CounterEvent>,
    /// What ended up in the slot. `None` means unknown was stored, or nothing was.
    pub stored: Option<Num>,
    pub bucket: i64,
}

/// A ring's bytes, borrowed. Read-only.
///
/// **Borrowed rather than owned, and that is the point.** The owned version copied the bin out with `to_vec()` and
/// pushed it back with `set_blob`, which was two of the four copies a read-modify-write paid. `RingView` reads straight
/// out of the value the host materialised, and [`RingMut`] writes straight into it.
#[derive(Clone, Copy)]
pub struct RingView<'a> {
    bytes: &'a [u8],
    shape: Shape,
}

/// A ring's bytes, borrowed mutably, so an append is performed in place.
pub struct RingMut<'a> {
    bytes: &'a mut [u8],
    shape: Shape,
}

/// Shape and position, **not** the samples.
///
/// Hand-written rather than derived: a derive would print up to half a megabyte of slot bytes into a test failure
/// message, which buries the one line that says what went wrong.
impl core::fmt::Debug for RingView<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Ring")
            .field("kind", &self.shape.kind())
            .field("mode", &self.shape.mode())
            .field("agg", &self.shape.agg())
            .field("resolution", &self.shape.resolution())
            .field("step_ms", &self.shape.step_ms())
            .field("slots", &self.shape.slots())
            .field("last_bucket", &self.last_bucket())
            .field("appends", &self.appends())
            .field("resets", &self.resets())
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl core::fmt::Debug for RingMut<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.view().fmt(f)
    }
}

/// Build a fresh ring's bytes: every slot unknown, no position yet.
///
/// Returns an owned buffer because there is nothing to borrow yet — `ts_init` stages it with `set_blob`. Every path
/// after creation borrows.
pub fn create(shape: &Shape) -> Vec<u8> {
    let mut bytes = vec![0u8; shape.ring_len()];
    let agg = shape.agg();

    bytes[0..4].copy_from_slice(&MAGIC);
    bytes[4] = VERSION;
    bytes[O_KIND] = shape.kind().as_u8();
    bytes[O_MODE] = shape.mode().as_u8();
    bytes[O_FLAGS] = 0;
    bytes[O_CFG_FP..O_CFG_FP + 4].copy_from_slice(&shape.fingerprint().to_le_bytes());
    bytes[O_LAST_BUCKET..O_LAST_BUCKET + 8].copy_from_slice(&NO_BUCKET.to_le_bytes());
    bytes[O_RES] = shape.resolution().map_or(RES_NONE, Resolution::as_u8);
    bytes[O_AGG] = agg.as_u8();
    bytes[O_XFF] = shape.xff_pct();

    // A mean archive's slots are already empty at zero - count zero is the unknown state - so only the single-value
    // layouts need their sentinel written.
    if agg != Agg::Mean {
        for slot in 0..shape.slots().max(0) as usize {
            let raw = match shape.kind() {
                Kind::F64 => f64::NAN.to_bits() as i64,
                Kind::I64 => UNKNOWN_I64,
            };
            let off = HEADER + slot * 8;

            bytes[off..off + 8].copy_from_slice(&raw.to_le_bytes());
        }
    }

    bytes
}

/// Everything that has to be true before a ring's bytes may be interpreted against a shape.
///
/// Each of these is a real way to get a wrong *answer* rather than an error: the magic (a different bin), the version
/// (an older layout), the length (a config whose `slots` changed), the config fingerprint (a config that was **edited in
/// place**, leaving every byte valid and every value reinterpreted), and — since archives — the resolution and
/// aggregation, which is what stops one archive being read as another.
fn validate(bytes: &[u8], shape: &Shape) -> Result<()> {
    if bytes.len() < HEADER {
        return Err(Error::msg(format!(
            "a ring is at least {HEADER} bytes, this bin holds {}",
            bytes.len()
        )));
    }

    if bytes[0..4] != MAGIC {
        return Err(Error::msg("that bin does not hold a time-series ring - wrong magic"));
    }

    if bytes[4] != VERSION {
        return Err(Error::msg(format!(
            "ring layout version {}, this build understands {VERSION}",
            bytes[4]
        )));
    }

    if bytes.len() != shape.ring_len() {
        return Err(Error::msg(format!(
            "the config describes {} slots ({} bytes) but this ring is {} bytes - \
             the config changed without the ring being rebuilt",
            shape.slots(),
            shape.ring_len(),
            bytes.len()
        )));
    }

    let kind = Kind::from_u8(bytes[O_KIND])?;
    let mode = Mode::from_u8(bytes[O_MODE])?;

    if kind != shape.kind() || mode != shape.mode() {
        return Err(Error::msg(format!(
            "the ring stores {}/{} but the config says {}/{}",
            kind.name(),
            mode.name(),
            shape.kind().name(),
            shape.mode().name()
        )));
    }

    // **The check that keeps one archive from being read as another.** Same series, same fingerprint, same length, and
    // the samples an hour apart from where the caller thinks they are.
    let stored_res = match bytes[O_RES] {
        RES_NONE => None,
        code => Some(Resolution::from_u8(code)?),
    };

    if stored_res != shape.resolution() {
        let describe = |r: Option<Resolution>| r.map_or("no resolution (a base ring)", Resolution::name);

        return Err(Error::msg(format!(
            "this ring holds {} buckets but it was opened as {} - check the bin name",
            describe(stored_res),
            describe(shape.resolution())
        )));
    }

    let stored_agg = Agg::from_u8(bytes[O_AGG])?;

    if stored_agg != shape.agg() {
        return Err(Error::msg(format!(
            "this ring aggregates with {} but the config says {} - the slot width differs for a mean, \
             and the values differ for everything else",
            stored_agg.name(),
            shape.agg().name()
        )));
    }

    // **The fingerprint is checked last, because it is the catch-all.** It reports that *something* about the config
    // moved; the checks above report *what*. Running it first would answer "check the bin name" with "the fingerprint
    // differs", which is true, unhelpful, and was the first version of this function.
    let stored_fp = le_u32(bytes, O_CFG_FP);

    if stored_fp != shape.fingerprint() {
        return Err(Error::msg(format!(
            "this ring was built against a different config (fingerprint {stored_fp:#010x}, \
             the config bin is now {:#010x}) - reinitialise it or restore the config",
            shape.fingerprint()
        )));
    }

    Ok(())
}

impl<'a> RingView<'a> {
    pub fn open(bytes: &'a [u8], shape: &Shape) -> Result<Self> {
        validate(bytes, shape)?;

        Ok(Self { bytes, shape: *shape })
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn config(&self) -> &crate::config::Config {
        self.shape.config()
    }

    pub fn last_bucket(&self) -> i64 {
        le_i64(self.bytes, O_LAST_BUCKET)
    }

    pub fn resets(&self) -> u32 {
        le_u32(self.bytes, O_RESETS)
    }

    pub fn appends(&self) -> i64 {
        le_i64(self.bytes, O_APPENDS)
    }

    pub fn saturated(&self) -> bool {
        self.bytes[O_FLAGS] & FLAG_SATURATED != 0
    }

    fn has_raw(&self) -> bool {
        self.bytes[O_FLAGS] & FLAG_HAS_RAW != 0
    }

    /// The previous counter reading, in the ring's own width.
    fn last_raw(&self) -> Option<Num> {
        if !self.has_raw() {
            return None;
        }

        let bits = le_i64(self.bytes, O_LAST_RAW);

        Some(match self.shape.kind() {
            Kind::F64 => Num::F(f64::from_bits(bits as u64)),
            Kind::I64 => Num::I(bits),
        })
    }

    /// When the previous counter reading arrived. Only meaningful with [`FLAG_HAS_RAW`] set.
    fn last_raw_ms(&self) -> i64 {
        le_i64(self.bytes, O_LAST_RAW_MS)
    }

    /// The bucket a timestamp belongs to. Floored for a fixed step, calendar-correct for a month or a year.
    pub fn bucket_of(&self, ts_ms: i64) -> i64 {
        self.shape.bucket_of(ts_ms)
    }

    /// The first millisecond of a bucket.
    pub fn bucket_start_ms(&self, bucket: i64) -> i64 {
        self.shape.bucket_start_ms(bucket)
    }

    fn slot_of(&self, bucket: i64) -> usize {
        bucket.rem_euclid(self.shape.slots()) as usize
    }

    /// The live bucket range, or `None` for a ring nothing has been written to.
    ///
    /// **Saturating, not wrapping.** A ring anchored near `i64::MIN` — reachable at a one-millisecond step with a
    /// pre-epoch timestamp — makes `last - slots + 1` underflow. In debug that panics; in release it wraps to a large
    /// positive number, so `at()` decides every bucket is outside the window and the whole ring reads as unknown.
    /// Clamping instead is exactly right: there are no buckets below `i64::MIN` to include.
    pub fn window(&self) -> Option<(i64, i64)> {
        let last = self.last_bucket();

        if last == NO_BUCKET {
            return None;
        }

        // `slots - 1` in one saturating step, **not** `saturating_sub(slots).saturating_add(1)`: that clamps at
        // `i64::MIN` and then adds one back, putting the window's floor a bucket too high. `slots >= 1` is validated, so
        // `slots - 1` cannot itself underflow.
        Some((last.saturating_sub(self.shape.slots() - 1), last))
    }

    fn slot_off(&self, slot: usize) -> usize {
        HEADER + slot * self.shape.slot_width()
    }

    fn read_slot(&self, slot: usize) -> Option<Num> {
        let off = self.slot_off(slot);

        // A mean slot needs no sentinel: a count of zero is the empty state, which is why it does not have to reserve a
        // NaN or an i64::MIN the way a single-value slot does.
        if self.shape.agg() == Agg::Mean {
            let count = le_i64(self.bytes, off + 8);

            return (count > 0).then(|| Num::Mean {
                sum: f64::from_bits(le_i64(self.bytes, off) as u64),
                count,
            });
        }

        let raw = le_i64(self.bytes, off);

        match self.shape.kind() {
            Kind::F64 => {
                let v = f64::from_bits(raw as u64);

                (!v.is_nan()).then_some(Num::F(v))
            }
            Kind::I64 => (raw != UNKNOWN_I64).then_some(Num::I(raw)),
        }
    }

    /// The value at an absolute bucket, or `None` if it is unknown, outside the window, or below this archive's `xff`.
    ///
    /// The cases collapse deliberately: a caller cannot act differently on "never written", "outside the ring" and
    /// "too sparse to report", and all three are nil. [`RingView::at_unfiltered`] separates the last one for a report
    /// that wants to say how many buckets `xff` suppressed.
    ///
    /// **`xff` belongs here and not in `read_slot`**, because an append has to combine with whatever a bucket already
    /// holds — including a bucket too sparse to *report*, which is exactly the bucket the next sample makes less sparse.
    /// Filtering on the way in would make a suppressed bucket permanently unreachable.
    pub fn at(&self, bucket: i64) -> Option<Num> {
        let value = self.at_unfiltered(bucket)?;

        self.shape.passes_xff(value.samples()).then_some(value)
    }

    /// The value at a bucket, ignoring `xff`.
    pub fn at_unfiltered(&self, bucket: i64) -> Option<Num> {
        let (first, last) = self.window()?;

        if bucket < first || bucket > last {
            return None;
        }

        self.read_slot(self.slot_of(bucket))
    }
}

impl<'a> RingMut<'a> {
    /// Borrow a bin's bytes for modification.
    pub fn open(bytes: &'a mut [u8], shape: &Shape) -> Result<Self> {
        validate(bytes, shape)?;

        Ok(Self { bytes, shape: *shape })
    }

    /// A read-only view of the same bytes.
    ///
    /// Always used as a temporary — `self.view().last_bucket()` — so the immutable reborrow ends within the statement
    /// and never overlaps a mutation. That is what lets every reader live on [`RingView`] and be reached from here
    /// without duplicating one of them.
    pub fn view(&self) -> RingView<'_> {
        RingView { bytes: self.bytes, shape: self.shape }
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn config(&self) -> &crate::config::Config {
        self.shape.config()
    }

    /// The bytes, read-only.
    ///
    /// An immutable reborrow, so it cannot be held across a mutation - which is why a test comparing a ring before and
    /// after a refused append has to ask for it here rather than keep its own handle on the buffer. The borrow checker
    /// refused the latter, correctly.
    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }

    pub fn last_bucket(&self) -> i64 {
        self.view().last_bucket()
    }

    pub fn resets(&self) -> u32 {
        self.view().resets()
    }

    pub fn appends(&self) -> i64 {
        self.view().appends()
    }

    pub fn saturated(&self) -> bool {
        self.view().saturated()
    }

    pub fn bucket_of(&self, ts_ms: i64) -> i64 {
        self.view().bucket_of(ts_ms)
    }

    pub fn bucket_start_ms(&self, bucket: i64) -> i64 {
        self.view().bucket_start_ms(bucket)
    }

    pub fn window(&self) -> Option<(i64, i64)> {
        self.view().window()
    }

    pub fn at(&self, bucket: i64) -> Option<Num> {
        self.view().at(bucket)
    }

    pub fn at_unfiltered(&self, bucket: i64) -> Option<Num> {
        self.view().at_unfiltered(bucket)
    }

    fn slot_of(&self, bucket: i64) -> usize {
        bucket.rem_euclid(self.shape.slots()) as usize
    }

    fn slot_off(&self, slot: usize) -> usize {
        HEADER + slot * self.shape.slot_width()
    }

    fn set_last_bucket(&mut self, v: i64) {
        self.bytes[O_LAST_BUCKET..O_LAST_BUCKET + 8].copy_from_slice(&v.to_le_bytes());
    }

    fn bump_resets(&mut self) {
        let next = self.view().resets().saturating_add(1);

        self.bytes[O_RESETS..O_RESETS + 4].copy_from_slice(&next.to_le_bytes());
    }

    fn bump_appends(&mut self) {
        let next = self.view().appends().saturating_add(1);

        self.bytes[O_APPENDS..O_APPENDS + 8].copy_from_slice(&next.to_le_bytes());
    }

    fn mark_saturated(&mut self) {
        self.bytes[O_FLAGS] |= FLAG_SATURATED;
    }

    /// Record a counter reading as the baseline the next one measures from, with the time it arrived.
    ///
    /// The timestamp is what the heartbeat tests, and it is stored beside the value rather than derived from
    /// `last_bucket`: a bucket start is rounded down to the resolution, so on the year archive it could be eleven months
    /// away from when the reading actually arrived.
    fn set_last_raw(&mut self, v: Num, ts_ms: i64) {
        let bits = match v {
            Num::I(i) => i,
            // Unreachable: a counter ring is F64 or I64, never Mean. Recording the value keeps this total rather than
            // panicking on a state the type system allows and the config does not.
            other => other.as_f64().to_bits() as i64,
        };

        self.bytes[O_LAST_RAW..O_LAST_RAW + 8].copy_from_slice(&bits.to_le_bytes());
        self.bytes[O_LAST_RAW_MS..O_LAST_RAW_MS + 8].copy_from_slice(&ts_ms.to_le_bytes());
        self.bytes[O_FLAGS] |= FLAG_HAS_RAW;
    }

    fn write_slot(&mut self, slot: usize, value: Option<Num>) {
        let off = self.slot_off(slot);

        if self.shape.agg() == Agg::Mean {
            let (sum, count) = match value {
                None => (0.0, 0),
                Some(n) => as_running(n),
            };

            self.bytes[off..off + 8].copy_from_slice(&(sum.to_bits() as i64).to_le_bytes());
            self.bytes[off + 8..off + 16].copy_from_slice(&count.to_le_bytes());

            return;
        }

        let raw = match (self.shape.kind(), value) {
            (Kind::F64, None) => f64::NAN.to_bits() as i64,
            (Kind::I64, None) => UNKNOWN_I64,
            (_, Some(Num::I(v))) => v,
            (_, Some(other)) => other.as_f64().to_bits() as i64,
        };

        self.bytes[off..off + 8].copy_from_slice(&raw.to_le_bytes());
    }

    /// One sample, written **into the bin's own bytes**.
    ///
    /// Feasibility is decided before anything is mutated, so a rejected append leaves the ring byte-identical —
    /// including a counter's baseline, which would otherwise be advanced by an append that never happened. That
    /// mattered less when the ring was a private copy that could simply be discarded; now the bytes are the bin's, so
    /// it is the only thing keeping a refusal non-destructive.
    pub fn append(&mut self, ts_ms: i64, raw: Num) -> Result<Applied> {
        if raw.kind() != self.shape.kind() {
            return Err(Error::msg(format!(
                "this ring stores {} but a {} was appended - a silent conversion is not offered",
                self.shape.kind().name(),
                raw.kind().name()
            )));
        }

        let bucket = self.bucket_of(ts_ms);

        // **`i64::MIN` is the "never written" sentinel**, so it cannot also be a written position: a ring whose newest
        // bucket were `i64::MIN` would report no window and read as entirely unknown. Reachable only at a one-millisecond
        // step with a timestamp of exactly `i64::MIN`, which is not a time anyone means - but a write being silently
        // discarded is not something to leave to how unlikely the input is.
        if bucket == NO_BUCKET {
            return Err(Error::msg(
                "a timestamp that lands in bucket i64::MIN cannot be stored - that value marks a ring \
                 as never written, so the sample would be indistinguishable from an empty series",
            ));
        }

        let last = self.last_bucket();
        let slots = self.shape.slots();

        // --- feasibility, before any mutation ---

        // Saturating differences throughout. A ring anchored near one end of `i64` and a sample near the other produce a
        // gap that does not fit an `i64`, and in release a wrapped subtraction turns "impossibly far apart" into a small
        // number - which is the one answer that makes the append proceed.
        let placement = if last == NO_BUCKET {
            Outcome::Placed
        } else if bucket > last {
            // **A one-slot ring is exempt**, because for it the guard protects nothing and forbids everything: its window
            // is a single bucket, so *every* forward advance erases the whole window and `gap >= slots` is always true.
            // `ts_init(bin, step, 1)` succeeds, so the shape is on offer; a shape that cannot be appended to twice
            // without `wipe=1` is a defect rather than a choice anybody made. With two or more slots the rule stands.
            if slots > 1 && bucket.saturating_sub(last) >= slots && !self.shape.allow_wipe() {
                Outcome::WouldWipe
            } else {
                Outcome::Placed
            }
        } else if bucket == last {
            Outcome::Combined
        } else if last.saturating_sub(bucket) >= slots {
            Outcome::TooOld
        } else if !self.shape.allow_backfill() {
            Outcome::BackfillRefused
        } else {
            Outcome::Backfilled
        };

        if !placement.accepted() {
            return Ok(Applied { outcome: placement, cleared: 0, counter: None, stored: None, bucket });
        }

        // --- a counter reading becomes a delta, or becomes unknown ---

        let (counter, value) = match self.shape.mode() {
            Mode::Gauge => (None, Some(raw)),
            Mode::Counter => {
                let (event, delta) = self.view().counter_delta(raw, ts_ms);

                self.set_last_raw(raw, ts_ms);

                if event == CounterEvent::Reset {
                    self.bump_resets();
                }

                (Some(event), delta)
            }
        };

        // --- retire whatever the advance passed over ---

        let mut cleared = 0i64;

        if placement == Outcome::Placed && last != NO_BUCKET {
            let gap = bucket.saturating_sub(last);
            // At most `slots - 1`: beyond that every slot dies anyway, and clearing one twice is wasted work.
            let to_clear = (gap - 1).min(slots - 1);

            for k in 1..=to_clear {
                // `last + k` can overflow when the gap saturated; the slot index only needs the value mod `slots`, and
                // wrapping is the arithmetic that preserves that.
                let slot = self.slot_of(last.wrapping_add(k));

                self.write_slot(slot, None);
                cleared += 1;
            }
        }

        // --- store ---

        let slot = self.slot_of(bucket);

        // On `Placed` the slot holds a retired bucket's leftovers, so there is nothing to combine with. On the other
        // two the existing sample is this bucket's own and combining is the whole point.
        let existing = if placement == Outcome::Placed {
            None
        } else {
            self.view().read_slot(slot)
        };

        let (stored, saturated) = combine(self.shape.agg(), existing, value);

        if saturated {
            self.mark_saturated();
        }

        self.write_slot(slot, stored);

        if bucket > last || last == NO_BUCKET {
            self.set_last_bucket(bucket);
        }

        self.bump_appends();

        Ok(Applied { outcome: placement, cleared, counter, stored, bucket })
    }
}

impl RingView<'_> {
    /// Turn a counter reading into the delta a slot should hold.
    ///
    /// The three interesting cases are all "the number went down", and the numbers alone cannot say why. With a width
    /// declared and both readings inside it, a decrease is reconstructed as a wraparound *if the result is plausible*.
    /// Otherwise it is a reset: the slot stores unknown rather than a spuriously enormous rate, which is the failure
    /// mode that makes a graph unreadable after a process restart.
    fn counter_delta(&self, raw: Num, ts_ms: i64) -> (CounterEvent, Option<Num>) {
        let Some(prev) = self.last_raw() else {
            return (CounterEvent::Baseline, None);
        };

        // **The heartbeat, before any arithmetic.** A gap longer than this means the delta spans time nobody measured,
        // and charging all of it to one bucket would draw a spike precisely where the data is thinnest. The reading is
        // still recorded as the new baseline by the caller, so the *next* one is a normal delta.
        let heartbeat = self.shape.heartbeat_ms();

        if heartbeat > 0 && ts_ms.saturating_sub(self.last_raw_ms()) > heartbeat {
            return (CounterEvent::Stale, None);
        }

        match (prev, raw) {
            (Num::I(p), Num::I(r)) => {
                if r >= p {
                    return (CounterEvent::Delta, Some(Num::I(r - p)));
                }

                let width = match self.shape.wrap_bits() {
                    32 => Some(1i128 << 32),
                    64 => Some(1i128 << 64),
                    _ => None,
                };

                if let Some(modulus) = width {
                    let (p, r) = (i128::from(p), i128::from(r));

                    if p >= 0 && r >= 0 && p < modulus && r < modulus {
                        let delta = r + modulus - p;

                        // **A reconstructed wrap has to be plausible, or declaring a width makes things worse.**
                        //
                        // Without this check, any decrease between two values that happen to fit the width was called
                        // a wrap - so a counter restarting from 1,250 to 7 became a delta of 4,294,966,053, which is
                        // the exact spurious spike this mode exists to avoid. The first version did that, and a live
                        // test caught it.
                        //
                        // The two cases separate cleanly by magnitude: a real wrap means the counter sat near its
                        // ceiling and rolled just past zero, so the delta is *small*; a reset from a low value
                        // reconstructs to nearly the whole width. Half the width is the line.
                        //
                        // The cost is a counter that genuinely advances more than half its range in one step, which
                        // is read as a reset. For 32 bits that is over two billion units per step - at which point
                        // 32 bits was the wrong width to declare.
                        if delta < modulus / 2 {
                            if let Ok(fits) = i64::try_from(delta) {
                                return (CounterEvent::Wrapped, Some(Num::I(fits)));
                            }
                        }
                    }
                }

                (CounterEvent::Reset, None)
            }
            (Num::F(p), Num::F(r)) => {
                // No defined wrap for a float counter: a decrease is a reset, and `wrap_bits` is documented as
                // ignored here rather than quietly doing something arbitrary.
                if r >= p {
                    (CounterEvent::Delta, Some(Num::F(r - p)))
                } else {
                    (CounterEvent::Reset, None)
                }
            }
            // Unreachable: `append` rejects a mismatched width before this is called.
            _ => (CounterEvent::Reset, None),
        }
    }
}

/// Fold a new sample into whatever a slot already held.
///
/// `None` is unknown on both sides, and unknown is a **no-op rather than an eraser**: a counter reset arriving in the
/// middle of a bucket must not delete the delta already recorded there. Returns whether an `I64` sum had to saturate.
pub fn combine(agg: Agg, existing: Option<Num>, new: Option<Num>) -> (Option<Num>, bool) {
    let Some(new) = new else {
        return (existing, false);
    };

    let Some(existing) = existing else {
        // `Count` counts samples, not values, so the first one is a 1 whatever it was.
        return match agg {
            Agg::Count => (Some(one_like(new)), false),
            _ => (Some(new), false),
        };
    };

    match agg {
        Agg::Last => (Some(new), false),
        Agg::First => (Some(existing), false),
        Agg::Min => (Some(if new.as_f64() < existing.as_f64() { new } else { existing }), false),
        Agg::Max => (Some(if new.as_f64() > existing.as_f64() { new } else { existing }), false),
        Agg::Sum => add(existing, new),
        Agg::Count => add(existing, one_like(new)),
        // Sums and counts add. That holds whether `new` is a fresh sample or another bucket's running mean, which is
        // what makes downsampling a mean archive exact rather than an average of averages.
        Agg::Mean => {
            let (s1, c1) = as_running(existing);
            let (s2, c2) = as_running(new);

            (
                Some(Num::Mean {
                    sum: s1 + s2,
                    count: c1.saturating_add(c2),
                }),
                false,
            )
        }
    }
}

/// A value as a `(sum, count)` pair: a mean contributes both, anything else is one sample of itself.
fn as_running(n: Num) -> (f64, i64) {
    match n {
        Num::Mean { sum, count } => (sum, count),
        other => (other.as_f64(), 1),
    }
}

fn one_like(n: Num) -> Num {
    match n {
        Num::I(_) => Num::I(1),
        _ => Num::F(1.0),
    }
}

/// Add two samples, saturating rather than wrapping an `I64`.
///
/// A wrapped counter total is a plausible wrong number; a saturated one is obviously wrong and the ring records that
/// it happened. The same choice as the summary example's integer sum, for the same reason.
fn add(a: Num, b: Num) -> (Option<Num>, bool) {
    match (a, b) {
        (Num::I(x), Num::I(y)) => match x.checked_add(y) {
            Some(sum) => (Some(Num::I(sum)), false),
            None => (Some(Num::I(if y > 0 { i64::MAX } else { i64::MIN })), true),
        },
        (x, y) => (Some(Num::F(x.as_f64() + y.as_f64())), false),
    }
}

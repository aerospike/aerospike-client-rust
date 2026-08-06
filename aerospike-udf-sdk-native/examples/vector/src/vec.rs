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

//! Reading a vector out of a record, and measuring the distance to it.
//!
//! # Two representations
//!
//! A **blob of packed `f32`** is the one to store. `rec_get_bytes` hands back a pointer to a value the per-call
//! arena owns, which the host allocated — so it is 16-byte aligned, and the distance kernel casts it to `&[f32]`
//! and reads it in place. Four bytes per dimension.
//!
//! A **list of doubles** also works, because it is what someone who did not plan for vectors already has. It
//! costs one bulk host call and a conversion per element, and eight bytes per dimension on disk.
//!
//! Either way the scratch buffer is allocated **once per accumulator** and reused, so the unaligned fallback costs
//! a copy and never an allocation.
//!
//! # Alignment is a property of the host, not of the data
//!
//! Worth writing down, because this file once got it wrong in both directions. The bytes are aligned only because
//! the host copies the bin into a value it allocated. A stored blob particle is **never** aligned — its data begins
//! five bytes into its allocation, after a type byte and a four-byte length — so a host that handed out the stored
//! particle directly would give `ptr % 4 == 1` on every record, forever.
//!
//! An experiment did exactly that, and the consequences were instructive: the memcpy it saved came straight back in
//! [`AlignedBuf::fill`], and hand-written unaligned kernels written the obvious way — one accumulator per sum —
//! measured *worse* than copying, because a float reduction is a dependency chain the optimiser may not reorder.
//! It was reverted for a better reason than the modest payoff, though: every accessor on [`Record`] takes `&self`,
//! so a borrowed particle can be read, then written, then read again through the first slice, and that is a
//! use-after-free the compiler cannot see.
//!
//! What survives is the counter — [`Measurer::path_counts`] — because a claim that nothing is copied should be
//! able to prove it.
//!
//! # Nothing is decided per record
//!
//! The element type and the metric are both fixed for a whole query, so neither is allowed near the per-record
//! path. [`Measurer::new`] resolves the pair to **one monomorphic function pointer**, once, and `accumulate`
//! makes a single indirect call with a target that never changes. There is no `match` on the metric and none on
//! the element type — which is *fewer* branches than the first version of this file, where the metric was a
//! five-way match evaluated once per record.
//!
//! Packed `f32` is little-endian. Every platform Aerospike runs on is little-endian, so this is stated rather
//! than handled — a big-endian client writing vectors would need a byte swap, and there isn't one.
//!
//! # A note on "simsimd" in the comments below
//!
//! The vendor library here is **NumKong 7.7.1**, from a patched local checkout. SimSIMD *is* NumKong — the repo was
//! renamed upstream and the `simsimd` crate is frozen at 6.5.x — so the two names describe the same project at
//! different ages.
//!
//! Comments mentioning `simsimd` are kept wherever they record a measurement or a mistake, because those were taken
//! against SimSIMD 6.5 and rewriting them would falsify the record. Where such a note is still true of NumKong it now
//! says "the vendor kernel"; where NumKong differs, it says so.
//!
//! The differences that matter here: `SpatialSimilarity` split into [`Angular`], [`Dot`] and [`Euclidean`] (the
//! combined trait survives as a marker with no methods of its own); `cos` became `angular`; the associated `Output`
//! type now varies by element type; `euclidean` is a native kernel rather than something to derive; and NumKong
//! accumulates `f32` inputs in `f64`, which is slower but more accurate than 6.5's `f32` accumulation.

use aerospike_udf_sdk_native::{Error, Record, Result};

use numkong::{bf16, f16, Angular, Dot, Euclidean};

/// How to compare two vectors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Metric {
    /// Cosine **distance**: `1 - cos(θ)`. Zero is identical, two is opposite. Lower is better.
    #[default]
    Cosine,
    /// Inner product. **Higher is better** — this is a similarity, not a distance, and it is the one that
    /// catches people out.
    Dot,
    /// Squared Euclidean. Lower is better, and cheaper than `Euclidean` because it skips the square root — for
    /// ranking it is equivalent, since `sqrt` is monotonic.
    SqEuclidean,
    /// Euclidean. Lower is better. Only worth asking for when the number itself is going to be shown to someone.
    Euclidean,
    /// Cosine distance computed by a plain scalar loop, **as a measurement instrument rather than a metric.**
    ///
    /// It exists so the bench can price the SIMD dependency against the obvious hand-written alternative on the
    /// same data, in the same server, through the same accumulator — the only difference being the kernel. Same
    /// idea as `stats_chunked` in the other example: a variant whose only job is to isolate one variable.
    CosineNaive,
}

impl Metric {
    /// Parse the name a client passes.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "cosine" => Ok(Metric::Cosine),
            "dot" | "ip" | "inner_product" => Ok(Metric::Dot),
            "sqeuclidean" | "l2sq" => Ok(Metric::SqEuclidean),
            "euclidean" | "l2" => Ok(Metric::Euclidean),
            "cosine_naive" => Ok(Metric::CosineNaive),
            other => Err(Error::msg(format!(
                "unknown metric {other:?} - expected cosine, dot, sqeuclidean or euclidean"
            ))),
        }
    }

    /// Whether a **smaller** number means a better match.
    ///
    /// **The one thing in this module that silently returns the wrong answer if it is wrong.** `dot` is a
    /// similarity — the largest inner product is the closest vector — while every other metric here is a
    /// distance. A top-k that got this backwards would return the *k worst* matches and look entirely
    /// plausible doing it, which is why it is a named function with a test rather than an inline `if`.
    pub fn lower_is_better(&self) -> bool {
        match self {
            Metric::Cosine | Metric::SqEuclidean | Metric::Euclidean | Metric::CosineNaive => true,
            Metric::Dot => false,
        }
    }

    /// Compare two vectors of equal length.
    pub fn measure(&self, a: &[f32], b: &[f32]) -> Option<f64> {
        match self {
            Metric::Cosine => f32::cosine(a, b),
            Metric::Dot => f32::dot(a, b),
            Metric::SqEuclidean => f32::sqeuclidean(a, b),
            Metric::Euclidean => f32::sqeuclidean(a, b).map(f64::sqrt),
            Metric::CosineNaive => cosine_naive(a, b),
        }
    }

    /// The score to rank by, arranged so **larger is always worse**.
    ///
    /// This is where the similarity/distance inversion lives, and it lives here so that everything downstream —
    /// the heap, the merge, the ordering — has exactly one rule and no special cases.
    pub fn score(&self, measured: f64) -> f64 {
        if self.lower_is_better() {
            measured
        } else {
            -measured
        }
    }

    /// Undo [`score`](Metric::score), for reporting the number a user actually asked about.
    pub fn unscore(&self, score: f64) -> f64 {
        self.score(score)
    }
}

/// Cosine distance, written the way anyone would write it without a library.
///
/// Not a fallback and not dead code: it is the bench's control. LLVM will autovectorise this to some degree, which
/// is exactly what makes it a fair comparison — the question is not "SIMD versus a loop", it is "hand-written
/// kernels versus what the compiler does with the obvious code".
fn cosine_naive(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }

    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += (*x as f64) * (*y as f64);
        na += (*x as f64) * (*x as f64);
        nb += (*y as f64) * (*y as f64);
    }

    if na == 0.0 || nb == 0.0 {
        return Some(1.0);
    }

    Some(1.0 - dot / (na.sqrt() * nb.sqrt()))
}

/// Why a record could not take part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skipped {
    /// No such bin, or it holds neither a blob nor a list.
    NoVector,
    /// A vector, but not the same byte length as the query — which now means the same dimensionality *and* the
    /// same element type, because the query is kept as the caller's own bytes.
    WrongDimension,
}

/// What the elements of a vector are.
///
/// **Declared by the caller, because nothing in the data says.** A blob is a pointer and a length; `f32` has no
/// invalid bit patterns, so any four aligned bytes are *some* float and no amount of inspection can tell a
/// vector of floats from a thumbnail of the right size. Before this existed the element type was assumed to be
/// `f32`, and a blob whose length happened to match the query returned a plausible distance computed from
/// nonsense.
///
/// Declaring it does not make a lie impossible — it makes one the caller's, and it makes the length check exact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DType {
    #[default]
    F32,
    F64,
    /// Half precision, IEEE 754 binary16. Two bytes an element, and the usual storage format for a large index.
    F16,
    /// Brain float: the same two bytes spent on exponent rather than mantissa. **Not interchangeable with
    /// `F16`** — the same sixteen bits mean different numbers, and nothing in the data distinguishes them.
    BF16,
    /// Quantized. The vendor kernel measures these directly rather than widening them first.
    I8,
    /// Four-byte integers — counts and histograms rather than embeddings.
    ///
    /// **The one element type here with no vendor kernel** - true of SimSIMD 6.5 and still true of NumKong, which
    /// has i8/u8/i4x2/u4x2 and four FP8 formats but no i32 - so its loops are hand-written below.
    I32,
}

impl DType {
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "f32" | "float" | "float32" => Ok(DType::F32),
            "f64" | "double" | "float64" => Ok(DType::F64),
            "f16" | "half" | "float16" => Ok(DType::F16),
            "bf16" | "bfloat16" => Ok(DType::BF16),
            "i8" | "int8" => Ok(DType::I8),
            "i32" | "int32" => Ok(DType::I32),
            other => Err(Error::msg(format!(
                "unknown element type {other:?} - expected f32, f64, f16, bf16, i8 or i32"
            ))),
        }
    }

    /// Bytes per element — which is also the alignment every one of these needs.
    pub fn size(&self) -> usize {
        match self {
            DType::F64 => 8,
            DType::F32 | DType::I32 => 4,
            DType::F16 | DType::BF16 => 2,
            DType::I8 => 1,
        }
    }

}

// **Why there is no `pad_for_alignment` here, though it is the obvious thing to reach for.**
//
// Since a borrowed blob is never aligned, the tempting fix is to pad at write time: leave `size - 1` slack bytes and
// have the reader skip forward to the next boundary with `align_offset`. It does not work, and the reason is short.
//
// The writer must place the elements at a **fixed offset from the blob's start**. The reader needs them at an
// **address that is 0 mod 4**. Those agree only if the blob's own start address mod 4 is known when writing - and it
// is not: it depends on the record's flat layout *and* on where that block happens to sit, which differs between a
// device read buffer and an in-memory allocation for the very same record. When a blob does start aligned,
// `align_offset` returns 0 and the reader eats the slack bytes as data.
//
// A test caught that at the first of the four offsets it tried. Nothing on the write side can fix it: alignment is a
// read-side problem, and the two read-side answers are the ones below - copy it, or read it where it lies.
//
// **And on this hardware, copying to get alignment costs more than it saves.** One 768-dimension cosine, measured by
// `kernel_paths_are_priced_against_each_other` on the dev VM (Neoverse-N1):
//
//     aligned, simsimd                       235 ns
//     unaligned, lane-parallel kernel        354 ns
//     copy into an aligned buffer, simsimd   367 ns   (the copy is 132 of that)
//
// So reading in place wins by 13 ns, and would win by 118 if the bytes arrived aligned for free. Worth knowing: the
// same benchmark on an Apple M-series ranks the last two the *other* way round, because its memcpy is much cheaper
// relative to its SIMD - 92 ns rather than 132. The choice here is right for the machine the server runs on, and a
// different one would deserve re-measuring rather than assuming.

/// One distance computation, with the element type and metric already resolved.
///
/// Both slices are **guaranteed by the caller** to be aligned for the element type and of equal length. That is
/// what lets the body be a cast and a kernel call with nothing to check.
pub type Kernel = fn(&[u8], &[u8]) -> Option<f64>;

/// Reinterpret aligned bytes as elements.
///
/// # Safety
///
/// `bytes` must be aligned for `T` and its length a multiple of `size_of::<T>()`. Every caller is a kernel
/// below, and every kernel is reached only through [`Measurer::distance`], which checks both.
unsafe fn cast<T>(bytes: &[u8]) -> &[T] {
    core::slice::from_raw_parts(
        bytes.as_ptr().cast::<T>(),
        bytes.len() / core::mem::size_of::<T>(),
    )
}

/// One kernel per (metric, element type). Two lines each, so the table is generated rather than written.
///
/// The trait is named per entry because NumKong splits what SimSIMD had as one `SpatialSimilarity` into `Angular`,
/// `Dot` and `Euclidean` - the combined trait still exists but is only a marker, with no methods of its own.
///
/// Each kernel also has to normalise the return type. NumKong's associated `Output` varies by element type: `f64` for
/// `f32`/`f64`, `f32` for the half formats, and `i32`/`u32` for the integer ones. This table promises `f64`, so every
/// entry converts.
macro_rules! kernels {
    ($($fname:ident = $t:ty : $trait:ident :: $method:ident),* $(,)?) => {
        $(
            fn $fname(a: &[u8], b: &[u8]) -> Option<f64> {
                // SAFETY: `Measurer::distance` checked alignment and equal length before reaching here.
                let (x, y) = unsafe { (cast::<$t>(a), cast::<$t>(b)) };

                <$t as $trait>::$method(x, y).map(|v| v as f64)
            }
        )*
    };
}

// The casts below reinterpret stored bytes as these types, which is sound only if each is exactly the width its
// name claims. Checked rather than assumed: a wrapper type that grew a tag or a padding byte would otherwise
// silently read every second element.
const _: () = assert!(core::mem::size_of::<f16>() == 2);
const _: () = assert!(core::mem::size_of::<bf16>() == 2);

// `euclidean` is in this table rather than derived, because NumKong has a native kernel for it. SimSIMD did too and
// this file did not use it - `euclidean_of!` below computed `sqrt(sqeuclidean)` instead, bypassing a vendor kernel for
// no reason. That macro now only covers the hand-written types, which genuinely have no vendor euclidean.
kernels! {
    cosine_f32 = f32 : Angular::angular,
    dot_f32 = f32 : Dot::dot,
    sqeuclidean_f32 = f32 : Euclidean::sqeuclidean,
    euclidean_f32 = f32 : Euclidean::euclidean,

    cosine_f64 = f64 : Angular::angular,
    dot_f64 = f64 : Dot::dot,
    sqeuclidean_f64 = f64 : Euclidean::sqeuclidean,
    euclidean_f64 = f64 : Euclidean::euclidean,

    cosine_i8 = i8 : Angular::angular,
    dot_i8 = i8 : Dot::dot,
    sqeuclidean_i8 = i8 : Euclidean::sqeuclidean,
    euclidean_i8 = i8 : Euclidean::euclidean,

    cosine_f16 = f16 : Angular::angular,
    dot_f16 = f16 : Dot::dot,
    sqeuclidean_f16 = f16 : Euclidean::sqeuclidean,
    euclidean_f16 = f16 : Euclidean::euclidean,

    cosine_bf16 = bf16 : Angular::angular,
    dot_bf16 = bf16 : Dot::dot,
    sqeuclidean_bf16 = bf16 : Euclidean::sqeuclidean,
    euclidean_bf16 = bf16 : Euclidean::euclidean,
}

/// The two bytes of a half-precision value.
///
/// **No `unsafe` here any more.** SimSIMD's wrapper types exposed no way to get at the bits, so this used to be a
/// `transmute` guarded by the `size_of` assertions above. NumKong declares `pub struct f16(pub u16)`, so the bits are
/// simply a field.
///
/// Native-endian, because the packed-blob path reads these bytes back as a `u16` through a pointer cast, and the two
/// have to agree about byte order.
fn f16_bytes(x: f16) -> [u8; 2] {
    x.0.to_ne_bytes()
}

fn bf16_bytes(x: bf16) -> [u8; 2] {
    x.0.to_ne_bytes()
}

/// `i32` by hand, because no vendor kernel exists for it: SimSIMD 6.5 implemented `SpatialSimilarity` for f32, f64,
/// f16, bf16 and i8, and NumKong adds u8, i4x2, u4x2 and the FP8 formats - but neither has i32.
///
/// Plain loops rather than widening into `f32` first, which would have cost a conversion pass per record *and*
/// been lossy: an `i32` above 2^24 does not survive a round trip through `f32`.
///
/// **The benchmark is what makes this acceptable rather than a compromise.** A hand-written scalar cosine measured
/// 28 ms against `simsimd`'s 23 ms over 50,000 records — 1.2x, because LLVM autovectorises the obvious loop
/// rather well. So the cost of having no kernel for this type is small and *measured*, not assumed.
///
/// Accumulated in `f64`, as `simsimd` does for `i8`. Two 32-bit integers multiply to 62 bits and summing hundreds
/// of those overflows `i64`; `f64` gives up exactness above 2^53 for a range that cannot overflow, which is the
/// right way round for a distance.
fn i32_parts(a: &[i32], b: &[i32]) -> Option<(f64, f64, f64)> {
    if a.len() != b.len() {
        return None;
    }

    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        let (x, y) = (*x as f64, *y as f64);

        dot += x * y;
        na += x * x;
        nb += y * y;
    }

    Some((dot, na, nb))
}

fn cosine_i32(a: &[u8], b: &[u8]) -> Option<f64> {
    // SAFETY: alignment and equal length were checked by `Measurer::measure_bytes`.
    let (x, y) = unsafe { (cast::<i32>(a), cast::<i32>(b)) };
    let (dot, na, nb) = i32_parts(x, y)?;

    if na == 0.0 || nb == 0.0 {
        // Matching `simsimd`, which calls the cosine distance to a zero vector 1 rather than undefined.
        return Some(1.0);
    }

    Some(1.0 - dot / (na.sqrt() * nb.sqrt()))
}

fn dot_i32(a: &[u8], b: &[u8]) -> Option<f64> {
    // SAFETY: as above.
    let (x, y) = unsafe { (cast::<i32>(a), cast::<i32>(b)) };

    i32_parts(x, y).map(|(dot, _, _)| dot)
}

fn sqeuclidean_i32(a: &[u8], b: &[u8]) -> Option<f64> {
    // SAFETY: as above.
    let (x, y) = unsafe { (cast::<i32>(a), cast::<i32>(b)) };

    if x.len() != y.len() {
        return None;
    }

    let mut sum = 0.0f64;

    for (p, q) in x.iter().zip(y.iter()) {
        let d = *p as f64 - *q as f64;

        sum += d * d;
    }

    Some(sum)
}

/// Euclidean is the square root of squared Euclidean, so it is the same kernel with one more instruction — not
/// three more entries in the table.
macro_rules! euclidean_of {
    ($($fname:ident = $inner:ident),* $(,)?) => {
        $(
            fn $fname(a: &[u8], b: &[u8]) -> Option<f64> {
                $inner(a, b).map(f64::sqrt)
            }
        )*
    };
}

// Only `i32` now: it is the one element type with no vendor kernel, so it is also the only one whose euclidean has to
// be derived from its squared form.
euclidean_of! {
    euclidean_i32 = sqeuclidean_i32,
}

/// The bench's control kernel. `f32` only — it exists to price `simsimd` against the obvious loop, and pricing
/// it once is enough.
fn cosine_naive_f32(a: &[u8], b: &[u8]) -> Option<f64> {
    // SAFETY: as above.
    let (x, y) = unsafe { (cast::<f32>(a), cast::<f32>(b)) };

    cosine_naive(x, y)
}

/// An over-aligned scratch buffer, used only when a record's bytes are not aligned for their element type.
///
/// Backed by `Vec<f64>` because its 8-byte alignment satisfies every element type here, and allocated once per
/// accumulator — so the unaligned path costs a `memcpy` and never an allocation.
#[derive(Debug, Default)]
pub struct AlignedBuf {
    words: Vec<f64>,
}

impl AlignedBuf {
    /// Copy `bytes` in and hand back an aligned view of them.
    pub fn fill(&mut self, bytes: &[u8]) -> &[u8] {
        let words = bytes.len().div_ceil(core::mem::size_of::<f64>());

        self.words.clear();
        self.words.resize(words, 0.0);

        // SAFETY: `words * 8` bytes were just reserved, and `f64` has no invalid bit patterns, so writing
        // arbitrary bytes over them is sound. The pointer is 8-aligned because it came from a `Vec<f64>`.
        let dst = unsafe {
            core::slice::from_raw_parts_mut(
                self.words.as_mut_ptr().cast::<u8>(),
                words * core::mem::size_of::<f64>(),
            )
        };

        dst[..bytes.len()].copy_from_slice(bytes);

        &dst[..bytes.len()]
    }

    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: as above, read-only.
        unsafe {
            core::slice::from_raw_parts(
                self.words.as_ptr().cast::<u8>(),
                self.words.len() * core::mem::size_of::<f64>(),
            )
        }
    }
}

/// A query vector, a metric and an element type, resolved to one function pointer.
///
/// Built once — in an aggregation's `configure`, or per call for a record UDF — and then asked for distances with
/// no dispatch of any kind.
#[derive(Debug)]
pub struct Measurer {
    kernel: Kernel,
    metric: Metric,
    dtype: DType,

    /// The caller's query bytes, copied into an aligned buffer once so that no record's kernel call has to
    /// re-check the *query* side.
    query: AlignedBuf,
    query_len: usize,

    /// How often each path was taken. `Cell` because `distance` takes `&self` - the accumulator that owns this is
    /// `&mut`, so there is no sharing, and a counter is not worth widening the signature for.
    aligned: core::cell::Cell<u64>,
    copied: core::cell::Cell<u64>,
}

impl Measurer {
    /// Resolve everything that is constant for a query. **The only `match` on metric or element type is here.**
    pub fn new(metric: Metric, dtype: DType, query: &[u8]) -> Result<Self> {
        let size = dtype.size();

        if query.is_empty() || query.len() % size != 0 {
            return Err(Error::msg(format!(
                "a query vector of {dtype:?} must be a whole number of elements, got {} bytes",
                query.len()
            )));
        }

        // Four metrics across six element types. **One `match`, executed once** — see the note at the top of
        // this file about nothing being decided per record.
        macro_rules! pick {
            ($cos:ident, $dot:ident, $sq:ident, $eu:ident) => {
                match metric {
                    Metric::Cosine => $cos as Kernel,
                    Metric::Dot => $dot as Kernel,
                    Metric::SqEuclidean => $sq as Kernel,
                    Metric::Euclidean => $eu as Kernel,
                    Metric::CosineNaive => {
                        return Err(Error::msg(format!(
                            "cosine_naive is the benchmark's f32 control and has no {dtype:?} kernel"
                        )))
                    }
                }
            };
        }

        let kernel: Kernel = match dtype {
            DType::F32 if metric == Metric::CosineNaive => cosine_naive_f32,
            DType::F32 => pick!(cosine_f32, dot_f32, sqeuclidean_f32, euclidean_f32),
            DType::F64 => pick!(cosine_f64, dot_f64, sqeuclidean_f64, euclidean_f64),
            DType::F16 => pick!(cosine_f16, dot_f16, sqeuclidean_f16, euclidean_f16),
            DType::BF16 => pick!(cosine_bf16, dot_bf16, sqeuclidean_bf16, euclidean_bf16),
            DType::I8 => pick!(cosine_i8, dot_i8, sqeuclidean_i8, euclidean_i8),
            DType::I32 => pick!(cosine_i32, dot_i32, sqeuclidean_i32, euclidean_i32),
        };

        let mut buf = AlignedBuf::default();

        buf.fill(query);

        Ok(Self {
            kernel,
            metric,
            dtype,
            query: buf,
            query_len: query.len(),
            aligned: core::cell::Cell::new(0),
            copied: core::cell::Cell::new(0),
        })
    }

    /// How many records took the zero-copy path, and how many were copied.
    ///
    /// **Kept because it is the counter that caught a false claim once already.** This file was written asserting
    /// that a bin's bytes are "usually" aligned for their element type, and for a while they were not aligned at
    /// all - see the note at the top. Anything that says "nothing is copied" should be able to prove it.
    pub fn path_counts(&self) -> (u64, u64) {
        (self.aligned.get(), self.copied.get())
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn dtype(&self) -> DType {
        self.dtype
    }

    /// Elements per vector.
    pub fn dimension(&self) -> usize {
        self.query_len / self.dtype.size()
    }

    /// Measure this record's vector against the query.
    ///
    /// Per record: one length comparison, one alignment comparison, one indirect call. Nothing else.
    pub fn distance(
        &self,
        rec: &Record,
        bin: &str,
        scratch: &mut AlignedBuf,
    ) -> Result<core::result::Result<f64, Skipped>> {
        if let Some(bytes) = rec.get_bytes(bin)? {
            return Ok(self.measure_bytes(bytes, scratch));
        }

        // A list of numbers, for data that was not stored as a packed blob. Converted to the declared element
        // type through the scratch buffer, which is the one place a conversion still happens per record.
        let Some(list) = rec.get(bin)?.as_list() else {
            return Ok(Err(Skipped::NoVector));
        };

        let n = list.len() as usize;

        if n != self.dimension() {
            return Ok(Err(Skipped::WrongDimension));
        }

        let mut doubles = vec![0.0f64; n];

        if list.doubles_into(0, &mut doubles)? != n {
            return Ok(Err(Skipped::NoVector));
        }

        let bytes: Vec<u8> = match self.dtype {
            DType::F64 => doubles.iter().flat_map(|d| d.to_le_bytes()).collect(),
            DType::F32 => doubles.iter().flat_map(|d| (*d as f32).to_le_bytes()).collect(),
            DType::F16 => doubles
                .iter()
                .flat_map(|d| f16_bytes(f16::from_f32(*d as f32)))
                .collect(),
            DType::BF16 => doubles
                .iter()
                .flat_map(|d| bf16_bytes(bf16::from_f32(*d as f32)))
                .collect(),
            DType::I8 => doubles.iter().map(|d| *d as i8 as u8).collect(),
            DType::I32 => doubles.iter().flat_map(|d| (*d as i32).to_le_bytes()).collect(),
        };

        Ok(self.measure_bytes(&bytes, scratch))
    }

    /// As [`distance`](Measurer::distance), for a caller that only wants the number and not the skip reason.
    ///
    /// Exists for the stage probes: `probe_dist` has to do exactly what `accumulate` does minus the heap, and
    /// going through `distance` would be that.
    pub fn measure_bytes_at(
        &self,
        rec: &Record,
        bin: &str,
        scratch: &mut AlignedBuf,
    ) -> Result<core::result::Result<f64, Skipped>> {
        self.distance(rec, bin, scratch)
    }

    /// Measure raw bytes against the query.
    ///
    /// Per call: **one length comparison, one alignment comparison, one indirect call.** Nothing is decided on
    /// the data.
    ///
    /// The length comparison does the work of two checks at once. The query is kept as the caller's own bytes, so
    /// equal byte length means equal element count *under the declared type* - where the first version of this
    /// file tested `len % 4` and `len / 4` against an assumed `f32`, and so accepted an `i8` vector of four times
    /// the dimension as though it were floats.
    ///
    /// It does not make a mismatch impossible: 3072 bytes are 768 `f32`s and also 384 `f64`s, so a caller who
    /// declares the wrong type of two that happen to share a byte length still gets a number. What it removes is
    /// the *accident* - the type is now something the caller stated rather than something this code assumed.
    pub fn measure_bytes(
        &self,
        bytes: &[u8],
        scratch: &mut AlignedBuf,
    ) -> core::result::Result<f64, Skipped> {
        if bytes.len() != self.query_len {
            return Err(Skipped::WrongDimension);
        }

        // **Counted, because the claim needs proving.** A blob particle's bytes are aligned only because the host
        // hands out a value it allocated rather than the stored particle; that is a property of the host, not of the
        // data, and it has been the other way round within this file's history.
        let query = &self.query.as_bytes()[..self.query_len];

        let aligned = if bytes.as_ptr() as usize % self.dtype.size() == 0 {
            self.aligned.set(self.aligned.get() + 1);

            bytes
        } else {
            self.copied.set(self.copied.get() + 1);

            scratch.fill(bytes)
        };

        match (self.kernel)(aligned, query) {
            Some(d) => Ok(d),
            None => Err(Skipped::WrongDimension),
        }
    }
}

/// This record's vector length in elements, given a declared element type, or `None` if it has no vector.
///
pub fn dimension(rec: &Record, bin: &str, dtype: DType) -> Result<Option<usize>> {
    if let Some(bytes) = rec.get_bytes(bin)? {
        return Ok(if bytes.len() % dtype.size() == 0 {
            Some(bytes.len() / dtype.size())
        } else {
            None
        });
    }

    Ok(rec.get(bin)?.as_list().map(|l| l.len() as usize))
}

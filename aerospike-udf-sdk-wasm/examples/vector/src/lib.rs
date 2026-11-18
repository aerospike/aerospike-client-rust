// Copyright 2015-2026 Aerospike, Inc.
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

//! Exact top-k over packed float vectors, as a WASM UDF in Rust.
//!
//! The same module exists in C (`aerospike-udf-sdk-wasm-c/examples/vector`) and Go
//! (`aerospike-udf-sdk-wasm-go/examples/vector`), **written to the same algorithm step for step**, so the
//! three can be timed against each other and against the Lua and native versions of the native SDK's
//! vector example on the same records. Every arithmetic choice below is therefore deliberate and shared:
//!
//! - the blob is copied **straight into an `f32` array** by the host (one import), with no decode loop;
//! - distances accumulate in `f64` from `f32` elements in one fixed order, so the three modules produce
//!   bit-identical scores and the exactness test can compare digests;
//! - **larger is worse**: `dot` is negated, so the bounded structure has one rule;
//! - ties break on the digest, which is fetched only for a candidate that beats the worst kept hit;
//! - the state is configured on the first record of a stream and the arguments are not looked at again.
//!
//! ```text
//! topk("v", <query blob>, "cosine", 10)     stream: the 10 nearest, one partial per stream
//! vec_distance("v", <query blob>, "dot")    record: one number
//! ```
//!
//! A partial is `{hits: [{score, digest, key}], skipped_dim, skipped_none}`, the shape the native module's
//! `to_partial` writes, so one client-side merge reads all of them.
//!
//! Raw exports rather than the `stream!` macro, on purpose: the macro decodes the typed arguments on every
//! call, and this module wants the arguments once per stream and nothing per record but the work.

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, Args, Blob, Error, Record, Text, Value};

const MAX_DIM: usize = 2048;
/// Bounded by the per-call handle table: each hit of the partial is one built map.
const MAX_K: usize = 200;
const DIGEST: usize = 20;
const KEY_MAX: usize = 64;

#[derive(Clone, Copy, PartialEq)]
enum Metric {
    Cosine,
    Dot,
    SqEuclidean,
    Euclidean,
}

impl Metric {
    fn parse(name: &str) -> Option<Metric> {
        match name {
            "" | "cosine" => Some(Metric::Cosine),
            "dot" | "ip" | "inner_product" => Some(Metric::Dot),
            "sqeuclidean" | "l2sq" => Some(Metric::SqEuclidean),
            "euclidean" | "l2" => Some(Metric::Euclidean),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct Hit {
    score: f64,
    digest: [u8; DIGEST],
    key_len: usize,
    key: [u8; KEY_MAX],
}

const NO_HIT: Hit = Hit {
    score: 0.0,
    digest: [0; DIGEST],
    key_len: 0,
    key: [0; KEY_MAX],
};

struct State {
    configured: bool,
    bin_len: usize,
    bin: [u8; 32],
    metric: Metric,
    dim: usize,
    query: [f32; MAX_DIM],
    qnorm: f64,
    k: usize,
    n: usize,
    hits: [Hit; MAX_K],
    skipped_dim: i64,
    skipped_none: i64,
}

static mut STATE: State = State {
    configured: false,
    bin_len: 0,
    bin: [0; 32],
    metric: Metric::Cosine,
    dim: 0,
    query: [0.0; MAX_DIM],
    qnorm: 0.0,
    k: 10,
    n: 0,
    hits: [NO_HIT; MAX_K],
    skipped_dim: 0,
    skipped_none: 0,
};

/// The record's vector, copied by the host straight into the float array.
static mut VEC: [f32; MAX_DIM] = [0.0; MAX_DIM];

fn state() -> &'static mut State {
    // One instance per stream, one record at a time: there is never a second borrow.
    unsafe { &mut *core::ptr::addr_of_mut!(STATE) }
}

fn vec_bytes() -> &'static mut [u8] {
    unsafe { core::slice::from_raw_parts_mut(core::ptr::addr_of_mut!(VEC).cast::<u8>(), MAX_DIM * 4) }
}

fn vec_floats(dim: usize) -> &'static [f32] {
    unsafe { core::slice::from_raw_parts(core::ptr::addr_of!(VEC).cast::<f32>(), dim) }
}

/// Read the arguments once: bin, query, metric, k.
fn configure(s: &mut State, args: &Args) -> Result<(), Error> {
    let mut buf = [0u8; 32];
    let first = args.get(0).ok_or(Error::Custom("topk: bin name missing"))?;
    let bin = first.as_str(&mut buf)?;

    s.bin_len = bin.len();
    s.bin[..bin.len()].copy_from_slice(bin.as_bytes());

    let query = match args.get(1) {
        Some(Value::Blob(b)) => b,
        _ => return Err(Error::Custom("topk: the query must be a blob of f32")),
    };

    if query.len() % 4 != 0 || query.len() / 4 > MAX_DIM {
        return Err(Error::Custom("topk: the query is not a vector of f32 this module can hold"));
    }

    s.dim = query.len() / 4;

    let qbytes = unsafe { core::slice::from_raw_parts_mut(s.query.as_mut_ptr().cast::<u8>(), MAX_DIM * 4) };

    query.bytes(qbytes)?;

    let mut qn = 0.0f64;

    for &q in &s.query[..s.dim] {
        let b = f64::from(q);

        qn += b * b;
    }

    s.qnorm = sqrt(qn);

    let mut mbuf = [0u8; 32];
    let third = args.get(2);
    let metric = match &third {
        Some(v) => v.as_str(&mut mbuf)?,
        None => "cosine",
    };

    s.metric = Metric::parse(metric).ok_or(Error::Custom("topk: unknown metric"))?;
    s.k = match args.get(3) {
        Some(Value::Int(k)) => (k.max(1) as usize).min(MAX_K),
        _ => 10,
    };
    s.n = 0;
    s.configured = true;

    Ok(())
}

fn sqrt(x: f64) -> f64 {
    // Correctly rounded, so bit for bit what the C and Go modules get from the `f64.sqrt` instruction.
    libm::sqrt(x)
}

/// The measured distance (or similarity, for `dot`) between the record's vector and the query.
///
/// **Four accumulators, lane `k` holding the elements whose index is `k` mod 4.** A single `f64` sum cannot
/// be vectorised - floating-point addition does not reassociate - so the loop is written as four independent
/// sums the compiler can keep in SIMD registers when built with `simd128` (`RUSTFLAGS="-C
/// target-feature=+simd128"`; off by default, scalar then), folded at the
/// end in one fixed order. The same order is in the C and Go modules and in the test's local computation,
/// which is what keeps the four bit-identical.
fn measure(metric: Metric, v: &[f32], q: &[f32], qnorm: f64) -> f64 {
    let n = v.len();
    let full = n - n % 4;

    match metric {
        Metric::Cosine => {
            let mut d = [0.0f64; 4];
            let mut s = [0.0f64; 4];
            let mut i = 0;

            while i < full {
                for k in 0..4 {
                    let a = f64::from(v[i + k]);
                    let b = f64::from(q[i + k]);

                    d[k] += a * b;
                    s[k] += a * a;
                }

                i += 4;
            }

            for k in 0..n - full {
                let a = f64::from(v[full + k]);
                let b = f64::from(q[full + k]);

                d[k] += a * b;
                s[k] += a * a;
            }

            let dot = ((d[0] + d[1]) + d[2]) + d[3];
            let na = ((s[0] + s[1]) + s[2]) + s[3];

            if na == 0.0 || qnorm == 0.0 {
                1.0
            } else {
                1.0 - dot / (sqrt(na) * qnorm)
            }
        }
        Metric::Dot => {
            let mut d = [0.0f64; 4];
            let mut i = 0;

            while i < full {
                for k in 0..4 {
                    d[k] += f64::from(v[i + k]) * f64::from(q[i + k]);
                }

                i += 4;
            }

            for k in 0..n - full {
                d[k] += f64::from(v[full + k]) * f64::from(q[full + k]);
            }

            ((d[0] + d[1]) + d[2]) + d[3]
        }
        Metric::SqEuclidean | Metric::Euclidean => {
            let mut s = [0.0f64; 4];
            let mut i = 0;

            while i < full {
                for k in 0..4 {
                    let e = f64::from(v[i + k]) - f64::from(q[i + k]);

                    s[k] += e * e;
                }

                i += 4;
            }

            for k in 0..n - full {
                let e = f64::from(v[full + k]) - f64::from(q[full + k]);

                s[k] += e * e;
            }

            let sum = ((s[0] + s[1]) + s[2]) + s[3];

            if metric == Metric::Euclidean {
                sqrt(sum)
            } else {
                sum
            }
        }
    }
}

/// The score to rank by: larger is worse.
fn score(metric: Metric, measured: f64) -> f64 {
    if metric == Metric::Dot {
        -measured
    } else {
        measured
    }
}

/// The record's vector into `VEC`, or why not: `Ok(None)` for a missing or wrong-size bin, counted.
fn read_vector(rec: &Record, s: &mut State) -> Result<bool, Error> {
    let bin = core::str::from_utf8(&s.bin[..s.bin_len]).unwrap_or("v");

    let blob = match rec.bin(bin) {
        Ok(Value::Blob(b)) => b,
        Ok(_) | Err(Error::Absent) => {
            s.skipped_none += 1;
            return Ok(false);
        }
        Err(e) => return Err(e),
    };

    if blob.len() != s.dim * 4 {
        s.skipped_dim += 1;
        return Ok(false);
    }

    blob.bytes(vec_bytes())?;

    Ok(true)
}

fn itoa(mut n: i64, out: &mut [u8; KEY_MAX]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = 0;
    let negative = n < 0;

    if n == 0 {
        out[0] = b'0';
        return 1;
    }

    while n != 0 {
        tmp[i] = b'0' + (n % 10).unsigned_abs() as u8;
        n /= 10;
        i += 1;
    }

    let mut len = 0;

    if negative {
        out[0] = b'-';
        len = 1;
    }

    while i > 0 {
        i -= 1;
        out[len] = tmp[i];
        len += 1;
    }

    len
}

/// Offer a candidate: kept if it beats the worst of the k, ties broken on the digest.
fn offer(rec: &Record, s: &mut State, score: f64) -> Result<(), Error> {
    if s.n == s.k && score > s.hits[s.n - 1].score {
        return Ok(());
    }

    let mut hit = NO_HIT;

    hit.score = score;
    rec.digest(&mut hit.digest)?;

    if s.n == s.k && score == s.hits[s.n - 1].score && hit.digest >= s.hits[s.n - 1].digest {
        return Ok(());
    }

    let mut kbuf = [0u8; KEY_MAX];

    hit.key_len = match rec.key(&mut kbuf) {
        Ok(Value::Int(i)) => itoa(i, &mut hit.key),
        Ok(Value::String(Text::Guest(t))) => {
            let n = t.len().min(KEY_MAX);

            hit.key[..n].copy_from_slice(&t.as_bytes()[..n]);
            n
        }
        Ok(Value::Blob(Blob::Guest(b))) => {
            let n = b.len().min(KEY_MAX);

            hit.key[..n].copy_from_slice(&b[..n]);
            n
        }
        _ => 0,
    };

    // Insert in order; the worst falls off the end once the structure is full.
    let mut at = s.n;

    while at > 0 && (hit.score, hit.digest) < (s.hits[at - 1].score, s.hits[at - 1].digest) {
        at -= 1;
    }

    let last = if s.n < s.k { s.n } else { s.k - 1 };

    if at > last {
        return Ok(());
    }

    let mut i = last;

    while i > at {
        s.hits[i] = s.hits[i - 1];
        i -= 1;
    }

    s.hits[at] = hit;

    if s.n < s.k {
        s.n += 1;
    }

    Ok(())
}

fn record(args: i32, argc: i32) -> Result<(), Error> {
    // These exports are written without the macros, which would pass the record in; so it is asked for.
    let rec = Record::current();
    let s = state();

    if !s.configured {
        configure(s, &Args::from_handle(args, argc))?;
    }

    if !read_vector(&rec, s)? {
        return Ok(());
    }

    let measured = measure(s.metric, vec_floats(s.dim), &s.query[..s.dim], s.qnorm);

    offer(&rec, s, score(s.metric, measured))
}

fn end() -> Result<Value<'static>, Error> {
    let s = state();
    let out = as_udf::map(3)?;
    let hits = as_udf::list(s.n)?;

    for hit in &s.hits[..s.n] {
        let entry = as_udf::map(3)?;
        let key = core::str::from_utf8(&hit.key[..hit.key_len]).unwrap_or("");

        entry.extend([
            ("score", Value::Double(hit.score)),
            ("digest", Value::from(&hit.digest[..])),
            ("key", Value::from(key)),
        ])?;
        hits.push(entry)?;
    }

    out.insert("hits", hits)?
        .insert("skipped_dim", s.skipped_dim)?
        .insert("skipped_none", s.skipped_none)?;

    Ok(out.into_value())
}

#[no_mangle]
pub extern "C" fn topk_record(args: i32, argc: i32) -> i32 {
    match record(args, argc) {
        Ok(()) => 0,
        Err(e) => {
            as_udf::fail(e.message());
            1
        }
    }
}

#[no_mangle]
pub extern "C" fn topk_end(_args: i32, _argc: i32) -> i32 {
    match end().and_then(|v| as_udf::result(&v)) {
        Ok(()) => 0,
        Err(e) => {
            as_udf::fail(e.message());
            1
        }
    }
}

/// The distance from this record's vector to the query: one record, one number, nil when the record has no
/// vector of the query's size.
#[no_mangle]
pub extern "C" fn vec_distance(args: i32, argc: i32) -> i32 {
    let rec = Record::current();
    let s = state();
    let r = configure(s, &Args::from_handle(args, argc)).and_then(|()| {
        if !read_vector(&rec, s)? {
            return Ok(None);
        }

        Ok(Some(measure(s.metric, vec_floats(s.dim), &s.query[..s.dim], s.qnorm)))
    });

    match r {
        Ok(Some(d)) => {
            let _ = as_udf::result(&Value::Double(d));
            0
        }
        Ok(None) => {
            let _ = as_udf::result(&Value::Nil);
            0
        }
        Err(e) => {
            as_udf::fail(e.message());
            1
        }
    }
}

static SIG_TOPK: &[u8] = b"stream topk { record(bin: str, query: bytes, metric: str, k?: int) end() }\0";
static SIG_DIST: &[u8] = b"fn vec_distance(bin: str, query: bytes, metric: str) -> value\0";

#[export_name = "__udf_sig_topk"]
pub extern "C" fn sig_topk() -> i32 {
    SIG_TOPK.as_ptr() as usize as i32
}

#[export_name = "__udf_sig_vec_distance"]
pub extern "C" fn sig_vec_distance() -> i32 {
    SIG_DIST.as_ptr() as usize as i32
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

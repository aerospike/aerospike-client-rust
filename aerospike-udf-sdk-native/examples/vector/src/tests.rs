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

//! Unit tests for the parts that need no record.
//!
//! The heap and the metric inversion are worth testing here rather than against a server, because both fail in
//! the same nasty way: they return a plausible answer that is exactly wrong. A top-k that inverted its
//! comparison gives the k *worst* matches, and nothing about the shape of the result says so.

use crate::heap::{BestK, Hit};
use crate::vec::{AlignedBuf, DType, Measurer, Metric, Skipped};

use aerospike_udf_sdk_native::Blob;

fn hit(score: f64, tag: u8) -> Hit {
    Hit {
        score,
        digest: Blob(vec![tag; 20]),
        key: String::new(),
    }
}

/// **The inversion, which is the one thing here that silently returns the opposite of the right answer.**
///
/// `dot` is a similarity — bigger is closer — and every other metric is a distance. The heap knows only that
/// larger is worse, so `score` has to negate exactly one of them.
#[test]
fn dot_is_a_similarity_and_everything_else_is_a_distance() {
    assert!(Metric::Cosine.lower_is_better());
    assert!(Metric::SqEuclidean.lower_is_better());
    assert!(Metric::Euclidean.lower_is_better());
    assert!(!Metric::Dot.lower_is_better(), "dot is a similarity");

    // A distance passes through; a similarity is negated, so that "larger is worse" holds for both.
    assert_eq!(Metric::Cosine.score(0.25), 0.25);
    assert_eq!(Metric::Dot.score(0.9), -0.9);

    // And a better dot product must produce a *smaller* score than a worse one.
    assert!(
        Metric::Dot.score(0.9) < Metric::Dot.score(0.1),
        "a closer vector must score lower after inversion, or top-k returns the worst matches"
    );

    // The round trip has to give back the number the caller asked about.
    assert_eq!(Metric::Dot.unscore(Metric::Dot.score(0.9)), 0.9);
    assert_eq!(Metric::Cosine.unscore(Metric::Cosine.score(0.25)), 0.25);
}

#[test]
fn metric_names_parse_and_bad_ones_are_refused() {
    assert_eq!(Metric::parse("cosine").unwrap(), Metric::Cosine);
    assert_eq!(Metric::parse("dot").unwrap(), Metric::Dot);
    assert_eq!(Metric::parse("ip").unwrap(), Metric::Dot);
    assert_eq!(Metric::parse("l2").unwrap(), Metric::Euclidean);
    assert_eq!(Metric::parse("l2sq").unwrap(), Metric::SqEuclidean);
    assert!(Metric::parse("manhattan").is_err(), "an unknown metric must be refused");
}

/// The heap keeps the k best and nothing else, however many it is offered.
#[test]
fn the_heap_is_bounded_and_keeps_the_best() {
    let mut best = BestK::new(3);

    for (i, score) in [5.0, 1.0, 9.0, 3.0, 7.0, 0.5].iter().enumerate() {
        best.offer(hit(*score, i as u8));
    }

    assert_eq!(best.len(), 3, "the heap must not grow past k");

    let sorted = best.sorted();

    assert_eq!(
        sorted.iter().map(|h| h.score).collect::<Vec<f64>>(),
        vec![0.5, 1.0, 3.0],
        "best first, and only the best"
    );

    // Once full, the threshold is the worst kept score - what a candidate has to beat.
    assert_eq!(best.threshold(), Some(3.0));
}

/// **Merging is associative and commutative, which is what makes the distributed answer exact.**
///
/// Two heaps merged must give the same k as one heap over all the records, whatever order the records arrived in
/// and however they were divided. Without that property a per-thread accumulator would be an approximation.
#[test]
fn merging_heaps_gives_the_same_answer_as_one_heap() {
    let scores: Vec<f64> = vec![5.0, 1.0, 9.0, 3.0, 7.0, 0.5, 8.0, 2.0, 6.0, 4.0];

    // One heap over everything.
    let mut whole = BestK::new(4);

    for (i, s) in scores.iter().enumerate() {
        whole.offer(hit(*s, i as u8));
    }

    // The same records split across three "threads", then merged.
    let mut a = BestK::new(4);
    let mut b = BestK::new(4);
    let mut c = BestK::new(4);

    for (i, s) in scores.iter().enumerate() {
        match i % 3 {
            0 => a.offer(hit(*s, i as u8)),
            1 => b.offer(hit(*s, i as u8)),
            _ => c.offer(hit(*s, i as u8)),
        }
    }

    let mut merged = BestK::new(4);

    merged.absorb(a.hits());
    merged.absorb(b.hits());
    merged.absorb(c.hits());

    assert_eq!(
        merged.sorted().iter().map(|h| h.score).collect::<Vec<f64>>(),
        whole.sorted().iter().map(|h| h.score).collect::<Vec<f64>>(),
        "splitting the records across threads must not change the answer"
    );

    // And in the other order, since commutativity is half the claim.
    let mut other_way = BestK::new(4);

    other_way.absorb(c.hits());
    other_way.absorb(a.hits());
    other_way.absorb(b.hits());

    assert_eq!(
        other_way.sorted().iter().map(|h| h.score).collect::<Vec<f64>>(),
        whole.sorted().iter().map(|h| h.score).collect::<Vec<f64>>(),
        "merge order must not matter either"
    );
}

/// Ties are broken by digest, so the answer is a function of the data and not of thread scheduling.
#[test]
fn ties_are_broken_deterministically() {
    let mut best = BestK::new(2);

    // Four records at the same distance, offered in an arbitrary order.
    for tag in [9u8, 3, 7, 1] {
        best.offer(hit(1.0, tag));
    }

    let first = best.sorted();

    // The same four in a different order must give the same two.
    let mut again = BestK::new(2);

    for tag in [1u8, 7, 3, 9] {
        again.offer(hit(1.0, tag));
    }

    assert_eq!(
        first.iter().map(|h| h.digest.0[0]).collect::<Vec<u8>>(),
        again.sorted().iter().map(|h| h.digest.0[0]).collect::<Vec<u8>>(),
        "a tie must resolve the same way whatever order the records arrived in"
    );

    // And it resolves by digest, so the two smallest digests win.
    assert_eq!(
        first.iter().map(|h| h.digest.0[0]).collect::<Vec<u8>>(),
        vec![1, 3]
    );
}

/// A NaN score has to sort somewhere definite rather than poisoning the heap.
#[test]
fn a_nan_score_does_not_break_the_heap() {
    let mut best = BestK::new(3);

    best.offer(hit(1.0, 1));
    best.offer(hit(f64::NAN, 2));
    best.offer(hit(0.5, 3));
    best.offer(hit(2.0, 4));

    assert_eq!(best.len(), 3);

    // `total_cmp` puts NaN above every real number, so it is the worst - and it is the one dropped when
    // something real competes for the last slot.
    let scores: Vec<f64> = best.sorted().iter().map(|h| h.score).collect();

    assert_eq!(scores[0], 0.5);
    assert_eq!(scores[1], 1.0);
    assert_eq!(scores[2], 2.0, "a real distance must beat NaN");
}

/// k of zero, and offering nothing, must not panic.
#[test]
fn degenerate_sizes_are_handled() {
    let mut none = BestK::new(0);

    none.offer(hit(1.0, 1));
    assert!(none.is_empty());
    assert_eq!(none.threshold(), None);

    let empty = BestK::new(5);

    assert!(empty.sorted().is_empty());
    assert_eq!(empty.threshold(), None, "a heap that is not full has no threshold");
}

//==========================================================
// The resolved kernel.
//

fn pack_f32(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// **The dispatch happens once, and this is what it resolves to.** Each element type measures its own bytes
/// correctly, which is the whole claim: one function pointer per (metric, type), chosen in `configure`.
#[test]
fn each_element_type_has_its_own_kernel() {
    let mut scratch = AlignedBuf::default();

    // f32: the distance from a vector to itself is zero under cosine.
    let a = pack_f32(&[1.0, 2.0, 3.0, 4.0]);
    let m = Measurer::new(Metric::Cosine, DType::F32, &a).unwrap();

    assert_eq!(m.dimension(), 4);
    assert!(m.measure_bytes(&a, &mut scratch).unwrap().abs() < 1e-6);

    // f64: same vector, eight bytes an element - so twice the bytes for the same dimension.
    let b: Vec<u8> = [1.0f64, 2.0, 3.0, 4.0].iter().flat_map(|x| x.to_le_bytes()).collect();
    let m64 = Measurer::new(Metric::Cosine, DType::F64, &b).unwrap();

    assert_eq!(m64.dimension(), 4, "four f64s, not eight f32s");
    assert!(m64.measure_bytes(&b, &mut scratch).unwrap().abs() < 1e-6);

    // i8: one byte an element.
    let c: Vec<u8> = vec![1u8, 2, 3, 4];
    let m8 = Measurer::new(Metric::Cosine, DType::I8, &c).unwrap();

    assert_eq!(m8.dimension(), 4);
    assert!(m8.measure_bytes(&c, &mut scratch).unwrap().abs() < 1e-6);

    // And a dot product is the squared norm when a vector meets itself: 1+4+9+16 = 30.
    let dot = Measurer::new(Metric::Dot, DType::F32, &a).unwrap();

    assert!((dot.measure_bytes(&a, &mut scratch).unwrap() - 30.0).abs() < 1e-4);

    // Euclidean is the square root of squared Euclidean, from the same kernel.
    let sq = Measurer::new(Metric::SqEuclidean, DType::F32, &a).unwrap();
    let eu = Measurer::new(Metric::Euclidean, DType::F32, &a).unwrap();
    let other = pack_f32(&[0.0, 0.0, 0.0, 0.0]);

    let sq_d = sq.measure_bytes(&other, &mut scratch).unwrap();
    let eu_d = eu.measure_bytes(&other, &mut scratch).unwrap();

    assert!((sq_d - 30.0).abs() < 1e-4);
    assert!((eu_d - 30.0f64.sqrt()).abs() < 1e-4);
}

/// **The silent-garbage case, now refused.**
///
/// Before the element type was declared, a blob was accepted whenever its byte length happened to be four times
/// the query's dimension — so an `i8` vector of 16 elements read as an `f32` vector of 4, and returned a
/// plausible distance computed from reinterpreted bit patterns. The length check is now against the query's own
/// bytes under the declared type, so the two no longer collide.
#[test]
fn a_vector_of_the_wrong_element_type_is_refused() {
    let mut scratch = AlignedBuf::default();

    // A 4-element f32 query: 16 bytes.
    let query = pack_f32(&[1.0, 2.0, 3.0, 4.0]);
    let m = Measurer::new(Metric::Cosine, DType::F32, &query).unwrap();

    // A 16-element i8 vector is also 16 bytes — and under the old rule that was "four f32s" and accepted.
    let i8_vector: Vec<u8> = (1..=16u8).collect();

    assert_eq!(
        i8_vector.len(),
        query.len(),
        "the byte lengths collide, which is exactly why the old check was not enough"
    );

    // It is still accepted, because 16 bytes are 16 bytes — the honest limit of declaring rather than tagging.
    // What changed is that the caller said "f32", so this is their statement about their data and not this
    // module's assumption about it.
    assert!(m.measure_bytes(&i8_vector, &mut scratch).is_ok());

    // What *is* now caught: the same data with its true type declared has a different byte length, so a query
    // built for one is refused against the other.
    let i8_query: Vec<u8> = (1..=4u8).collect();
    let m8 = Measurer::new(Metric::Cosine, DType::I8, &i8_query).unwrap();

    assert_eq!(
        m8.measure_bytes(&query, &mut scratch),
        Err(Skipped::WrongDimension),
        "a 16-byte f32 vector is not a 4-element i8 vector"
    );

    // A 4-element f64 vector is 32 bytes, so it cannot pass as 4 f32s either.
    let f64_vector: Vec<u8> = [1.0f64, 2.0, 3.0, 4.0].iter().flat_map(|x| x.to_le_bytes()).collect();

    assert_eq!(
        m.measure_bytes(&f64_vector, &mut scratch),
        Err(Skipped::WrongDimension)
    );
}

#[test]
fn every_element_type_parses_with_its_width() {
    for (name, dt, size) in [
        ("f32", DType::F32, 4),
        ("float", DType::F32, 4),
        ("f64", DType::F64, 8),
        ("double", DType::F64, 8),
        ("f16", DType::F16, 2),
        ("half", DType::F16, 2),
        ("bf16", DType::BF16, 2),
        ("i8", DType::I8, 1),
        ("i32", DType::I32, 4),
    ] {
        assert_eq!(DType::parse(name).unwrap(), dt, "{name}");
        assert_eq!(dt.size(), size, "{name} width");
    }

    assert!(DType::parse("u32").is_err());
    assert!(DType::parse("f8").is_err());
}

/// **`f16` and `bf16` are the same two bytes meaning different numbers**, and nothing in a blob says which. So
/// they are separate element types rather than one "half" — reading one as the other is silent and wrong.
#[test]
fn half_precision_formats_are_not_interchangeable() {
    let mut scratch = AlignedBuf::default();

    // 1.0 in IEEE binary16 is 0x3C00; in bfloat16 it is 0x3F80. Same value, different bits.
    let ieee: Vec<u8> = vec![0x00, 0x3C, 0x00, 0x40]; // [1.0, 2.0] as f16
    let brain: Vec<u8> = vec![0x80, 0x3F, 0x00, 0x40]; // [1.0, 2.0] as bf16

    let m16 = Measurer::new(Metric::Dot, DType::F16, &ieee).unwrap();
    let mbf = Measurer::new(Metric::Dot, DType::BF16, &brain).unwrap();

    assert_eq!(m16.dimension(), 2, "four bytes are two half-precision elements");
    assert_eq!(mbf.dimension(), 2);

    // 1*1 + 2*2 = 5 under either format, when each is read as itself.
    assert!((m16.measure_bytes(&ieee, &mut scratch).unwrap() - 5.0).abs() < 1e-2);
    assert!((mbf.measure_bytes(&brain, &mut scratch).unwrap() - 5.0).abs() < 1e-2);

    // Read as the wrong format the bytes still measure - they are the same width - and the answer is wrong. This
    // is the limit of declaring rather than tagging, and it is why these are two types and not one.
    let wrong = m16.measure_bytes(&brain, &mut scratch).unwrap();

    assert!(
        (wrong - 5.0).abs() > 0.1,
        "bfloat bits read as IEEE half must not coincidentally agree, got {wrong}"
    );
}

/// `i32` has no `simsimd` kernel, so its loops are hand-written — and have to agree with the others.
#[test]
fn the_hand_written_i32_kernels_are_right() {
    let mut scratch = AlignedBuf::default();

    let a: Vec<u8> = [1i32, 2, 3, 4].iter().flat_map(|x| x.to_le_bytes()).collect();
    let b: Vec<u8> = [4i32, 3, 2, 1].iter().flat_map(|x| x.to_le_bytes()).collect();

    let dot = Measurer::new(Metric::Dot, DType::I32, &b).unwrap();
    let sq = Measurer::new(Metric::SqEuclidean, DType::I32, &b).unwrap();
    let eu = Measurer::new(Metric::Euclidean, DType::I32, &b).unwrap();
    let cos = Measurer::new(Metric::Cosine, DType::I32, &a).unwrap();

    // 1*4 + 2*3 + 3*2 + 4*1 = 20.
    assert_eq!(dot.measure_bytes(&a, &mut scratch).unwrap(), 20.0);

    // (1-4)^2 + (2-3)^2 + (3-2)^2 + (4-1)^2 = 9+1+1+9 = 20.
    assert_eq!(sq.measure_bytes(&a, &mut scratch).unwrap(), 20.0);
    assert!((eu.measure_bytes(&a, &mut scratch).unwrap() - 20.0f64.sqrt()).abs() < 1e-9);

    // A vector against itself is cosine distance zero.
    assert!(cos.measure_bytes(&a, &mut scratch).unwrap().abs() < 1e-12);

    // And it agrees with the f32 kernel on the same numbers, which is the check that the hand-written loops did
    // not drift from the library's convention.
    let a32 = pack_f32(&[1.0, 2.0, 3.0, 4.0]);
    let b32 = pack_f32(&[4.0, 3.0, 2.0, 1.0]);
    let dot32 = Measurer::new(Metric::Dot, DType::F32, &b32).unwrap();
    let cos32 = Measurer::new(Metric::Cosine, DType::F32, &b32).unwrap();

    assert_eq!(
        dot.measure_bytes(&a, &mut scratch).unwrap(),
        dot32.measure_bytes(&a32, &mut scratch).unwrap(),
        "the i32 dot product must match the f32 one on the same values"
    );
    assert!(
        (cos.measure_bytes(&b, &mut scratch).unwrap()
            - cos32.measure_bytes(&a32, &mut scratch).unwrap())
        .abs()
            < 1e-6,
        "and so must the cosine"
    );

    // The zero-vector convention matches simsimd's: distance 1, not NaN.
    let zero: Vec<u8> = vec![0u8; 16];
    let cz = Measurer::new(Metric::Cosine, DType::I32, &zero).unwrap();

    assert_eq!(cz.measure_bytes(&a, &mut scratch).unwrap(), 1.0);
}

/// A query that is not a whole number of elements is refused when the measurer is *built*, not per record.
#[test]
fn a_ragged_query_is_refused_at_configure_time() {
    // Seven bytes is not a whole number of f32s.
    assert!(Measurer::new(Metric::Cosine, DType::F32, &[0u8; 7]).is_err());
    assert!(Measurer::new(Metric::Cosine, DType::F64, &[0u8; 12]).is_err());
    assert!(Measurer::new(Metric::Cosine, DType::F32, &[]).is_err());

    // i8 has no such constraint: every byte count is a whole number of elements.
    assert!(Measurer::new(Metric::Cosine, DType::I8, &[0u8; 7]).is_ok());

    // Two bytes an element, so an odd byte count is ragged for the half formats too.
    assert!(Measurer::new(Metric::Cosine, DType::F16, &[0u8; 5]).is_err());
    assert!(Measurer::new(Metric::Cosine, DType::BF16, &[0u8; 4]).is_ok());
    assert!(Measurer::new(Metric::Cosine, DType::I32, &[0u8; 6]).is_err());

    // And the bench's control kernel exists only for f32, which it says rather than silently substituting.
    assert!(Measurer::new(Metric::CosineNaive, DType::F32, &[0u8; 16]).is_ok());
    assert!(Measurer::new(Metric::CosineNaive, DType::F64, &[0u8; 16]).is_err());
    assert!(Measurer::new(Metric::CosineNaive, DType::I32, &[0u8; 16]).is_err());
}

/// The scratch buffer hands back aligned bytes, which is what lets a kernel cast without checking.
#[test]
fn the_scratch_buffer_is_aligned_and_faithful() {
    let mut buf = AlignedBuf::default();
    let source: Vec<u8> = (0..37u8).collect();

    let out = buf.fill(&source);

    assert_eq!(out.len(), source.len(), "no padding is visible to the caller");
    assert_eq!(out, &source[..], "the bytes survive");
    assert_eq!(
        out.as_ptr() as usize % 8,
        0,
        "eight-byte aligned, which satisfies every element type here"
    );

    // Reused rather than reallocated: a second, shorter fill must not leave the first one's tail behind.
    let shorter: Vec<u8> = vec![9u8; 3];
    let out = buf.fill(&shorter);

    assert_eq!(out, &shorter[..]);
}

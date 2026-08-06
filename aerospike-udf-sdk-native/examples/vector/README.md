# A vector-search native UDF

Exact top-k, bounded ordering, and per-record distances — over any number of records, in O(k) memory.

```text
topk("embedding", <query blob>, "cosine", 10)     the 10 nearest, exactly
orderby("price", "asc", 20)                       the 20 cheapest
vec_distance("embedding", <query>, "dot")         one record, one number
```

This is **not** approximate nearest neighbour. Every record is measured. What makes that affordable is that
nothing is *kept*: the accumulator holds k hits, not n.

## What it costs

50,000 records of 768 dimensions (146 MB of vectors), best of three, `cargo test --test bench --release`:

| how | ms | records/sec | on the wire |
|---|---|---|---|
| **in memory, contiguous** | **9.0** | 5,567,000 | — |
| in memory, scattered | 8.8 | 5,665,000 | — |
| server, vendor cosine, k=10 | **16** | 3,035,000 | 40 hits |
| server, vendor cosine, k=100 | 17 | 2,941,000 | 400 hits |
| server, scalar cosine, k=10 | 23 | 2,183,000 | 40 hits |
| client, fetch every vector and sort | 126 | 398,000 | 50,000 vectors |

**The whole database costs about 1.9× the arithmetic.** The first row is the floor — the plugin's *own* `Measurer`
and `BestK` over a `Vec` of structs, same kernel, same heap, same tiebreak, no server. ~177 ns per record. The
server does the same work for **330 ns**, and that ~150 ns covers reading 50,000 records off storage, four query
threads, building partials, and the wire.

The ratio is worse than it was earlier in this file's history and the *absolute* numbers are better, which is worth
saying out loud: patching the vendor kernel made the floor much faster (223 → 177 ns) while the server's fixed costs
stayed put. A ratio moves for reasons that have nothing to do with the thing it appears to measure.

At 50,000 records the stage deltas wander ±30 ns between runs, so the million-record table below is the one to
read for attribution.

**Memory layout is not the difference**: scattering the vectors instead of packing them costs 1.01×. At 3 KB each
every vector is a streaming read either way.

**`k=100` costs the same as `k=10`** — the bounded heap doing its job: O(k) state, one comparison per record once
full.

### It is linear, and the database gets *cheaper* per record

`AEROSPIKE_BENCH_RECORDS=1000000` — 2.9 GB of vectors:

| | 50,000 | 1,000,000 | per-record |
|---|---|---|---|
| in memory (the floor) | 11.2 ms | 172.9 ms | 223 → **173 ns** |
| server, k=10 | 16 ms | 283 ms | 325 → **283 ns** |
| client, fetch and sort | 130 ms | 2,498 ms | — |
| database overhead | 1.5× | **1.6×** | — |

The floor moved from 202 to 173 ns when the vendor library was patched (see *The vendor kernel* below) and the server
row did not move at all — 284 to 283 ns. That is not a disappointment, it is the alignment finding restated: the
packed-blob path never calls the vendor kernel, so patching it improves the *control* and nothing else. The overhead
ratio got worse (1.4× → 1.6×) for the same reason, which is a good illustration of why a ratio is a worse headline
than the two numbers it divides.

Twenty times the data, twenty times the time — the arithmetic is genuinely linear and insensitive to cache at this
vector size. The *server* row improves with scale, 325 → 283 ns, because a query's fixed setup amortises over more
records, so the overhead figure is if anything pessimistic at small scale.

### An experiment that was reverted, and what it cost to undo

For a while the server handed this plugin a pointer to the record's **stored particle** instead of a copy of it. That
removed a `memcpy` per record — and a stored blob particle is never 4-byte aligned, so it also removed the ability to
cast the bytes to `&[f32]` and reach the vendor kernel. The replacement was hand-written unaligned kernels.

It was reverted, for a better reason than the size of the win: every accessor on `Record` takes `&self`, so a borrowed
particle can be read, then written, then read again through the first slice — a use-after-free the compiler cannot
see. With the copying read the value belongs to the per-call arena and outlives the write.

Undoing it cost **1.5%**, and the stage attribution shows why:

| stage | borrowed particle | copied (now) |
|---|---|---|
| read the blob bin | 68 ns | **157 ns** |
| measure the distance | 155 ns | **37 ns** |
| whole query, per record | 325 ns | **330 ns** |

The borrowed read moved cost between stages rather than removing it. Reading the bin got 89 ns cheaper; measuring got
118 ns dearer, because an unaligned hand-written kernel is no match for an aligned vendor one. And the patched vendor
kernel means the reverted version is still faster than where this file started (347 ns).

The counter above — `read in place for 100.0% of records` — is how that is checked rather than assumed, and it read
**0%** while the experiment was live.

### Where the 330 ns goes

Each probe is a separate aggregation doing strictly less than the next, so the differences attribute the cost.
Nothing here is a guess about what the server is doing — it is the same accumulator machinery with one stage
removed at a time, which is also why there is no mode argument anywhere near the per-record path.

| stage | ns/record | delta |
|---|---|---|
| query + record pipeline | 132 | — |
| + read the blob bin | 289 | **+157** |
| + measure the distance | 326 | **+37** |
| + the bounded heap | 330 | +4 |
| *(control)* read + an unconditional memcpy | 305 | *+16* |

The control row now costs only 16 ns more than the plain read, because the plain read *already* copies — the host
allocates the value. What that row prices is the plugin's own extra `memcpy` on top, which is small.

**Reading the bin is now the dominant cost, at 157 ns of 330.** That is the honest shape of this workload: a 3 KB
blob has to be materialised before any arithmetic can touch it, and the arithmetic itself is 37 ns.

A million 3 KB records loaded in 15.5 s (~190 MB/s at 64-way concurrency) and fit in a 4 GiB memory namespace with
`avail-pct 95`; `asd` sat at 4.3 GB RSS on a 7.9 GB VM, leaving ~3 GB. Not much further headroom on this box, so
the contiguous-layout row is skipped past 200,000 records — it would mean a second 2.9 GB copy of the data to
re-measure a difference of 1.01×.

### A wrong number I published, and how it happened

An earlier version of this table reported the client-side row at **2,182 ms** and claimed the server was **96×**
faster. That comparison loop lives in the test file, and it was being compiled **without optimisation** — with
`--release` it takes 129 ms. The server row barely moved, so the 96× was almost entirely the harness handicapping
its own control. **The real figure is about 8× at 50,000 records and 9× at a million**, and the wire saving (10
hits against 146 MB) is what makes it.

The bench now refuses to run under `debug_assertions` rather than quietly reporting numbers like that again. It
came to light only because adding this in-memory floor made me care whether the arithmetic was optimised.

### And a prediction it falsified — three times

I justified the `simsimd` dependency by arguing this would be the first case where the UDF, not the record
pipeline, dominates — a 768-dim cosine at ~300 ns scalar against ~75 ns SIMD. Measured, swapping the SIMD kernel
for a plain scalar loop moved the server row from 17 ms to 23 ms: **1.2×, not 3×**, because LLVM autovectorises
the obvious loop rather well.

Then the alignment discovery below removed `simsimd` from the blob path altogether — a borrowed particle cannot be
cast to `&[f32]`, so packed vectors now go through hand-written unaligned kernels and `simsimd` sees only list
bins, `f16`/`bf16` and `i8`. The dependency is still earning something on those, and half-precision arithmetic is
a real reason not to write your own, but the case for it is much narrower than the one I opened with. **A
pure-Rust crate would have been the better choice here**, and it took two measurements to see it.

Then, moving to NumKong, the vendor kernel turned out to be **slower than a hand-written loop by 2.7×** until
patched — see *The vendor kernel* below. Three predictions about this dependency, three falsified by measurement. It
stays because half-precision is genuinely worth not writing yourself, and because it is a useful differential-test
oracle; not because it was ever the fast path it was chosen to be.

Cost on the record: ~120 KB resident, and a C compiler at build time rather than anything at run time.

## The vendor kernel: SimSIMD, then NumKong, then a patch

**SimSIMD was renamed to NumKong upstream** — the GitHub repo 301-redirects, and the `simsimd` crate is frozen at
6.5.x. This example now depends on **NumKong 7.7.1 from a local checkout**, because that checkout carries a patch.

The move is not a version bump. `SpatialSimilarity` split into `Angular`/`Dot`/`Euclidean` (the combined trait survives
as a marker with no methods), `cos` became `angular`, the associated `Output` type varies by element type — `f64` for
`f32`, `f32` for the half formats, `i32`/`u32` for the integer ones — and `euclidean` became a native kernel, so
deriving it from `sqeuclidean` as this file used to is no longer necessary. On the other hand `pub struct f16(pub u16)`
exposes its bits, which let a `transmute` and two `size_of` assertions be deleted.

### Why it needed patching

NumKong's f32 NEON kernels were **slower than the SimSIMD 6.5 they replaced**: one FMA accumulator per sum *and* only
two elements per iteration, because they switched to f64 accumulation for numerical stability. A `vfmaq_f64` has
~4-cycle latency and issues once per cycle, so a single chain stalls on itself.

Four independent accumulators and full 128-bit loads — the shape `nk_dot_f32_neon` in the same tree already used, and
whose comment already said "two independent FMA chains for ILP" — fixed it:

| kernel | shipped | patched | |
|---|---|---|---|
| `angular_f32` | 500.3 ns | **183.3 ns** | 2.73× |
| `dot_f32` | 219.9 ns | **117.5 ns** | 1.87× |
| `sqeuclidean_f32` | 454.5 ns | **114.4 ns** | 3.97× |
| `euclidean_f32` | 467.6 ns | **114.6 ns** | 4.08× |

768 dimensions, one call, latency not throughput. For reference SimSIMD 6.5 measures 209 ns and 195 ns for cosine and
dot on the same data, so patched NumKong beats it on every metric while keeping the more accurate f64 accumulation.

### How it was validated

Three ways, because a hand-written SIMD patch is exactly where plausible-but-wrong code lives:

- **NumKong's own suite**: 137 lib tests and 21 doctests pass, unchanged before and after. One doctest
  (`mesh::kabsch`) fails, and it fails on pristine 7.7.1 too — checked by stashing the patch.
- **An independent f64 reference** over 54 lengths × 3 seeds: 0 failures. It has to be loose (1e-6), because
  NumKong's `sqeuclidean` subtracts in f32 inside the vector loop but in f64 in the scalar tail, so a reference tight
  enough to catch a reordering bug would have to replicate that split and would then be testing nothing.
- **A golden file captured from pristine sources**, compared at 1e-12: 0 regressions across 648 samples. This is the
  check that actually validates the patch, and it needs no model of the algorithm at all. Only 4 samples moved at all,
  all `dot` at n ≥ 768, worst 8.3e-14 — near-cancelling sums where reordering the last bit of an f64 accumulator shows
  up amplified in the result.

### And it did not speed up a single query

The packed-blob path never calls the vendor kernel — a borrowed particle is never aligned, and the API takes
`&[f32]`. So the patch moved the *in-memory floor* from 202 to 173 ns and left the server row at 283. Worth doing for
the floor's honesty as a control, and worth sending upstream; not worth expecting a production gain from.

## Why the answer is exact

Merging two bounded heaps keeps the k best of both, and that operation is **associative and commutative**. So it
does not matter how the records were divided between threads, nodes or dumps: the client combining one heap per
thread gets the same k as a single heap over everything. A unit test asserts exactly that, by splitting records
three ways and merging in two different orders.

It also means an accumulator that *dumps* stays correct, since the client merges dumped heaps the same way.

## Two things that silently return the wrong answer if they are wrong

**`dot` is a similarity; everything else is a distance.** The largest inner product is the *closest* vector. A
top-k that got this backwards would return the k worst matches and look entirely plausible. So the inversion
lives in one function — `Metric::score`, arranged so **larger is always worse** — and the heap, the merge and the
ordering share one comparison with no special cases.

**Ties are broken by digest.** Two records at equal distance would otherwise be ordered by whichever thread saw
one first, so the answer would vary run to run and no test could pin it.

## Nothing is decided per record

The element type and the metric are both fixed for a whole query, so neither goes near the per-record path.
`Measurer::new` resolves the pair to **one monomorphic function pointer**, once, in `configure`:

```rust
pub struct TopK {
    measurer: Option<Measurer>,   // kernel + aligned query, resolved once per query thread
    …
}

// per record: one length compare, one alignment compare, one indirect call
let d = measurer.distance(rec, &self.bin, &mut self.scratch)?;
```

`type Kernel = fn(&[u8], &[u8]) -> Option<f64>` — raw bytes on **both** sides, which is what keeps it
monomorphic: no enum to unwrap, no variant to check. 12 kernels (4 metrics × 3 element types) generated by a
macro, two lines each.

This is **fewer** branches than the first version, which matched five ways on the metric once per record. The
benchmark is unchanged at 23 ms either way, so the indirect call costs nothing measurable — and the `match` it
replaced is gone.

A name per element type (`topk_f32`, `topk_i8`, …) would have been literally zero-cost, since the server's
existing manifest lookup would *become* the dispatch. Rejected because the metric cannot sensibly go in a name
too, so it would mean 3× the exports and still need a pointer for the metric.

## The element type is declared, not detected

`topk(bin, query, metric, k, dtype)` — `f32` (default), `f64`, `f16`, `bf16`, `i8`, `i32`.

**Nothing in the data says which.** A blob is a pointer and a length; `f32` has no invalid bit patterns, so any
four aligned bytes are *some* float and no inspection distinguishes a vector from a thumbnail of the right size.
Before this argument existed the type was assumed to be `f32`, and any blob whose length happened to be 4× the
query dimension returned a plausible distance computed from reinterpreted bits — an `i8` vector of 16 elements
read as 4 floats.

Declaring it makes the length check exact: the query is kept as **the caller's own bytes**, so one comparison
covers the dimension *and* the element type. What it does not do is make a mismatch impossible — 3072 bytes are
768 `f32`s and also 384 `f64`s. The accident is gone; a wrong declaration is now the caller's statement rather
than this code's assumption. A test asserts both halves of that, including the byte-length collision that
remains.

**`f16` and `bf16` are separate types, not one "half".** The same sixteen bits mean different numbers — 1.0 is
`0x3C00` in IEEE binary16 and `0x3F80` in bfloat16 — and nothing in a blob says which. Reading one as the other
measures fine and answers wrong, so a test pins that they disagree.

**`i32` is the one element type `simsimd` does not implement**, so its four kernels are hand-written. Plain loops
rather than widening into `f32` first, which would cost a conversion pass per record *and* be lossy above 2^24.
Accumulated in `f64`, as `simsimd` does for `i8`: two 32-bit integers multiply to 62 bits and summing hundreds
overflows `i64`, so `f64` trades exactness above 2^53 for a range that cannot overflow. A test asserts the
hand-written loops agree with the `f32` kernels on the same values, including the zero-vector convention.

**The benchmark is what makes that acceptable rather than a compromise**: the scalar control kernel measured 28 ms
against `simsimd`'s 23 ms, so the cost of having no vendor kernel for a type is 1.2× and *measured*. That number
also stopped being hypothetical — the packed-blob path has no vendor kernel for *any* type now, for the alignment
reason below, and it costs about what this predicted.

## Storing vectors

**A blob of packed little-endian f32** is the one to store. `rec_get_bytes` returns a borrowed pointer into the
record's own memory and the kernel reads it there — **nothing is copied**. Four bytes per dimension.

### A borrowed blob is never aligned

The original design here assumed those bytes would "usually" be 4-byte aligned, so a kernel could cast them to
`&[f32]` and the scratch-buffer copy would be a rare fallback. A counter was added because the claim was
unverified, and the answer was **0 aligned out of 450,000**: a blob particle puts a type byte and a four-byte
length ahead of its data, packed, so the data begins five bytes into a 16-byte-aligned allocation and `ptr % 4` is
1 for every blob that has ever been stored.

That mattered because the server had just been changed to hand out the stored particle rather than a `cf_malloc`ed
copy of it — which removed a 127 ns malloc-and-memcpy per record and then paid it straight back in
`AlignedBuf::fill`. The copy had moved, not gone.

The fix is kernels that read each element with `from_ne_bytes` and need no alignment at all. **Written the obvious
way they were slower than copying** — 194 ns against 136 for the measure stage — because a floating-point
reduction is a dependency chain the optimiser is not allowed to reorder, so one accumulator per sum is 768 serial
additions. Eight independent accumulators, folded at the end, brought it to 146 ns and the whole query from 347 to
325 ns per record.

`f16`/`bf16` keep the copy: their element conversion lives inside `simsimd`, and reimplementing half-precision
arithmetic to avoid a memcpy is the worse trade. `i8` is one byte wide and so aligned wherever it lands.

A list of doubles still arrives through the scratch buffer, aligned, and still uses `simsimd`.

### Why not just align the vectors when writing them?

Because a writer cannot. It has to place the elements at a **fixed offset from the blob's start**, and the reader
needs them at an **address that is 0 mod 4** — which depends on where the record's flat block lands, and differs
between a device read buffer and an in-memory allocation for the same record. Leave three slack bytes and have the
reader skip to the boundary with `align_offset`, and a blob that happens to start aligned skips 0 and eats the slack
as data. `a_writer_cannot_place_elements_where_a_reader_will_look` records the arithmetic: the four possible offsets
need four different skips, so no single write-time choice satisfies them.

Aligning it in the *storage format* would work and is the only route that does — but a blob particle is cast straight
out of the flat on-device bytes (`blob_from_flat`: "we can do this only because the flat and in-memory formats are
identical"), so it would mean an on-disk format change with variable padding before every blob particle, and it
would cost the zero-copy load that cast currently gives for free.

**And it is worth less than it looks.** One 768-dimension cosine, on the dev VM:

| path | ns |
|---|---|
| aligned, `simsimd` | 235 |
| unaligned, lane-parallel kernel | **354** |
| copy into an aligned buffer, `simsimd` | 367 |

Reading in place beats copying to get alignment by 13 ns — so paying a memcpy for alignment is a *loss* here. Free
alignment would be worth 118 ns per record, about 40% of the query's 284 ns, which is what a storage-format change
would be buying.

The same benchmark on an Apple M-series ranks the bottom two the other way round (copy 321, unaligned 432), because
its memcpy is cheaper relative to its SIMD. The choice above is right for the machine the server runs on; another
machine deserves re-measuring rather than inheriting this conclusion.

**A list of doubles** also works, because it is what someone who did not plan for vectors already has: one bulk
host call, a conversion per element, eight bytes per dimension.

Little-endian is assumed and stated rather than handled; every platform Aerospike runs on is.

## A wrong-dimension record is skipped and counted

Not ignored, and not fatal. A stray 512-dimension vector among 768s is a data problem the caller needs to hear
about — failing a scan of a billion records over it would be worse, and dropping it silently would make a top-k
that ignored a fifth of the data look exactly like a correct one. So the partial carries `skipped_dim` and
`skipped_none`, and a test asserts three short vectors and two missing ones are both counted.

## Two server bugs this found

**The value arena was fixed at 64 entries.** `to_partial` builds one map per hit, so `k + 2` values — and a
`k` of 70 exceeded it. The failure surfaced during the end-of-thread flush, which only logged, so **a top-70
returned a query that reported success and produced nothing at all**; a top-63 returned a partial silently
truncated to 62. The arena now spills to the heap past its inline bound, so the common case still allocates
nothing. The header's old claim that needing more than 64 values "is a sign of a different problem" was simply
false: a top-100 is not a different problem.

**A flush failure was only logged.** `mod_native_flush_thread` had nothing to report to, which was true and the
wrong conclusion — the caller can abandon the job, and now does.

Both were found by the `k=100` row of this benchmark coming back empty.

## Layout

| file | what |
|---|---|
| `src/vec.rs` | reading a vector, the metrics, and the similarity/distance inversion |
| `src/heap.rs` | the bounded heap and why merging it is exact |
| `src/lib.rs` | three record functions, `topk`, `orderby` |
| `src/tests.rs` | the heap, the inversion, ties, NaN |
| `tests/against_a_server.rs` | exactness against a locally computed top-k, all four metrics |
| `tests/bench.rs` | the table above |

## The surface

| function | answer |
|---|---|
| `vec_distance(bin, query, metric)` | distance from this record's vector to the query |
| `vec_dim(bin)` | dimensionality, or nil |
| `vec_norm(bin)` | L2 norm |
| `topk(bin, query, metric, k)` | `{hits: [{distance, digest, key}], skipped_dim, skipped_none}` |
| `orderby(bin, direction, limit)` | `{rows: [{value, digest, key}], skipped}` |

Metrics: `cosine`, `dot` (`ip`), `sqeuclidean` (`l2sq`), `euclidean` (`l2`). Plus `cosine_naive`, which is a
measurement instrument rather than a metric — it exists so the bench can price the kernel.

A hit carries the record's digest, and its primary key if one was stored — **not its bins.** A heap of
k×(vector + bins) is what this design exists to avoid; the caller batch-reads the k winners.

**`orderby` requires a limit.** Ordering without one means holding every record plus a client-side merge of
sorted runs — real work for no advantage over scanning and sorting client-side, so it is refused rather than
half-implemented.

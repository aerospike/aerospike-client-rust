# An aligned, self-describing vector particle

> ## Verdict: measured, and **the alignment half is not worth building**
>
> Both open questions from the last section are now answered, and the answers remove most of the case for this
> document.
>
> **Alignment is worth 1–4 ns per record, not 118.** A hand-written NEON cosine over *unaligned* bytes measures
> 68–72 ns; the same kernel over aligned bytes measures 65–69 ns. Three runs on the dev VM, ratio 1.01–1.06×. ARM's
> `ldr q` genuinely does not care about the offset, and the earlier 118 ns estimate was an artifact of comparing
> against `simsimd`, which cannot *accept* unaligned input rather than being slowed by it. Against a 284 ns query
> that is under 1.5% — nowhere near enough for an on-disk format addition.
>
> **`AS_PARTICLE_TYPE_VECTOR = 16` is allocated but unused.** It is a blob subtype tag (`AS_BYTES_VECTOR`, in the
> shared `common` submodule), mapped to `blob_vtable`, accepted by `safe_particle_type`, not deprecated — and absent
> from the Java, Go, Rust and Python clients. So nothing depends on its contents, and nothing writes it.
>
> **And a third thing fell out that matters more than either.** The hand-written kernel is **3.2× faster than
> `simsimd`** (68 ns against 222) and 4.8× faster than the portable lane-parallel kernel this example currently uses.
> The dependency is not pulling its weight, and the win is in the plugin, not the storage engine.
>
> What survives is the **self-description** argument — element type, dimension, write-time rejection of ragged
> vectors — which is real but is now the *whole* justification rather than a bonus on top of a performance win. The
> rest of this document stands as the design if that argument is ever accepted on its own.

A design, not an implementation. It exists because a native UDF reading a packed vector out of a blob bin pays for
two things it should not have to: the bytes are never aligned, and nothing in them says what they are.

Both are measured, not assumed — see `aerospike-udf-sdk-native/examples/vector/` for the benchmark and
`NATIVE_UDF_DESIGN.md` for the borrowed-read work this follows.

## What is wrong with a blob today

**It is never aligned.** A blob particle stores a type byte and a four-byte length ahead of its data, packed, so the
data begins five bytes into the particle. A borrowed pointer to it is therefore misaligned for any element type wider
than a byte — measured at **0 aligned out of 450,000** records. That is structural, not luck.

**Nothing in it says what it holds.** A blob is a pointer and a length. 3072 bytes is 768 `f32`s and also 384
`f64`s, so the element type has to be passed as a query argument and a wrong declaration computes a plausible wrong
answer rather than failing. The current example documents this as an unavoidable limit. With a header it becomes
avoidable.

**A ragged vector is only noticed at query time.** The plugin skips and counts it. The server could have refused the
write.

## What it costs today, measured

One 768-dimension cosine, dev VM (Neoverse-N1), `kernel_paths_are_priced_against_each_other`:

| path | ns |
|---|---|
| aligned, `simsimd` | 235 |
| unaligned, lane-parallel kernel | 354 |
| copy into an aligned buffer, `simsimd` | 367 |

Against a whole query at **284 ns per record**, so free alignment is worth about **118 ns — roughly 40%**. Paying a
memcpy for it is a net loss (367 > 354), which is why there is no read-side fix and why the storage format is the
only place left to look.

## Read this before building it

**The performance half is available much cheaper, and the design should not be justified on it.**

Hardware does not need alignment. NEON `ld1` and x86 `vmovups` load vectors from unaligned addresses with no
penalty. The 354 ns above is not the CPU objecting to an offset — it is LLVM autovectorising a `from_ne_bytes` loop
worse than `simsimd`'s hand-written intrinsics. A NEON/AVX kernel using unaligned loads should reach roughly the
same 235 ns **with no change to storage at all**, entirely inside the plugin.

So the honest case for this particle is **self-description** — the element type, the dimension, write-time
validation, and a foundation for expressions and indexes on vectors later. Alignment is a bonus that a much smaller
change may also deliver. That is worth settling with a one-afternoon intrinsics experiment before committing to a
format.

## Resolve first: `AS_PARTICLE_TYPE_VECTOR = 16` already exists

It is currently a **wire alias for a blob** — `particle_vtable[AS_PARTICLE_TYPE_VECTOR] = &blob_vtable` — so the
type ID is taken, and taken by something that presumably belongs to Aerospike Vector Search. That is a product
question, not a technical one, and it comes first:

- If AVS owns that ID and has a representation, this should **extend** it rather than compete with it. Two
  server-side vector formats would be worse than either.
- If it is only a labelling convention over blobs, then this design supersedes it and should probably reuse the
  name.

Free IDs if a new one is needed: **5, 13, 14, 15, 22**. Not 6 or 21, which are absent from the server's enum but
still carry meanings in the Go client (`DIGEST = 6`, `LDT = 21`) — the server's enum is not the whole allocation
list, which is worth knowing before picking a number.

## The layout

Flat and in-memory forms are **identical**, which is what lets `from_flat` cast rather than copy — the property the
whole borrowed-read path depends on:

```c
typedef struct vector_mem_s {
	uint8_t  type;      // the new AS_PARTICLE_TYPE_*
	uint8_t  elem_type; // F32 | F64 | F16 | BF16 | I8 | I32 | U8 …
	uint8_t  pad;       // 0..15 — bytes between this header and the data
	uint8_t  flags;     // 0; reserved for normalized / quantized / byte order
	uint32_t dim;       // elements, so the byte size is dim * width(elem_type)
	uint8_t  data[];    // `pad` bytes, then dim * width(elem_type)
} __attribute__((__packed__)) vector_mem;
```

Eight bytes of header, so the data begins at `+8 + pad`.

**`dim` rather than a byte count** is the point of the exercise: with the width implied by `elem_type`, a length can
no longer be read as two different vectors.

**Align to 16, not to the element width.** One constant instead of per-type logic, and it suits 128-bit SIMD rather
than merely satisfying Rust's slice rule. It costs at most 15 bytes on a 3 KB vector — 0.5%.

**`flags` should carry a byte-order bit from day one.** The current blob convention is little-endian by assertion,
documented and unenforced. A header is the place to stop assuming.

## Alignment can only be a hint, and the reader must verify

This is the part that took reading the storage code to get right, and it is not what I expected.

The pad has to be computed where the destination offset is known — `flatten_bins` in `as/src/storage/flat.c`, which
calls `as_bin_particle_to_flat(bin, buf)` with `buf` pointing into the flat buffer. Records begin on 16-byte
boundaries (`RBLOCK_SIZE 16`), so a pad computed from the offset **relative to the record's start** is stable across
a device read.

**Except under compression.** `as_flat_decompress_buffer(&rr->cm, WBLOCK_SZ, &flat_bins, &end, NULL)` inflates
**only the bins section**, into a buffer whose base has no relation to the record's. Any write-time pad is then
wrong.

So:

- The pad is **computed at flatten time** and is right for the common path — uncompressed, whether the record is
  read from device or held in memory.
- The reader **always checks** `ptr % 16 == 0` and falls back to the unaligned kernel when it fails. Correctness
  never depends on the pad being right.
- The hit rate is therefore **empirical, not guaranteed**, and must be counted rather than claimed. The example
  plugin already reports `zero_copy`/`copied` per query — that counter is what caught the last false zero-copy
  claim, and it is what would catch this one.

A compressed namespace simply does not get the benefit. Worth saying out loud rather than discovering later.

## Wire form

Same struct with **`pad = 0`**: a client cannot know the record layout, so it cannot compute the pad, and the server
overwrites it at flatten time. The framework already separates the two sizes — `size_from_wire` returns
`8 + dim * width`, `flat_size` returns `8 + pad + dim * width`.

A consequence worth stating: a record read back **before** it is flattened sees `pad = 0` and an unaligned vector.
Harmless, because of the fallback, but it means a test that writes and immediately reads will not exercise the fast
path.

`from_wire` is where **validation** lives, and it is most of the value that is not alignment:

- `elem_type` is known
- `dim * width(elem_type)` equals the payload length exactly — a ragged vector is refused at write
- `dim` is within a configured maximum

## What has to be implemented

`as_particle_vtable` has **21 entries**. Most are mechanical (`size`, `skip_flat`, `flat_size`, `to_flat`,
`from_flat`, the wire quartet). Three need decisions:

**`to_asval` / `from_asval`** — what a vector *is* to Lua and to anything that goes through `as_val`. Options:

1. `as_bytes` carrying the whole particle including the header. Round-trips losslessly, costs nothing, and Lua sees
   an opaque blob. **Recommended**, with Lua and CDT support explicitly out of scope.
2. A list of doubles. Lossy for `i32` and `f64`, and expensive per record.
3. A new `as_val` type. `as_val_t` lives in the `common` submodule and every language binding switches on it — the
   largest blast radius of anything here, for a case that does not need it yet.

**`concat`/`append`/`prepend`/`incr`** — meaningless for a vector. They should refuse with
`AS_ERR_INCOMPATIBLE_TYPE`, as the non-blob particles already do.

**`size_from_msgpack` / `from_msgpack`** — only reachable from CDT contexts. Refuse; a vector is not a collection
element in this design.

Then, outside the particle: XDR ships it as opaque bytes and needs checking rather than changing; secondary indexes
and expressions must **refuse** it rather than misread it; and `as_particle_type_from_asval` needs a rule.

## The native UDF ABI

A new host call rather than a new `as_native_value` variant, because `as_native_value` is 16 bytes and full, and
widening it is an ABI break:

```c
int32_t (*rec_get_vector_raw)(const as_native_rec* rec, const char* name, uint32_t name_len,
		const uint8_t** data, uint32_t* dim, uint8_t* elem_type);
```

Appended after the five `_raw` calls, so `size` gates it and nothing above moves — the rule
`abi.rs::the_borrowed_reads_only_extend_the_vtable` now pins after that mistake nearly shipped.

The SDK exposes it as `Record::get_vector(&self) -> Result<Option<Vector<'_>>>` with `elem_type` and `dim` on the
struct, and the vector example drops its `dtype` argument for this particle while keeping it for blobs.

## Migration

Existing blob vectors keep working unchanged — the plugin already handles unaligned blobs and list bins, and both
paths stay. The `dtype` argument becomes *required for a blob, ignored for a vector particle*, which is a
deprecation rather than a break.

## Scope, honestly

This is a **storage format addition**: a wire-protocol type that every client eventually has to know, 21 vtable
functions, and a permanent second way to store a vector. It buys self-description now and possibly nothing in
performance that a hand-written unaligned SIMD kernel would not also buy.

**Suggested order:**

1. Settle the `VECTOR = 16` / AVS question. It may change everything below.
2. Measure a NEON kernel with unaligned loads in the example plugin — an afternoon. If it reaches ~235 ns, the
   performance argument for this design is gone and only self-description remains.
3. Decide whether self-description alone justifies a format addition. It might; it is a real limit and I documented
   it as unavoidable before this.
4. Only then build it.

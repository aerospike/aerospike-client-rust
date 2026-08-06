# mod-native — compiled UDFs as server plugins

A third UDF language, `NATIVE`: functions written in any language that can produce a C-ABI shared object,
declared in the server's config file, loaded at startup, and called with **no interpreter, no sandbox and no
serialization**. The target is that a native UDF costs what a function written inside the server costs.

This is the third engine, and it inherits everything the first two taught us. The one lesson that dominates
the design: in both `mod-lua` and `mod-wamr`, every case where a UDF looked slow was **an API that forced too
many boundary calls**, not a limit of the execution engine. mod-native removes the engine entirely, so the API
*is* the whole design.

## 1. What it is not

Registration is **config-only**. There is no `udf-put` path, no SMD replication, no client-side registration —
by requirement, and because it is the only defensible position: a native module runs in the server's address
space with the server's privileges, so publishing one is an *operator* act, like installing a plugin.

The honest consequence, which must be documented in the same breath as the performance:

- **A native UDF cannot be sandboxed.** A bad plugin can corrupt or crash `asd`. WASM's memory cap and
  Lua's interpreter both go away.
- **A native UDF cannot be interrupted.** mod-wamr enforces a deadline by trapping the guest mid-execution;
  native code cannot be preempted safely. The deadline can only be *offered* — `ctx.deadline_exceeded()` —
  and a plugin that ignores it holds a record lock until it returns.
- **It runs on transaction threads, concurrently.** A plugin must be thread-safe and must not block: no
  network calls, no file I/O, no unbounded compute.

These are the terms of "as if they were written inside the server". They are acceptable *because* only an
operator can install one.

## 2. Dispatch

The server already dispatches by language, so this is additive — the same property that made WASM tractable.

```c
typedef enum { AS_UDF_LANG_LUA = 0, AS_UDF_LANG_WASM = 1, AS_UDF_LANG_NATIVE = 2 } as_udf_lang;
```

`udf_cask_get_lang(filename)` today consults an SMD-backed shash and answers LUA for anything unregistered.
Native modules are not in SMD, so the lookup gains a **first** step: a name→plugin table built once at startup
from the config and **never mutated again**, which is what makes the read path lock-free.

The three call sites already switch on language (`udf_cask.c`, `transaction/udf.c`, `base/aggr.c`); each gains
a third arm. No wire change: the client still sends `apply(module, function, args)` and cannot tell which
engine answers.

**Security obligations at the boundary:**

- `udf_cask_put` refuses a `native` type or a `.so` payload outright.
- `udf_cask_put` also refuses a Lua/WASM module whose *name* collides with a configured native module —
  otherwise a client's registration silently does nothing, which is worse than an error.
- `udf-list` reports native modules as `type=native` with no content. They exist operationally; hiding them
  makes a live server unexplainable.

## 3. Configuration

```
mod-native {
    module stats {
        path /opt/aerospike/native/libstats.so
        option window-size 4096          # opaque key/value, handed to the plugin's init
    }
    module geo {
        path /opt/aerospike/native/libgeo.so
    }
}
```

Nested named stanzas, following the `namespace`/`set` precedent in `cfg.c`. `option` lines are passed to the
plugin verbatim as a string map, so a plugin is configurable without inventing a second config file.

Loading: `dlopen(path, RTLD_NOW | RTLD_LOCAL)` at startup.

- `RTLD_NOW` — unresolved symbols fail at boot, not inside a transaction.
- `RTLD_LOCAL` — no symbol pollution between plugins.
- **Never `RTLD_DEEPBIND`.** The server uses jemalloc; DEEPBIND would bind the plugin to a different `malloc`
  and every pointer crossing the boundary would risk a cross-allocator free.
- Never `dlclose`. Modules live for the process. Reload = restart, which is the same contract as any other
  config item that names a file.
- The server currently links no `-ldl`; that gets added.

## 4. The C ABI

Two directions, one header — `modules/mod-native/src/include/aerospike/as_native.h` — which is the shippable
artifact a plugin author compiles against, in any language.

### Plugin → host: one exported symbol

```c
#define AS_NATIVE_ABI_VERSION 1

const as_native_manifest* as_native_module_init(const as_native_host* host,
        const as_native_module_config* cfg);
```

The manifest is static data the plugin owns for the life of the process:

```c
typedef struct as_native_manifest_s {
    uint32_t abi_version;                        // must equal AS_NATIVE_ABI_VERSION
    const char* module_name;
    const char* module_version;                  // the plugin's own, for the log line
    uint32_t n_record_fns;
    const as_native_record_export* record_fns;
    uint32_t n_agg_fns;
    const as_native_agg_export* agg_fns;
} as_native_manifest;
```

**Everything checkable is checked once, at load:** ABI version, non-null unique names, sane arities, symbol
presence. Nothing on the per-call path validates anything the manifest already promised — that is how safety
avoids costing throughput.

### Host → plugin: a vtable, passed at init

`as_native_host` is a struct of function pointers with `abi_version` and `size` at the front, so it can grow
without breaking a compiled plugin. Record access, collection access, value construction, logging, and the
`as_aerospike` write verbs (`rec_update`/`rec_create`/`rec_remove`/`rec_exists`) all live here.

### Values

```c
typedef struct as_native_value_s {
    uint8_t type;                                // AS_NATIVE_TYPE_{NIL,BOOL,INT,DOUBLE,STR,BLOB,LIST,MAP,GEOJSON}
    union {
        int64_t i;
        double  d;
        bool    b;
        struct { const uint8_t* p; uint32_t len; } bytes;   // borrowed, not owned
        const void* handle;                                 // opaque as_val* for list/map
    };
} as_native_value;
```

16 bytes, flat, **borrowed**. A string bin is a pointer and a length into the record's own memory — no copy, no
allocation, and nothing to free. Collections cross as opaque handles walked in place through host calls, which
is the mod-wamr model, and it is right for the same reason.

#### "Borrowed" is half true, and an experiment that removed the other half was reverted

The value struct carries a pointer, but the thing it points at is a **copy**: `as_rec_get` boxes a bin into an
`as_val` first, and for a blob that is a `cf_malloc` plus a `memcpy` of the particle. On a 768-dimension vector
search that measured **111 ns of a 283 ns per-record budget**.

Five host calls were added to hand out the *stored* particle instead — `rec_get_raw` and friends, appended so the
`size` field gated them and no ABI bump was needed — and then removed again. Three findings, in the order they
arrived, because each one changed the answer:

1. **It worked**: an in-run control measured 68 ns to read a bin borrowed against 111 ns more with the copy.
2. **It barely mattered.** The query went from 323 to 283 ns per record — about 12%, against 187 ns the stage
   attribution had predicted. A blob particle is never 4-byte aligned (its data starts five bytes into its
   allocation), so the memcpy the server stopped doing came straight back in the plugin's alignment buffer. And it
   bought *nothing at all* for a collection bin, because msgpack has no borrowed view — which is most of what the
   JSONPath example reads.
3. **It was unsound.** Every accessor on `Record` takes `&self`, so this compiles:

   ```rust
   let bytes = rec.get_bytes("doc")?.unwrap();  // into the stored particle
   rec.set_blob("doc", b"...")?;                // &self - allowed
   rec.write()?;                                // frees that particle
   use(bytes);                                  // use-after-free
   ```

   With the copying read that sequence is safe: the `as_val` belongs to the per-call arena and outlives the write.
   The borrowed read turned it into a use-after-free the compiler cannot see, in exchange for 12% on one workload.

#### Blob subtypes: what this ABI cannot carry, and now refuses to destroy

A blob particle's *subtype* is what makes it an HLL rather than bytes — the server maps `AS_BYTES_HLL` onto
`AS_PARTICLE_TYPE_HLL`, `AS_BYTES_VECTOR` onto `AS_PARTICLE_TYPE_VECTOR`, and so on. A 16-byte flat value has one field
for "what is this", spent on `BLOB`, so **the subtype cannot cross this boundary in either direction**.

Reading is merely lossy: an HLL arrives as `TYPE_BLOB` and a plugin cannot tell it from bytes. Writing was worse.
Everything a plugin hands back is stamped `AS_BYTES_BLOB` by `as_bytes_init_wrap`, so reading an HLL bin and writing it
back **silently changed particle type 18 into 4**. Verified live: 3083 bytes byte-identical, the bin still readable,
and every later HLL operation on it failing with `BinTypeError`. No error at write time, nothing in the log.

Lua is unaffected, and not by design — it hands a UDF the `as_bytes` *object* and never flattens it, so the subtype
field is simply never touched. `mod_lua_bytes.c` even exposes `get_type`/`set_type`.

**Both `mod-native` and `mod-wamr` now refuse the write** — `blob_write_would_relabel` — returning `TYPE_MISMATCH`
when a plain blob would overwrite a bin whose existing particle is a *subtyped* blob. WASM gained `WAMR_TYPE_MISMATCH`
(-3) for it, which was the gap its numbering had always left next to the native ABI's.

The guard is deliberately narrow, and the test pins all three cases:

- **Refused**: writing bytes over an existing HLL, VECTOR or legacy-tagged blob. Nothing is written; the bin still
  works as an HLL afterwards.
- **Allowed**: writing those same bytes to a *different* bin. Nothing is relabelled — the caller asked for a blob and
  gets one.
- **Allowed**: any write to a bin holding an ordinary blob, which is the control. A guard that broke normal blob
  writes would be worse than the bug.

Writing an *integer* over an HLL is also allowed: that is an unambiguous replacement, and refusing it would be
surprising. It is writing *bytes* that reads as "put this value back" while meaning something else.

This brings both ABIs to Lua's standard — pass-through fidelity — rather than to HLL support. **No UDF language can
perform HLL operations**; those are client-side `operate()` commands against `particle_hll.c`. Full subtype support
(distinct type codes, so a plugin could recognise and deliberately rewrite an HLL) is additive and separate, and worth
doing only if something actually needs it.

**GeoJSON in WASM, which was the same shape of gap, is now closed.** `mod-wamr` had no `GEOJSON` kind: a geojson bin
read as `WAMR_TYPE_OTHER`, its text was unreachable, and the only way to write text back was `set_str` — which keeps the
document and silently turns a geospatially indexable bin into an ordinary string. Added `WAMR_TYPE_GEOJSON` (9, matching
native), the read path in `val_bytes_out`, and a `set_geojson` native; the guest SDK gained `Type::GeoJson` and
`set_geojson`. A round-trip test writes a document, has a WASM UDF read and rewrite it, and asserts the bin is still
geojson with the text unchanged.

**If it is ever revisited, the precondition is `&mut self` on the write path** — so the borrow checker enforces the
lifetime — not a better benchmark. What was kept from the exercise is the knowledge that a stored blob particle is
never aligned, which killed a proposed storage-format change before it was built (see `VECTOR_PARTICLE_DESIGN.md`),
and a per-query counter in the vector example that proves the zero-copy claim rather than asserting it.

### The record function

```c
typedef int32_t (*as_native_record_fn)(as_native_ctx* ctx, as_native_rec* rec,
        const as_native_value* args, uint32_t n_args, as_native_value* result);
```

Arguments are materialised once per call into a **stack** array by mod-native — `n_args` is small, so this is a
short loop with no allocation. That loop is the only marshalling that exists anywhere in the design.

### Collection accessors, present from day one

The mod-wamr experience is unambiguous: a by-index accessor used in a loop is what made a map-heavy aggregation
1.48× *slower* than Lua, and adding by-key and bulk accessors made it level. So the vtable ships with them from
the start, not as an optimization pass:

- `map_get(handle, key) -> value` — one hash lookup, key built on the stack.
- `list_get_ints(handle, from, n, int64_t* out)` — bulk, one call for n elements.
- `list_size`, `list_get`, `map_size`, `map_foreach` for the general cases.

Here the argument is even stronger than for WASM, because a host call is a direct call rather than a runtime
import — but the number of calls is still what shows up in a profile.

## 5. Aggregation

This is where the design earns the phrase "extremely efficient communication between map and reduce", and where
the current architecture leaves the most on the table.

**What happens today:** `as_aggr_process` is called **once per partition slice**, inside a multi-threaded query
job. Each call runs a complete Lua map/reduce pipeline and writes its result to the output stream. A single node
therefore returns **one partial per slice** — potentially hundreds — and the client combines them all. Per
record, Lua boxes values into `as_val` and calls through a closure chain.

**What a native aggregation does instead** — the plugin declares an accumulator and four hooks:

```c
typedef struct as_native_agg_export_s {
    const char* name;
    uint32_t state_size;                         // the host allocates it
    uint32_t state_align;
    int32_t (*init)     (as_native_agg_ctx*, void* state, const as_native_value* args, uint32_t n_args);
    int32_t (*accumulate)(as_native_agg_ctx*, void* state, as_native_rec* rec);
    int32_t (*merge)    (as_native_agg_ctx*, void* dst, const void* src);
    int32_t (*finalize) (as_native_agg_ctx*, void* state, as_native_value* out);
    void    (*destroy)  (void* state);           // optional; only for state owning heap
} as_native_agg_export;
```

Four properties follow, and together they are the point of the whole exercise:

1. **One indirect call per record.** `accumulate(state, rec)` — no `as_val` boxing, no stream write, no
   intermediate value of any kind. Lua boxes per record; mod-wamr costs two-plus import calls per record.
2. **`state_size` means the host owns the memory.** A POD accumulator — count, sum, min/max, a fixed
   histogram — makes an entire aggregation over millions of records perform **zero allocations**. The plugin
   declares its footprint instead of calling `malloc`; this is the same trick that let the WASM guest drop its
   allocator entirely, and it is worth more here.
3. **`merge` collapses partials** — specified, not yet wired. Per-slice accumulators would merge into one
   node-level accumulator: a pointer-to-pointer merge with no serialization, once per slice rather than per
   record, so a mutex on it is uncontended in practice, and the node emits **one** partial instead of hundreds.
   What blocks it is where the merged answer gets emitted: `aggr_query_job_finish` closes the client connection
   before anything else, so a job-level emission has to be threaded into the query response path. The hook is
   in the ABI and both examples implement it, so this is a wiring change rather than an ABI change — but as
   shipped, a node still emits one partial per slice, exactly as Lua does.
4. **`finalize` boxes exactly one value per node.** The only `as_val` construction in the entire aggregation.

**Cross-node combining stays with the client**, and that is forced, not chosen: nodes do not communicate during
a query, so the coordinator is the client — which cannot run the plugin's `merge`. So the contract is the same
one WASM has: `finalize` must emit a *mergeable* partial (for a mean: `{sum, count}`, not the mean), and the
caller combines. The Rust SDK will express this as a trait so the shape is impossible to miss, and a later phase
can let a Rust *client* link the same plugin crate and reuse its real `merge`.

## 6. The Rust SDK

`aerospike-udf-sdk-native/` in the **client** repo, beside `aerospike-udf-sdk-wasm` — writing a UDF is a
client-side activity; only the host half of the ABI belongs with the server. (Named without a `-rust` suffix to
match the WASM crate: the language is already implied by it being a Rust crate, and an SDK for another language
would be a separate artifact.)

Target API:

```rust
use aerospike_udf_sdk_native::{module, Record, Result, Value};

/// Add `by` to bin "n" and return the new value.
#[udf]
fn increment(rec: &Record, by: i64) -> Result<i64> {
    let next = rec.get_int("n")?.unwrap_or(0) + by;
    rec.set_int("n", next)?;
    rec.write()?;                                  // explicit, #[must_use] — see below
    Ok(next)
}

#[udf(agg)]
struct Stats { n: u64, sum: i64 }                  // POD: state_size is sizeof, zero allocation

impl Aggregation for Stats {
    fn accumulate(&mut self, rec: &Record) -> Result<()> { self.n += 1; self.sum += rec.get_int("v")?.unwrap_or(0); Ok(()) }
    fn merge(&mut self, other: &Self) { self.n += other.n; self.sum += other.sum; }
    fn finalize(self) -> Result<Value<'static>> { /* {sum, count} — mergeable */ }
}

module! { name: "stats", record: [increment], agg: [Stats] }
```

How the safety is free:

- **Typed parameters.** The `#[udf]` macro generates the arity and tag checks from the signature, so
  `by: i64` is one tag compare and a union read. A mismatch is a clean error to the client, not UB. `Value<'a>`
  is available for a genuinely dynamic parameter.
- **Lifetimes pin borrowed data.** `Record<'a>` and `Value<'a>` are invariant in the call's lifetime and are
  neither `Send` nor `Sync`, so a borrowed bin pointer cannot outlive the call or escape to another thread —
  the two ways this ABI could be misused from safe code.
- **`catch_unwind` at every export.** A panic unwinding into C is UB; the landing pad costs nothing on the
  happy path and converts a panic into a UDF error instead of a dead `asd`.
- **Writes stay explicit.** `rec.write()` mirrors Lua's `aerospike:update()` and WASM's `update()`. The WASM
  SDK's own README documents forgetting it as "the mistake that looks like it worked" — and I hit exactly that
  bug during the WAMR work, where a benchmark measured a write that never committed. Explicit and `#[must_use]`
  is the fix; implicit writes would be a worse trade.

`module!` builds the manifest as a `static` — no linker-section tricks, no `inventory`, so it works on every
target and the contents are obvious at a glance. The crate is `no_std`-compatible and needs no allocator for a
POD accumulator.

A **C example** ships too, small, purely to keep the "any language" claim honest and to prove nothing in the
header depends on Rust.

## 7. Server-side layout

```
modules/mod-native/
  Makefile
  src/include/aerospike/as_native.h          # the public plugin ABI — the shippable header
  src/include/aerospike/mod_native.h         # extern as_module mod_native
  src/include/aerospike/mod_native_config.h  # mod_native_config: the module table
  src/main/mod_native.c                      # as_module_hooks: apply_record, apply_stream
  src/main/mod_native_registry.c             # dlopen, manifest validation, name -> fn tables
  src/main/mod_native_host.c                 # the host vtable: record, collections, values
  test/unit.c                                # host-side unit tests
  examples/stats.c                           # the C plugin example
```

`as_module_hooks` for mod-native:

- `validate` — always refuses. Nothing is ever registered through this path; a reachable `validate` would mean
  a security hole, so it says so.
- `update` — ignores `FILE_ADD`/`FILE_REMOVE`; native modules are not cask files.
- `apply_record` — registry lookup, arg marshalling, one indirect call.
- `apply_stream` — drives the record loop calling `accumulate`, then `merge`/`finalize`.
- `destroy` — nothing. Modules are never unloaded.

Reusing what the WAMR work already established: a hook must **return 0 with the failure on the `as_result`**,
or `udf.c` overwrites the message with mod-lua's error map; and the stream owns its records, so `apply_stream`
must never destroy one (that double free took 100k records to fault).

## 8. Phases

Each phase ends green — buildable, tested, and measurable.

1. **ABI + skeleton.** The header, `mod-native` module, config parsing, dispatch in the three sites, registry
   with `dlopen` and manifest validation, security refusals in `udf_cask_put`. A C plugin with one function.
   Host-side unit tests for registry validation and rejection paths.
2. **Record functions, complete.** Full record surface (bins, metadata, write verbs), values, collection
   accessors including the by-key and bulk forms. Client integration tests.
3. **Rust SDK.** `aerospike-udf-sdk-native` with `#[udf]`, typed params, lifetimes, `catch_unwind`;
   the example plugin; a checked-in fixture for the client suite, built in-repo the way the WASM one now is.
4. **Aggregation.** The accumulator ABI, `apply_stream`, per-slice `merge` into a node accumulator, the SDK's
   `Aggregation` trait, and the mergeable-partial contract in the docs.
5. **Measurement.** Against Lua, WASM AoT, **and an in-server C baseline** — the last one is the only honest
   reference for "as if written inside the server", and it is what tells us whether the remaining overhead is
   the ABI or the transaction path. Same hygiene as before: match optimization levels, seed compute loops from
   the record, and sweep collection sizes to 10k, since below n≈100 everything is under the round-trip noise.

## 9. What it actually measures

> **The 50,000-record numbers below predate the aggregation redesign** and are kept because their *reasoning*
> still holds. They were taken when a native aggregation emitted one partial per partition slice, exactly as Lua
> does, so 1.24× was the engine and nothing else. Section 9a is the current measurement.

Measured on the dev VM, 50,000 records, one integer bin, best of three — `tests/src/udf_native_bench.rs`.

**Aggregation, end to end: 39.6 ms native against 48.9 ms Lua, so 1.24–1.31× across runs.** The same for the
C plugin and the Rust one, to within noise — which is the more interesting half of the result, because it says
the SDK costs nothing over hand-written C.

**1.24× is smaller than "one indirect call per record" suggests, and that is the finding.** Most of an
aggregation is not the UDF: it is the server reading 50,000 records and building a record object per row. The
engine was already a minority of the cost in Lua, so removing it entirely could only ever recover that
minority. For scale, a plain scan of the same rows costs **313 ms** — six times the whole aggregation — because
it ships every record while an aggregation ships one partial per slice.

That last number also killed the decomposition I had planned: I intended to subtract a scan as the floor an
aggregation builds on, and a scan turned out to cost several times *more*. The benchmark now says so where it
measures it, rather than reporting a negative difference.

**Per call, the difference is below this setup's noise floor.** A plain write round trip is ~100–114 µs from
here to the VM; across runs Lua came out +3.2 µs and +14.3 µs over that baseline, native +0.2 µs and +10.6 µs.
Run-to-run variation is larger than the gap, so the honest statement is that a native call's overhead is
**unmeasurable at ~10 µs resolution** — not that it is zero. Measuring it properly needs a server-side harness,
the way the WASM tiers were measured.

Not yet measured: an in-server C baseline. It would need a function compiled into `asd` and reachable the same
way, which is a change to the server rather than a plugin, and the per-call resolution above is too coarse for
the comparison to mean anything yet.

Two earlier measurements in this project contradicted my predictions — a "4.5 µs cheaper data path" that was an
uncommitted write, and a 15× compute gap that was 4× once both sides used the same optimization level — which
is why the numbers above are the ones the file prints and nothing more.

## 9a. All three engines, 200,000 records

`tests/src/udf_agg_bench.rs`, best of three per row, against a namespace the bench had to itself. Lua, WASM
(AoT) and both native plugins are given the same work over the same records.

**Count and sum of one integer bin:**

| engine | ms | records/sec | vs native | partials |
|---|---|---|---|---|
| lua | 94 | 2,120,000 | 2.8× | 4096 |
| wasm (aot) | 97 | 2,052,000 | 2.9× | 4096 |
| native (C) | 33 | 5,971,000 | 1.0× | **4** |
| native (Rust) | 34 | 5,956,000 | 1.0× | **4** |

**The `partials` column is half the story, and the table cannot separate the halves — so the bench does it
directly.** Running the same native aggregation against *itself*, dumping at Lua's rate or not at all:
4 partials in 31 ms against 4168 partials in 48 ms. So of the ~3×, about **1.5× is shipping 4168 partials
instead of 4** and about **2× is the engine**. They multiply; neither alone is the headline.

A scalar accumulator was measured too, because Lua's count+sum has to index a map twice per record where
native writes two struct fields — a real confound. It is not the dominant one: lua 82 ms, wasm 94 ms, native
33 ms. So Lua's map indexing is worth ~13% and the engine advantage holds at ~2.5× without it.

**Emitting 20,000 matching records of 200,000 — and here native's advantage disappears entirely:**

| engine | ms | records |
|---|---|---|
| lua | 165 | 20,000 |
| native (C) | 163 | 20,000 |
| native (Rust) | 167 | 20,000 |

Indistinguishable, because the bottleneck moved: the cost is building and shipping 20,000 record messages, and
every engine hands that to the same response path. **A native filter is not faster than a Lua one.** What it is,
is *expressible without an accumulator* — and WASM cannot do it at all, because a WASM stream UDF's `end` writes
exactly one value and its `record` hook returns only a status, so there is nowhere for a per-record emission to
go. That would need an emit call in the WASM ABI.

For scale, a plain scan of the same 200,000 records costs **1439 ms** — 44× the count+sum aggregation, and 9×
the filter that returns a tenth of them. That is the number that puts the rest in perspective: the engine is a
minority of a query, and the response path is most of it.

**One artifact worth naming**, because it looked like a real effect for four runs: a set-scoped query with no set
index reduces over the namespace's whole primary index, so records a previous run left behind are records the
next one walks. Four consecutive runs drifted the native row 33 → 47 ms and compressed every ratio with it. The
bench now truncates its set at the end, and the numbers above are from a clean namespace.

---

# 10. Merging, and running the same code on the client

Wiring `merge` turned out to have a much better shape than section 5 assumed, because of what the query
worker loop actually does (`as/src/query/query_job.c`):

```c
while ((pid = as_faa_uint32(&_job->pid, 1)) < AS_PARTITIONS) {
    ...
    _job->vtable.slice_fn(_job, &rsv, &bb);      // one call per partition slice
}

if (bb != NULL) {
    _job->vtable.slice_fn(_job, NULL, &bb);      // <-- once per THREAD, with a buffer in hand
    cf_buf_builder_free(bb);
}
```

That second call is a **per-thread finish hook that already has a `cf_buf_builder`**. So the accumulator does
not have to be node-level to collapse partials, and that matters:

|                | partials per node | locking | plumbing to change |
|---|---|---|---|
| per slice (today, and Lua) | hundreds | none | none |
| **per thread** | one per query thread (a handful) | **none** — a thread owns its accumulator | the existing `slice_fn(NULL)` hook |
| per node | one | a mutex on the job accumulator | job-finish emission, into the response path |

Per-thread gets nearly all of the reduction for none of the risk, and needs no mutex on the accumulate path at
all. Per-node remains a later refinement, not a prerequisite.

## The dump

`accumulate` gains a third answer besides ok and error: **`AS_NATIVE_FULL` — "I took the record, and I am now
at capacity."** The host then finalizes the accumulator, writes that partial to the current slice's stream,
zeroes the state, re-runs `init`, and keeps going. That is the requirement directly: merge until out of records
or out of room, then dump the buffer and let the client carry on.

A fixed-size accumulator never returns it. One that grows — a group-by holding many keys — decides its own
ceiling, which is the only place that decision can sensibly live.

## One accumulator, one merge, and the wire it already had

Everything folds through the **same** operation, at every level:

```text
record   ->  acc.merge(map(&rec))          in memory, native call
slice    ->  thread_acc.merge(&slice_acc)  in memory, native call
thread   ->  to_partial()  -- the wire --  from_partial()   at the end, or whenever full
client   ->  combined.merge(&partial_acc)  in memory, native call
final    ->  combined.finalize()           once, on complete data
```

No serialization on the server. None on the client. The only encoding is the one crossing the wire, and it is
**the convention that is already there**: a partial is an ordinary value in the query result stream, msgpack
like every other value on that path, exactly as a Lua partial is. Nothing in the protocol changes.

That deletes everything I had bolted on in the previous draft — no raw-byte blob, no size or byte-order guard
(msgpack is portable), and **no POD constraint**, so an accumulator owning a map or a `Vec` is fine. It also
means a client that does not link the plugin still receives something it can read.

### Partials are mergeable by construction, and there are many of them

An earlier draft argued mergeability was unnecessary because `finalize` runs once on complete data. **That is
wrong**, and the counterexample is an accumulator that *collects*: run an expression over a billion records and
keep the matches. It grows with its input, so it never reaches the end — it must dump when it hits a memory
threshold, repeatedly. Many partials per thread is the normal case for an unbounded accumulator, not an edge
case, so they have to combine.

What makes that painless is that **a partial is the accumulator, not a summary of it**. `to_partial` and
`from_partial` round-trip it faithfully, and two accumulators combine by definition — that is `merge`. So the
author never designs a mergeable representation and never faces the old trap of emitting a mean that cannot be
combined: `finalize` is a *different* operation, computing the user's answer from a fully merged accumulator, and
it is free to return a mean precisely because it is not the thing being merged.

### Dumping, and the threshold

`accumulate` gains a third answer beside ok and error: **`AS_NATIVE_FULL` — "I took the record and I am now at
capacity."** The host serializes the accumulator to a partial, writes it to the current slice's stream, zeroes
the state, re-runs `init`, and continues. A fixed-size accumulator never returns it; a collecting one decides its
own ceiling, which is the only place that decision can live.

An operator needs a say too, since a plugin's idea of "full" should not be the only bound on server memory. So
the accumulator may report its footprint — `heap_used()`, defaulting to zero — and the host dumps when a
configured ceiling is crossed. Two independent brakes: the plugin's own, and the operator's.

### Two client shapes, because collecting is not reducing

A reducing aggregation (sum, stats) wants every partial merged into one, then `finalize`. A collecting one does
not: its partials *are* the output, in chunks, and a client should hand them to the user as they arrive rather
than merge a billion records back into one accumulator — which would work, and would defeat the point of having
dumped them. So `Combiner` offers both: absorb-and-merge, and iterate-as-they-arrive.

## The Rust SDK does the serializing, and the author does not

The SDK owns the conversion in both directions. Rust cannot reflect over a struct's fields, so the accumulator
is declared *through* a macro, which generates the pair — no proc macro, no dependency, consistent with `udfs!`
and `module!` being `macro_rules!`:

```rust
partial! {
    pub struct Stats {
        count: i64,
        sum: i64,
        min: i64,
        max: i64,
    }
}

impl Aggregation for Stats {
    const NAME: &'static CStr = c"stats";

    fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold> { ... }
    fn merge(&mut self, other: &Self) { ... }
    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> { ... }   // once, on everything
}
```

`partial!` generates `Default` and `AsPartial`: `to_partial(&self, &Ctx) -> Result<Value>`, building a map of
field name to value through the ordinary `Ctx` builders, and `from_partial(&mut self, Value) -> Result<()>`
reading it back. Nothing calls either by hand.

A field's **name is its wire key** — the trade for not writing the format down is that it is derived from the
code, and the code is the only place it exists. Field types go through one trait, `PartialField`: `i64`, `f64`,
`bool`, `String`, and a `Vec` of any of those or of another `partial!` struct. That last case is what lets a
collecting accumulator hold `Vec<Row>` and still have its serialization written for it.

There is deliberately **no `impl` for `Vec<u8>`**: it would collide with the blanket `Vec<T>` one, and silently
putting a byte buffer on the wire as a list of integers is worse than not compiling. A blob field writes
`AsPartial` by hand, which is what `partial! { @no_impl … }` leaves room for.

The author therefore writes: the field list, `accumulate`, `merge`, `finalize`. Serialization is not in that
list. Other languages solve it their own way — the ABI only requires that a partial be a value, which is what
the wire has always carried.

## The client half

The plugin's `finalize` builds its answer through `Ctx` — `ctx.map(4)`, `map.put_str_int(...)`. On the client
there is no host to build through, and that is the only thing standing between the server and the client
running identical code.

So the SDK ships a **local host**: the same `as_native_host` vtable implemented in Rust over ordinary Rust
collections. `finalize` cannot tell the difference, so it runs unchanged in a client process, and what it
builds converts to `aerospike::Value` for the caller.

On top of that, one type:

```rust
use aerospike_udf_sdk_native::client::Combiner;
use my_plugin::Stats;                       // the same crate the server dlopen's

let mut combiner = Combiner::<Stats>::new(&args)?;

for partial in partials_from_the_query {    // ordinary values, in any order
    combiner.absorb(partial)?;              // from_partial, then the plugin's own merge
}

let answer = combiner.finish()?;            // the plugin's own finalize, on complete data, once
```

The plugin crate becomes `crate-type = ["cdylib", "lib"]` — the same source, dlopen'd by the server and linked
into the client.

## Status: built, and measured

**Done and green.** ABI **v2** — the bump was not optional: `accumulate` gained a parameter and `heap_used`,
`from_partial` and `finalize` arrived, all of which move fields inside `as_native_agg_export`. The version gate
earned its keep immediately: with the header updated and only the C plugin rebuilt, the still-v1 Rust plugin was
refused at startup with its name in the log rather than reading a function pointer from the wrong offset.

What the numbers say, against the dev VM with 50 records:

| | before | after |
|---|---|---|
| partials for a reducing aggregation | one per non-empty partition slice (up to 50) | **4** — one per query thread |
| a collecting aggregation | inexpressible | dumps on `Fold::Full`, 13+ chunks at 4 per chunk |
| a filter | inexpressible | records straight to the response, no partial at all |
| the client's answer | the caller re-derived it | the plugin's own `finalize`, on merged data |

**The operator ceiling works, and overshoots by one record.** With `agg-memory-limit 80` — ten `int64`s — and a
plugin chunk of 1000 that can never fire over 50 records, both plugins dumped 7 times with a largest chunk of
**11**. Eleven rather than ten because the host checks `heap_used() > limit` *after* taking the record: the
ceiling is a threshold you cross, not a cap you cannot exceed. Worth knowing before sizing one.

**Two other things the tests found.** A thread that dumps and then finishes emits a **final empty partial** —
harmless, and a client reading chunks has to tolerate it. And the two plugins' `sum` partials now differ in
shape on purpose: the C example hand-writes `to_partial` as a bare integer, the Rust one has `partial!` generate
a map of its fields. The suite accepts either, which is the right amount of opinion for an ABI to have about a
format that belongs to the plugin.

### What is deliberately still not done

**Per-node partials.** A handful per node, not one. Collapsing further means emitting when the *job* finishes
rather than when a thread does, and `aggr_query_job_finish` closes the client connection before anything else
runs — so it needs a mutex on a job-level accumulator and a change to the response path, for a gain of "four
partials become one". Per-thread got the reduction that mattered for none of that risk.

**`merge` on the server.** Never called there, and that is not an omission: a thread keeps one accumulator
across all its slices, so no two accumulators ever meet. It is the client's combine, and it is the same
function — which is the whole point of it living in the ABI rather than in a client library.

---

# 11. The three shapes, in code

> The examples in this section were written before section 12 added the `out: &Emit` parameter, and are left as
> they were because the argument in 12 only makes sense against them. **The shipped signature is
> `accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold>`** — these three ignore `out`; see
> `aerospike-udf-sdk-native/examples/plugin/src/lib.rs` for the four shapes as they actually compile.

The trait, with what the `partial!` macro generates marked:

```rust
pub trait Aggregation: Default + AsPartial + Sized {
    const NAME: &'static CStr;

    fn configure(&mut self, args: Args<'_>) -> Result<()> { Ok(()) }

    /// Fold one record in. Answer `Fold::Full` to be dumped and reset.
    fn accumulate(&mut self, rec: &Record) -> Result<Fold>;

    /// Combine two accumulators. Used for records within a slice, slices within a thread, and
    /// partials on the client — the same operation at every level.
    fn merge(&mut self, other: &Self);

    /// Footprint, so the host can enforce an operator-configured ceiling as well as your own.
    fn heap_used(&self) -> usize { 0 }

    /// The user's answer, from a fully merged accumulator. Not the thing that gets merged, so it
    /// may return something unmergeable — a mean, a ratio, a formatted string.
    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>>;

    // to_partial / from_partial come from `partial!` — you never write or call them.
}

pub enum Fold { Continue, Full }
```

## 1. Sum — bounded, reducing

Fixed size whatever the record count, so it never dumps: one partial per thread, and the client merges
them into one number.

```rust
partial! {
    pub struct Sum {
        total: i64,
        bin: String,        // allocated once per accumulator, not per record
    }
}

impl Aggregation for Sum {
    const NAME: &'static CStr = c"sum";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args.get(0).as_str().unwrap_or("v").to_string();
        Ok(())
    }

    fn accumulate(&mut self, rec: &Record) -> Result<Fold> {
        if let Some(v) = rec.get_int(&self.bin)? {
            self.total += v;
        }

        Ok(Fold::Continue)          // never full - nothing here grows
    }

    fn merge(&mut self, other: &Self) {
        self.total += other.total;
    }

    fn finalize<'c>(&mut self, _ctx: &'c Ctx) -> Result<Value<'c>> {
        Ok(Value::int(self.total))
    }
}
```

Client — reduce mode:

```rust
let mut combiner = Combiner::<Sum>::new(&args)?;

for partial in query_results {
    combiner.absorb(partial)?;      // from_partial, then Sum::merge
}

let total = combiner.finish()?;     // Sum::finalize, once
```

## 2. Return matching records — unbounded, collecting, dumps

The case that makes dumping mandatory: a billion records in, an unknown number out.

```rust
partial! {
    /// Owned, because a `Record` is borrowed for the length of one call and cannot be kept.
    pub struct Row {
        id: i64,
        name: String,
    }
}

partial! {
    pub struct Matches {
        pub rows: Vec<Row>,
        min_score: i64,
    }
}

impl Aggregation for Matches {
    const NAME: &'static CStr = c"matches";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.min_score = args.get(0).as_i64().unwrap_or(0);
        Ok(())
    }

    fn accumulate(&mut self, rec: &Record) -> Result<Fold> {
        if rec.get_int("score")?.unwrap_or(0) < self.min_score {
            return Ok(Fold::Continue);
        }

        // Copy out what we want to keep. This is the whole reason `Row` exists.
        self.rows.push(Row {
            id: rec.get_int("id")?.unwrap_or(0),
            name: rec.get_str("name")?.unwrap_or("").to_string(),
        });

        // Dump when a chunk is worth a round trip. Bounded memory over unbounded input.
        Ok(if self.rows.len() >= 10_000 { Fold::Full } else { Fold::Continue })
    }

    fn heap_used(&self) -> usize {
        self.rows.iter().map(|r| r.name.len() + 40).sum()
    }

    fn merge(&mut self, other: &Self) {
        self.rows.extend(other.rows.iter().cloned());
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let out = ctx.list(self.rows.len() as u32)?;

        for row in &self.rows {
            let m = ctx.map(2)?;

            m.put_str_int("id", row.id)?;
            m.put_str_str("name", &row.name)?;
            out.push_map(&m)?;
        }

        Ok(out.as_value())
    }
}
```

Client — **stream mode**, which is the point of having dumped:

```rust
let combiner = Combiner::<Matches>::new(&args)?;

for partial in query_results {
    for row in &combiner.read(&partial)?.rows {   // one chunk's worth in hand, never more
        present(row);
    }
}
```

Calling `absorb` in a loop here would also work, and would rebuild the billion rows the dumping existed to
avoid. That is why there are two modes rather than one. Note also that a thread which dumps and then finishes
emits a final **empty** partial, so a reader has to tolerate a chunk of zero rows.

## 3. A variable number of maps per record — fan-out

Output size is not a function of input size: each record contributes zero, one or many.

```rust
partial! {
    pub struct Item {
        id: i64,
        tag: String,
        weight: f64,
    }
}

partial! {
    pub struct Exploded {
        pub items: Vec<Item>,
    }
}

impl Aggregation for Exploded {
    const NAME: &'static CStr = c"explode";

    fn accumulate(&mut self, rec: &Record) -> Result<Fold> {
        let id = rec.get_int("id")?.unwrap_or(0);
        let weight = rec.get_f64("weight")?.unwrap_or(1.0);
        let items = &mut self.items;

        // One map per tag - a record with no tags contributes nothing, one with fifty contributes fifty.
        if let Some(tags) = rec.get("tags")?.as_list() {
            tags.for_each(|_, tag| {
                if let Some(t) = tag.as_str() {
                    items.push(Item { id, tag: t.to_string(), weight });
                }

                true
            });
        }

        Ok(if items.len() >= 10_000 { Fold::Full } else { Fold::Continue })
    }

    fn heap_used(&self) -> usize {
        self.items.iter().map(|i| i.tag.len() + 56).sum()
    }

    fn merge(&mut self, other: &Self) {
        self.items.extend(other.items.iter().cloned());
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let out = ctx.list(self.items.len() as u32)?;

        for item in &self.items {
            let m = ctx.map(3)?;

            m.put_str_int("id", item.id)?;
            m.put_str_str("tag", &item.tag)?;
            m.put_str_f64("weight", item.weight)?;
            out.push_map(&m)?;
        }

        Ok(out.as_value())
    }
}
```

## What the three have in common

`merge` is the only combining operation, and it is the same call whether it is folding a record, a slice, a
thread or a partial from the wire. `finalize` runs on whatever has been merged and is the only place an
unmergeable answer may appear. Nobody writes serialization.

The difference between them is a single line: whether `accumulate` can answer `Fold::Full`. That is what makes
2 and 3 survive a billion records, and it is the only thing shape 1 does not need.

---

# 12. Emitting as you go, with no accumulator at all

Section 11's `accumulate` can only *store*, which means the cheapest possible filter — "return each matching
record, immediately, allocating nothing" — cannot be expressed. It should be, and the fix is one parameter:

```rust
fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold>;
```

`Emit` writes to the same output stream the partials go to. The three examples in section 11 gain a parameter
they ignore; what it buys is the other end of the spectrum:

```rust
partial! { pub struct Filter; }          // zero-sized: state_size is 0

impl Aggregation for Filter {
    const NAME: &'static CStr = c"filter";

    fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold> {
        if rec.get_int("score")?.unwrap_or(0) >= 100 {
            // Straight through to the response. No accumulator, no copy, no allocation.
            out.record(rec)?;
        }

        Ok(Fold::Continue)               // never full: nothing is being held
    }

    fn merge(&mut self, _other: &Self) {}                                    // nothing to combine
    fn finalize<'c>(&mut self, _: &'c Ctx) -> Result<Value<'c>> { Ok(Value::nil()) }
}
```

The client reads records off the query stream exactly as it would from a plain query. `Combiner` is not
involved, because there is nothing to combine.

## Two emit calls, and only one of them is free

- **`out.record(rec)`** — pass the record through. The server already has it laid out; this writes it to the
  response the way a plain query does, so there is no `as_val`, no msgpack built by the plugin, and no
  allocation anywhere. This is the one that is actually free, and it is exactly the filter case.
- **`out.value(v)`** — emit something derived. Building `v` allocates in the *server's* allocator, as any
  constructed value does. Cheaper than accumulating it, but not free, and it should not be described as such.

## The spectrum this opens up

| shape | state | emits | partials | client |
|---|---|---|---|---|
| filter | zero-sized | per record, via `Emit` | none | reads the stream |
| chunked collect | `Vec<Row>` | on `Fold::Full` | many | `Combiner::read` |
| reduce | fixed | once, at the end | one per thread | `Combiner::absorb` |

All three are the same trait. The differences are whether `accumulate` emits, and whether it can answer
`Fold::Full`.

## One rule that has to come with it

**An aggregation should emit output values or partials, not both.** They travel on the same stream, and a client
receiving a mixture has no way to tell which is which. If a UDF genuinely needs both — matches as they are found
*and* a running total — the values have to be self-describing, and that is the author's problem to solve rather
than something the ABI can fix. The three shapes above each pick one, which is why none of them has to think
about it.

# WASM UDFs on WAMR — implementation and measurements

`modules/mod-wamr/` implements WASM UDFs: pure C, on WebAssembly Micro Runtime at upstream `main` (9058db92, 2026-10-21; its version header still reads 2.4.3), with ahead-of-time
compilation. **It is the only WASM engine.** `modules/mod-wasm` — the Rust/wasmtime implementation this
document originally compared it against — has been removed; the comparison that justified the removal is kept
below, because the numbers are why.

```text
mod-wamr {
    running-mode aot          # aot (default) | interp | jit | fast-jit
    aot-compiler /path/to/wamrc
    aot-cache-dir /opt/aerospike/usr/udf/lua-aot    # optional; derived if unset
    max-memory 64M
    cache-enabled true
}
```

`running-mode` selects the execution tier **at run time**, among the tiers the build compiled in. That
qualification is inherent rather than an implementation shortcut: the LLVM JIT *is* LLVM, so offering it means
linking LLVM into `asd` whether or not anyone selects it. The build decides what is available
(`make WAMR_TIER=aot|jit|fastjit|interp`), the config decides what is used, and a mode that is not in the build
is **reported and falls back to the interpreter** rather than refusing to start. That fallback needs saying out
loud, because the interpreter measured only ~1.8× faster than Lua on compute — silently landing there would
look like a WASM that was not worth having.

---

## 1. What the numbers say

Lua is measured in **every** run, and the column that matters is the comparison against it: the network
baseline drifts between runs, "how much more than Lua does this cost" does not. `engine` is not dynamic, so
the two engines cannot be live in one process — hence Lua as the common reference rather than a single run.

Three functions, identical arithmetic in every language, against **all three WAMR tiers** plus wasmtime. Each
tier was confirmed from the server log (`wamr: executing in <mode> mode`) rather than assumed — every compiled
tier silently falls back to the interpreter when its compiled form is unavailable, and benchmarking that
fallback as though it were a JIT is the easiest mistake here to make.

| | Lua | wasmtime (JIT) | wamr interp | wamr LLVM JIT | **wamr AoT** |
|---|---|---|---|---|---|
| `answer` — return 42, touch nothing | — | **+8 µs** | +10 µs | +15 µs | **+10 µs** |
| `increment` — read a bin, write it, return it | — | +12.5 µs | +16 µs | +12.5 µs | +12.5 µs |
| `work` — 200,000 multiply-adds, over baseline | 3380 µs | 293 µs (11×) | 1922 µs (**1.8×**) | 149 µs (23×) | **74 µs (45×)** |
| runtime linked into `asd` | — | ~100 MB archive | **686 KB** | 2 MB **+ libLLVM at run time** | 830 KB |
| needs LLVM | no | no | no | **yes, inside `asd`** | only to build `wamrc` |

**AoT wins outright, including against WAMR's own JIT.** That is the answer to "how does the JIT compare":
on this platform it is **2× slower than AoT** and it puts LLVM inside the server process — `ldd asd` gains
`libLLVM-15.so.1`, so LLVM becomes a package dependency of the database rather than of a build host. Both
tiers are LLVM; the likely explanation for the gap is that `wamrc` optimises fully ahead of time while WAMR's
ORC JIT in lazy mode keeps its optimisation level low to bound compile latency. That is an inference from the
build configuration, not something measured directly.

**The interpreter is barely faster than Lua** — 1.8× on compute, against AoT's 45×. This matters beyond
curiosity: the interpreter is the fallback whenever `wamrc` is missing or an `.aot` is stale or foreign. So a
misconfigured AoT deployment does not degrade gracefully to "a bit slower"; it degrades to roughly Lua's
performance while carrying all of WASM's per-call overhead — strictly worse than Lua for compute-bound work.
That makes the `wamr: executing in <mode> mode` log line an operational necessity, not a nicety.

One result stands out, and one prediction did not survive contact:

**Retracted: the data-path advantage.** This section previously read "on the data path WAMR is ~4.5 µs
cheaper per call (`increment`: +8 vs +12.5)", and attributed it to the ABI. **That was an artifact of a bug in
mod-wamr's own example**: its `increment` called `set_int` and never committed, so it was doing strictly less
work than the wasmtime version — it wrote nothing. The client test caught it (the second call returned 11
again instead of 12); with the commit added, `increment` costs +12.3 µs against wasmtime's +12.3 µs, measured
three times each. **The two engines are indistinguishable on this workload.**

The lesson is not subtle: the faster number came from the version that was broken, and the benchmark could not
tell. It took a correctness test to notice.

That did not disprove the ABI argument, and the right place to look was **collections** — which mod-wamr walks
in place through handles while mod-wasm serialises, copies and parses the whole thing. Measured, summing a list
bin, over the network baseline:

| n | Lua | wasmtime | **wamr AoT** | wamr vs wasmtime |
|---|---|---|---|---|
| 100 | ~7 µs | 19.0 | **10.1** | 1.9× faster |
| 1,000 | 74.5 | 89.9 | **67.5** | 1.33× |
| 10,000 | 713–723 | 884.7 | **639.6** | **1.38×** |

Per element that is ~0.087 µs for wasmtime, ~0.064 µs for wamr and ~0.071 µs for Lua. **So the data-model
claim holds after all, on collections rather than on a scalar** — and the margin at n=10,000 is ~245 µs, far
above the few microseconds of noise that made the `increment` comparison unreadable.

The sharper statement is the comparison with Lua: **mod-wamr beats Lua on collections (0.89×) while mod-wasm
loses to it (1.24×)**. Serialising a list to msgpack and parsing it in the guest costs more than Lua's own
object access; reading it in place through handles costs less.

One prediction that did *not* hold: a crossover. One import call per element should eventually lose to a single
bulk decode, so small lists were expected to favour wasmtime — they do not, at any size from 1 to 10,000. At
n=1 both are inside the noise, and from n=100 upward mod-wamr leads throughout.

Measurement note: the first attempt at this sweep stopped at n=1,000 and produced a *negative* Lua delta at
n=10 — the signal was a few microseconds against an ~80 µs round trip. Nothing below n≈100 here is worth
reading, which is why the sweep goes to 10,000.

**On compute WAMR AoT is ~4× faster than wasmtime** (74 µs vs 293 µs over baseline). `wamrc` is LLVM at
`-O3`; wasmtime's cranelift trades code quality for compile speed. Both are far ahead of Lua.

`answer` is the one place wasmtime is (marginally) ahead — WAMR's per-instantiation cost is slightly higher
here — but the gap is ~2 µs against a run-to-run spread of ~2 µs, so it is not worth claiming.

### Size

| | wasmtime | wamr |
|---|---|---|
| runtime linked into `asd` | ~100 MB archive | **1.2 MB** (830 KB vmlib + 371 KB module) |
| a compiled UDF module | 42.8 KB | **3.5 KB** |

The guest is 12× smaller for the same functions because **this ABI needs no allocator**: the host never
pushes bytes into guest memory uninvited, so a UDF can be `no_std` with no `alloc` at all. That also removes
what profiling previously identified as the single largest per-call cost on the wasmtime path — the guest's
first heap allocation.

### Startup and registration

- Registration: ~1.1 s end to end for both engines, dominated by the SMD round trip. AoT compilation is **not**
  part of that any more — it happens on a background thread and the module runs interpreted until it lands,
  which is what took the client test suite from flaky-at-a-30 s-timeout to four consecutive clean runs at the
  default 3 s. See §3.
- Startup with 89 registered modules: **35 s cold, then instant** — a restart recompiles nothing (measured:
  zero compiles), because freshness is decided by a content hash rather than by mtime.

---

## 2. Why the ABI is different, and why that was the right call

The instruction was to implement WAMR *as well as possible* rather than mirror the Rust module, because C can
model the data differently. It can, and the second generation of the ABI (2026-10-20, see §7 and
`WASM_UDF_DESIGN.md`) is where that ended up:

- **No msgpack anywhere, and nothing deserialised.** A C module holds the record natively and lends the
  guest its data **by position**: every read is a 16-byte value, and a string or nested collection carries
  where it sits in the stored bytes rather than a copy of them.
- **Collections are read in place, 128 values per import.** The wasmtime path serialised the whole
  collection, copied it into guest memory and had the guest parse it; the first WAMR ABI walked it one
  import per element. Both lost to a window of values decoded straight from the packed particle.
- **Arguments are one import.** The guest receives their values directly from the `as_list` the server
  built, strings borrowed in place.
- **Results are built directly** in the server's representation - and a value read from the record is
  returned, stored or nested in a built collection from its bytes, with the server's own unpacker building
  it. The guest never decodes a document it only passes along.

The cost is that the two guest ABIs are incompatible: a module built for one engine does not run on the other,
and switching engines means re-registering UDFs.

**What it is measurably worth: the allocator, and collections.** A guest needs no allocator, which makes its
module 12x smaller and removes what profiling had found to be the largest per-call cost on the wasmtime path.
And reading a collection in place is **1.38x faster than serialising it** at n=10,000 with the first ABI, and
**2.7x faster than that ABI's own handle walk** with the second - see §7. What it is *not* worth is latency on
a single scalar bin, where the engines are level; a claim to the contrary turned out to be a bug in the
example.

Two consequences worth stating plainly:

- **`mod-wamr` duplicates the record accessors and value handling.** That is inherent: a shared
  implementation would mean one side calling into the other, which is the C shim arrangement that was just
  removed. The duplication is deliberate and temporary - it exists so one can win.
- **Stream UDFs (aggregation) work.** See §6.

---

## 3. What went wrong, and what it cost to find out

Five real bugs, four of them mine. They are recorded because each was invisible to the obvious test.

**The transaction deadline was enforced from the wrong thread.** `wasm_runtime_terminate` only sets an
exception, so a watchdog thread is needed to fire it — and my watchdog called `as_timer_timedout`, whose
server-side implementation reads `static __thread uint64_t g_end_ns`. On the watchdog's thread that is zero,
so **every UDF appeared to have already timed out**. `answer` and `increment` survived because they finish
inside one 2 ms tick; only the compute-bound function exposed it, failing with `Exception: terminated by user`.
Fixed by asking for the remaining time once, on the transaction thread, and comparing an absolute deadline.
*Without a compute-bound benchmark this would have shipped.*

**AoT output was cached where the server deletes it.** `udf_cask_init` removes **every file** in the UDF
directory on startup, because SMD is the source of truth for what is registered. An `.aot` written beside its
`.wasm` is therefore wiped on every boot, so all 89 modules were recompiled every time — ~430 ms each,
serially, before the port opened. The benchmark's "connection refused" was the symptom. Fixed with a separate
`aot-cache-dir`, and:

**The freshness check was wrong even after that.** Comparing `.aot` mtime against `.wasm` mtime cannot work,
because SMD rewrites every UDF file as it replays — the `.wasm` is always newer. Fixed with a content hash in
a tag file: identical bytes reuse the `.aot` however often the file is rewritten. **35 s cold → 1 s warm.**

**Reference types were disabled, so no Rust module could load.** WAMR defaults them off; a current Rust
wasm32 toolchain emits funcref tables even for a `no_std` library with no indirect calls. Every UDF failed with
"zero byte expected. The module uses reference types feature which is disabled in the runtime" — in both the
AoT and interpreter paths. The AoT→interpreter fallback worked exactly as designed, which is how it was clear
the problem was not the AoT path.

**`cf_malloc` is a macro** (under `-DENHANCED_ALLOC`), so there is no symbol to hand WAMR's allocator hooks.
This is the third time the same trap has bitten this work from a different direction. Now the module Makefile
defines `-DENHANCED_ALLOC` itself, so a standalone build cannot produce objects that fail only when `asd` is
linked.

**A benchmark that measured nothing.** The first `work` comparison showed WAMR 15× faster than wasmtime — and
was invalid: the two example crates were built at different `opt-level`s (`"s"` vs `3`), and `sum(i*i)` over a
literal range is a constant an optimiser may fold away entirely. Both fixed: matched optimisation levels, and
the loop is now seeded from the record so no compiler can precompute it. The corrected gap is 4×, not 15×.

---

## 4. What this does not settle

- **Fast JIT is not available on aarch64**, and this is now enforced rather than discovered: `core/iwasm/
  fast-jit/cg/x86-64` is the entire code-generator list, and WAMR's own cmake responds to
  `WAMR_BUILD_FAST_JIT=1` on another architecture by printing "Fast JIT disabled" and building an
  interpreter-only runtime — which would then be benchmarked as a JIT. `WAMR_TIER=fastjit` therefore fails the
  configure step outright on anything but x86-64. **It is the one tier still unmeasured**, and the interesting
  one: LLVM-free like AoT, but compiling at load time like the JIT. Measuring it needs an x86-64 host.
- **AoT needs `wamrc`, which needs LLVM 15+ to build.** The runtime does not: loading an `.aot` needs only
  `WAMR_BUILD_AOT=1`, and `wamrc` is a separate executable the server spawns. So `asd` links no LLVM, and the
  dependency is on the build host and on whichever hosts register modules. The LLVM **JIT** tier is the
  opposite trade — no `wamrc`, no `.aot` files and no cache to keep fresh, but `libLLVM` becomes a runtime
  dependency of the database, and it is 2× slower. Measured; see §1.
- **`.aot` files are architecture- and version-specific.** A mismatch is detected at load and falls back to
  the interpreter rather than refusing the UDF, which is tested by construction (it is how the reference-types
  failure surfaced) but not by an automated test.
- **Test parity now exists and passes.** `test/unit.c` (`make unit`) covers the six places this module
  transfers `as_val` ownership: **1329 checks, 0 failures**. It uses a mock `as_rec` and a refcount technique
  that turns a leak or a double free into an *equality check* rather than a crash somewhere unrelated — every
  value is reserved first, so `count == 1` afterwards proves exactly one release and `count == 2` proves none.
  It was mutation-tested rather than trusted: deleting the `as_val_destroy` from `set_bin`'s bad-name path
  fails exactly two checks and nothing else.

  The client suite carries a fixture per engine and picks the one that runs: **14 tests green on wasmtime,
  8/8 on WAMR**, including new tests for the memory cap and for reading a list bin.

- **Registration stalls the SMD thread, and clients notice.** AoT compilation happens inside the `FILE_ADD`
  handler, which is the same thread that answers `udf-put` *and* the info commands a client uses to tend the
  cluster. A compile is 50 ms for a 4 KB module and ~500 ms for a 40 KB one, and concurrent registrations
  serialise behind each other — a parallel test suite registering sixteen modules intermittently saw `udf-put`
  time out at the default 3 s, and occasionally saw a client fail to validate its seed host at all.

  Moving the compile off the SMD thread is the fix: return from `FILE_ADD` immediately, serve applies from the
  interpreter meanwhile, and swap the AoT module in when it is ready. Until then, registering WASM modules is
  measurably slower than registering Lua ones and should not be done in bulk against a live cluster.

  Two things this is *not*, both checked rather than assumed: registration is **idempotent** (one registration
  produces exactly one compile — 1051 → 1052 in the log), and the doubling seen in the suite is the test
  probe registering both fixtures under one package name, not the server compiling twice.

---

## 4b. Considered and rejected: compiling in-process

`wamrc` is invoked with fork/exec. The alternative is to compile inside `asd`, which WAMR supports properly —
`aot_export.h` sits in `core/iwasm/include/` **beside `wasm_export.h`**, so the AoT compiler is a public
embedding API and not merely a `main()`. The full sequence is available (`aot_compiler_init` →
`aot_create_comp_data` → `aot_create_comp_context` → `aot_compile_wasm` → `aot_emit_aot_file_buf`), including
emitting to **memory** rather than a file, and `aotclib` already exists as a build target. Either linking it or
`dlopen`ing a `libwamrc.so` would work.

Sizes first, because they turn out not to be the deciding factor:

| | size |
|---|---|
| `wamrc` binary | 1.22 MB |
| `libLLVM-15.so.1` behind it | **106 MB** |
| `aotclib`, LLVM excluded | 1.3 MB |
| `libvmlib.a`, what `asd` actually links | 0.83 MB |

That 106 MB is identical whether LLVM is fork/exec'd, `dlopen`ed or statically linked. Only *where it lives*
differs.

**The deciding factor is crash isolation.** AoT compilation is LLVM chewing on bytes a client uploaded. A
malformed module that makes LLVM assert, abort or exhaust memory kills a forked child harmlessly today, and
the next apply reports it; in-process it takes down the node. fork+exec costs a few milliseconds against a
430 ms compile — about 1%.

The in-process design would buy emit-to-memory and no second binary whose version must match. Neither is
worth much here: the version concern applies to a `.so` equally, and the on-disk `.aot` is not a wart but the
thing that makes a restart 1 s instead of 35 s — emitting to memory would undo that.

**Decision (2026-08-05): keep fork/exec.** If this is ever revisited, the number to get first is how much of
the 106 MB becomes *resident* once loaded — it is 106 MB mapped, but RSS depends on what a compile touches,
and that was not measured. The `jit` tier links the same library, so it can be measured by comparing `asd` RSS
between tiers.

## 6. Stream UDFs — aggregation

Implemented for mod-wamr. Three things had to change, and one of them was a bug in the server rather than in
either module:

**`aggr.c` was hardcoded to `&mod_lua`.** Record UDFs already dispatched by language; aggregation was simply
missed, so a stream UDF registered as WASM was handed to Lua, which reported a syntax error on a binary file.

**One instance serves the whole stream.** An aggregation has to accumulate, and a guest gets a fresh instance
per *call* — so per-record instantiation would leave it nowhere to keep a running total. Instantiating once per
query lets the accumulator live in the guest's own memory (ordinary `static mut`), which is its natural home,
and costs one instantiation per query instead of one per record. The per-call-freshness property the record
path relies on is not weakened: an aggregation *is* one operation, and nothing leaks between queries.

What must **not** persist across records is this side's state. Every handle borrows from the record that
produced it, so carrying handles over would hand the guest values belonging to a released record — and 1,000
records would overflow a 256-entry table besides. `mod_wamr_ctx_next_record` releases them between records and
leaves the guest's memory alone.

**No stream-of-closures.** Lua builds a pipeline out of `map`/`filter`/`reduce` closures; there are no closures
to build one from here, and `as_stream` is already a pull interface. So the host drives the loop and the guest
exports two functions, derived from the name the client asks for:

```text
<function>_record   once per record, with that record bound to the record natives
<function>_end      once at the end, with no record, to deliver the result
```

The guest SDK's `stream!` macro generates both from one declaration.

### One partial per node, and why

`_end` runs **once per node**, so a client receives one partial per node and combines them itself. That is not
laziness: `query_aggregate` gets its seamless cross-node reduce by piping the server's partials into a
**client-side Lua** pipeline (mlua), which a WASM module cannot join. WASM aggregation is therefore reached
through an ordinary `query()` with an aggregate function set, which hands the caller the partials directly.
Summing them is usually trivial, but it is the caller's job and it is not automatic.

Giving WASM full parity would mean a WASM runtime **in the client** — the same trade the `lua` feature makes
for mlua, with a much larger dependency. Not attempted.

### Map/reduce over 100,000 records — where mod-wamr loses

The workload asked for: each record holds a **map** bin, one of whose keys holds a **list** of 1–10 random
integers; the aggregation sums those integers across every record. It exercises the whole read path — record,
map, key lookup, nested list, elements — and both sides are measured in the **same run** against the same data,
since mod-wasm has no `apply_stream` to compete.

| 100,000 records | median | per record | runs |
|---|---|---|---|
| Lua | 99.0 ms | 1.0 µs | 96.8, 99.0, 145.8 |
| wamr AoT, **as first written** | 146.2 ms | 1.5 µs | 144.4, 146.2, 147.4 |
| wamr AoT, **with by-key access** | **102.7–106.1 ms** | 1.0 µs | 99.8, 102.7, 105.2 |

Both produced the same total (274,601,186; checked every round, because timing a wrong computation is
worthless).

**The first version was 1.48× slower than Lua**, and the cause was the map API rather than anything about WASM.
Two costs, both design choices in this ABI:

1. **A map handle was materialised.** `as_map` has no indexed accessor, so the host flattened its entries into a
   `cf_malloc`ed array on first access — **once per record**, plus the free.
2. **There was no by-key lookup.** The guest had to *scan* entries, paying an import call and a copy of the
   key's bytes for every key it compared. Three keys per record.

Lua pays neither: `m['vals']` is a hash lookup on a native object it was handed directly.

**Fixed by adding `map_get_*` over `as_map_get`** — one hash lookup, no allocation, no scan — plus `list_ints`,
a bulk read of an integer list in a single call rather than one per element. The record path went from ~15
import calls and an allocation to **two calls**, and the gap closed: 146 ms → 103 ms, i.e. **level with Lua**
(1.02× and 0.95× across two runs). The map lookup key is built as a *stack* `as_string`, because allocating one
per lookup would have reintroduced exactly the per-record malloc being removed.

Where that leaves the four data shapes now measured, all against Lua:

| workload | wamr AoT vs Lua |
|---|---|
| compute (200k multiply-adds) | **45× faster** |
| flat list, 10,000 elements | **1.1× faster** (and 1.38× faster than wasmtime) |
| single scalar bin | level |
| map → nested list, 100k records | level |

The pattern is consistent: this ABI is fast when it can read in bulk or in place, and its cost is *per import
call*. Every case where it looked slow has turned out to be an API that forced too many calls, not a limit of
the approach.

### The audit that followed

All 73 natives, asked two questions: how many calls does a realistic use cost, and does it allocate?

**Every remaining `cf_malloc` is on a *write* path and is unavoidable** — `as_string_new` and `as_bytes_new`
take ownership of a copy, so a guest writing a string or a blob must produce one. Exactly one allocation sat on
a **read** path, and it is now gone:

- **`materialise_map` allocated per map handle.** Indexing a map means flattening its entries, because `as_map`
  has no indexed accessor — and in a stream over 100,000 records that is 100,000 allocations to read data.
  Fixed with one 512-byte buffer (32 entries) in the per-call context, which covers what actually happens: a
  guest iterating one map at a time. A second concurrent map, or a wider one, still allocates. Per-handle
  inline storage would have been 256 × 512 bytes on a transaction thread's stack, which is why it is per
  *context* instead.

  This one needed its own test, because the failure mode is not a wrong answer: teardown must free the heap
  array and must **not** free the inline one, and freeing a stack address aborts the process. The test pins
  which buffer each map got, that a second map falls back to the heap, that teardown releases the claim, and
  that a later map gets the buffer back — the last of which is what a stream depends on between records.

**Bulk reads, where one call can replace n.** `list_ints` existed; `list_doubles` did not, which was an
arbitrary gap rather than a decision, and is now filled.

**Indexed list access: no hazard, and one redundant call removed.**

Worth checking rather than assuming, because the bad case would have been severe: if a list bin were backed by
lazily-parsed msgpack, indexed access would be O(n) and walking a list O(n²) — and `list_ints` would not have
helped, since it indexes too. It is not. `list_to_asval` deserialises the particle into an `as_arraylist`, and
`as_arraylist_get` is `index < size ? elements[index] : NULL` — O(1) and range-checked.

That range check made `list_element`'s own `as_list_size` call redundant, so per-element access was two hook
indirections where one suffices. Removed: `as_list_get` is *documented* to answer NULL for an index it cannot
serve, so this relies on the interface rather than on one implementation, and the existing
`list_elements_are_bounded` test already pins it.

**The effect is not measurable.** At n=10,000 the ratio to Lua read 0.885 before and 0.91–0.93 after — i.e. it
moved the wrong way, which is noise rather than a regression (the same runs produced a *negative* Lua delta at
n=1,000, which is the noise floor announcing itself). Single-digit nanoseconds against ±40 µs of spread. The
change stands because it removes real work and simplifies the function, **not** because it was shown to be
faster. For list-heavy work the measurable advice is `ints_into` — one call instead of n.

**Left alone, deliberately:**

- **Map *iteration* is still two calls per entry** (key, then value). A bulk form would have to commit to a
  layout for mixed-type entries; keyed access is now cheap and iteration is the rarer case, so this waits for a
  workload that shows it matters.
- **A typed read after a type check is two calls** (`bin_type` then `bin_int`). Packing them would complicate
  every accessor's return convention to save one call on a path that mostly does not need the check.
- **The six metadata natives are one call each.** A `describe`-style UDF makes six; at ~0.06 µs that is 0.36 µs,
  below what this measurement setup can resolve.

### Not covered

- **No deadline.** `aggr.c` builds its context as `{ .as = &as }` with no timer at all, so there is nothing for
  the watchdog to ask and a runaway stream UDF is bounded only by the query being abandoned. **mod_lua is in
  exactly the same position**, so this is a server-side gap rather than a regression — but it is a gap.
- **The client test suite has one fixture again.** It briefly had two, with a probe to work out which engine a
  server was running, since the ABIs were incompatible and the choice was invisible from a client. Both went
  when mod-wasm did.

## 5. Recommendation

**On these numbers WAMR is the better engine**: **4× faster on compute**, **1.38× faster on collections** (and
faster than Lua there, which mod-wasm is not), 1.2 MB instead of ~100 MB linked, 3.5 KB instead of 42.8 KB per
guest, and no Rust toolchain in the server build. Per-call cost and the single-bin data path are **level** —
an earlier claim of an advantage there was a bug in the measurement, see §1.

That is not the same as "switch now". What would decide it:

1. **Test parity.** mod-wasm's 32 unit tests exist because the ownership rules are easy to get wrong, and
   mod-wamr has the same rules with none of the tests. Its handle table transfers `as_val` ownership in six
   places; a refcount mistake there leaks or double-frees under load.
2. **The interpreter path, measured.** It is the fallback whenever `wamrc` is absent or an `.aot` is stale, so
   it is a configuration that will happen in production whether or not anyone chooses it.
3. **x86-64, with Fast JIT.** Both a different code generator and the deployment target most likely to matter.
4. **The AoT operational story.** Compilation is ~430 ms per module on the SMD thread, and every node
   compiles independently on first registration. That is fine at 89 modules and a question at 1000.

---

## 7. The second ABI, measured (2026-10-20)

The first ABI read a collection through handles, one import per element, and that was where it lost to Lua
on a map-heavy aggregation (§6, 1.48x slower). mod-native had meanwhile grown packed-form readers - decoding
a bin's stored msgpack in place, no `as_val` per element - and porting them turned into a second guest ABI
once one fact was measured: **in WASM the import call is the unit of cost, not the decode.**

Three cuts, each measured interleaved against the previous fixture on the same server, 20 calls each for the
record UDF and three runs for the aggregation:

| `sum_list` over a 10,000-element list, best | | `sumnested` over 50,000 records |
|---|---|---|
| handle walk (first ABI) | 0.74 ms | 115-123 ms |
| packed reads windowed by offset | 2.54 ms | 110-118 ms |
| cursor reads, 256 integers per import | 0.23 ms | 113-119 ms |
| **values by position, 128 per import (second ABI)** | **0.27 ms** | 114-125 ms |

Both rows are now permanent, `#[ignore]`d tests (`list_bench` in `tests/src/udf_native_bench.rs`,
`agg_bench_nested` in `tests/src/udf_agg_bench.rs`). Their first run, 2026-10-20, on the same node: the list at
335 µs per call for WASM against 598 µs for the native C module and 689 µs for Lua, round trip included; the
nested aggregation at 120 ms for WASM against **103 ms for Lua**. So on the nested shape Lua is 16% ahead, not
level - the earlier "flat ~120 ms" was WASM against its own previous cuts, and the Lua row says where the
remaining cost is: a `coll_get` plus a cursor per record, two imports where Lua indexes a table.

The second row is the lesson: windowing a list with a `from` offset re-skipped the prefix on every window, and
a cursor walk of a ten-element nested list spent fifteen imports where the handle path spent three. Bulk
reads from a cursor's position fixed the first; a one-call "the list under this key" fixed the second; and
the uniform 16-byte value - 128 per import instead of 256 typed integers - costs 0.04 ms on the big list,
which bought one way of reading for every kind. The nested aggregation never moved: at ~2.3 us per record
the query itself dominates, and nothing an engine does shows through.

What the second ABI added beyond speed, each pinned by a unit test and a client test: ordered lists and
key-ordered maps (their packed metadata element was skipped by neither module before; mod-native was fixed in
the same change), HLL as a kind of its own written back by position, a stored value returned or stored without
the guest decoding it, typed arguments rejected before the body runs, and `Result`-returning UDFs. The
first-generation natives were retired, not kept beside the new ones.

---

## 8. Three languages, one module (2026-10-20)

The question was whether the guest language matters on this ABI. The answer needed three SDKs - the Rust one
that existed, a C header (`aerospike-udf-sdk-wasm-c`) and a Go package for TinyGo (`aerospike-udf-sdk-wasm-go`;
standard Go's WebAssembly needs WASI or JavaScript, so TinyGo's freestanding `wasm-unknown` target is what makes
Go possible here) - and one program written three times to the same algorithm, step for step: the exact top-k
over packed `f32` vectors that the native SDK's vector example and its Lua port already do. `tests/src/udf_vector.rs`
holds the three to agree with each other, with Lua and with a local computation, digest for digest, and then
times them. Cosine, k = 10, 768 dimensions, best of three, one podman node:

| engine | 20,000 records | 100,000 records | per record at 100k | partials per query | module |
|---|---|---|---|---|---|
| Lua (`vector.lua`) | 213 ms | 750 ms | 7.5 us | 4,096 | - |
| WASM, Rust | 118 ms | 239 ms | 2.4 us | 4,096 | 38 KB |
| WASM, C | 117 ms | 222 ms | 2.2 us | 4,096 | 6.9 KB |
| WASM, Go (TinyGo) | 116 ms | 214 ms | 2.1 us | 4,096 | 10.5 KB |
| **native (`vector`, mod-native)** | **10.7 ms** | **37.7 ms** | **0.38 us** | **4** | 3.6 MB |

(The native row was added on a later run of the same node, with the plugin built in the container and
configured into the image; the WASM and Lua rows above are from that run too. Run to run the WASM rows move
by up to 10%.)

**The language does not matter.** The three are within 1.5% of each other at 100,000 records and within 7% at
20,000, and all three are 3.3x Lua. That is what the ABI predicts: per record the work is two imports (the bin,
then its 3 KB copied straight into a float array) and a 768-element `f64` loop that every compiler turns into
the same code; the differences between toolchains are in what they do with memory and runtime, which the hot
path never touches. A single `vec_distance` call costs 250-290 us from every engine, round trip included -
instantiation and argument decoding are not where a language shows either.

**Against the native module, before the fix: 6x at 100,000 records, 11x at 20,000.** The rows above are
the first measurement, and the two record counts said where the time went. Between 20,000 and 100,000 records
each WASM module added 1.2-1.5 us per record and the native one 0.34 us; the rest was fixed per query: ~95 ms
for WASM, ~4 ms for native. Two causes, of about equal weight:

- **Per stream.** The server ran one Lua or WASM stream per partition: 4,096 instantiations, argument
  decodings, query copies and partials per query, each partial carrying up to k hits (40,960 hits on the wire
  for an answer of 10). mod-native keeps one accumulator per query thread, merges them, and sends one partial
  per node: 4 partials, 40 hits.
- **Per record.** The 3 KB copy into guest memory and a scalar `f64` loop - the same loop in all three
  languages, which is why they agreed to the bit - against the native module's NEON kernel over a borrowed
  particle.

**After the fix: level with native.** Both halves were addressable without leaving WAMR, and were addressed the
same day (`mod_wamr.c` `apply_stream` + `mod_wamr_flush_thread`; the kernels in the three modules):

- A WASM stream is now **one instance per query thread**, kept across partition slices and finalised when the
  thread's last slice is done, through the same `as_aggr_flush` hook mod-native uses. A query is 4
  instantiations and 4 partials instead of 4,096. The guest contract did not change: `_record` per record,
  `_end` once - a module just sees more records before its `_end`.
- The kernels sum in **four lanes** (lane `k` holds the elements with index `k` mod 4, folded in one fixed
  order), which lets each compiler keep the sums in `simd128` registers - plain `f64` reductions do not
  vectorise - and keeps the three modules and the test's local computation bit-identical. The runtime build
  gained `WAMR_BUILD_SIMD`, without which a module using SIMD is refused at registration.

| engine | 20,000 records | 100,000 records | per record at 100k | partials per query |
|---|---|---|---|---|
| Lua (`vector.lua`) | 214 ms | 738 ms | 7.4 us | 4,096 |
| WASM, Rust | 13.9 ms | 51.1 ms | 0.51 us | 4 |
| WASM, C | 11.5 ms | 41.4 ms | 0.41 us | 4 |
| WASM, Go (TinyGo) | 13.0 ms | 54.4 ms | 0.54 us | 4 |
| native (`vector`) | 10.6 ms | 42.1 ms | 0.42 us | 4 |

So: **WASM C level with native, Rust and Go within 1.3x, all of them 14-19x Lua**, with the sandbox, `udf-put`
deployment and per-UDF failure isolation intact. The remaining 0.1 us per record between the Rust/Go rows and
the C one is a compiler difference in what it made of the lane-split loop, not an ABI cost, and is the next
thing to look at with the disassembly rather than the stopwatch. A single `vec_distance` call is level across
all five engines (250-290 us, round trip included).

**The tiers, on the same binary.** A `WAMR_TIER=jit` build keeps the interpreter and AoT as well, so the
three modes can be compared by `running-mode` alone (LLVM 18 linked into `asd`, 86 MB; the `aot` build links
none). Same node, same modules, same queries:

| tier | 20,000 records (Rust / C / Go) | 100,000 records (Rust / C / Go) | notes |
|---|---|---|---|
| AoT, SIMD kernels | 13.9 / 11.3 / 13.1 ms | 51.3 / 40.8 / 54.6 ms | native: 10.6 / 42.2 ms |
| LLVM JIT, SIMD kernels | 14.1 / 12.3 / 14.0 ms | 50.8 / 42.8 / 53.2 ms | lazy, per function on first call |
| AoT, scalar kernels | 14.3 / 13.0 / 13.7 ms | 56.4 / 52.2 / 51.8 ms | the same lane-split loops without `simd128` |
| interpreter, scalar kernels | 220 / 138 / 161 ms | 1,070 / 653 / 793 ms | Lua: 210 / 723 ms |
| interpreter, SIMD kernels | - | - | **traps: `unsupported opcode`** |

**Java (2026-10-27).** A fifth row, `wasm java` (TeaVM 0.13.1, linear-memory backend, no SIMD): top-k at
20,000 records 14.7 ms against Rust 13.3 / C 11.6 / Go 13.2 / native 10.8, i.e. 1.35x native and within
10 percent of Rust and Go; `vec_distance` single calls 285 us against 244 to 265 for the others and 348 for
Lua, the difference being the Java runtime's start-up per instance. The lane kernels give bit-identical
results without SIMD because the fold order is the same.

**WAMR moved twice on 2026-10-21.** `modules/wamr` went from a shallow `main` snapshot of 2026-08-07 to
the `WAMR-2.4.5` release tag (2026-06-29: three CVE fixes, one in the fast interpreter; older code than
that snapshot, since the release branch does not track main) and then, at the user's request, to upstream
`main` at 9058db92 (2026-10-21, 77 commits past the old snapshot; version header still 2.4.3). Nothing
in mod-wamr changed either time; unit 1427/0 and the four suites green on both. AoT top-k at 20,000
records, Rust / C / Go against native: 2.4.3 snapshot 13.9 / 11.5 / 13.0 vs 10.6 ms; 2.4.5 15.1 / 12.8 /
15.0 vs 10.8; main 13.7 / 11.3 / 13.0 vs 10.4. Single calls 245-259 µs against 232 µs native on main.
All within the usual run-to-run spread; Fast JIT is x86-64 only on every one of them.

Three things fall out:

- **JIT equals AoT.** Both are LLVM's code; the JIT compiles each function on its first call and the three
  rounds amortise that. What AoT buys is the deployment: no LLVM in `asd`, and a cold node that loads
  compiled modules from the cache at boot rather than compiling them on first use.
- **SIMD bought 10-25% on this kernel, not the 2x the first guess was.** Scalar lane-split code under AoT is
  already within 1.3x of native at 100,000 records; `simd128` closes the rest for C and some of it for Rust,
  and nothing measurable for Go. The per-thread accumulator was the large half of the fix; the kernel was the
  small one.
- **The interpreter is Lua speed** - 0.9-1.5x of it - which is the number to remember for the window between
  `udf-put` and the AoT landing, and for a `running-mode interp` node. And WAMR's fast interpreter does not
  implement the whole SIMD instruction set (SIMDe covers a subset): a module built with `simd128` **traps**
  under it with `unsupported opcode`. On an `aot` node that was the compile window after registration and the
  first calls after a cold start. **Guarded now:** the loader walks a module's code section once and marks
  the ones that use SIMD, and a call on such a module waits for its build - promoted to the front of the
  compile queue, bounded by the call's deadline - instead of running interpreted; every other module runs
  interpreted meanwhile as before. The first cut waited for *every* module and was measured too: behind the
  suite's burst of twenty registrations, first calls waited 1-5 s on the serial compiler and blew the
  client's second. The remaining soft spot is that same burst for a SIMD module: a single compiler process
  under load did not always finish within a 900 ms budget (`WAMR.todo` 23).

Two bugs found on the way, both fixed: `running-mode interp` on a JIT build ran the JIT, because
`wasm_runtime_full_init` resets the default running mode after `update` had set it (the mode now goes into the
init arguments); and the JIT tier's link line hard-coded `-lLLVM-15` (now `LLVM_LINK_LIB`).

**Benchmark hygiene, learned here:** the AoT compiler is one thread and compiles in registration order. A
bench run straight after the test suite found the vector modules still queued behind twenty fixture modules,
running interpreted - with SIMD, under SIMDe - and measured 3-5 us per record where the compiled module costs
0.5. A stream on a SIMD module now waits for its build, so that particular trap is closed; the advice stands
for anything else: check the log for `aot-compiled` lines before believing a WASM row.

**A finding on the way, now moot: the module's memory layout was a per-instance cost.** First runs had C and
Go 40-50 ms behind Rust at 20,000 records and level at 100,000 - a per-stream difference. Both compile to a
2-page initial memory (clang's default 64 KB stack, TinyGo's 4 KB); Rust's 1 MB default stack makes 17 pages.
Rebuilding Rust with a 64 KB stack made it the slow one (117 -> 197 ms); C with a 1 MB stack the fast one
(167 -> 119 ms): ~15 us per instance, 60 ms over 4,096 streams. Giving every instance a 1 MB WAMR app heap
instead was 2x *worse* (the pool is initialised per instance). The mechanism was never pinned down - this
build runs software bounds checks and mmaps each memory at its exact size - and it stopped mattering: a
single call costs the same at either layout, and with one instance per query thread C and Go at their default
2-page layout time the same as at 17 pages (11.9 / 41.0 ms against 11.3 / 40.8 for C; 13.4 / 52.9 against
13.1 / 54.6 for Go). The builds use their toolchains' default stacks.

Two things the comparison needed from the host: a `digest` native (a top-k hit identifies a record rather than
shipping it), and running a module's `_initialize` export once per instance, which TinyGo emits for its
runtime and which no earlier guest had.

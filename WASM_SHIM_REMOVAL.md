# Removing the C shim — exploration

Asked: can the C shim go, in a way where **a change on the C side fails on the Rust side during tests**,
while moving data faster than now.

Short answer: the safety property is straightforwardly achievable, the slowness is real and confirmed,
but **the shim is not established as the bottleneck** — it accounts for at most half of it, and part of
that half is work Rust would still have to do. There is a much cheaper change that targets the same
cost. Numbers first, because two earlier optimisation attempts here were aimed at the wrong thing.

---

## 1. The slowness is real

Measured within one run, so baseline drift cannot flatter either side (an earlier reading of "3.5µs"
was drift — the `get` baseline had moved 15µs, not WASM):

| | p50 | over baseline |
|---|---|---|
| `get` (network baseline) | 77.7 µs | — |
| lua: `nothing` (returns nil) | 77.0 | −0.7 |
| lua: `answer` (returns an int) | 78.4 | **+0.7** |
| wasm: `nothing` | 84.3 | **+6.6** |
| wasm: `answer` | 95.0 | **+17.3** |

Two things fall out:

- **Returning one integer costs WASM ~10.7 µs and Lua ~1.4 µs.** That is the gap, and it is where
  essentially all of it lives.
- **A host call is cheap.** A guest doing 1000 `bin_get`s took 1001 µs against 335 µs for the same loop
  with no call — **0.67 µs per call**, including the msgpack encode, three copies and the guest-side
  decode. So the per-value marshalling that the shim does is *not* the problem.

## 2. Where the 10.7 µs is

From the runtime's own microbenchmark, three repeats to bound the noise (±1 µs):

| | 1 | 2 | 3 |
|---|---|---|---|
| `apply_record`, 2-byte result | 6.9 | 8.9 | 8.4 µs |
| `apply_record`, no result | 2.3 | 3.5 | 4.4 µs |

A consistent **~4.7 µs inside the Rust runtime** for the result path — read the packed pointer,
bounds-check it, `Memory::data`, copy two bytes. That is far more than that work should cost and **it is
not localised further than this**; the remaining ~6 µs is the C side (`as_serializer_deserialize`, the
`as_val` allocation, `as_result_setsuccess`).

**So removing the shim addresses at most the C half.** And not all of it: constructing an `as_val` costs
a `cf_malloc` per node whether the bytes came from msgpack or from Rust building it directly. The
realistic prize is perhaps 3–5 µs of the 10.7, and the ~4.7 µs on the Rust side would be untouched.

---

## 3. Design A — remove the shim (what was asked)

### The safety property — corrected after testing it

This section first claimed that bindgen's `test_layout_*` assertions catch a layout change. **They do
not, and the claim was tested rather than left standing:** a field was added to the middle of `as_val_s`,
generation was forced, and the build succeeded. bindgen regenerates from the changed header, so the
assertion's expected size changes with it. The assertions only confirm that bindgen's own view is
self-consistent; they cannot detect a header change, because both sides derive from the same header.

What actually makes it safe is **single-sourcing**. The bindings are generated at build time from the
same headers the C compiler uses, in the same build, so Rust and C *cannot* disagree about layout —
there is no drift to detect rather than drift detected late. What remains:

- **A shape change** — a field renamed, removed or retyped — is a **compile error** in Rust, once Rust
  uses that field. This is the load-bearing property, and it is stronger than a test.
- **A layout change** needs no failure: Rust regenerates and agrees.
- **A semantic change** — same type, different meaning — is caught by nothing, exactly as it would be
  between two C files.

**The shape-change property is now demonstrated, not argued.** Once Rust actually read these fields it
became testable, so it was tested. Renaming the `gen` hook in `as_rec.h` *and* its use in the inline
wrapper — a change C accepts without a word —

```text
    C: compiles clean
error[E0609]: no field `gen_` on type `&as_rec_hooks_s`
   --> src/rec.rs:172:40
```

The first attempt at this test was too crude to prove anything: renaming only the field left
`as_rec.h`'s own `static inline as_rec_gen` referring to a member that no longer existed, so **clang**
rejected it and the C build would have failed too. Renaming both is the change that isolates the claim.

It also confirms the `rerun-if-changed` fix: under the old per-directory watches this edit would have
regenerated nothing and passed.

That serves the original request better than assertions would, but by a different mechanism than the one
first written here.

One build-system trap found on the way: `cargo:rerun-if-changed` on a **directory** does not fire when a
file inside it changes. The first run of the experiment above rebuilt nothing and produced no output,
which reads exactly like success. `build.rs` needs per-file watches to be trustworthy.

Viability was checked, not assumed:

- **libclang is present** (`/usr/lib/llvm-14/lib/libclang.so.1`), so bindgen runs here. It becomes a
  build dependency of the server, which is the main cost of this approach — every build machine and CI
  image needs it.
- **The accessors are trivial to replicate.** `as_rec_get` is a `static inline` expanding to
  `rec->hooks->get(rec, name)` via the `as_util_hook` macro. Rust calls the function pointer directly;
  bindgen's layout tests protect the offsets it reads it through. No `--wrap-static-fns` needed.
- **`as_module_hooks` is five function pointers**, so Rust can define a `#[repr(C)] static` and export
  `mod_wasm` itself. `udf_cask.c` and `udf.c` reference it by symbol; nothing else changes.

### Status: done

**The shim is gone.** `mod_wasm.c` and `mod_wasm_host.h` are deleted, Rust defines `mod_wasm` and all five
`as_module` hooks, and the module builds as a single archive with no C in it. `nm asd | grep mod_wasm` shows
one definition, in `D` — writable data, matching the storage class the C declaration promises.

Steps 1 and 2, which the rest of this section describes, are what made that safe:

- **bindgen foundation** — `build.rs` + `wrapper.h`, 31 structs and the UDF function surface. Nothing in
  the live path uses it yet.
- **`Val` / `ValRef` ownership wrappers** — borrowing and owning encoded in the *type*, with the rules
  read out of the server rather than assumed, and 9 tests over refcounting, the `as_nil` static, type
  mismatches and `into_raw`. Three things this surfaced: `cf_malloc` has **no symbol** under
  `ENHANCED_ALLOC` (it is a macro — the same trap the C shim hit), so `Val::bytes` lets C allocate; the
  tests could not link until `build.rs` emitted `-l aerospike-common`, and those are precisely the tests
  that would catch a refcount mistake; and an empty blob read back as "not bytes", which would tell a UDF
  the bin holds something else.

Step 3 could not be split into green increments the way the first two were: `mod_wasm` can be defined
once, so the C definition had to go in the same change that added the Rust one.

### Steps 3 and 4, as executed

**The hook table.** `src/module.rs` defines `#[no_mangle] pub static mut mod_wasm` and five
`extern "C" fn`s whose signatures come from the generated `as_module_hooks` — which is the property this
whole exercise is for. `static mut` rather than an immutable static behind a `Sync` wrapper, deliberately:
C declares a plain `as_module` and passes `&mod_wasm` to functions taking `as_module*`, so an immutable
static in `.rodata` would fault the first time anything wrote to it.

**The accessors.** `src/rec.rs` — one `Rec` type holding the `as_rec` and `as_aerospike` for the call,
with a method per hook. The `static inline` wrappers bindgen cannot emit (`as_rec_get` and friends) are
written out as the `hooks->get(rec, name)` they expand to. Every hook is `Option<fn>` because C allows a
null, so a missing one answers "no" rather than jumping through null — which is what `Rec::none()` is, and
what the unit tests run against.

**Two more files, both earning their place.** `src/msgpack.rs` wraps the serializer with RAII, and
`src/log.rs` reaches `cf_log_write`. The second has a limitation worth stating: **`cf` is not an archive**
— it is compiled straight into `asd` — so there is no `libcf.a` for a test binary to link, and every log
call sits behind `cfg(not(test))`. Logging is therefore not exercised by `cargo test`. It *was* verified
live: a guest's `log` reaches the server log at warning and info, with debug correctly filtered by the
context's level.

**Three deviations from the plan.**

1. **`host.rs` stayed.** The plan said delete it, but that conflated two things: the C *callback table*
   (`Host`, 19 function pointers) and the file that defines the *guest imports*. The table is gone; the
   imports still need a home, and they now call `Rec` directly. The guest ABI is byte-identical, which is
   not optional — the checked-in fixture is a compiled binary.
2. **The result no longer round-trips through a buffer.** The plan kept the `(out, out_cap, out_len)`
   convention with its `TOO_BIG` retry. There is no reason to: Rust can deserialize straight out of the
   guest's memory while the store is alive and hand back an `as_val`. That removed the 2 KB stack buffer,
   the heap retry path, the `AS_WASM_KIND_*` constants and `ERR_TOO_BIG` — the whole apparatus existed
   only to cross a boundary that no longer exists.
3. **The three `mod_wasm_*lock` functions are gone.** Nothing ever called them. `mod_lua` needs them
   because its script cache is not internally synchronised; this one is behind an `RwLock`, so an external
   lock would have guarded nothing while looking as though it did.

**What the port found.** Two bugs, one of them live:

- **A guest could segfault the server.** `msgpack::unpack` on an empty slice sends the deserializer
  through the dangling pointer an empty slice's `as_ptr()` returns — reachable from any UDF calling
  `bin_set(name, ptr, 0)`. **The C shim had the same hole**, in `val_from_msgpack(value, 0)`; it surfaced
  here only because a Rust unit test can call the function directly, where the C helper was reachable only
  through the shim. Fixed, with a test.
- **`base_name` disagreed with itself.** `strip_suffix` turned `.wasm` into the empty string where the C
  version left it alone, and an empty cache key would collide with every other name that reduced to
  nothing. Caught by a test written from the C behaviour before the Rust was run.

**Verification.** 32 runtime unit tests (from 13); the server builds clean; `nm asd` shows one `mod_wasm`
in `D` and no `as_wasm_rt_*` left; the 6 client integration tests pass **without self-skipping**; and the
benchmark is unchanged — see below.

**The accessors the fixture does not reach.** The integration tests exercise 6 of the 19. The rest were
driven live against the SDK example rather than assumed: `key` (needs `send_key`), `ttl` (needs
`allow-ttl-without-nsup`, which this cluster does not have by default — without it a TTL is answered
FORBIDDEN before a UDF is reached, and a TTL of 0 is indistinguishable from a failed accessor because the
guest SDK maps both to `None`), `log` at all three levels, and the memory limiter. A `chatter` function
was added to the SDK example for the log check, because `hog` only logs after growing 16 MiB — so the
moment the cap trips slightly earlier, its silence looks like a broken log rather than a working limiter.
That is exactly what happened while checking this, and it is why the log now has a test of its own.

### The genuinely dangerous part

**`as_val` is reference counted** — `{ uint32_t count; as_val_t type; bool free; }` with
`as_val_reserve` / `as_val_destroy`. Rust taking over means Rust owns that discipline. Getting it wrong
leaks (a bin value never freed, per call) or double-frees (a crash under load, far from the cause).
bindgen protects the *layout*; it cannot protect the *protocol*. That needs RAII wrappers and would be
the bulk of the review effort.

The TSO plugin concern from the original design still applies and still looks fine: access is within one
transaction on one thread, with no lock-free structures shared with C.

### Where the speed would come from

Not from removing a hop — the hop is 0.67 µs. From **not going through msgpack for scalars**: read an
`as_integer`'s `value` field directly, build an `as_string` directly. Collections would still need a
structured walk, which is where msgpack currently earns its place.

---

## 4. Design B — keep the shim, skip msgpack for scalars

The same win, without binding refcounted C structs.

Today every value crosses as msgpack, including a bare integer. Add a **tagged scalar fast path** beside
it: the guest returns `(tag, i64)` for the common cases — nil, bool, int, double — and only falls back
to a msgpack buffer for strings, blobs and collections. On the C side that is
`as_integer_new(value)` with no serializer involved; on the guest side no encode at all.

- Touches: the return convention in `invoke`, one branch in `apply_record`, and `to_host` in the guest
  SDK. Perhaps 100 lines, no new build dependency, no ownership rewrite.
- Gets: the whole C-side msgpack cost for scalar results, which is most results.
- Leaves: the ~4.7 µs on the Rust side, exactly as Design A does.

The same trick applies to `bin_get`, though at 0.67 µs per call it is not urgent.

---

## 4b. The profiling pass — what it actually is

Instrumented `invoke` with per-phase nanosecond accounting, over 3000 real calls each. This replaces the
guesses in sections 2 and 3, two of which were wrong.

| phase | `nothing` (returns nil) | `answer` (returns an int) |
|---|---|---|
| `store_new` | 0.84 | 0.88 |
| `instantiate` | 4.02 | 4.59 |
| `exports` (3 lookups by name) | 0.46 | 0.52 |
| `args_in` | 0.02 | 0.03 |
| **`guest_call`** | **0.23** | **5.34** |
| `result_out` | 0.02 | 0.04 |
| **total in the runtime** | **5.59** | **11.40 µs** |

Three findings, in order of size:

**1. A guest allocation costs ~5.1 µs — the single largest item.** `nothing` returns `0` and allocates
nothing: 0.23 µs. `answer` returns a msgpack byte for `42`, which means a `Vec` — 5.34 µs. The
difference is not the encoding, which is one byte; it is that **every call gets a fresh instance, so the
guest's Rust allocator initialises from scratch and the first allocation grows memory.** The
per-invocation-instance design that made `as_free` unnecessary is what makes the first `malloc`
expensive.

**2. `instantiate` is 4.0–4.6 µs, not the 2.2 µs the microbenchmark suggested.** The microbenchmark was
instantiating the tiny hand-assembled fixture; the real module is 32 KB with a larger memory. This also
revises the earlier "InstancePre buys nothing" conclusion: it was measured on the wrong module.

**3. `result_out` is 0.02–0.04 µs.** The Rust-side result path is free. **Section 2's claim of "~4.7 µs
inside the Rust runtime for the result path" was microbenchmark noise, not a real cost.** The
`answer`−`nothing` difference is guest allocation, not result handling.

The C side still accounts for ~7 µs (18.3 µs end-to-end for `answer`, minus 11.4 µs in the runtime),
which is `as_serializer_deserialize`, the `as_val` allocation, and whatever `udf.c` does to turn a
non-nil result into a response.

## 4c. The benchmark after the port

A refactor should not move the numbers, and it did not. Three runs, because the run-to-run spread turned
out to be larger than anything worth claiming:

| run | `get` baseline p50 | lua over baseline | wasm over baseline | gap |
|---|---|---|---|---|
| 1 | 75.6 µs | 5.5 | 10.4 | 4.9 |
| 2 | 74.4 | 5.1 | 12.1 | 7.0 |
| 3 | 74.5 | 2.0 | 13.8 | 11.8 |

So **wasm ≈ 12 ± 2 µs over baseline, lua ≈ 4 ± 2, gap ≈ 8 µs** — the same as the ~7.4–8.6 µs measured
before the port. **No claim of improvement is made:** removing a staging copy and a msgpack round trip on
the result path should help slightly, but the noise here is ±3 µs and the measurement cannot see it.

Cold start is now 976 µs against Lua's 221 — a long way from the 27 ms it was, because FILE_ADD compiles
on the SMD thread instead of leaving the first caller to do it while holding a record lock.

## 5. Recommendation

Written before the port, and kept because the measurements behind it still hold. Items 1–3 are **done**,
and so, now, is Design A — as a refactor, which is what this section predicted it would be worth.

The reasoning it records is the part worth keeping: **Design A was never the performance fix.** It targets
the ~7 µs C side, while ~9.7 µs of the 11.4 µs measured inside the runtime is guest allocation plus
instantiation — which removing the shim does not touch at all. The benchmark above confirms that
prediction: the shim is gone and the numbers are unchanged.

In order of measured prize:

1. **Stop the guest allocating for its result (~5.1 µs).** The SDK's `to_host` writes into a `Vec`. A
   `static mut` result buffer, sized for ordinary values, removes the allocation entirely — the guest
   never touches its allocator for a typical UDF, so the cold-heap cost disappears. Small change, in one
   place, and it is the SDK's job rather than the protocol's. A scalar fast path (Design B) achieves the
   same thing for scalars *and* removes the C-side msgpack, so it is worth more than it looked.
2. **wasmtime's pooling instance allocator (~4 µs).** `PoolingAllocationConfig` recycles memory slots
   and uses copy-on-write images of a module's initial memory, which is the standard answer to
   per-call instantiation cost. Crucially it **keeps instances fresh**, so the no-`as_free` contract and
   the "no state leaks between calls" property both survive. This is the single best-understood fix.
3. **Cache the three export lookups (~0.5 µs).** Resolve them once per module beside the `InstancePre`
   rather than by name on every call.
4. **Then**, if the remaining ~7 µs on the C side matters, Design B — and only then is Design A worth
   considering, as a refactor for clarity rather than for speed.

The premise that started this exploration — that the shim's data transfer is what makes it slow — does
not survive measurement. The shim is roughly a third of the cost; the per-call instance is two thirds.

## 6. Footgun found while measuring

With `epoch_interruption(true)` on the engine, a `Store` that never calls `set_epoch_deadline` has a
deadline of **zero** and every call into it traps immediately. `invoke` always sets it, so production is
correct — but the benchmark loops did not, and reported a trap instead of a timing. Any future code path
that builds its own store will hit this. Worth a single constructor that sets both the limiter and the
deadline, so the two cannot be separated.

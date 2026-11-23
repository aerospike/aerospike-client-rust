# WASM UDFs — design

`WASM` is a UDF language beside `LUA`, run on the server by `modules/mod-wamr`: pure C on WebAssembly Micro
Runtime (upstream main, 2026-10-21; 2.4.x line), ahead-of-time compiled. This document is the design as it stands after the second guest ABI
(2026-10-20). The measurements that chose the engine and shaped the ABI are in `WAMR_EVALUATION.md`; the
guest side is the `aerospike-udf-sdk-wasm` crate in this repository, whose crate docs are the user's
reference. The original design, written for a Rust/wasmtime module with a msgpack boundary, is superseded;
its one surviving idea is the last section.

---

## 1. Where it sits in the server

The server already had a language abstraction. `as_module_hooks` (destroy, update, validate, apply_record,
apply_stream) is what `mod_lua` implements, and `mod_wamr` is a second implementation of the same five hooks.
Dispatch is by the language recorded against the file name at registration (`udf_cask_get_lang`), because
`udf_def.type` is the *operation*, not the language — the language is never on the wire at apply time. A
client registers a `.wasm` with `udf-type=WASM`; a call names package and function as it does for Lua.

Each module is instantiated once per registration and cached; `apply_record` creates an execution
environment per call, and both paths hand the natives a `wamr_ctx` through `wasm_runtime_get_user_data`. A
stream UDF runs in **one instance per query thread**: the server drives an aggregation as one stream per
partition, and the instance is kept across those slices in a thread-local slot and finalised - `_end`, one
partial - by `mod_wamr_flush_thread` when the thread's last slice is done, through the same `as_aggr_flush`
hook mod-native uses. A query is 4 instances and 4 partials per node instead of 4,096, which is what took the
vector top-k from 6x slower than the native module to level with it (`WAMR_EVALUATION.md` §8). A module that exports `_initialize` - TinyGo
does, for its runtime - has it run once per instance before any UDF. Nothing is global, so natives cost a few
instructions each. A runaway record UDF is stopped by the transaction's own deadline through the watchdog;
a stream UDF has no deadline, because `aggr.c` gives its context none — a server issue, not this module's.

AoT compilation runs on a background thread after `udf-put`, so registration and cluster tend are not
stalled; a module runs interpreted until its `.aot` is ready, and the log says which (`wamr: executing in
<mode> mode`). The exception is a module whose code uses 128-bit SIMD, which the interpreter cannot run
(`unsupported opcode`): the loader walks the code section once at load, and a call on such a module waits for
its build - promoted to the front of the compile queue, bounded by the call's own deadline (a stream has
none and waits up to 10 s) - rather than running interpreted. Any other module runs interpreted meanwhile, as
before; waiting for every module was tried and cost a burst of registrations seconds per first call. The compiled form lives in `aot-cache-dir`, outside the UDF directory that `udf_cask_init`
wipes at startup, under a tag of the bytecode's content hash, so a restart loads every registered module
ahead-of-time compiled from the cache and compiles nothing (verified 2026-10-20: twenty modules, no wamrc
run, the first call after the restart logged `loaded as aot`). A module's `.aot` and tag are removed when it
is unregistered.

## 2. The guest contract

Two files define it: `mod_wamr_internal.h` (the ABI comment, the status codes, the value kinds) and
`mod_wamr_packed.c` (the reading side). The rules:

**Nothing is msgpack, nothing is deserialised, nothing is allocated in the guest.** The host hands a guest
its data **by position**. Every read produces a 16-byte value:

```
[0..4)  i32  kind     NIL, BOOL, INT, DOUBLE, STR, BLOB, LIST, MAP, GEOJSON, HLL, OTHER, or ABSENT
[4..8)  u32  len      bytes for STR/BLOB/GEOJSON/HLL, elements for LIST, entries for MAP, else 0
[8..16) i64  payload  INT: the integer   BOOL: 0/1   DOUBLE: the bits
                      otherwise (source handle << 32) | offset — where the value is
```

A *source* is a handle the host keeps for the duration of the call: the bin's packed particle, borrowed in
place; a packed copy of a value the record holds only as an `as_val` (staged by this call, in a masked set,
an argument, a collection the guest built); or a raw byte source for a string, blob or HLL bin. The offset
is the value's msgpack header within the source. The bytes do not move during a call, so a value stays
usable as long as its source does, and a batch of 128 values crosses in one import.

**The import call is the unit of cost, not the decode.** Decoding a value in place costs nanoseconds;
crossing into the host costs well under a microsecond but is paid per call. Every native that can take a
buffer does, every walk reads windows rather than elements, and the guest decides what it needs: a string's
bytes, a nested collection's cursor, or neither.

The natives, by what a guest does:

| Read | |
|---|---|
| `bin_value(name, out)` | a bin of any kind, one call; never declines — a scalar, a staged value or a masked set comes through the record's `as_val` in the same shape |
| `cursor_enter_at(src, at)` | a cursor over the list or map at a position, at its first element |
| `cursor_values(cursor, out, cap)` | the next values, 128 per call; a nested collection is reported with its position and count and stepped over whole |
| `cursor_bytes_at(src, at, out, cap)` | the bytes of a string, blob, GeoJson or HLL at a position; a short buffer reports the size needed |
| `coll_get(src, at, key, out)`, `coll_get_i` | the value under a string or integer key of the map at a position, a linear packed walk |
| `args_values(args, out, cap)` | every argument of the call as values, one import |
| `digest(out, cap)` | the record's digest, 20 bytes: what a UDF that points at a record rather than shipping it carries |
| `cursor_close(cursor)` | give the cursor's slot back, if it is the most recent |

| Hand a stored value back | |
|---|---|
| `result_at(src, at)` | the call's result, unpacked by the server from the record's bytes |
| `set_at(name, src, at)` | into a bin |
| `list_push_at(list, src, at)`, `map_put_str_at`, `map_put_i_at` | into a collection the guest is building |
| `list_push_values(list, values)`, `map_put_entries(map, pairs)` | a batch of 128 values (64 pairs) into a collection the guest is building, each value scalar, stored by position, of the guest's by address (kinds `GUEST_STR` 16, `GUEST_BLOB` 17, `GUEST_GEOJSON` 18) or built by handle (`HANDLE` 19); the host answers with how many it took |

| Write, build, report | (`mod_wamr_natives.c`) |
|---|---|
| `set_int/double/bool/str/blob/geojson/nil/handle`, `bin_remove` | staged in the record's cache until `rec_update` / `rec_create` |
| `set_list_ints/doubles` | a guest array packed straight into a LIST particle and applied as one op |
| `list_new`, `map_new`, `val_*`, `list_push*`, `map_put*`, `coll_size` | collections built on the host, as handles |
| `result_*`, `fail`, metadata (`gen`, `ttl`, `set_name`, `key_*`, …), `log` | |

Ordered lists and key-ordered maps carry a metadata element in their packed form; every entry into a
collection goes through one function (`cursor_enter`) that skips it and excludes it from the count.

**The signature export.** Beside every UDF the guest SDK exports `__udf_sig_<name>`, a `() -> i32` returning
the address of a NUL-terminated description: `fn add_all(name: str[256], numbers: any...) -> value`, or
`stream sum { record() end() }`. `apply_record` recognises the prefix and serves the text as the call's
string result without running any body, so a client reads a module's contract with an ordinary execute:
to check a call before making it, or to generate typed bindings. A parameter is `name: type`, with `[N]`
for the most bytes a string or blob parameter takes, `?` on one that may be absent and `...` on one that
takes the rest; the types are `int`, `double`, `bool`, `str`, `bytes`, `any` and `coll`.

**Values and ownership.** A value the guest reads is borrowed from the record and valid for the rest of
the call, or until the bin it came from is written. The per-call handle table has 256 slots; a cursor takes
one, and an iterator gives it back on drop (`cursor_close` pops the most recent slot, which is the order
iterators end in). A value handed to `set_*`, `result_*` or a collection moves ownership to the server. A
guest that returns before the end leaks nothing: the context is torn down with the call.

**HLL** is the one kind a guest can read but not make: the subtype that makes bytes an HLL is the server's to
set, so an HLL is written back by position only, and a plain blob written over an HLL bin is refused.

## 3. The guest SDKs

There are three, one per language, all speaking the contract above: `aerospike-udf-sdk-wasm` (Rust, the
reference and the one with the macros), `aerospike-udf-sdk-wasm-c` (a single header, freestanding C with
clang and wasm-ld) and `aerospike-udf-sdk-wasm-go` (a package for TinyGo's `wasm-unknown` target, which is
what makes Go possible here: standard Go's WebAssembly needs WASI or JavaScript, neither of which the server
has). `examples/vector` under each is one algorithm written three times, and `tests/src/udf_vector.rs` is
where they are held to agree and timed; `WAMR_EVALUATION.md` §8 has the numbers.

`aerospike-udf-sdk-wasm` is `no_std` with no allocator, and turns the contract into three ideas:

- **Every read is a `Value`**, the borrowed analogue of the client's: scalars inline, `Str`/`Blob`/`GeoJson`/
  `Hll` as a `Text` or `Blob` that fetches bytes into a buffer the UDF owns - a `&mut [u8]`, a `Str<N>` /
  `Bytes<N>` of a declared capacity, or with the `alloc` feature a `String` / `Vec<u8>` - and `List`/`Map` as
  a `Coll` that iterates in windows (`iter()`, `entries()`) or looks up (`get`, `get_i`). `Value` is also
  what a UDF builds (`Value::Int(3)`, `"text".into()`, a `List`/`Map` built on the host, filled one value at
  a time with `push`/`put` or a window at a time with `extend`) and returns.
- **Every failure is an `Error`**, and a UDF is `fn name(params) -> Result<Value, Error>`: `Ok` is the
  result the client receives, `Err` fails the call with the error's message, `Error::Custom` carries the
  UDF's own.
- **Arguments are typed in the signature.** `export!` decodes `i64`, `f64`, `bool`, `&str`, `&[u8]`,
  `Str<N>`, `Bytes<N>`, `String`, `Vec<u8>`, `Value`, `Coll`, `Option<T>` and a trailing `Args`, checks the
  count and kinds, and rejects a mismatched call with `name: argument N (param): expected X, got Y` - or
  `is N bytes, at most M` for a string past its parameter's capacity, judged from the reported length before
  any copy - before the body runs. `stream!` does the same for `record` and `end`. The same declaration is
  what the signature export describes (§2).

A UDF therefore reads as ordinary Rust, and the one thing it has to know is that strings and collections are
borrowed from the record. `aerospike-udf-sdk/wasm-rust/examples/ledger` is the module to read first - a guarded
withdrawal, a history trimmed on the host by position, a summary, an aggregation; `examples/udf` (checked in
as `tests/fixtures/wasm/udf_test.rs`) is the one the test suite asserts against.

## 4. Limits, because a UDF runs while holding a record lock

- `max-memory` bounds the guest's linear memory (default 64 MiB); a guest that grows past it is refused and
  traps or reports, never grows on.
- A record UDF is bounded by the transaction deadline through the watchdog; a stream UDF is not (see §1).
- 256 handles per call; 32 arguments per call; a `&str` parameter holds 256 bytes, a `Str<N>` holds `N`, a
  `String` (the `alloc` feature) holds whatever the call sent.
- A guest has no `std`, and no allocator unless its crate asks for one: the SDK is `no_std`, and a module
  built against it is a 10–35 KB `cdylib`. The `allocator` feature links dlmalloc; the test fixture went from
  39 KB to 62 KB when it enabled it for two functions. The guest's memory persists across calls - a module is
  instantiated once - so the allocator is a real one that frees, not a bump arena reset per call, which would
  clobber a stream UDF's state between records.

## 5. Configuration

```text
mod-wamr {
    running-mode aot          # aot (default) | interp | jit | fast-jit
    aot-compiler /path/to/wamrc
    aot-cache-dir /opt/aerospike/usr/udf/wasm-aot
    aot-compile-threads 2     # wamrc processes at once, 1..8 (§1: SIMD modules wait for their build)
    max-memory 67108864
    cache-enabled true
}
```

`WAMR_TIER` at build time decides which tiers exist (`aot` keeps LLVM out of `asd`; `jit` links it in;
`fastjit` is x86-64 only); `running-mode` picks among them at run time and falls back to the interpreter,
loudly, when the chosen one is unavailable. Measured (`WAMR_EVALUATION.md` §8): the JIT equals AoT, the
interpreter is Lua speed, and the interpreter cannot run a module built with `simd128` at all - which is why
a call on such a module waits for its AoT build (§1) and why `running-mode interp` refuses it.

## 6. Testing

- `modules/mod-wamr/test/unit.c`: the natives against a mock record, with cf's real msgpack decoder compiled
  in. Ownership, the handle table, the value layout, ordered lists and maps, HLL, stored values into built
  collections, arguments. `make unit` in the module directory.
- `tests/src/udf_wasm.rs` in the client: the fixture end to end against a live server, including the
  argument contract and the by-position pass-through. Self-skips on a server without WASM.
- `tests/src/udf_vector.rs`: the vector top-k in Rust, C and Go against Lua and a local computation,
  digest for digest; and, `#[ignore]`d, the same timed over 768-dimension vectors.
- `tests/src/events.rs`: every event kind's management, firing and dry run against `examples/guard`
  and `examples/resolver` in three languages, and `examples/crdt` through the dry run's three laws (§7).
  What needs two clusters - `xdr-merge` firing inside a real XDR write - is not covered by any test yet.
- `tests/src/udf_agg_bench.rs` and `udf_native_bench.rs`: `#[ignore]`d measurements across the three
  engines. `list_bench` (a 10,000-element list summed per call) and `agg_bench_nested` (a nested document
  per record over 50,000 records) are the two rows that shaped the by-position ABI, kept so the numbers in
  `WAMR_EVALUATION.md` §7 can be reproduced on demand.

## 7. Events

A WASM function bound to a point in the server's own processing. Fifteen kinds, 2026-10-20/21;
`as/include/base/event.h` and `as/src/base/event.c` hold the registry, the payload builders, the generic
call, the queue, the timers and the info commands. `event-kinds` lists what a server serves.

**Bindings.** `event-set:event=<kind>;namespace=…;set=…;module=…;function=…` plus `name=` (several
bindings of one kind on one scope, default the function name), `order=` (a chain runs ascending, then by
name), `exp=<base64 wire expression>` or `ael=<expression text>` (a filter evaluated against the record
before the function runs; stored as given, built once at `event-set` and again at load), `on-failure=fail`
and `timeout=<ms>`. SMD key `kind|ns|set|name`, value `module|function|version|on-failure|timeout|order|filter`.
Set-level bindings win over namespace-level; global kinds (`cluster-change`, `node-lifecycle`) take no
namespace. `event-set` checks the module is WASM and that its `__udf_sig_<function>` reads
`event <kind>/<version>` with the version the server serves for that kind. `event-remove` takes an optional
`name=`; without it every binding of the kind on the scope goes. `event-list` prints one row per binding
with name, order, filter and the counters `fired`, `filtered`, `rejected`, `keep-local`, `take-remote`,
`record`, `policy`, `fallback`, `dropped`. `udf-remove` refuses a bound module. Mutating commands need
`event-admin`.

**The call.** `as_event_call` runs one binding with up to two records (read by the guest through
`event_bin_value(which, name)`) and a description map (`kind`, `namespace`, `set`, `now`, `empty`, and the
kind's own entries), within the operation's deadline capped by the binding's timeout, and reads back a
verdict word or a `{verdict, code, message, bins, ttl}` map. A `record` answer's bins replace the record
through `as_event_apply_bins` (CE: nil or absent deletes) or the Enterprise `as_event_apply_resolution`
(stamped for convergence). In a chain, a `record` answer is the next handler's input; a refusal stops it.

| Kind | Fires | Handler sees | Answers | Verified by |
| --- | --- | --- | --- | --- |
| `xdr-merge` | every incoming XDR write to an existing record, before its ops (`trigger=always`), or only when bins would lose under bin convergence (`trigger=conflict`, EE preview); XDR deletes from the delete path | `current`, `incoming` (gen and LUT zeroed), trigger, policy, lost, per-bin LUTs, deletes | `keep-local`, `take-remote`, `policy`, `record` (equal to current: nothing written, `LOST_CONFLICT`) | dry run with the three laws; Rust/C/Go resolvers agree; `examples/crdt` eight scenarios |
| `before-write` | `write_master` after the ops, before storage | `before`, `after`, write (source, generation, ttl, create) | `accept`, `reject {code, message}`, `record` | reject, stamp, filter, chain |
| `before-delete` | `drop_master` after filter/key/masking checks | record, delete (source, durable) | `allow`, `refuse` | locked record |
| `after-write` / `after-delete` | after commit / before the index drop, queued (4096 deep, drops counted) | before/after or record, write/delete | nothing | counters catch up |
| `before-expire` | nsup, before `drop_local` | record, expire (void-time, now) | `expire`, `keep {ttl}` (index void time moved) | marked record kept |
| `before-evict` | nsup's evict branch | record, evict (+ evict-void-time) | `evict`, `protect` | not live (needs memory pressure) |
| `after-read` | `read_local` after the bins are gathered (full load when bound) | record, read (source, all) | `unchanged`, `record` (becomes the response) | hidden bin |
| `before-truncate` / `after-truncate` | the command, before SMD / each node's accept | truncate (namespace, set, lut) | `allow`, `refuse` / nothing | fenced set, counter |
| `before-ship` | EE `create_write_request`, before bin selection | record, ship (dc) | `ship`, `skip`, `record` | not live (no DC here) |
| `partition-transfer` | after emigrate/immigrate done | transfer (pid, direction, peer) | nothing | not live (one node) |
| `cluster-change` | end of `as_partition_balance` (unlocked accessors) | cluster (key, size, nodes, self) | nothing | restart: fired once |
| `node-lifecycle` | `service-ready`, `shutdown` (main thread), `binding-deployed` (not during SMD replay) | lifecycle (phase, node, binding) | nothing | deploy phase; restart |
| `timer` | scheduler thread, 100 ms tick | runs as a record UDF with `{name, due, now}` | the UDF's | three ticks with re-arm, Rust/C/Go |

**Timers.** SMD module `timer`, key `ns|set|digest|name`, value the due time in ms; mirrored in a vector
for the scheduler, which removes a due timer from SMD first and then runs the set's `timer` binding on the
record through an internal UDF transaction (`as_transaction_init_iudf`), the way background UDF jobs do.
`event-timer-set:namespace=;set=;key=<string>|digest=<hex>;name=;in=<ms>|at=<epoch ms>`,
`event-timer-remove`, `event-timer-list`; from a UDF or timer handler, the natives `timer_set(name, delay_ms)`
and `timer_cancel`. A timer with no handler bound when it fires is dropped with a warning.

**Guest SDKs.** Rust: `event!` with one arm per kind, `Record`, `Conflict`, `Pair`, `Both`, `Write`,
`Delete`, `Truncate`, `Expire`, `Evict`, `Read`, `Ship`, `Lifecycle`, `Cluster`, `Transfer`, `Timer`, the
answer enums, `timer_set`/`timer_cancel`. C: `AS_UDF_EVENT_<KIND>(name)` declarations, `as_udf_event_desc`,
`as_udf_event_get_record`, the `as_udf_*_info` decoders, `as_udf_answer_*`. Go: `EventDesc`, `EventSide`,
the `*Info` decoders, `Answer` and its words, `EventSignature` with the `Params*` texts. `examples/guard`
in each SDK, held to one behaviour by `guards_in_three_languages_agree`.

**Found on the way.** `cf_vector_get_ptr` is for vectors of pointers (`cf_vector_getp` is the element's
address); `as_exchange_succession` takes the committed-cluster lock the balance already holds (use the
unlocked accessor there); a read of a record past its void time answers "not found" before nsup ever
visits, so an expiry test waits on the binding's counter; a chain's description must be refreshed after a
`record` answer or the next handler walks stale bin names; the deploy phase counts every binding on the
node, so tests assert it monotone.

**`xdr-merge`** (2026-10-21, replacing `xdr-conflict`). The hook sits in `write_master` before the ops
loop (`rd->event_existed` says the record was there) and in the delete path; the source id guard (0 or
this cluster's own: `AS_ERR_FORBIDDEN`) runs before the handler, and `event-set` refuses the binding
while `xdr { src-id }` is unset. A `record` answer is compared with the current record by logical
equality (`vals_equal_logical`: maps entry by entry regardless of order) and an equal one writes nothing
and answers `AS_ERR_LOST_CONFLICT`, which the source counts as success; `on-failure=abandon`, this kind
only, answers the same for a failed handler so the source does not retry a deterministic failure. The
payload zeroes `gen` and `lut` on both sides and the clock native fails in any event context, because a
merge that reads either cannot converge. The source ships its id with `ship-src-id true` (EE `ship.c`
sends the LUT field with the LUT part zero when bin LUTs are not shipped). The stored bytes of a value are
readable from a handler (`coll_bytes_at`, `coll_cmp_at`; SDK `Value::stored_bytes`/`cmp_stored`,
`Coll::bytes`) for a data-only tie-break. The dry run answers `symmetric`, `idempotent` and, given a
third record, `associative`, compared as stored bytes, and touches no counter.

**Edition and feature key (2026-10-21).** The event system is Enterprise code: `event.c` moved to the
Enterprise repository as `as/src/base/event_ee.c`, and the Community tree keeps the header
(`as/include/base/event.h`, which every hook site includes) plus `as/src/base/event_ce.c`, a stub in
which no binding exists, every hook lets its operation proceed, timers and the dry run answer
`AS_ERR_ENTERPRISE_ONLY`, and the seven `event-*` info commands carry `ee_only` in `thr_info.c`. The
Enterprise code is gated by a new feature key, `asdb-event-system` (`features_ee.c`:
`as_features_event_system()`, `as_features_by_name("event-system")`), read once in `as_event_init`
after the keys are loaded: without it the after-event and timer threads are not started, the SMD accept
callbacks ignore bindings and timers with a ticker warning (the modules stay loaded so the cluster-wide
state stays coherent), and the info commands, the dry run and the timer natives refuse with "feature key
does not allow the event system". Feature key files are signed, so the key cannot be added locally: the
development nodes here run without it, and `tests/src/events.rs` treats that refusal as "unsupported"
and skips. Verified by two builds: the move alone (events 18/18, privilege, udf_wasm 21/21 on
`asd-wasm:8.1.3.0-eventee`), then the gate (refusals as above, suites skipping, UDFs green on
`asd-wasm:8.1.3.0-eventkey`), plus a Community build that links against the stub. **The check itself
is commented out for now** (`as_event_init`: `g_enabled = true` with the real line beside it), at the
user's request, until issued keys carry `asdb-event-system`; the parsing, the getter and every use of
`g_enabled` stay, so enforcing it is restoring one line. The running image is `asd-wasm:8.1.3.0-eventee2`.

**Two clusters, live (2026-10-21).** `tests/src/xdr_merge.rs` runs against two Enterprise containers
(`asd-dc1` :3400, `asd-dc2` :3500 on the podman network `asd-xdr`, configurations in
`_temp/asd-image-wasm/xdr/`), each the other's XDR source and destination with `src-id` 1 and 2 and
`ship-src-id true`, the CRDT module bound on both with `on-failure=abandon`: a record born on one side
reaches the other as a create (no merge); one side's write arrives at the other as an XDR write to an
existing record and merges; both sides write at once and converge on the union; a write that changes
nothing is merged to an equal record on the receiver, which writes nothing and counts `keep-local`, while
the source counts the shipment as a success. It found two bugs the dry run could not: an XDR shipment is
a **replace** write, for which `write_master_apply` skips loading the existing bins, so the merge saw an
empty current record and returned the incoming one (now the bins are loaded whenever an `xdr-merge`,
`before-write` or `after-write` binding applies to an existing record); and the "unchanged" comparison
counted a nil entry in the answer (a bin delete) as different from the bin's absence (now
`bins_equal_logical` skips nils on both sides, and the dry run's three laws compare through it too, or a
kept-local dry merge would compare an answer carrying nils against a nil-free copy of the record). Skips without `AEROSPIKE_XDR_DC1`/`DC2`.

**Not done.** Two-cluster tests of `before-ship`; three or more sites (mesh); `before-evict` and
`partition-transfer` under real pressure and migration; a read of other records from a handler; an
outbox for the after-events; `before-login` and `before-admin-command` (deferred).

## 7a. Java

A fourth guest language (2026-10-27): `aerospike-udf-sdk-wasm-java`, a Maven library (`Udf`, `Value`,
`Events`, `Conflict`, `EventRecord`) over the same natives, compiled by **TeaVM 0.13.1's linear-memory
WebAssembly backend**. The spike that chose it: TeaVM 0.16 (current) keeps only a WebAssembly GC backend,
whose output carries a tag section (the exception-handling proposal, which WAMR runs in its classic
interpreter only), imports `wasm:js-string.*` builtins and `teavmJso.*` (a JavaScript host) and an
`env.memory`; `wamrc` cannot even load it. 0.14 removed the old backend, so 0.13.1 is pinned; its modules
have one exported memory, a start function (the runtime's initialisation, which WAMR runs at
instantiation), no GC or exception sections, and compile ahead of time. The plugin needs `teavm-core` at
compile scope (the class library lists it at runtime scope). GraalVM's Web Image backend was dismissed:
WasmGC, JavaScript wrapper, not open source.

Host side: two more import modules in `mod_wamr_natives.c`, `teavm` (`logString` - decoded through the
module's own `teavm_stringData`/`teavm_charArrayData`/`teavm_arrayLength` exports -, `logInt`,
`logOutOfMemory`, `currentTimeMillis`, which answers 0 in an event handler like `current_time`) and
`teavmMath` (`sqrt`, `pow`, the transcendental functions). Nothing else changed.

Guest side, the rule this backend imposes: the collector may move an array and `Address.pin` is not
implemented (it compiles to `unreachable`, the spike's first trap), so `Address.ofData(buf)` is taken
inside the expression that hands it to the host and never kept. `Udf.floats` copies a blob straight into
a `float[]`; `Value` objects are reused; every `String` method has a `byte[]` twin. An uncaught exception
traps the call (`Exception: unreachable`, a failure the client sees); a caught one is ordinary Java.

Results: `hello`, `vector`, `guard`, `resolver` and `crdt` examples (the CRDT rule for rule the Rust one,
through the same nine dry-run scenarios); the vector top-k bit-identical with the other
engines (`wasm_vector_modules_agree`), and 14.7 ms at 20,000 records against Rust 13.3 / C 11.6 / Go 13.2
/ native 10.8 (1.35x native); a single call 285 us against 244-265 for the others, which is the runtime's
start-up per instance. Modules are 67 to 222 KB. The guard and resolver pass the four-language agreement
tests. Book chapters in each part; the API reference generator parses the Java classes.

**Exports by annotation (2026-10-27).** The hand-written pair per UDF (the `@Export` trampoline over
`(int args, int argc)` plus the `__udf_sig_` static) is now generated: `@Function("name")`,
`@Stream.Record` / `@Stream.End` and `@On.<Kind>` (fifteen nested annotations, one per event kind) on
plain static methods with typed parameters, and a `javax.annotation.processing` processor
(`aerospike-udf-sdk-wasm-java-processor`, JDK only, matches the annotations by name) that writes
`<Class>Exports` beside the class: the decode-call-deliver trampoline (`Params.*` decoders, arity check,
`UdfException` → `Udf.fail`, any other `Throwable` → failure with its message), the signature export
derived from the Java types (`long`/`int` → int, `double`, `boolean` → bool, `String` → str, `byte[]` →
bytes, `Value` → any, `Value[]` last → any..., `@Nullable` → `?`; boxed scalars for nullable), and the
module's `main`. Why a processor and not reflection: exports are decided by TeaVM at compile time from
`@Export`, and TeaVM has no runtime reflection worth the name; TeaVM's own metaprogramming API generates
bodies, not exports. Why the generated class is the `mainClass`: `WasmDependencyListener.classReached`
links `@Export` methods only of classes the dependency walk reaches, so the entry point must be the
class that holds them. The raw `(int, int) -> int` form is kept under every annotation (exported as is,
`params` supplies the signature text) for a hot path that reads its arguments once per stream, which is
how `examples/vector` stays bit-identical and as fast as before. The annotations are `SOURCE`-retained:
TeaVM never sees them. All five SDK examples moved onto the annotations (the guard, resolver, counter and
CRDT typed; the vector raw), every suite green, and `generated_exports_decode_arguments` in
`tests/src/udf_java.rs` drives the generated rejections (missing, mismatched, surplus argument; a thrown
`UdfException`) through the counter module.

**Parity with the Rust SDK (2026-10-27).** A source-level comparison of the three SDKs (the Rust public
surface and import block against `udf.go` and the Java classes) found the Java and Go wrappers stopped at
what the vector and guard examples needed. Java now carries the whole Rust record and event surface:
`Cursor` (`Udf.list` / `Udf.map`, a 32-value window, self-closing, `AutoCloseable`, `Iterable`), the
typed bin readers (`binInt` ... `binMap`, throwing `UdfException`), `setBool` / `setNil` / `setGeoJson` /
`setListInts` / `setListDoubles`, `mapPutI*` and `mapPut` (integer and arbitrary built keys),
`listPushDouble` / `listPushBool` / `mapPutDouble` / `mapPutBool`, `resultGeoJson` and `Value.ofGeoJson`,
`lastUpdateTime` / `recordSize` / `binCount` / `setName` / `setTtl` / `removeRecord`; on the event side
`EventRecord.binInt` ... `binValue` (null for absent or nil), `withBins`, `Conflict.pair()` → `Pair` /
`Both<T>` (`newer`, `older`, `max`, `min`, `merge`, `sumInt`, `sumDouble`, `withNewer`), `involvesDelete`,
`remoteDeleted`. Twelve natives added to `Natives.java` for it. The resolver is now the Rust one line for
line over `Pair`/`Both`, the guard and counter use `withBins`, the counter walks its map with a `Cursor`.
`examples/udf` is the Java fixture (the counterpart of `examples/udf` in Rust) and
`java_sdk_covers_the_rust_surface` holds it to the Rust fixture tests' expectations. Go keeps the gaps the
comparison listed; it was not in scope.

**The idiomatic tier (2026-10-27).** The surface above was written outward from the natives and showed it:
`int` handles, status codes everywhere, out-parameters and `byte[]` twins as the default form. A second
tier now sits on top and the examples use it. `Bin` (a bin by name, encoded once; typed getters with
optional defaults, setters of every kind, `asList`/`asMap` cursors), `UdfMap` / `UdfList` (chainable
`put`/`add` of every kind, string and integer keys, `putAll`/`addAll` by position; `wrap(handle)` bridges
from below), `Value.asLong/asString/asBytes/asList/asMap` and `is*`, `Cursor` for-each handing out fresh
values (the `while` form stays allocation-free), `Udf.commit()` / `Udf.delete()` throwing. Event handlers
return answer objects the processor delivers (`Decision`, `Gate`, `Expiry`, `Eviction`, `ReadResult`,
`ShipDecision`, `Resolution`; `void` for the done kinds), with `Answer.deliver()` behind them. `@Stream` is
now a class annotation: `record` and `end` methods, fields as state, one instance built by the generated
class; exported instance methods in general run on that instance (a no-argument constructor is required),
which is how `Vector` lost its statics. The low-level tier (`Udf` statics over handles and statuses) stays
documented as the hot-path form and is what `Vector` and `Crdt` use. Not done, by choice: `Events.*Info`
fields stay public mutable with a public `decode` (the raw form needs it), the `@Function` name stays
(`java.util.function.Function` collides only on import), and `Value`'s public `kind`/`len`/`payload` stay
for `switch` speed. All suites green on the rewritten examples; the vector module is still bit-identical
with the other engines.

## 7b. Go, caught up to Rust

A source comparison of the three SDKs (2026-10-27) put Go last: a package of functions over handles and
status codes, typed exports absent, event and stream helpers limited to decoders and `int32` answers, and
a dozen natives (bool/nil/GeoJSON writes, packed list writes, integer keys, GeoJSON result, TTL, clock,
LUT, size, bin count, set name, set-TTL, delete) never wrapped. The Go SDK now has the typed tier the Java
one got, in Go's own idioms:

- **`udfgen`** (`cmd/udfgen`, run by `go generate`): a `go/ast` reader of `udf:` directives in doc
  comments, `//udf:function <name>`, `//udf:on <kind> <name>`, `//udf:stream <name>` on a `var x T`
  whose type has `Record` and `End`, writing `udf_exports.go` (checked in) with the `//export` trampolines
  that decode arguments (`udf.Arg*` / `udf.Opt*`, `udf.Arity`), deliver returns (`(T, error)`), decode
  events (`EventDesc`/`EventSide`/`Event*Info`/`DecodeConflict`) and deliver answers (`Answer.Deliver`),
  plus the `__udf_sig_` exports derived from the Go types. A generator is the Go analogue of a macro or an
  annotation processor; the raw `(args, argc int32) int32` form passes through with `params` for the
  signature, which is how the vector stays as it was.
- **`error` returns** everywhere on the typed tier (`udf.Error{Status, Msg}`), `udf.Commit()` / `Delete()`,
  `udf.FailErr`; the `int32` statuses stay on the low-level tier.
- **`Kind`** is a typed enum (`udf.KindInt` ...): the bare `Int`/`Map`/`List` constants had to go so the
  builder types could take the names `Map` and `List`. `KeyType()` returns a `Kind`.
- **`Bin`** (`udf.NewBin`, typed getters with `Or` defaults, setters of every kind, `List()`/`Map()`
  iterators), **`*Map` / `*List`** builders (`Put(key, any)` over a type switch, `PutI`, `PutGeoJSON`,
  `PutAll`/`AddAll` by position, chainable with a sticky `Err()`, `WrapMap`/`WrapList`), **`Value`**
  accessors (`Int()`, `Str()`, `Bytes()`, `Floats()`, `Lookup`, `IsX`), **range-over-func** `List()`
  `iter.Seq[Value]` and `Map()` `iter.Seq2[Value, Value]` (Go 1.23; TinyGo 0.34+), the cursor released
  when the loop ends or breaks.
- **Events**: `EventRecord.Int/Float64/Bool/Str/Get` `(T, bool)`, `List`/`Map`/`Names` iterators,
  `WithBins`, `DeletedAny`; `Conflict.Pair()` → `Pair` / `Both[T]` (generic: `Newer`, `Older`, `Ordered`,
  `Merge`, and `udf.Max`/`Min`/`Sum` as constrained functions), `InvolvesDelete`; answer values
  `Decision`/`Gate`/`Expiry`/`Eviction`/`ReadResult`/`ShipDecision`/`Resolution` with constructors
  (`Accept`, `AcceptRecord`, `Resolve`, ...) replacing the `int32`-returning functions.
- **Examples**: guard, resolver (over `Pair`/`Both`), vector (a `//udf:stream topk` over a `TopK` variable,
  raw form), and two new ones, the counter (`map[string]int64`) and the CRDT (`map[string]candidate`,
  `sort.Slice`, range over stored collections), both passing the same dry-run scenarios and the counter the
  live two-cluster test; `examples/udf` is the Go fixture. Tests: `tests/src/udf_go.rs` (two tests), Go rows
  in `crdt_merge_converges_in_the_eight_scenarios`, `counter_merge_converges`, `counter_converges_two_clusters`.
  Book: the three Go chapters rewritten, `events/crdt-go.md`, a Go section on the counter page;
  `gen-api.py` parses the package's files by area. Module sizes 11 to 42 KB.

## 7c. C++

A fifth SDK (2026-10-27): `aerospike-udf-sdk-wasm-cpp`, one header over the C one, built with
`clang -x c++ -std=c++20 --target=wasm32 -nostdlib -nostdinc++ -ffreestanding -fno-exceptions -fno-rtti
-fvisibility=hidden`. No standard library at all: the header carries its own `Result<T>`, `Optional<T>`,
`StrView`, `Text<N>`/`Bytes<N>` (the Rust `Str<N>`/`Bytes<N>`), minimal type traits, index sequences and a
static message buffer, so nothing allocates and no global constructor runs (every SDK object is
`constexpr`-constructible, which matters because there is no `main` and `__wasm_call_ctors` is never
called). The typed exports are the interesting part: `UDF_FUNCTION(name, Ret, params...)` defines the
function with the given parameter list, a `template <auto Fn>` trampoline deduces the parameter types from
the function pointer and decodes each through `Param<T>` (first error wins, delivered through `fail`), and
the signature text is built at compile time by a `constexpr` `Signature` from `Param<T>::name` plus a
`constexpr` parser (`ParamNames`) that pulls the parameter names out of `#__VA_ARGS__`, so
`fn increment(site: str[32], n: int) -> value` costs nothing at run time and lives in rodata. Event handlers
are `UDF_ON_<KIND>(name)` macros with fixed parameter names returning `Result<Answer>`; streams are
`UDF_STREAM_RECORD`/`UDF_STREAM_END` (the end macro emits the combined signature) or `UDF_STREAM_RAW`.
Range-for over stored collections through `ListRange`/`MapRange` whose iterators own the cursor (move-only,
closed in the destructor, so `break` releases it). Traps on the way: a member named `error()` shadowing
`udf::error` inside `Map`; `Result<T>` needs `T` default-constructible, so `Map()`/`List()` are null wrappers
and `Answer()` is empty; a braced-init-list in a range-for needs `<initializer_list>`, which is not
there - use an array. Examples: guard, resolver (over `Pair`/`Both`), vector (the C kernels under the raw
macros, bit-identical with the other five engines), counter and CRDT (fixed arrays like Rust), and the
`examples/udf` fixture; `tests/src/udf_cpp.rs` plus C++ rows in the vector, guard, resolver, CRDT and
counter tests, all green first run. Modules 8 to 85 KB. Book: three C++ chapters, `events/crdt-cpp.md`, a
C++ section on the counter page, a `parse_cpp` in `gen-api.py`.

## 7d. The PN-Counter

A second small CRDT beside the counter (2026-10-28), `examples/pncounter` in all four SDKs: two grow-only
maps `inc` and `dec`, `increment`/`decrement` add a non-negative amount to this site's entry of one map
(a negative amount is refused: it would shrink an entry and reintroduce the lost update), `value` is the
difference of the sums, and `merge_pncounter` runs the counter's per-site maximum over each map on its
own. It has no floor by design: both sides taking four from six converge on minus two, and the tests
assert that rather than clamp it. `pncounter_merge_converges` (dry run, four languages, negative amount
refused, value below zero, three laws over both maps) and `pncounter_converges_two_clusters` (live DCs)
are green. Book chapter `events/pncounter.md` sits between the counter and the device map. Modules:
Rust 26 KB, Go 22 KB, C++ 36 KB, Java 250 KB.

## 7e. The CRDT tutorial: 2P-Set and LWW-Element-Set

Two more steps (2026-10-28) between the PN-Counter and the device map, in all four SDKs, and the book's
CRDT pages regrouped into a part of their own, `aerospike-udf-sdk/book/src/crdt/` ("CRDT Tutorial": intro page with the
laws, the server's role and the ladder table; steps 1-5 = counter, pncounter, twopset, lwwset,
device-map; the device map's three language pages folded into one page with "The same module in
Java/Go/C++" sections). `examples/twopset`: two sorted string lists `added`/`removed`, union merge,
`add`/`remove`/`members` returning the member list, `remove` of a non-member refused, re-add a documented
no-op (the limit). `examples/lwwset`: two maps `addedAt`/`removedAt` of ms stamps from `now()` (refused in
the handler, fine in a record UDF), merge = per-tag max over each map (the PN-Counter's), member iff
add > remove (tie → removal), stamp = max(now, last stamp for the tag + 1) so one site never ties or goes
backwards; the clock-skew limit is stated and is what the device map's versions fix. Tests:
`twopset_merge_converges`/`lwwset_merge_converges` (dry run incl. the bias written straight into the
record) and `*_converges_two_clusters` (live), all green first run. Module sizes: Rust 25/27 KB, Go
28/28 KB, C++ 36/38 KB, Java 247/259 KB.

Diagrams (2026-10-28): seven inline SVG timelines generated by `aerospike-udf-sdk/book/gen-diagrams.py` into
`aerospike-udf-sdk/book/src/crdt/diagrams/` and pulled in with `{{#include}}` so `currentColor` follows the theme: the
plain-integer failure and the G-Counter fix and the merge loop (tutorial intro), the 2P-Set race, the LWW
race and the clock-skew case, and the purge-horizon bars. Verified in light and dark with `rsvg-convert
--stylesheet`. Four more for the overview chapters, in `aerospike-udf-sdk/book/src/diagrams/` (same generator, box-and-arrow
primitives `region`/`node`/`cells`/`arrow`): the module contract (host record, the sixteen-byte value,
lazy bytes and cursors, hand-back by position) in the introduction; the read/stage/commit/answer call
with the no-commit bypass in red, in the record UDF overview; one instance per query thread and the
client merging partials, in the stream UDF overview; the interpreter → AoT → cached-after-restart
lifecycle with the simd128 lane, in the configuration chapter.

## 8. History

The first design put a Rust/wasmtime module (`mod_wasm`) behind a C shim, with msgpack as the value boundary
and bindgen generating the server's types in the same build — which made a changed server header a Rust
compile error, the one property worth keeping in mind for any future binding. It was removed after the
measurements in `WAMR_EVALUATION.md`: the allocator the guest needed was its largest per-call cost, and a
collection serialised across the boundary lost to one walked in place.

The first WAMR ABI walked collections through handles and typed accessors (`bin_int`, `list_int(h, i)`,
`map_get_*`), one import per element. It measured 1.48× slower than Lua on a map-heavy aggregation and was
replaced on 2026-10-20 by the by-position contract above, which reads a 10,000-element list in 0.27 ms
against 0.74 ms, round trip included. Its natives were retired rather than kept beside the new ones, so there
is one way of reading.

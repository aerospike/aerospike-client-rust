# How the engine runs a module

What `mod-wamr` keeps between calls, what it builds for each one, how that compares with the Lua engine
next to it, and what the two cost. Everything here is read from the engine's source on the
`preview/wasm-udf-event-system` branch and from the measurements this project has printed; nothing is a
prediction.

## What is cached, and what is not

| | WASM (`mod-wamr`) | Lua (`mod-lua`) |
| --- | --- | --- |
| **Code** | The loaded module, in a reference-counted cache keyed by module name; on the Fast JIT, its functions compiled to machine code in the JIT's code cache, for as long as the module is loaded. Nothing compiled is kept on disk, so a restart compiles again. | The script's source, re-read whenever a state is created. |
| **Execution state, record UDF** | None. Every call instantiates the module afresh, linear memory and globals included, creates an execution environment, runs the module's initializer if it exports one, calls, and tears all of it down. | A `lua_State` per module, checked out of a queue for the call and returned afterwards, up to 128 per module; a new state is created only on a miss. The sandbox is what makes a reused state safe. |
| **Execution state, stream UDF** | One instance per query thread, held in a thread-local slot across every partition slice that thread runs, and finalised once, with one call of `_end`, when the thread's last slice is done. | A checked-out state per slice, as above. |

So the two engines reuse different things: Lua reuses interpreter states, WASM reuses compiled code.

The per-call instance for a record UDF is a choice, not an omission. The guest ABI never asks a module
to allocate, the host writes only into memory the guest already owns, so an instance carries no app heap
and is cheap to make; and a fresh instance gives each call a clean memory, with nothing of an earlier
call's state in it to leak between records. A module that brings its own allocator allocates inside its
linear memory, which starts empty on every call for the same reason.

What is not new on every call is the memory *mapping* under the instance. Measured on its own, with the
same counter function in each language on the interpreter tier, making and destroying an instance was:

| Callers | Rust | C++ | Go | Java |
| --- | --- | --- | --- | --- |
| 1, a new mapping per call | 12 µs | 11 µs | 10 µs | 35 µs |
| 16, a new mapping per call | 130 µs | not measured | not measured | 675 µs |
| 16, the thread's block reused | 29 µs | 12 µs | 8 µs | 700 µs |
| 64, the thread's block reused | 38 µs | 17 µs | 9 µs | 1,700 µs |

The guest's own work in those calls was 3 to 6 µs, and 55 to 70 µs for Java, which starts its runtime in
the call. With one caller the instance is cheap. With many it did not scale: mapping and unmapping
memory takes a lock the whole process shares. So each thread keeps the last block it freed, cleared, up
to 2 MiB, and gives it to the next instance it makes. The Rust module is the slowest of the three small
ones because its memory is 17 pages, 1.1 MiB, all of which is cleared when it is freed. A Java module
grows a memory larger than 2 MiB, stays on the mapped path, and is not helped. End-to-end throughput on
the test machine moved by about 4%, because the client shared its six cores; the gain is in server time
per call and in the tail.

The stream path shows what per-call instantiation costs when it does matter. Until October 2026 each of
the 4,096 partition slices of a query instantiated its own instance, decoded its own arguments and
shipped its own partial, and a top-k over 100,000 records spent about 95 ms of its 215 ms on that. The
per-thread slot removed it, and took the vector top-k from six times slower than a native plugin to
level with it.

The module cache is reference counted because it has to be: a call looks its module up, releases the
cache's lock, and then spends microseconds to milliseconds in the guest. A re-registration would
otherwise unload the module and free its bytes under that call. The cache holds one reference and every running call holds one; whoever drops the last frees
the module, so an eviction never waits for a slow UDF and a slow UDF never reads freed memory.

## The tiers

Which tier a server runs is fixed by its architecture, when it is built, and is not read from the
configuration: WAMR's own **Fast JIT** on x86-64, the only architecture it has a code generator for, and
WAMR's fast interpreter on aarch64, which is for development. The stanza has no `running-mode`, and the
ahead-of-time compiler's keys (`aot-compiler`, `aot-cache-dir`, `aot-compile-threads`) are not read
either: no default build compiles a module ahead of time. (`make WAMR_TIER=aot|jit` still builds those
tiers for experiments; neither is a supported server, and the LLVM JIT puts LLVM into the server binary.)

A node says which tier it runs: the startup log has `wamr: runtime ready, mode fast-jit, ...` (or
`interp`), and `udf-stats` prints `tier=` for every module it has loaded. A benchmark of an interpreter
mistaken for a JIT is a mistake those make impossible to sustain; read them before reading any number.

The Fast JIT compiles every function into one code cache, 256 MB on this server where WAMR's own default
is 10 MB, which a handful of Java modules fill, after which every compile fails until a restart. It has
no SIMD code generator, so a module built with `simd128` is refused at registration on an x86-64 node.
The interpreter takes such a module, and runs it until it reaches a SIMD instruction the fast interpreter
does not implement, which traps; the SDKs build without SIMD by default for both reasons.

## What it costs

Measured in this project; the files that print each number are named so it can be reproduced.

- **The interpreter is within a factor of two of Lua on compute.** It is the aarch64 tier, for
  development. No bench in this project yet compares the Fast JIT with Lua; the client suites run on it
  (see [Security](security.md#the-fast-jit)), and the numbers below say which tier printed them.
- **A SIMD stream UDF** on the vector top-k measured level with the native plugin and fourteen to
  nineteen times faster than Lua (`aerospike-udf-sdk-native/examples/vector/tests/lua_vs_native.rs`,
  against the Lua in `aerospike-udf-sdk-native/examples/vector/lua/vector.lua` and the WASM module in
  `aerospike-udf-sdk/wasm-rust/examples/vector`). That was the ahead-of-time tier, compiling SIMD to NEON,
  which no default build runs now: the Fast JIT refuses a SIMD module, and the interpreter cannot be
  relied on to run one.
- **A record UDF's per-call engine cost is below the round-trip noise floor.** The native bench's per-call
  section subtracts a plain-write baseline and still reports noise as large as the difference it is
  after; what it bounds is that the difference between engines on one small call is a handful of
  microseconds against a round trip of hundreds (`tests/src/udf_native_bench.rs`). The cost of the
  per-call instance is inside that bound with one caller; the table above has it measured on its own.
- **A 10,000-element list summed by a record UDF**: 0.27 ms per call including the round trip, against
  0.74 ms under the earlier handle-per-element ABI it replaced; the ratio to Lua and to the native module
  on the same record is what the bench prints (`list_bench` in the same file).
- **Aggregation at scale**, count and sum over 200,000 records across all three engines, is
  `tests/src/udf_agg_bench.rs`; it truncates its set afterwards because a set-scoped query with no set
  index walks the whole primary index, and records another run left behind drift every row.

Two of this project's measurements contradicted their prediction, a cheaper data path that was a write
that never committed, and a fifteen-fold compute gap that was fourfold once both sides used the same
optimization level, which is why every bench here asserts its own answer and nothing above is a claim a
file does not print.

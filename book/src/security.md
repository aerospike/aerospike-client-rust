# Security

What a WASM module can and cannot do inside the server, what stops it, and where the design is weaker
than it should be. The first half is the model; the second half is the limitations, stated as plainly
as the guarantees. Everything is read from the engine on the `preview/wasm-udf-event-system` branch and
from the conformance suite that tests it; the open work is tracked in `WASM_SECURITY_TODO.md` at the
root of the client repository.

## The trust model

A module is code an administrator chose to run in the server. The sandbox exists so that a module with
a bug, or one written carelessly, cannot corrupt the server or read what it was not given; it is not a
licence to register modules from people the cluster does not trust.

- **Registering a module** needs the UDF admin permission, the same one Lua registration needs.
- **Binding a module to an event** needs a separate event admin permission, for `event-set` and
  `event-remove`. Listing bindings needs none.
- **Calling a record UDF** needs what calling a Lua UDF needs: the caller's write permission on the
  record.
- The `xdr-merge` event is an Enterprise feature gated by a feature key, and **that check is commented out
  for now**, because no issued key carries it yet. Until it is restored, an Enterprise server with any
  key accepts event bindings.

The machine is inside the trust boundary. Anyone who can write to the server's directories can replace
the server itself, so files on disk are trusted, with one consequence spelled out under *The compiled
cache* below.

## What a guest can reach

**The surface is removed, not configured away.** The runtime is built without WASI and without a
built-in libc, so a guest has no filesystem, no sockets, no environment and no process. It is built
without multi-module support, so a module cannot import from another module by name, and without
shared memory or guest threads, so a guest cannot outlive the record lock it runs under.

A guest's only imports are the functions this engine exports to it:

- **The `aerospike` imports**: read the record's bins, metadata, key and digest; stage and commit
  writes to *that* record; build lists and maps; set the call's result or fail it; write a line to the
  server log. There is no import that names another record, runs a query,
  or sends an info command.
- **The `teavm` imports**, for modules built by the Java toolchain: elementary math functions, the
  runtime's logging, and a clock.

A record UDF's writes go through the same record-update path a Lua UDF's do, under the same
transaction and the same lock. A stream UDF is handed records to read and has no record to write.

Reference types are enabled in the loader because no module built by a current Rust toolchain loads
without them. That is a loader capability, not a guest permission: a module gains nothing it could not
already do.

## Memory safety

A guest addresses its own linear memory and nothing else. Every load and store is bounds-checked **in
software**, on every tier.

The hardware variant, guard pages and a signal handler, is disabled in this runtime on purpose: it
relies on signal handlers and a memory layout, and the server installs its own handlers for crash
reporting. The software check cannot interfere with that.

For a module compiled ahead of time, the checks are emitted by the compiler, so what the compiler is
told is part of the sandbox. On a 64-bit target the compiler's own default is to emit **no** software
bounds checks and rely on the hardware trap this runtime does not have. The engine therefore always
passes `--bounds-checks=1` and `--stack-bounds-checks=1`, and never leaves it to the default. The same
option text is covered by the keyed tag that ties a compiled file to its bytecode (see *The compiled
cache*), so a file compiled with any other options, by an older server or by hand, has a tag that does
not match and is recompiled rather than loaded.

**Pointers a guest passes to the host are validated before the host function runs.** Every import that
takes a buffer declares it as a pointer paired with its length, and the runtime checks the pair against
the guest's memory on the way in, so no host function bounds-checks a guest pointer itself.

**Collections and cursors are reached by handle, not by address.** A call has a table of handles that
grows as it needs, charged to the call's memory budget, so the budget is the only limit on how many it
holds; every use is checked against the table, a handle given back and reused is refused if used again,
and the table is destroyed with the call. A function takes at
most 128 arguments, and a call with more fails rather than losing the rest.

**Instantiating a module runs none of its code.** A module may name a start function, and the runtime
calls it while it makes the instance, along with two constructor exports of older toolchains. That is
guest code running before there is an instance for a deadline to stop, and a module whose start
function looped held a server thread, and its record's lock, until the node was restarted. Refusing
such modules is not an option: every module the Java SDK builds has one. So a module is rewritten in
memory before the runtime sees it. The start section is dropped and its function exported under a
reserved name, and the two constructor exports are renamed. The server then calls them itself, first
and in the runtime's order, under the watchdog. The rewrite fails closed: a module whose layout it does
not fully understand is refused, and so is one that exports a name beginning `__as_`.

**The server calls an export only as the type it is.** The runtime copies a call's results into the
caller's array, as many words as the export's own type declares. The server's array is two words, for
`(i32, i32) -> i32`, and a module can export a function of any type under a UDF's name. One declared
to return several 64-bit values wrote them over the server's stack. Every call into a guest now goes
through one function that checks the export's type first and refuses anything else with an error that
says so.

**A value in a handle is shared, never moved.** Writing a handle's value to a bin, or putting it in a
list or map, gives the new owner a reference of its own. The handle used to give up its reference and
keep its pointer, so a write the record refused, which frees the value at once, left the guest holding
a handle to freed memory. A boolean is one of the value library's two constants, because the library
does not count references to booleans and a heap one could not be shared.

**A collection cannot contain itself, or nest past 64 levels.** Packing a value and releasing it both
recurse into it, so a list a guest pushed into itself ran the server's stack out. A collection going
into another is walked first, and refused if the walk finds the destination or goes too deep. A chain
built from the top down, one empty list at a time, is not seen by that walk; it is bounded by the call's
memory budget and refused where it would leave the call, as a bin or as the result.

**A module cannot buy memory with a number.** Every section of a module but two begins with a count,
and the runtime's loader made room for that many entries before reading any. Thirty-nine bytes claiming
half a billion types cost four gigabytes at registration. A section that declares more entries than it
has bytes is refused before the runtime sees it, and the runtime is refused any single allocation over
16 MiB, which also bounds the tables a module can declare.

**What the host is handed, it releases.** A record's key is built afresh for each call that asks, and a
string pushed onto a list is copied by the list. Both used to be left behind, so a guest reading its
key in a loop, or building a list of strings at all, grew the server's heap. A write the record refuses
is released too.

**A guest adds only to what it built.** The natives that append to a list or put into a map accepted
any handle, and a call's arguments and the values read from a record are handles too. A guest could
append to its own argument list, or to a list the record still owned. Those natives now refuse a
handle the call did not create. Found while making the account below balance.

## Limits

| What | Limit | Where it comes from |
| --- | --- | --- |
| Linear memory per instance | 64 MB by default, 2 GiB at most | `max-memory` in the `mod-wamr` stanza, rounded up to 64 KiB pages |
| Linear memory, all instances | 4 GiB by default | `max-total-memory`; counted exactly, growth included |
| Values built on the server's side, per call | the same as `max-memory` | counted as each is made and given back as each is freed; see below |
| Operand stack per call | 64 KiB | fixed |
| Running time, record UDF | the transaction's deadline | a watchdog checks every 2 ms and terminates an instance past its deadline |
| Running time, stream UDF | 10 s per batch by default | `stream-timeout-ms`; also stopped when its query is abandoned |
| Registered modules | 256 by default | `max-modules`; the server's own 1 MiB per UDF bounds each one |
| Log lines | 16 per call, 100 per second per module | fixed; what is over is dropped and counted |
| Initial room of a list or map a guest builds | 1,024 elements | fixed; the guest's number is a hint, not an allocation |
| Nesting of collections a guest builds | 64 levels, and never a cycle | fixed; checked on insert and where a value leaves the call |
| One allocation by the runtime, other than guest memory | 16 MiB | fixed; a module's tables and structures |
| Entries a module section may declare | as many as it has bytes | checked before the runtime loads it |
| Reading a signature export | 1 s | fixed; it runs from an info command, with no transaction to borrow a deadline from |
| Calls the watchdog can track at once | 1,024 | fixed |
| Handles per call | as many as the call's memory holds | `max-memory`; no separate ceiling |
| Arguments per call | 128 | fixed; more fails the call |
| Queued compiles | 256, deduplicated by module | fixed; never fewer than `max-modules`, so a registration cannot find it full |
| Compiler processes at once | 1 to 8 | `aot-compile-threads`, when the tier reads it |

The deadline is the transaction's own, captured on the transaction thread, not a limit invented by the
engine. The thread manager is built into the runtime specifically so that a compute-bound guest notices
a termination; without it a guest would ignore its deadline until it returned.

**A stream UDF is on a clock of its own.** An aggregation is a long query, and the server gives those
no total timeout, so there is no deadline for the engine to borrow. Each batch of records, and the
final call, gets `stream-timeout-ms`. The guest is also stopped the moment its query job is abandoned,
by `query-abort` or by another of its threads failing: the query layer hands the engine a callback that
reads the job's state, and the watchdog asks it every tick. Before this, a stream module that looped
held its query thread until the node was restarted, and an abort could not reach it.

**Memory is counted, not estimated.** The runtime is built to pass every linear memory allocation
through the engine, tagged as such, so the engine sees each instance's memory and every `memory.grow`.
At the ceiling a new instance fails its call with an error that names `max-total-memory`, and a grow is
refused, which a guest already handles as running out of memory. The runtime's own structures are not
counted: they are bounded by the number of modules and of calls in flight.

**What a guest logs is data.** Every line is escaped before it reaches the server log: a control byte,
a DEL or a backslash becomes `\xNN`, so a guest cannot start a line that looks like the server's own.
Bytes above 0x7f pass, because that is how most languages are written. A call may write 16 lines and a
module 100 a second. What is over is dropped, and the count is reported when the call ends, at most
once a second per module.

## Isolation between calls

A record UDF gets a fresh instance for every call: a linear memory of zeroes, new globals, and nothing
of any earlier call. Two calls cannot see each other's data even when they run the same module on the
same record.

The memory under an instance may be one the same thread used before. Each thread keeps the last block
it freed, up to 2 MiB, because mapping a new one for every call does not scale. A block is cleared by
the thread that dirtied it, as it is freed and before it is kept, so a kept block never holds anything
a guest wrote; the bytes past an instance's own size were never reachable and are zeroes too. Larger
blocks are returned to the system. Kept blocks are outside `max-total-memory`: at most 2 MiB for each
thread that runs UDFs.

A stream UDF gets one instance per query thread, kept across every partition slice that thread runs.
That is the feature, a stream accumulates, and it is also the isolation it does not have: whatever a
stream module keeps in memory, it keeps for every record that thread reads until the query ends. It
does not keep it any longer. The instance is destroyed when the thread finishes the query, whether the
query completed, was abandoned or had a batch fail, so nothing passes from one query to the next.

Both are checked by `nothing_a_guest_wrote_reaches_the_next_instance` in
`tests/src/udf_wasm_limits.rs`: a module marks its memory, and the next record call and the next query
must find none, after a normal end and after a query stopped mid-call. Run against a server built with
the clearing removed, the test fails on its first round.

The module cache is reference counted so that re-registering a module, or the compiler swapping in a
compiled build, cannot unload code a call is still running.

## Registration

Registration loads the module into the runtime, on a private copy of the bytes, and refuses it if it
does not load. A module that needs SIMD is refused on a tier that cannot run it.

Registration does **not** check that the module's imports resolve or that it exports any particular
function; a function name is not part of a registration. A module the server cannot run registers
successfully and fails at its first call, with the reason.

## The compiler

Ahead-of-time compilation runs `wamrc` as a separate process, forked and executed directly rather than
through a shell. Module names come from clients, and a shell would turn a metacharacter in a name into
a command; with a direct exec the arguments never pass through a parser. The compile runs on a
background thread, off the transaction path, with a bounded pool so a burst of registrations cannot
fork a dozen compilers.

The compiler parses bytecode a client supplied, and it is LLVM, so its process is confined:

| What | How |
| --- | --- |
| Environment | none but a fixed locale; nothing the server was started with reaches it |
| Open files | `/dev/null` on the three standard descriptors; every descriptor of the server is closed |
| Wall-clock time | killed, with its process group, after 60 s |
| Processor time | 60 s |
| Memory | 4 GiB of address space |
| Output | 256 MiB per file, created private; no core file |
| Privileges | cannot gain any through a set-user-id program |
| System calls | a seccomp filter refuses sockets of every family, tracing or reading another process, signals to any process but itself, and mounting, module loading and the like |
| Lifetime | dies with the server |

If any of that cannot be set up, the compiler is not run and the module stays interpreted. The filter
is a list of refusals, not of permissions: LLVM's legitimate calls are many and vary by version, and a
permission list that missed one would fail every build.

## The compiled cache

A compiled file is machine code, so the cache directory is treated like a directory of shared
libraries.

- **The directory is the server user's alone.** It is created mode 0700. One that exists is narrowed to
  that if the server owns it. If it belongs to another user, is a symbolic link, or stays writable by
  group or others, ahead-of-time compilation is switched off for the run and modules are interpreted.
- **Every file is tagged with a keyed hash.** The tag is HMAC-SHA256 over the compiler options, the
  bytecode and the compiled bytes, under a 32-byte random key in `.key` inside the directory, private
  to the server user and made on first use. A file with no matching tag is not loaded and is rebuilt:
  one placed by hand, copied from another node, compiled with other options, cut short, or changed by
  one bit.
- **A file is read once.** The bytes that are checked are the bytes that are loaded, and a file is read
  only if it is a regular file, not reached through a link, owned by the server user and not writable
  by anyone else.
- **Nothing appears half-written.** The compiler works on a private copy of the bytecode and writes
  under a working name. The result is tagged, then renamed into place. A crash leaves the previous
  pair, or a file whose tag does not match, which is rebuilt.

## What the node reports

**Counters.** The `statistics` info command carries the engine's own, prefixed `udf_wasm_`: calls,
traps, timeouts and errors of record and event calls; stream batches and the ones stopped, by reason;
instances that could not be made and allocations the node ceiling refused; registrations refused;
compiles and their failures; cache hits and rejects; log lines dropped; and, as of now, guest memory in
use, modules loaded and compiles queued. A trap and a timeout are told apart by who ended the call.

**Per module.** `udf-stats` lists every module the node has loaded with the tier it runs on and its
own calls, traps, timeouts, errors and dropped log lines. It is a command of its own because clients
parse `udf-list` and not all would take new fields. The counts restart when a module is registered
again, and carry over when the compiler replaces it with its compiled build.

**An audit record.** Registering a module is how code gets onto a node, and binding it to an event
decides when it runs. Each `udf-put`, `udf-remove`, `event-set` and `event-remove` writes one line to
the server log, in both editions: what, from which client address, and for a module its size and the
SHA-256 of its bytes, which is what `sha256sum` prints for the file. With security configured the same
facts go to the audit trail, with the authenticated user, whenever the action succeeds. Before this a
successful registration was not audited at all: the trail records a successful command only for
permissions of the sys-admin role, which the UDF and event permissions are not, and when it did record
one its detail was the whole module in base64 and no hash.

### What a guest builds on the server's side

A string, a blob, a list or a map that a guest makes through the host calls lives on the server's heap,
not in the guest's memory, so the two ceilings above did not see it: each piece was bounded by the
guest's memory, since it is copied from there, but a list could be appended to until the deadline.

Each call now has an account. Every value a host call makes for the guest is charged in full when it
is made, and refunded in full when its last reference goes: a string by its length, a blob by its
capacity, a list or a map by its slots. The budget is `max-memory`, so a call can hold on the server's
side at most what its instance can hold in its own memory, and the bytes are reserved against
`max-total-memory` as well, so the node's ceiling covers both. A host call the account cannot cover is
refused before anything is copied, the guest sees a failed call, and `udf_wasm_host_refusals` counts it.
The arithmetic is 64-bit and `max-memory` is capped at 2 GiB, so a length of 4 GiB minus one, which
used to wrap to zero when its terminator was added, is now only a refused charge.

The fuzz targets check the account on every input: a call that ends owing bytes, or that is refunded
more than it was charged, is a finding. The first version charged a string by the length up to its
first zero byte while allocating the whole of it; an end-to-end test found that, and strings are now
made and charged by their full length.

## Fuzzing

`modules/mod-wamr/fuzz/` in the server tree has five libFuzzer targets, built with clang and the
address and undefined-behaviour sanitizers, run by `make fuzz`:

| Target | What it exercises |
| --- | --- |
| `defer` | The module rewriter, with the runtime's loader as judge: a module the rewriter passes and the runtime loads must have no start function left. |
| `load` | What a registration does with a client's bytes: rewrite, load, instantiate. |
| `call` | A whole call of a mutated module: its initialisers, then every export, with a record, under a watchdog. |
| `natives` | The host calls one at a time, with fuzzed arguments on a fuzzed record, several per instance so that handles carry over. The module that calls them is generated from the tables the server registers. |
| `aot` | The loader of compiled files, which in the server sits behind the keyed tag. Load only. |

The targets include the module's sources as the unit test does, so the code under test is what the
server compiles. Linear memory comes from `malloc` there, not from a mapping, so that a host function
reading one byte past a guest's memory hits a red zone.

Of the six bugs described under *Memory safety* and *Limits*, four were found by the `natives` target:
the up-front allocation of a guest's list capacity and the handle left pointing at a freed value in its
first two minutes, the unshareable boolean after half a million executions, and the list inside itself
after 4.7 million. The other two, the start function and the export type, were found while writing the
targets. A second round, steered by a coverage measurement of what the targets were not reaching, found
six more: the Java log callbacks called without the type check, a length that wrapped in its bounds
check, three leaks, and the count that bought four gigabytes. On the final code of the first round the
four bytecode targets ran 10.8 million executions between them with no finding; after the second, the
host-call target ran 66 million. The `aot` target crashed inside the runtime's loader after 1,435, as expected of a loader not
written for hostile files; in the server that loader is only ever given a file whose tag has matched.

### The Fast JIT

`make fuzz FUZZ_TIER=fastjit` builds the same targets on the tier an x86-64 node runs. On an x86-64
container emulated on aarch64, `call` and `load` ran 1.6 million executions between them with no
finding in the module. The run found one piece of undefined behaviour in the runtime's JIT, a float
constant converted to an integer without a range check, which produces correct code on x86-64 and is
on the list to send upstream. AddressSanitizer does not run under that emulation, so this was with the
undefined-behaviour sanitizer alone.

### Keeping what was found

Every input that has broken a target is kept in `fuzz/regressions/`, and `make check` replays all of
them through the targets built with the ordinary compiler, so a fix that comes undone fails the build
with no clang and no fuzzer.

### The module under sanitizers

The fuzz targets leave out the module's server side: the module cache, the stream slot, the watchdog,
the statistics. `test/module.c` drives those through the hooks the server calls, from nine threads at
once: callers of UDFs that succeed, trap and time out, a registrar adding and removing modules under
them, a query thread running stream UDFs, a reader of the statistics. `make module-address` and `make
module-thread` build it and everything it runs under AddressSanitizer and ThreadSanitizer.
AddressSanitizer found nothing. ThreadSanitizer found the statistics being read without the atomic
loads their writers' atomic adds call for, which is fixed, and two races inside the runtime, which are
suppressed in `test/tsan.supp` with the reason each is harmless as the module uses it.

## The conformance suite

`tests/src/udf_wasm_sandbox.rs` in the client repository registers a module assembled by hand in the
test, so its bytes are in review and no toolchain is needed. Each export steps outside what a guest may
do, and each must end in an error with the server still serving and the record intact:

| Export | What it does | Interpreter | Compiled |
| --- | --- | --- | --- |
| `oob_read` | loads from the last word of the address space | trapped | trapped, "out of bounds memory access" |
| `oob_write` | stores to the last word of the address space | trapped | trapped, "out of bounds memory access" |
| `oob_edge` | loads the first byte past its one page | trapped | trapped, "out of bounds memory access" |
| `recurse` | calls itself without end | trapped | trapped, "native stack overflow" |
| `bad_indirect` | calls through a table slot that is not there | trapped | trapped, "undefined element" |
| `spin` | loops without end | stopped by the deadline | stopped by the deadline, "terminated by user" |

Measured on 2026-11-16 on aarch64, the compiled column with the patched compiler described below. The Fast JIT tier, which is x86-64 only, has not been run through it.
Every case runs twice, before and after the background compile, so a server that compiles is tested on
compiled code.

## Limitations and open concerns

These are the places the design is weaker than the sections above might suggest. Each has an entry in
`WASM_SECURITY_TODO.md`.

**The deadline on compiled code depends on a patched compiler.** Terminating an instance only sets a
flag. The interpreter and the Fast JIT poll it. Stock `wamrc` emits the poll only for a module that
declares a shared memory, which no UDF does, so a looping guest compiled by it holds its transaction
thread past the timeout: the suite's `spin` row failed exactly that way. The server tree carries one
compiler patch, `modules/mod-wamr/patches/0001-wamrc-terminate-check.patch`, which adds
`--enable-terminate-check`: the poll at every loop back-edge and call, whatever the memory is.
`make wamrc` applies it to a copy of the sources, so the WAMR submodule stays at its pinned upstream
commit, and only the compiler is patched, never the runtime. The server always passes the option, and it
is part of the cache tag, so a file compiled without it is never loaded. A stock `wamrc` rejects the
option and exits with an error, so nothing is compiled and every module runs interpreted: slower, and
still stoppable. The patch should go upstream; until it does, a WAMR upgrade means re-applying it.

**Ahead-of-time compilation is still switched off.** On this branch the tier is fixed per architecture,
the Fast JIT on x86-64 and the interpreter elsewhere, and the configuration keys for the compiler are
not read. Neither the deadline nor the cache stands in the way of reading them any more. That the tier
has only been exercised through test builds does.

**The stream clock covers the whole batch.** `stream-timeout-ms` runs from the start of a batch to its
end, including the time to read the batch's records, because claiming the watchdog once per record
would put a lock on every record of every query. A batch slowed by storage can therefore be stopped
with a guest that did nothing wrong. Lua stream UDFs are not on this clock at all: the Lua engine has
no interrupt for streams, and a looping Lua aggregation still holds its thread.

**The cache key protects against a file being placed, not against the server's own user.** Anyone who
can read `.key` can tag a file of their choosing, and the server's user can read it. That user can
already replace the server, so nothing is lost, but the key is not a defence against a compromised
server account. The directory is checked once, at startup, and its parents are not checked at all.
Someone who can write a parent can swap the directory afterwards; what they put there is still refused
file by file, for want of the right owner and a matching tag.

**Confining the compiler does not make its output trustworthy.** The limits bound what the compiler
process can do to the machine. They cannot bound what it writes: a compiler subverted by its input
produces whatever compiled file that input chose, the server tags it and loads it. The compiler is part
of what the server trusts, which is the price of native code. The filter also leaves the file system
open: the compiler runs as the server's user and can read what that user can.

**The module ceiling can be passed by a race.** The count is of files in the UDF directory, and a
registration is counted only once it is written, so registrations arriving together at the ceiling can
each pass. The overshoot is bounded by how many are in flight. There is no per-user quota: the engine
is not told who registers. The Fast JIT's single 256 MB code cache is still not accounted per module;
the module ceiling bounds how many can draw on it, not how much each takes.

**Only linear memory is counted against the node ceiling.** Tables, module structures and the
compiled code of the JIT tiers are outside it.

**A record UDF with no transaction timer has no deadline.** The watchdog takes its deadline from the
transaction. A call the server makes without one runs unwatched. Every path a client can reach has one.

**The fuzzers have run for hours, not weeks.** Three rounds found fourteen bugs in the module, and each
new angle found something within minutes: a coverage measurement, a mock that followed the real
record's rules, the leak check, a limit on allocation size, an account that had to balance. The Fast
JIT has been fuzzed only under emulation, without AddressSanitizer, and `asd` itself has not been run
under a sanitizer: the harness described above runs the module with a stand-in for the server.

## Native modules are not sandboxed

The server on this branch can also load native plugins, compiled shared objects. Those run as server
code with no sandbox at all, which is why they cannot be registered by a client: a native module is
named in the server's configuration file, and that file is the security boundary. Nothing in this
chapter applies to them.

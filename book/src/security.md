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
- The event system is an Enterprise feature gated by a feature key, and **that check is commented out
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
  writes to *that* record; build lists and maps; set the call's result or fail it; set and cancel
  timers; write a line to the server log. There is no import that names another record, runs a query,
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
option text is folded into the tag that ties a compiled file to its bytecode, so a file compiled with
any other options, by an older server or by hand, has a tag that does not match and is recompiled
rather than loaded.

**Pointers a guest passes to the host are validated before the host function runs.** Every import that
takes a buffer declares it as a pointer paired with its length, and the runtime checks the pair against
the guest's memory on the way in, so no host function bounds-checks a guest pointer itself.

**Collections and cursors are reached by handle, not by address.** A call has a table of 256 handles;
every use is checked against the table, and the table is destroyed with the call. A function takes at
most 32 arguments.

## Limits

| What | Limit | Where it comes from |
| --- | --- | --- |
| Linear memory per instance | 64 MB by default | `max-memory` in the `mod-wamr` stanza, rounded up to 64 KiB pages |
| Operand stack per call | 64 KiB | fixed |
| Running time, record UDF | the transaction's deadline | a watchdog checks every 2 ms and terminates an instance past its deadline |
| Calls the watchdog can track at once | 1,024 | fixed |
| Handles per call | 256 | fixed |
| Arguments per call | 32 | fixed |
| Queued compiles | 256, deduplicated by module | fixed |
| Compiler processes at once | 1 to 8 | `aot-compile-threads`, when the tier reads it |

The deadline is the transaction's own, captured on the transaction thread, not a limit invented by the
engine. The thread manager is built into the runtime specifically so that a compute-bound guest notices
a termination; without it a guest would ignore its deadline until it returned.

## Isolation between calls

A record UDF gets a fresh instance for every call: a new linear memory, new globals, and nothing of any
earlier call. Two calls cannot see each other's data even when they run the same module on the same
record.

A stream UDF gets one instance per query thread, kept across every partition slice that thread runs.
That is the feature, a stream accumulates, and it is also the isolation it does not have: whatever a
stream module keeps in memory, it keeps for every record that thread reads until the query ends.

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
| `spin` | loops without end | stopped by the deadline | **not stopped** |

Measured on 2026-11-16 on aarch64. The Fast JIT tier, which is x86-64 only, has not been run through it.
Every case runs twice, before and after the background compile, so a server that compiles is tested on
compiled code.

## Limitations and open concerns

These are the places the design is weaker than the sections above might suggest. Each has an entry in
`WASM_SECURITY_TODO.md`.

**Ahead-of-time compilation is switched off, and must stay off until the deadline works on compiled
code.** On this branch the tier is fixed per architecture, the Fast JIT on x86-64 and the interpreter
elsewhere, and the configuration keys for the compiler are not read. That is fortunate, because the
suite's last row is a real gap: terminating an instance only sets a flag, the interpreter polls it, and
compiled code polls it only if the compiler emitted the checks. The one compiler switch that emits them
also marks the file as needing threads, which the loader refuses because this runtime is built without
shared memory. A looping compiled guest would hold its transaction thread past the timeout. The fix is
either to build the runtime with shared memory and refuse shared memories at registration, or to change
the compiler to emit the check on its own.

**A stream UDF has no deadline.** The server's aggregation path builds its context with no timer, for
Lua as for WASM, so a stream module that loops holds a query thread until the query is abandoned.

**The compiled cache is checked for staleness, not for integrity.** The tag beside a compiled file is a
plain hash of the bytecode and the safety options. It tells the same module from a different one; it
does not authenticate the file. Someone who can write into the cache directory can place a compiled
file the server will load as native code. The directory is created mode 0750 and the machine is inside
the trust boundary, so this is defence in depth, but it would stop being acceptable the moment the
cache lived anywhere shared.

**The compiler is not confined.** It runs with the server's privileges, with no time limit, no memory
limit and no syscall filter, over bytecode a client supplied. The fork keeps a compiler crash out of
the server's memory; it does not contain a compiler that is made to misbehave.

**There is no aggregate accounting.** Memory is capped per instance, not across instances, so the
ceiling is the cap times the calls in flight. There is no limit on a module's size, on how many modules
are registered, or on how much of the Fast JIT's single 256 MB code cache one module's functions take;
when that cache fills, every later compile fails until a restart.

**The log import is unmetered.** A guest can write to the server log on every call, and what it writes
is not escaped.

**Registration is not audited beyond what the server already logs**, and the engine exports no metrics
for traps, deadline terminations or compile failures, so a module being stopped repeatedly is visible
only in the log.

**The WAMR runtime is not pinned** in the server tree. The submodule is untracked, so what is built is
whatever is checked out.

**The parsers have not been fuzzed.** The loader and the host functions have unit and integration
tests, not adversarial ones.

## Native modules are not sandboxed

The server on this branch can also load native plugins, compiled shared objects. Those run as server
code with no sandbox at all, which is why they cannot be registered by a client: a native module is
named in the server's configuration file, and that file is the security boundary. Nothing in this
chapter applies to them.

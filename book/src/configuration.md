# Configuration

Everything the engine needs sits in one stanza of `aerospike.conf`, and everything the `xdr-merge` event
needs is runtime state kept in SMD, so there is less here than the size of the feature suggests.

## The `mod-wamr` stanza

```text
mod-wamr {
    running-mode aot
    aot-compiler /opt/aerospike/bin/wamrc
    aot-cache-dir /opt/aerospike/usr/udf/wasm-aot
    aot-compile-threads 2
    max-memory 67108864
    max-total-memory 4294967296
    stream-timeout-ms 10000
    max-modules 256
    cache-enabled true
}
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `running-mode` | `aot` | Which tier runs a module: `aot` (compiled by `wamrc` at registration), `interp` (WAMR's bytecode interpreter), `jit` (LLVM ORC JIT, compiled on load; only in a build that linked LLVM), `fast-jit` (WAMR's own JIT; its code generator exists for x86-64 only, so it cannot be built on an arm64 host, and it has no SIMD, so a build with it refuses a `simd128` module at registration). An unavailable mode is reported at startup and falls back to the interpreter. |
| `aot-compiler` | empty | The path to `wamrc`. **Empty means no ahead-of-time compilation**: every module runs interpreted. `wamrc` must match the server's WAMR version and architecture; a mismatch is detected when the compiled module is loaded and that module falls back to the interpreter. |
| `aot-cache-dir` | `<udf dir>-aot` | Where compiled modules are kept, each beside a keyed tag (see [Security](security.md)). It must belong to the server's user alone. It must not be the UDF directory itself, which the server empties at startup (SMD is the source of truth for registered modules) and which would make every start recompile everything. |
| `aot-compile-threads` | `2` | How many `wamrc` processes may run at once, 1 to 8. One keeps a burst of registrations from loading the node; more shortens how long a SIMD module's first call waits for its own build, since the interpreter cannot run SIMD and such a call waits for the compiled form. |
| `max-memory` | 64 MiB | The ceiling on one instance's linear memory, rounded up to wasm pages, at most 2 GiB. A module that grows past it is refused the growth and traps or reports. It is also the ceiling on what one call may build on the server's side through the host calls: strings, blobs, lists and maps. |
| `max-total-memory` | 4 GiB | The ceiling on the linear memory of every instance on the node together, and on what their calls have built on the server's side. At the ceiling a new instance is refused, and its call fails with an error naming this setting; a `memory.grow` is refused, which the guest sees as an ordinary out-of-memory. |
| `stream-timeout-ms` | `10000` | How long one batch of a stream UDF, or its final call, may run, 1 ms to 1 hour. An aggregation query has no deadline of its own, so this is the only clock a stream UDF is on. Past it the guest is stopped and the query fails. The time covers reading the batch's records as well as the guest's work on them. |
| `max-modules` | `256` | How many WASM modules may be registered, 1 to 256. A registration past it is refused with an error naming this setting. Registering a name that exists replaces it and is always allowed. |
| `cache-enabled` | `true` | Whether loaded modules are kept between calls. `false` reloads on every call, which is for working on the runtime itself. |

The UDF directory is not set here: the server keeps one directory of registered UDF files, named by the
`mod-lua` stanza's `user-path`, and `mod-wamr` reads the same one.

WASM UDFs themselves need no feature key. The `xdr-merge` event does: `asdb-event-system true` in the
Enterprise feature key file (`feature-key-file`, or `FEATURE_KEY_FILE` in the container), alongside
`asdb-xdr` and the others. Without it the server starts, runs UDFs, and refuses every event binding; see
[the event's chapter](events/README.md). For now the check is commented out, since no issued key carries
it yet.

Which tier a server has is decided when it is built, and for now it is fixed per architecture rather than
read from the configuration: WAMR's own Fast JIT on x86-64, the interpreter on aarch64 (`make
WAMR_TIER=aot|jit|fastjit|interp` overrides it). `aot` keeps LLVM out of the server binary, since only
`wamrc`, a separate executable, needs LLVM; the Fast JIT needs no LLVM either, but has no SIMD, so a
`simd128` module is refused on an x86-64 node. The Fast JIT compiles every function into one code cache,
256 MB on this server (WAMR's own default is 10 MB, which a handful of Java modules fill, after which every
compile fails until a restart). Measured on the vector top-k, the LLVM JIT equals AoT, and the interpreter
is Lua speed. The runtime is WAMR at upstream `main` (the 2.4 line); the AoT numbers were unchanged across
the 2.4.3 snapshot, the 2.4.5 release and main.

## What the node reports

| Info command | What it answers |
| --- | --- |
| `statistics` | Among the node's statistics, the engine's counters, each prefixed `udf_wasm_`: `calls`, `traps`, `timeouts`, `errors`, `stream_batches`, `stream_timeouts`, `stream_abandoned`, `instantiate_failures`, `memory_refusals`, `host_refusals` (a host call refused because the value it would build is over the call's budget or the node's), `registrations_refused`, `compiles`, `compile_failures`, `cache_hits`, `cache_rejects`, `log_lines_dropped`, and the gauges `guest_memory_bytes`, `modules_loaded` and `compile_queue`. |
| `udf-stats` | One entry per module this node has loaded: `filename=<name>,type=WASM,tier=<tier>,calls=<n>,traps=<n>,timeouts=<n>,errors=<n>,log_lines_dropped=<n>;`. |

Every registration, removal and event binding change is written to the server log as a line beginning
`audit:`, with the client's address and the module's SHA-256, and to the security audit trail when
security is configured. [Security](security.md) has the detail.

## What a running module costs

- A record UDF runs under the transaction's deadline: the engine's watchdog stops a runaway module when the
  transaction times out.
- A stream UDF has no deadline from the query, so a stream module that loops will hold a query thread.
- Each call has a table of 256 handles for cursors and built collections, and takes at most 32 arguments.
- A record UDF gets a fresh instance per call and a stream UDF one instance per query thread, kept across
  that thread's partition slices; linear memory persists across the calls of one instance, so the Rust
  SDK's optional allocator is a real one that frees, not an arena reset per call. [How the engine runs a
  module](engine.md) has the whole model and what it costs.

## Registering a module

A module is registered like a Lua one, with the type `WASM`:

```text
udf-put:filename=ledger.wasm;udf-type=WASM;content-len=<n>;content=<base64>
```

From the Rust client:

```rust
let task = client
    .register_udf(&AdminPolicy::default(), include_bytes!("ledger.wasm"), "ledger.wasm", UdfLang::Wasm)
    .await?;
task.wait_till_complete(None).await?;
```

Registration validates and loads the module but does not resolve its imports; a module built against a
newer SDK than the server fails at its first call with `failed to call unlinked import function`. The
compiled form is built in the background after registration; the log line `wamr: <module> is now running
ahead-of-time compiled` says when it is ready. `udf-remove` removes the module, its compiled form and its
cache tag, and is refused while an event binding refers to the module.

{{#include diagrams/module-lifecycle.svg}}

## What the merge event needs

The `xdr-merge` event is configured at run time, through info commands, and its state lives in one SMD
module, written beside the others under the server's SMD directory:

| File | Holds |
| --- | --- |
| `event.smd` | the bindings: one entry per `event-set`, replicated to every node and replayed at startup |

Two settings elsewhere matter to it:

- **Security.** On an Enterprise server with security enabled, `event-set` and `event-remove` need the
  `event-admin` privilege, which is part of `data-admin`. A fresh node's bootstrap `admin` user has only
  `user-admin`; grant it `data-admin` (and `read-write-udf`, to register modules) and log in again, since
  a session's permissions refresh every few minutes. The listing commands need nothing.
- **XDR.** The event needs to know which cluster wrote what. This cluster's `xdr` stanza must have a
  `src-id`, or `event-set` refuses the binding, and the cluster that ships to it must send its own:
  under bin convergence the id travels with the bin LUTs; otherwise `ship-src-id true` in the source's
  `dc` / `namespace` stanza (dynamic) sends it with every record.

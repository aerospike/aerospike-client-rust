# Configuration

Everything the engine needs sits in one stanza of `aerospike.conf`, and everything the event system needs
is runtime state kept in SMD, so there is less here than the size of the feature suggests.

## The `mod-wamr` stanza

```text
mod-wamr {
    running-mode aot
    aot-compiler /opt/aerospike/bin/wamrc
    aot-cache-dir /opt/aerospike/usr/udf/wasm-aot
    aot-compile-threads 2
    max-memory 67108864
    cache-enabled true
}
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `running-mode` | `aot` | Which tier runs a module: `aot` (compiled by `wamrc` at registration), `interp` (WAMR's bytecode interpreter), `jit` (LLVM ORC JIT, compiled on load; only in a build that linked LLVM), `fast-jit` (WAMR's own JIT; its code generator exists for x86-64 only, so it cannot be built on an arm64 host, and it has no SIMD, so a build with it refuses a `simd128` module at registration). An unavailable mode is reported at startup and falls back to the interpreter. |
| `aot-compiler` | empty | The path to `wamrc`. **Empty means no ahead-of-time compilation**: every module runs interpreted. `wamrc` must match the server's WAMR version and architecture; a mismatch is detected when the compiled module is loaded and that module falls back to the interpreter. |
| `aot-cache-dir` | `<udf dir>-aot` | Where compiled modules are kept, under a tag of the module's content hash. It must not be the UDF directory itself, which the server empties at startup (SMD is the source of truth for registered modules) and which would make every start recompile everything. |
| `aot-compile-threads` | `2` | How many `wamrc` processes may run at once, 1 to 8. One keeps a burst of registrations from loading the node; more shortens how long a SIMD module's first call waits for its own build, since the interpreter cannot run SIMD and such a call waits for the compiled form. |
| `max-memory` | 64 MiB | The ceiling on one instance's linear memory, rounded up to wasm pages. A module that grows past it is refused the growth and traps or reports. |
| `cache-enabled` | `true` | Whether loaded modules are kept between calls. `false` reloads on every call, which is for working on the runtime itself. |

The UDF directory is not set here: the server keeps one directory of registered UDF files, named by the
`mod-lua` stanza's `user-path`, and `mod-wamr` reads the same one.

WASM UDFs themselves need no feature key. The event system does: `asdb-event-system true` in the
Enterprise feature key file (`feature-key-file`, or `FEATURE_KEY_FILE` in the container), alongside
`asdb-xdr` and the others. Without it the server starts, runs UDFs, and refuses every event binding; see
the event system chapter. For now the check is commented out, since no issued key carries it yet.

Which tier a server has is decided when it is built, and for now it is fixed per architecture rather than
read from the configuration: WAMR's own Fast JIT on x86-64, the interpreter on aarch64 (`make
WAMR_TIER=aot|jit|fastjit|interp` overrides it). `aot` keeps LLVM out of the server binary, since only
`wamrc`, a separate executable, needs LLVM; the Fast JIT needs no LLVM either, but has no SIMD, so a
`simd128` module is refused on an x86-64 node. The Fast JIT compiles every function into one code cache,
256 MB on this server (WAMR's own default is 10 MB, which a handful of Java modules fill, after which every
compile fails until a restart). Measured on the vector top-k, the LLVM JIT equals AoT, and the interpreter
is Lua speed. The runtime is WAMR at upstream `main` (the 2.4 line); the AoT numbers were unchanged across
the 2.4.3 snapshot, the 2.4.5 release and main.

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

## What the events need

The event system is configured at run time, through info commands, and its state lives in two SMD
modules that are written beside the others under the server's SMD directory:

| File | Holds |
| --- | --- |
| `event.smd` | the bindings: one entry per `event-set`, replicated to every node and replayed at startup |
| `timer.smd` | the timers: one entry per pending timer, removed when it fires |

Two settings elsewhere matter to it:

- **Security.** On an Enterprise server with security enabled, `event-set`, `event-remove`,
  `event-timer-set` and `event-timer-remove` need the `event-admin` privilege, which is part of
  `data-admin`. A fresh node's bootstrap `admin` user has only `user-admin`; grant it `data-admin` (and
  `read-write-udf`, to register modules) and log in again, since a session's permissions refresh every few
  minutes. The listing commands need nothing.
- **`nsup-period`.** The `before-expire` and `before-evict` events fire from the namespace supervisor, so
  they run as often as it does. The default is two minutes; a test that wants to see an expiry lowers it
  with `set-config:context=namespace;id=<ns>;nsup-period=1` and restores it.

`xdr-merge` fires on the cluster that receives XDR writes. It needs that cluster's `xdr { src-id }` set
(`event-set` refuses the binding otherwise) and the source to ship its id: under bin convergence
(`conflict-resolve-writes true` with bin LUTs shipped) it already does, and otherwise `ship-src-id true`
in the source's `xdr` stanza, a dynamic setting, sends it with every record. A binding with
`trigger=conflict` also needs bin convergence. `before-ship` fires only on a node that ships to a DC.
Nothing else in the catalogue needs any configuration beyond the binding.

## The container used in this repository

`_temp/asd-image-wasm/aerospike.conf` is the configuration the tests run against: an Enterprise build from
the `wasm` branch with the stanza above, `wamrc` and its LLVM beside the binary, a `test` namespace and a
strong-consistency `testsc` one. `aerospike-security.conf` beside it is the same node with `security {}`
on and the ports moved to 3100, for the privilege tests.

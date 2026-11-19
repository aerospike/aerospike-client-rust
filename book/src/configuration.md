# Configuration

Everything the engine needs sits in one stanza of `aerospike.conf`, and everything the `xdr-merge` event
needs is runtime state kept in SMD, so there is less here than the size of the feature suggests.

## The `mod-wamr` stanza

```text
mod-wamr {
    max-memory 67108864
    max-total-memory 4294967296
    stream-timeout-ms 10000
    max-modules 256
    cache-enabled true
}
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `max-memory` | 64 MiB | The ceiling on one instance's linear memory, rounded up to wasm pages, at most 2 GiB. A module that grows past it is refused the growth and traps or reports. It is also the ceiling on what one call may build on the server's side through the host calls: strings, blobs, lists and maps. |
| `max-total-memory` | 4 GiB | The ceiling on the linear memory of every instance on the node together, and on what their calls have built on the server's side. At the ceiling a new instance is refused, and its call fails with an error naming this setting; a `memory.grow` is refused, which the guest sees as an ordinary out-of-memory. |
| `stream-timeout-ms` | `10000` | How long one batch of a stream UDF, or its final call, may run, 1 ms to 1 hour. An aggregation query has no deadline of its own, so this is the only clock a stream UDF is on. Past it the guest is stopped and the query fails. The time covers reading the batch's records as well as the guest's work on them. |
| `max-modules` | `256` | How many WASM modules may be registered, 1 to 256. A registration past it is refused with an error naming this setting. Registering a name that exists replaces it and is always allowed. |
| `cache-enabled` | `true` | Whether loaded modules are kept between calls. `false` loads the module again on every call, which is for working on the runtime itself. |

All five are read once, at start. There is nothing to set for the compiler: it is part of the server.

The UDF directory is not set here: the server keeps one directory of registered UDF files, named by the
`mod-lua` stanza's `user-path`, and `mod-wamr` reads the same one.

WASM UDFs themselves need no feature key. The `xdr-merge` event does: `asdb-event-system true` in the
Enterprise feature key file (`feature-key-file`, or `FEATURE_KEY_FILE` in the container), alongside
`asdb-xdr` and the others. Without it the server starts, runs UDFs, and refuses every event binding; see
[the event's chapter](events/README.md). For now the check is commented out, since no issued key carries
it yet.

## The Fast JIT

A module runs as machine code compiled by WAMR's own Fast JIT, inside the server process. There is no
separate compiler to install, nothing to configure for it, and no compiled file on disk: the code lives
in the server's memory for as long as the module is loaded.

- **x86-64 only.** The Fast JIT's code generator exists for x86-64. A server built for aarch64 runs
  modules in WAMR's interpreter instead, at about the speed of Lua; that build is for development.
- **No SIMD.** The Fast JIT does not implement `simd128`, and a module that uses it is refused at
  registration.
- **One code cache, 256 MB.** Every function the Fast JIT compiles goes into one cache of that size,
  fixed when the server is built. A module that is removed gives its share back.

**Nothing in the stanza selects it.** Which tier a server runs is decided when the server is built, by
its architecture, and is not a setting: there is no `running-mode` to write, and the stanza does not
accept one. To see which tier a node is on:

- the startup log has one line, `wamr: runtime ready, mode fast-jit, ...`;
- `udf-stats` prints `tier=fast-jit` for every module the node has loaded.

The runtime is WAMR at a pinned commit of upstream `main` (the 2.4 line).

## What the node reports

| Info command | What it answers |
| --- | --- |
| `statistics` | Among the node's statistics, the engine's counters, each prefixed `udf_wasm_`: `calls`, `traps`, `timeouts`, `errors`, `stream_batches`, `stream_timeouts`, `stream_abandoned`, `instantiate_failures`, `memory_refusals`, `host_refusals` (a host call refused because the value it would build is over the call's budget or the node's), `registrations_refused`, `log_lines_dropped`, and the gauges `guest_memory_bytes` and `modules_loaded`. (`compiles`, `compile_failures`, `cache_hits`, `cache_rejects` and `compile_queue` are also printed; they count ahead-of-time compilation, which this server does not do, and stay at zero.) |
| `udf-stats` | One entry per module this node has loaded: `filename=<name>,type=WASM,tier=<tier>,calls=<n>,traps=<n>,timeouts=<n>,errors=<n>,log_lines_dropped=<n>;`. |

Every registration, removal and event binding change is written to the server log as a line beginning
`audit:`, with the client's address and the module's SHA-256, and to the security audit trail when
security is configured. [Security](security.md) has the detail.

## What a running module costs

- A record UDF runs under the transaction's deadline: the engine's watchdog stops a runaway module when the
  transaction times out.
- A stream UDF has no deadline from the query, so a stream module that loops will hold a query thread.
- Each call has a table of 256 handles for cursors and built collections, and takes at most 128 arguments; a call with more fails.
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
newer SDK than the server fails at its first call with `failed to call unlinked import function`.
`udf-remove` removes the module, and is refused while an event binding refers to it.

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

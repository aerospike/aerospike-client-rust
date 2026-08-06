# WASM UDFs — design

Adding `WASM` as a second UDF language beside `LUA`, run on the server by **wasmtime**.

Everything below was read out of the code rather than assumed; file and line references are to
`_temp/aerospike-server` (server, C) and `aerospike-core` (client, Rust).

---

## What the code already gives us

**The server already has a language abstraction.** `as_module_hooks`
(`modules/common/src/include/aerospike/as_module.h`) is a table of five function pointers —
`destroy`, `update`, `validate`, `apply_record`, `apply_stream` — and `mod_lua` is one
implementation of it. A second language is a second implementation plus a dispatch decision. This
is the single fact that makes the whole feature tractable.

**Registration already carries the language.** `udf-put` parses `udf-type` (`udf_cask.c:311`),
validates it against a one-element whitelist (`is_valid_udf_type`, `udf_cask.c:90`), and stores it
in the SMD record as JSON: `{"content64": …, "type": "LUA", "name": …}` (`udf_cask.c:397-399`). So
the language is already persisted and replicated per file. Nothing new is needed on the wire.

**Calling does *not* carry the language.** `udf_def.type` (`as/include/transaction/udf.h:59`) is the
*operation* type — `AS_UDF_OP_KVS` and friends (`udf.c:338`) — not the language. The client sends
only filename, function and arglist. **So dispatch must come from the registered file, not from the
request.** This is the main structural consequence for the design.

**Content is already binary-safe.** The SMD accept callback base64-decodes and calls
`file_write(item->key, (uint8_t*)decoded, decoded_len)` (`udf_cask.c:554`) — a byte buffer with a
length, so a `.wasm` binary needs no encoding change. The client's `register_udf` already takes
`&[u8]`.

**Two places that will silently skip a `.wasm` file** and must be changed:

- `udf_cask.c:517` — when `unsafe_lua_disabled` is set, the accept callback ignores any item whose
  filename does not end in `.lua`.
- `mod_lua.c`'s `cache_scan_dir` / `cache_add_file` walk the UDF directory at startup; `mod_wasm`
  needs its own scan, and each module must claim only its own extension.

---

## Architecture

### 1. `mod_wasm`: a Rust static library behind a C shim

wasmtime is a Rust crate; the server is C compiled with `-Werror` and a gcc plugin. The split:

```
modules/mod-wasm/
  Makefile              # runs cargo, then bundles into libmod_wasm.a
  src/main/mod_wasm.c   # implements as_module_hooks; owns all as_* struct knowledge
  src/include/aerospike/mod_wasm.h
  rust/
    Cargo.toml          # crate-type = ["staticlib"]
    src/lib.rs          # wasmtime Engine/Module cache, instantiation, host imports
```

`as/src/Makefile` gains one line beside the existing `libmod_lua.a`:

```make
AS_LIBRARIES += $(MOD_WASM)/target/$(PLATFORM)/lib/libmod_wasm.a
```

**Why a C shim rather than implementing `as_module` in Rust.** The hooks traffic in `as_val*`,
`as_rec*`, `as_result*` — C structs whose behaviour lives in *their own* function-pointer hook
tables. Binding that faithfully in Rust means mirroring several struct layouts and trusting them to
stay in step with the C headers. The shim keeps every `as_*` type on the C side and hands Rust a
flat ABI of integers and byte buffers, so a header change breaks a C compile rather than corrupting
memory at run time.

**Risk to note:** the server compiles its C with a TSO gcc plugin (`-fplugin=tso/tso.so`) for the
aarch64 memory model. Rust code goes through LLVM and gets no such treatment. Our boundary is
call-in/call-out with no shared lock-free structures, so this should not bite — but it is a real
difference and worth being deliberate about rather than discovering later.

### 2. Dispatch: a filename → language registry

`udf_apply_record` (`udf.c:1088`) hardcodes `&mod_lua`. It becomes a lookup:

- `udf_cask` already sees the language for every file, in the SMD JSON it parses on accept. It
  populates a small registry (filename → `AS_UDF_LANG_LUA | AS_UDF_LANG_WASM`) under the same lock
  that guards the file write, so the registry and the file on disk change together.
- `udf_apply_record` asks the registry and passes the matching module.
- An unregistered filename keeps today's behaviour: the module's own "file not found" error.

`aggr.c:284` (`apply_stream`, for `query_aggregate`) gets the same treatment when stream UDFs are
supported — see phasing.

### 3. The guest contract

No WASI. A UDF has no business touching a filesystem, a clock or a socket, and leaving WASI out
removes that whole surface. Guests target `wasm32-unknown-unknown` and get only the imports below.

The guest exports:

| Export | Signature | Purpose |
|---|---|---|
| the UDF function, by name | `(args_ptr: i32, args_len: i32) -> i64` | packed `(ptr << 32) | len` of the msgpack result |
| `as_alloc` | `(len: i32) -> i32` | so the host can place the argument buffer in guest memory |
| `as_free` | `(ptr: i32, len: i32)` | released after the call |
| `memory` | — | the linear memory the host reads and writes |

### 4. Value marshalling: msgpack

Every value crossing the boundary — arguments, bin values, the return value — is **msgpack**.

The reason is that it is not new code: the server already converts `as_val` ↔ msgpack for CDTs and
expressions (`as_msgpack` / `as_serializer` in common), and Aerospike's own wire format for
collections *is* msgpack. So `as_list* arglist` → bytes and bytes → `as_val* result` are existing,
already-exercised conversions, and every `as_val` type — including `AS_LIST`, `AS_MAP`,
`AS_GEOJSON`, `AS_BYTES`, `AS_DOUBLE` — is representable without inventing an encoding or losing a
type.

The alternative, a family of typed accessors (`rec_get_int`, `rec_get_str`, …), is simpler for
scalars and gets ugly exactly where UDFs are most used: nested lists and maps.

### 5. Host imports

A direct mirror of the two hook tables the Lua binding exposes, so the two languages can do the
same things. Buffers are `(ptr, len)` pairs in guest memory; every call returns a status.

From `as_rec_hooks` (`as_rec.h:106-176`):

```
rec_get(name_ptr, name_len, out_ptr, out_cap) -> i32   // msgpack value, or needed size
rec_set(name_ptr, name_len, val_ptr, val_len) -> i32
rec_remove(name_ptr, name_len) -> i32
rec_ttl() -> i32                rec_gen() -> i32
rec_set_ttl(ttl) -> i32
rec_last_update_time() -> i64   rec_size() -> i32
rec_key(out_ptr, out_cap) -> i32
rec_setname(out_ptr, out_cap) -> i32
rec_numbins() -> i32
rec_bin_names(out_ptr, out_cap) -> i32
rec_digest(out_ptr, out_cap) -> i32
```

From `as_aerospike_hooks` (`as_aerospike.h:44-52`):

```
rec_create() -> i32   rec_update() -> i32
rec_remove_record() -> i32   rec_exists() -> i32
log(level, msg_ptr, msg_len)
get_current_time() -> i64
```

A `(ptr, cap)` too small returns the required size rather than truncating, so the guest retries with
a bigger buffer — the one shape that cannot silently lose data.

### 6. Limits, because a UDF runs while holding a record lock

Non-negotiable for a JIT that runs user code inside a write transaction.

- **Deadline.** The Lua path already has one: `udf_timer_timedout` against `g_end_ns`, the
  transaction's own end time (`udf.c:1094`). The wasmtime equivalent is **epoch interruption** — a
  monotonic counter bumped by one background thread, checked cheaply in compiled code — because it
  measures the same thing the existing timeout does (wall clock against the transaction deadline).
  Fuel counts *instructions*, which is a different and less useful quantity here.
- **Memory.** A `StoreLimiter` capping linear memory growth, so a guest cannot balloon the
  server's heap.
- **No threads, no host imports beyond the list above.** A guest can compute and touch its record;
  nothing else.
- **Compilation is cached, instantiation is not.** `Engine` is global; `Module` is compiled once per
  file and cached, invalidated on `AS_MODULE_EVENT_FILE_ADD` / `FILE_REMOVE` exactly as
  `mod_lua`'s `g_lua_hash` is (`mod_lua.c:123`); a fresh `Store` and `Instance` per invocation, so
  one call cannot leak state into the next. `InstancePre` keeps that cheap.

### 7. Validation at registration

`as_module_validate` is already called on `udf-put` (`udf_cask.c:366`) and its failure is already
reported to the client as `error=compile_error` with file, line and a base64 message. For WASM,
validate means: the bytes are a valid module, it compiles, its imports are a subset of ours, and
`as_alloc`/`as_free`/`memory` are exported. A missing export is a registration-time error rather
than a mystery at call time.

### 8. Client changes — three lines and some prose

Exactly what you asked for: no API change beyond the language.

```rust
pub enum UDFLang { Lua, Wasm }        // query/udf.rs
// Display / Into<&str> gain  Wasm => "WASM"
```

`register_udf` already takes `&[u8]` and base64-encodes it, so passing `.wasm` bytes works
unchanged. `register_udf_from_file` reads bytes. `execute_udf` needs nothing. The doc comments that
say "Lua is the only supported scripting language" become accurate again.

### 9. Configuration

The UDF directory is shared: SMD owns one namespace of filenames and writes them all to
`mod_lua.user_path`, so `mod_wasm` reads from the same directory and claims `*.wasm`. Renaming that
config item is a user-visible change for no gain, so it stays.

A new block carries only what is genuinely WASM's:

```
mod_wasm {
    cache-enabled true
    max-memory 64M       # per invocation
}
```

---

## Task list

**Phase 0 — prerequisites.** No Rust in the VM (`cargo not found`); install via rustup and confirm
wasmtime builds for aarch64 Linux. Confirm a Rust staticlib links into `asd` alongside static
jemalloc without symbol trouble. *This phase is pure risk-retirement and should happen first.*

**Phase 1 — end-to-end plumbing, with a trivial guest.** `UDFLang::Wasm`; `is_valid_udf_type`
accepts `WASM`; the `.lua`-only skip extended; filename→language registry; dispatch in
`udf_apply_record`; `mod_wasm` skeleton whose `apply_record` ignores the record and returns a
constant. Proves registration, replication, dispatch and the link, with almost no surface.

**Phase 2 — the host ABI.** msgpack marshalling both ways; the `rec_*` and `aerospike_*` imports;
`as_result` population; error mapping to Aerospike result codes.

**Phase 3 — limits and caching.** Epoch deadline wired to the transaction end time; memory cap;
module cache with SMD invalidation; `InstancePre`.

**Phase 4 — a guest SDK and tests.** A small `aerospike-udf` guest crate so a UDF is ordinary Rust
rather than hand-written imports; `.wasm` fixtures checked in beside their source; client
integration tests mirroring the existing Lua ones.

**Phase 5 — stream UDFs, if wanted.** `apply_stream` for `query_aggregate`. A distinctly larger
surface (`as_stream`, `as_aggr`) and not needed for record UDFs, so it is deliberately last.

---

## Decisions I want confirmed before writing code

1. **msgpack for all values**, versus typed accessors per scalar type. I recommend msgpack — the
   conversions already exist and collections work for free.
2. **No WASI**, guests are `wasm32-unknown-unknown` with only our imports. Recommend no; adding it
   later is compatible, removing it would not be.
3. **Epoch interruption** for the deadline rather than fuel. Recommend epoch: it measures the same
   thing the existing Lua timeout does.
4. **Record UDFs in this pass, stream/aggregation later** (phase 5). Recommend yes.
5. **Ship a Rust guest SDK crate** so UDFs are written as ordinary Rust. Recommend yes, small.
6. **Share `mod_lua.user_path`** for the UDF directory rather than adding a second one. Recommend
   yes — SMD is one filename namespace.
7. **Where the server work lands**: `_temp/aerospike-server` is a clone with its own git. Do you
   want commits there, a patch series, or a branch pushed somewhere?

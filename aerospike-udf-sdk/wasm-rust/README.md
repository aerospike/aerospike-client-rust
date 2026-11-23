# aerospike-udf-sdk-wasm

Write Aerospike user-defined functions in Rust and run them as WebAssembly on the server's `mod-wamr` engine.

```rust
#![no_std]

use aerospike_udf_sdk_wasm::{export, Error, Record, Value};

export! {
    /// Move `amount` out of this record's balance, unless `dry_run`.
    fn withdraw(rec: &mut Record, amount: i64, dry_run: Option<bool>) -> Result<Value, Error> {
        let balance = rec.bin("balance")?.int()?;

        if balance < amount {
            return Err(Error::Custom("insufficient funds"));
        }

        if dry_run != Some(true) {
            rec.set("balance", balance - amount)?;
            rec.save()?;
        }

        Ok(Value::Int(balance - amount))
    }
}
```

Build it as a `cdylib` for `wasm32-unknown-unknown` and register the `.wasm` with `UdfLang::Wasm`. Read
`examples/ledger` first: a guarded withdrawal, a posting history trimmed on the host, a summary and an
aggregation, written to be read. `examples/udf` exercises the whole surface and is the fixture the Rust
client's tests pin it with.

## The ideas

**The record is a `Record`**, the function's first parameter: `rec: &Record` to read it, `rec: &mut Record`
to write it too, and left out by a function that does neither. Everything about the record is a method on it:
`rec.bin("x")` and the typed readers (`rec.int("x")`, `rec.map("x")`, each an `Option`), `rec.set("x", v)`
and `rec.remove("x")`, the metadata (`generation()`, `ttl()`, `key()`, `digest()`), and the two verbs that
change the server's record, `rec.save()` and `rec.delete()`. **A bin write is staged until `save()`**, which
creates the record when there is none. It is not an argument: a client does not pass it. A stream UDF's
`record` takes `&Record`; an event handler takes `EventRecord`s, which read the same way (the `ReadRecord`
trait is the common part).

**Every read is a `Value`**, the guest-side analogue of the client's: `rec.bin("x")` returns one, a collection
iterates them, a map lookup returns one, and a UDF returns one as its result. It is *borrowed*: a string is
its length and where it sits, fetched on request into a buffer you own (`as_str(&mut buf)`), a `Str<N>` of a
declared capacity, or — with the `alloc` feature — a `String`. A nested list or map is a `Coll` you iterate
(`iter()`, `entries()`), look up by key (`get("k")`, `get(7)`) or by index (`at(3)`), 128 values per import
call; a missing key or an index past the end is `Ok(None)`. Typed readers do not convert: `int` is not
`double`, `string` is not `geojson`, `blob` is not `string`, and `bytes` is the one reader of raw bytes
whatever their kind. A list or map you build is a `List` (`push`, `extend`) or a `Map` (`insert`, `extend`, `contains_key`).
Each owns its handle on the host and gives it back when dropped, as an iterator gives back its cursor; set or
nest one by reference (`rec.set("x", &list)`, `outer.push(&inner)`), since the host keeps its own reference,
and return one by value (`-> Result<List, Error>`, or `Ok(list.into())` into an `Output` when other branches
return other values); the macro drops it once the host has taken the result. A `List` or `Map` never becomes a
`Value` that outlives it, so nothing holds a handle past its use: what holds one is a live builder or a live
iterator, until it is dropped. A call is bounded by its memory on the server, not by a count of
handles.

**Every failure is an `Error`**, and a UDF is `fn name(params) -> Result<Value, Error>`: `Ok` is the result the
client receives, `Err` fails the call with the error's message, `Error::Custom` carries your own, and
`Error::status()` gives the host's status. A UDF that builds a string or blob returns it owned:
`Result<String, Error>`, `Result<Vec<u8>, Error>` (with `alloc`), `Str<N>`, `Bytes<N>`.

**Arguments are typed in the signature.** `export!` decodes `i64`, `f64`, `bool`, `&str`, `&[u8]`, `Str<N>`,
`Bytes<N>`, `String`, `Vec<u8>`, `Value`, `Coll`, `Option<T>` and a trailing `Args`, checks the count and the
kinds, and rejects a mismatched call before the body runs — `withdraw: argument 1 (amount) is not an int`,
`withdraw: argument 1 (amount) is missing`, or `... is 300 bytes, at most 64` for a string past its
parameter's capacity. Arguments count from 1, and the messages are word for word those of the Go, C, C++ and
Java SDKs. A call with more arguments than the function declares fails before any is decoded
(`withdraw takes at most 2 arguments, got 3`), and a call takes at most 128 arguments in all (`... takes at
most 128 arguments, got 129`); no argument is ever dropped.

**A module describes its exports.** Beside each UDF the macros export `__udf_sig_<name>`, which the server
serves as a string when a client executes it like any other function, without running a body:
`fn withdraw(amount: int, dry_run?: bool) -> value`, or `stream balances { record() end() }`. A client can
check a call against it, or generate typed bindings from it.

## What is unusual about it

**Nothing is msgpack, and nothing is deserialised.** The host is C, holds the record natively, and hands the
guest its data *by position*: every read is a 16-byte value, and a string or nested collection carries where
it sits in the stored bytes rather than a copy of them. A value read from the record can be returned, written
to a bin, or put into a collection you are building without the guest ever decoding it.

**The import call is the unit of cost, not the decode.** Collections cross 128 values per import, in both
directions: `Coll::iter` reads a window at a time, and `List::extend` / `Map::extend` write one — including
straight from a stored collection's iterator, `list.extend(stored.iter())`. Reading a 10,000-element list is
0.27 ms round trip.

**No allocator unless you ask for one.** Nothing here allocates by default; buffers are yours, and a module
built this way is a few tens of KB. The `alloc` feature adds `String` and `Vec<u8>` as parameter and return types and as
what `Text::to_string` / `Blob::to_vec` return; the `allocator` feature adds dlmalloc as the global allocator
so a UDF crate needs nothing else. The example module grew from 39 KB to 62 KB when it turned that on.

## Stream (aggregation) UDFs

A stream UDF is driven by the host one record at a time, and **one instance sees every record a query thread
reads** — so unlike a record UDF it can accumulate in ordinary `static mut`. `stream!` generates the two exports
the host looks for, `<name>_record` and `<name>_end`, with the same typed arguments as a record UDF.

`end` runs once **per query thread per node**, so a client receives a handful of partials and combines them
itself. Lua's aggregation looks seamless across nodes only because the Aerospike clients run its final reduce
for it; nothing does that for WASM.

## Events

An event handler is a function the server calls at a point in its own processing rather than at a
client's request. There is one event, `xdr-merge`, declared with `event!`: every incoming XDR write
to a record that exists on this node is handed, with the current record, to the handler bound to the
namespace or set, and the handler decides the final record.

```rust
event! {
    on xdr_merge
    fn merge_account(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let pair = Pair::of(current, incoming);
        let out = as_udf::map(4)?;
        if let Some(visits) = pair.int("visits")?.max() { out.insert("visits", visits)?; }
        Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
    }
}
```

`EventRecord` is a read-only record with its metadata (`ttl`, `source_id`, and per bin `bin_last_update_time`
and `bin_source_id` where the server keeps them) and bins read as `Option`s, since on either side a bin may
not exist. What the server knows about the merge is asked of the record it is true of:
`incoming.is_delete()`, `incoming.is_partial()`, `incoming.lost_bins()` (the bins that would lose under
bin convergence), `current.is_tombstone()`, and on either `uses_bin_convergence()`.
`Pair::of(current, incoming)` reads a bin from both
sides into a `Both<T>` with the merges a resolver reaches for: `max`, `min`, `sum`, `merge(f)`, `newer`,
`older`, `ordered()` (`sum` answers nothing when neither side has the bin, as `max` and `min` do, and
wraps), and its two members are
`current` and `incoming`. `Resolution` is `KeepLocal`, `TakeRemote`, `Policy`, `Delete` or
`Record { bins, ttl }`, where the bins are the whole record; a record equal to the current one writes
nothing.

Two rules, enforced by nothing but the two DCs disagreeing forever if they are broken. **Pure**: no clock
(`now()` fails in a handler), no other records, no record-level last update time or generation (the payload carries
none). **Convergent**: DC A merges (A's record, B's write) while DC B merges (B's record, A's write), and
the answers must match, in any delivery order and however many times a write is delivered. There are two
ways to keep it. The time-based way: `newer`/`older`/`ordered` order the sides by a bin's own last update time and then
source id, the way the server's bin convergence does, which is symmetric where "mine first" is not, and
is why `EventRecord::with_bins` carries a warning and `Pair::with_newer` is the one to use; `examples/resolver`
is this. The data-only way: versions and removal markers in the record itself, with
`Value::cmp_stored` (the stored bytes of two values compared) as the tie-break, so the merge is a function
of the data alone; `examples/counter` (a grow-only counter, the smallest possible), `examples/pncounter` (the same with a second
map, so it can go down), `examples/twopset` (a set with removals), `examples/lwwset` (a set with re-adds, on
a clock) and `examples/crdt` are this, and the book's CRDT tutorial walks them in that order. Its last step, `examples/registers`, is the first way done explicitly: a merge that reads `bin_last_update_time` and `bin_source_id` bin by bin, on a namespace with bin convergence, beside one bin it merges on the data. The server's dry run (`execute_udf` on package `__event__`,
function `xdr-merge`, arguments `[incoming bins, incoming metadata, other bins?, other metadata?]`) runs a handler against
a real record and reports `symmetric`, `idempotent` and, given the third record, `associative`.

The macro exports the handler and a signature `event xdr-merge/1 ...`, which `event-set` checks before
binding: the version is the contract this SDK was written against, and a server that does not serve it
refuses the binding rather than handing the handler a payload it would misread.

## The other languages

The same contract has a C header (`aerospike-udf-sdk-wasm-c`) and a Go package for TinyGo
(`aerospike-udf-sdk-wasm-go`), and each carries the vector top-k example written to the same algorithm as
`examples/vector` here, so the three languages can be compared on identical work.

## Versioning

This SDK is paired with the server's `mod-wamr` host module: they are two halves of one ABI. A module built
against a newer SDK than the server may ask for imports the server does not provide, which surfaces at the
first apply as `failed to call unlinked import function` rather than at registration — registration only loads
a module, it does not resolve imports.

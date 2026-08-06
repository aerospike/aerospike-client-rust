# aerospike-udf-sdk-native

Write Aerospike user-defined functions in Rust and run them as compiled server plugins.

```rust
use aerospike_udf_sdk_native::{module, udfs, Record, Result};

udfs! {
    /// Add `by` to bin "n" and return the new value.
    fn increment(rec: &Record, by: Option<i64>) -> Result<i64> {
        let next = rec.get_int("n")?.unwrap_or(0) + by.unwrap_or(1);

        rec.set_int("n", next)?;
        rec.write()?;                    // without this it returns a number and changes nothing

        Ok(next)
    }
}

module! {
    name: "mymodule",
    version: "1.0.0",
    functions: [increment],
}
```

Build it as a `cdylib`, then name it in the server's configuration:

```
mod-native {
    module mymodule {
        path /opt/aerospike/native/libmymodule.so
    }
}
```

There is **no client-side registration**, by design — see below. `examples/plugin` is a complete module
exercising the whole surface: records, typed arguments, collections, metadata, nested structures, failure,
and aggregation.

## The bargain

A native UDF runs *inside* the database — in the server's address space, with its privileges, on transaction
threads, holding a record lock. Nothing is serialized: a string bin is a pointer and a length into the
record's own memory, and a collection is walked in place. That is what makes it cost what a function written
inside the server costs.

The price is real and this SDK cannot remove it:

- **No sandbox.** A wild pointer corrupts the server, not a guest.
- **No interruption.** Native code cannot be trapped mid-execution the way a WebAssembly guest can, so the
  transaction deadline is *cooperative*: `ctx.deadline_exceeded()` is the only form it takes. A long loop must
  ask.
- **Concurrency is yours.** Many threads, the same function, at once.
- **Do not block.** No network, no files, no unbounded compute — a record lock is held throughout.

Which is why only an operator can install one. A client naming `udf-type=NATIVE` is refused, and so is
registering a Lua or WASM module whose name would collide with a configured native one.

## What the SDK does for you

- **Typed arguments, checked before your body runs.** `by: Option<i64>` is one tag comparison; a client
  sending a string gets a clean error rather than reaching code that has to check.
- **Borrows that cannot escape.** `Value`, `List`, `Map` and `Args` take their lifetime from the `&Record` or
  `&Ctx` they came from. `Record` and `Ctx` are neither `Send` nor `Sync`, which closes the one misuse that
  would actually corrupt a server: handing a record to another thread.
- **Panics become errors.** A panic crossing into C is undefined behaviour, so every generated entry point
  catches it. There is a test for this, because the failure mode if it were wrong is a dead server.

## Cost is per host call, not per byte

The thing that shows up in a profile is how many times you cross into the host:

| prefer | over | why |
|---|---|---|
| `rec.get_int("n")` | `rec.get("n")` then match | one call, one register, no `Value` |
| `list.sum_ints()`, `list.ints_into()` | `list.get(i)` in a loop | one call per 256 elements, not per element |
| `map.get_str("k")` | `map.for_each(...)` scanning | one hash lookup, no allocation |

This is not a micro-optimization. In the WebAssembly module — same lesson, different engine — an aggregation
that scanned map keys by index ran **1.48× slower than the equivalent Lua**, and the same aggregation
rewritten against the by-key and bulk calls came out level with it.

## Aggregation

Declare the accumulator with `partial!`, then implement `Aggregation`:

```rust
partial! {
    pub struct Stats { count: i64, sum: i64, bin: String }
}

impl Aggregation for Stats {
    const NAME: &'static CStr = c"stats";    // a CStr: the host reads it with strlen

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args.get(0).as_str().unwrap_or("v").to_string();
        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        if let Some(v) = rec.get_int(&self.bin)? { self.count += 1; self.sum += v; }
        Ok(Fold::Continue)                   // `Fold::Full` to be dumped and reset
    }

    fn merge(&mut self, other: &Self) { self.count += other.count; self.sum += other.sum; }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(1)?;
        map.put_str_f64("mean", self.sum as f64 / self.count as f64)?;   // runs on the client
        Ok(map.as_value())
    }
}
```

Three things make this cheap, and they are the point of the whole design:

1. **One indirect call per record.** `accumulate` is the entire per-record cost — no boxed value, no stream
   write, no intermediate representation.
2. **The host owns the state.** It allocates `size_of::<Self>()`, zeroed, so an accumulator of plain fields
   makes an aggregation over millions of records perform **zero allocations**.
3. **One accumulator per query thread**, kept across every partition slice that thread processes. Lua finalizes
   at the end of each slice and so returns hundreds of partials from one node; this returns one per thread — four
   for a 50-record query on the dev cluster.

### You never write serialization

`partial!` generates `AsPartial`, which is what crosses the wire: a map of field name to value, built and read
through the ordinary `Ctx` builders. So a **field's name is its wire key**, and the format lives in the code
rather than in a document beside it.

That is also what makes `finalize` free to return a mean. A partial is the *serialized accumulator*, not a
summary you design, so two partials combine by definition — that is `merge` — and `finalize` runs once, on the
client, over everything. The old trap of emitting a mean and discovering it cannot be added to another node's
mean does not exist here.

### Growing accumulators dump

An accumulator that *collects* rather than reduces never reaches the end of a billion records. Answer
`Fold::Full` and the host writes it out, resets it, re-runs `configure`, and carries on. Report `heap_used()` as
well, so an operator's `agg-memory-limit` can bound it independently of your own idea of full — note that limit is
a threshold crossed *after* taking a record, so a chunk can exceed it by one.

### The client runs the same code

Build the plugin as `crate-type = ["cdylib", "lib"]`, enable this crate's `client` feature, and combine the
server's partials with the plugin's own `merge` and `finalize`:

```rust
let mut combiner = Combiner::<Stats>::new(&args)?;

for partial in query_results { combiner.absorb(&partial)?; }

let answer = combiner.finish()?;         // Stats::finalize, once, on everything
```

`finalize` builds through `Ctx` and there is no server here — so the SDK ships a **local host**: the same
`as_native_host` vtable implemented in Rust over ordinary Rust collections. Your code cannot tell the difference,
which is the whole reason the host is a vtable. For a *collecting* aggregation use `combiner.read(&partial)?`
instead, one chunk at a time; absorbing them all would rebuild in memory exactly what the dumping avoided.

## Versioning

This SDK is one half of an ABI whose other half is the server's `mod-native`. They are matched by
`AS_NATIVE_ABI_VERSION`: a plugin built against a version the server does not speak is **refused at startup**,
with the module named in the log, rather than loading and behaving unpredictably.

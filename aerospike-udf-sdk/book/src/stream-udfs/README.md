# Stream UDFs

A stream UDF is driven by the server over the records a query selects, one record at a time, and answers
once at the end. It is the aggregation function of Lua's `aggregate` queries, with one difference that
changes how it is written and how it is consumed.

## Two entry points, one instance

A stream UDF named `topk` is two exports: `topk_record`, called once per record the query reaches, and
`topk_end`, called once when a stream is done. Between the two, state lives in the module's own memory:
`static mut` in Rust, package variables in Go, statics in C. The record function gets the call's arguments
on every call, so a stream configures itself on the first record; the end function gets them too.

**One instance sees every record a query thread reads.** The server drives a query as one stream per
partition, but keeps a module's instance in a thread-local slot across those slices and finalises it,
one call of `_end`, when the thread's last slice is done. A query over a node is four instances and four
partials, not four thousand; that is what took the vector top-k from six times slower than a native plugin
to level with it.

## Partials, not a reduce

Lua's aggregation looks seamless across nodes because the clients run its final `reduce` for it. Nothing
does that for WASM: **a client receives one partial per query thread per node and combines them itself.**
A stream UDF's end function therefore returns a partial that is cheap to merge, a map with the state the
client needs, and the client merges the handful it receives. The vector example returns
`{hits: [...], skipped_dim: n, skipped_none: n}` and the test merges the hit lists.

{{#include ../diagrams/stream-partials.svg}}

A record function that fails ends the stream with its message; a stream that ends short is reported, not
silently completed.

## Reading records

Inside `_record` the record natives work exactly as in a record UDF: `bin`, cursors, metadata, the digest.
Writing is not available: a stream reads. The arguments are there on every call, by position, so a stream
can read a query vector from them on each record without copying it.

## Calling from a client

```rust
let mut statement = Statement::new(namespace, set, Bins::All);
statement.set_aggregate_function("vector", "topk", &[Value::from("vec"), query_bytes, Value::from("cosine"), Value::from(10)]);

let records = client.query(&QueryPolicy::default(), PartitionFilter::all(), statement).await?;
let mut stream = records.into_stream();

while let Some(record) = stream.next().await {
    // one record per partial; the partial is the value under the single bin
}
```

## A deadline per batch

A record UDF is stopped by its transaction's timeout. A query gives an aggregation no deadline, so a
stream UDF is on a clock of its own: each batch of records, and the final call, may run for
`stream-timeout-ms` (10 seconds by default; see [Configuration](../configuration.md)). Past it the guest
is stopped and the query fails. The time covers reading the batch's records as well as the guest's work
on them. A stream is also stopped the moment its query is aborted. Write the end function to be bounded
by what it saw.

## SIMD

SIMD is opt-in in every SDK build, and off by default: `make SIMD=1` for C, C++ and Go,
`RUSTFLAGS="-C target-feature=+simd128"` for Rust. A module built with it is refused at registration on
an x86-64 node, whose Fast JIT has no SIMD, and on an aarch64 node the interpreter traps on the SIMD
instructions it does not implement. So build without it: the same source runs scalar on every server.
The vector kernels were written as four-lane sums with a fixed fold order for the ahead-of-time tier,
where they measured level with a native plugin in C and within 1.3x in Rust and Go; no default build
runs that tier now. The modules checked in under `tests/fixtures/wasm` are the scalar builds.

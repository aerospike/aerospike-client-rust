# aerospike-udf-sdk-wasm-java

Write Aerospike WASM UDFs in Java and compile them with [TeaVM](https://teavm.org), for the server's
`mod-wamr` engine. Package `com.aerospike.udf` is the Java face of the same ABI the Rust SDK
(`aerospike-udf-sdk-wasm`), the C header (`aerospike-udf-sdk-wasm-c`) and the Go package
(`aerospike-udf-sdk-wasm-go`) speak, and the Rust SDK's documentation is the reference for the contract.

```java
package com.example;

import com.aerospike.udf.Function;
import com.aerospike.udf.Record;

public final class Counter {
    @Function("increment")
    static long increment(Record rec, long by) {
        long n = rec.getInt("n", 0) + by;

        rec.set("n", n);
        rec.save();

        return n;
    }
}
```

`@Function` is the whole export: the SDK's annotation processor generates `CounterExports` beside the
class, with the TeaVM `@Export` method that decodes the arguments into the parameters and delivers the
return as the result, the `__udf_sig_increment` export (`fn increment(by: int) -> value`, derived from the
parameter types), and the `main` TeaVM wants. The `Record` is the record the call is about: a method that
takes one as its first parameter is handed it, and it is not an argument, so it appears neither in the
signature nor in the count of arguments. Everything a UDF does to the record is a method of it; `save()`
writes the staged bins, creating the record when there is none, and anything that fails throws
`UdfException`, which becomes the failure the client sees.
Build with Maven and the TeaVM plugin, naming the generated class as the entry point and the processor as
a `provided` dependency so that `javac` finds it, as `examples/*/pom.xml` do:

```xml
<dependencies>
  <dependency>
    <groupId>com.aerospike.udf</groupId>
    <artifactId>aerospike-udf-sdk-wasm-java</artifactId>
  </dependency>
  <dependency>
    <groupId>com.aerospike.udf</groupId>
    <artifactId>aerospike-udf-sdk-wasm-java-processor</artifactId>
    <scope>provided</scope>
  </dependency>
</dependencies>

<plugin>
  <groupId>org.teavm</groupId>
  <artifactId>teavm-maven-plugin</artifactId>
  <version>0.13.1</version>
  <executions><execution>
    <goals><goal>compile</goal></goals>
    <configuration>
      <mainClass>com.example.CounterExports</mainClass>
      <targetType>WEBASSEMBLY</targetType>
      <optimizationLevel>FULL</optimizationLevel>
      <targetFileName>counter.wasm</targetFileName>
    </configuration>
  </execution></executions>
</plugin>
```

and register the `.wasm` with `UdfLang::Wasm`. `make` builds the examples; `make fixtures FIXTURES=<dir>`
copies them to `<dir>` as `*_java.wasm`.

## The annotations

The three annotations are the Java face of the Rust SDK's `export!`, `stream!` and `event!`; the
processor (`processor/`, artifact `aerospike-udf-sdk-wasm-java-processor`) turns them into the exports
at compile time, inside `javac`, before TeaVM runs.

- **`@Function("name")`** on a method: a record UDF. A first parameter of type `Record` is the record of
  the call. The other parameters are the arguments, decoded by type (`long`/`int` → `int`, `double`,
  `boolean` → `bool`, `String` → `str`, `byte[]` → `bytes`, `Value` → `any`, `Value[]` last → `any...`;
  `@Nullable` for one that may be absent or nil, boxed when scalar), and the return is the result (`void`
  nil, `long`/`int`, `double`, `boolean`, `String`, `byte[]`, `Value`, `MapBuilder`, `ListBuilder`,
  `MapValue`, `ListValue`, and a `java.util.Map` or `java.util.List` of plain Java values). A missing,
  mismatched or surplus argument fails the call before the method runs, naming the argument, and so
  does a call with more than 128 arguments, `Value[]` or not: none is dropped. A `String` parameter
  replaces bytes that are not UTF-8; `byte[]` takes a string or blob argument's bytes untouched. Throw `UdfException` to fail the call with a message; any other exception fails it
  with its message.
- **`@Stream("name")`** on a class with a `record` method and an `end` method: a stream UDF, exported as
  `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`. The processor
  builds one instance, so the class's fields are the stream's state; `record` returns `void`, `end` the
  partial. `record` may take the `Record` first, as a function does; a stream's records are read-only,
  and a write to one throws. `end` runs after the last record and takes none.
- **`@On.XdrMerge("name")`** on a method: an event handler. The server has one event kind, `xdr-merge`,
  the merge of an incoming XDR write with the current record. The parameters are what it hands over,
  `(EventRecord current, EventRecord incoming)`, the record as it is on this cluster and the one that
  arrived over XDR, and the return is its answer, a `Resolution`: `keepLocal()`, `takeRemote()`,
  `policy()`, `delete()` or `record(bins[, ttl])`. A handler with other parameters or another return is a
  compile error that states this shape.
- **Instance methods.** An exported method may be an instance method; the generated class then builds one
  instance of the module's class (a no-argument constructor it can reach) and calls through it, so state
  is ordinary fields.
- **The raw form.** A method declared `(int args, int argc)` and returning `int` is exported as it is,
  under any of the three, and decodes its arguments itself with `LowLevel.args`; the annotation's `params` (and
  `endParams` for a stream) gives the signature's parameter text. This is for a hot path that reads its
  arguments once and keeps them, as `examples/vector` does.

The generated class is `<Class>Exports` in the same package: the `@Export` methods, one `__udf_sig_<name>`
per export, and `main`. Name it as the TeaVM `mainClass`; the annotated class needs no `main` of its own.

## Shape of the API

- **`Record`**: the record a call is about, and everything a UDF does to it. Bins are addressed by name.
  `get(name)` gives a bin as a `Value`, or null, `bin(name)` gives it or throws, and `has(name)` says
  whether it is there. The typed readers (`getInt`, `getDouble`, `getBool`, `getString`, `getBlob`,
  `getList`, `getMap`) have three outcomes: null for an absent bin, the value for a bin of that kind,
  and `UdfException` with the status `LowLevel.TYPE_MISMATCH` for a bin of another kind; nothing is
  widened, and `getBlob` takes a blob only, `getBytes` the raw bytes of a string, blob, GeoJSON or HLL.
  Given a default (`getInt("n", 0)`) a reader answers it for an absent bin and still throws for another
  kind; `bin(name).asInt()` is the read that insists. `getFloats(name, float[])` reads a vector. `set(name, value)` stages a bin of any kind (a scalar,
  a string, a blob, a `Value`, a builder, a stored list or map, a `java.util.List` or `Map`),
  `setGeoJson` a GeoJSON one, `remove(name)` stages a removal, and `save()` writes what is staged,
  creating the record when it does not exist. `setInts(long[])` and `setDoubles(double[])` write a packed
  list in one call, at once. `delete()` deletes the record. The metadata is here too: `exists`,
  `generation`, `ttl` / `setTtl`, `lastUpdateTime`, `size`, `binCount`, `setName`, `key`, `digest`.
  There is one `Record` type: a write where the record is read-only (a stream's records) throws.
- **`RecordReader`**: the readers `Record` and `EventRecord` share, so a helper that only reads bins takes
  either.
- **`Value`**: one value, read-only: `kind()`, the `is*` tests (`isGeoJson()` and `isHll()` among them),
  `asInt()`, `asDouble()`, `asBool()`, `asString()`, `asBlob()` (a blob only), `asBytes()` (the raw bytes
  of a string, blob, GeoJSON or HLL), `asList()`, `asMap()`, `toObject()`, `cmpStored(other)` (-1, 0 or
  1, and a throw for values it cannot compare); `Value.ofInt`, `ofString`, `ofGeoJson`
  and the rest make one of the guest's own.
- **`ListValue` and `MapValue`**: a stored list or map, read where it is. A list has `size()`,
  `isEmpty()`, `get(index)` and the typed `getInt(index)` and so on (null for a nil element; an index
  out of range throws, as on a `java.util.List`), `contains`, and is an `Iterable<Value>`; a map has
  `get(key)` (null for no such key), `containsKey`, the typed `getInt(key)` (null for no such key or a
  nil entry) / `getInt(key, dflt)` and so on for string and integer keys of any length, and is an
  `Iterable<MapValue.Entry>`. Every element a loop yields is a fresh `Value`, safe to keep, and a loop
  may be left early. A wrong kind throws `UdfException` naming the key or the index. `asValue()` hands
  the collection on; a view of nothing (a side's deleted bins when it deleted none) is an empty one, one
  empty list and one empty map per call however many views ask. `toList()` and
  `toMap()` copy the whole collection out as `java.util` ones.
- **`ListBuilder` and `MapBuilder`**: a list or map being built on the server, started with `Udf.list()`
  and `Udf.map()`: chained `add` / `put` of every kind, string and integer keys, `addNil` / `putNil`,
  `addGeoJson` / `putGeoJson`, `addAll` / `putAll` from a stored collection (by position) or from a
  `java.util` one, `MapBuilder.containsKey`; written to a bin, returned as the result, nested, or handed
  back from a handler. They are builders, not collections: what went in is not read back. An `add` or a
  `put` takes no handle, so a loop of any length builds until the server refuses (`LowLevel.REFUSED`).
  Each builder holds a handle until `close()` gives it back (they are `AutoCloseable`, so
  try-with-resources does), or the call ends: close one once it has been set, added, put or returned,
  since the host keeps its own reference to what it is handed. The one exception is the map given to
  `Resolution.record`, which is read after the handler returns. A call is bounded by its memory on the
  server, not by a count of handles, so a loop that builds and closes a collection per turn runs as long
  as that memory lasts.
- **`Udf`**: what is not about the record: `now()`, `log`, the builder factories, and for a function in
  the raw form the `result*` calls and `fail`.
- **Events**: `EventRecord` for each side of a merge (the same readers as `Record`, plus `binNames()`,
  `deletedBins()`, `binLastUpdateTime` / `binSourceId`, and `withBins(out)` to keep the rest of a record
  under a few decided bins; and what the merge says of one side: `isDelete()` and `isPartial()` of
  the incoming write, `isTombstone()` of the current record, `usesBinConvergence()` of
  either), `Resolution` for the answer, and the merge itself through `Pair.of(current, incoming)` (`Pair`
  and `Both` with `current`, `incoming`, `newer`, `older`, `ordered`, `max`, `min`, `merge`, `sumInt`
  (null when neither side has the bin, as `max` and `min`), `withNewer`, which leaves out a bin the later side deleted). A handler in the raw form reads the two records with `Pair.decode(args, argc)` and
  returns a resolution's `deliver()`. `examples/resolver` is the worked example, held to the same
  behaviour as the other languages' by the client tests.
- **`LowLevel`**: the server's calls one at a time, for a hot path that must allocate nothing: names as
  `byte[]`, `bin(name, value)` into a `Value` the caller owns and reuses, `enter` / `next` / `close` over
  a caller-owned window, `newMap` and the `mapPut*` / `listPush*` calls over `int` handles, the record's
  key and digest into caller buffers, and status codes instead of exceptions. Nothing in it is needed to
  write a UDF; the vector example is written on it because a scan runs its `record` once per record.
  `Params` holds what the generated exports call.

### Names are encoded once

A bin name crosses to the server as UTF-8 bytes. `Record` (and the map classes, for keys) keeps the
encoded form of the names it has seen, found by the string itself, so `rec.getInt("ticks", 0)` on a string
literal allocates nothing and needs no constant declared for the name. The last eight names used are
matched by identity, which is the whole cost for a literal; a module that rotates through more than eight
in one call pays a hash lookup for the others. `tests/src/udf_java.rs` has the measurement
(`java_record_bench`).

### Leaving a loop early

A for-each over a `ListValue` or a `MapValue` holds no cursor while the loop runs: the iterator reads the
position of every element from the server, 128 values per call, and gives the server's cursor back before
it hands out the first one. So `break`, `return` and an exception hold nothing, whatever the collection's
length. The positions are 16-byte values in a Java array, 16 bytes per element and 32 per map entry (16 KB
for a list of 1000), kept as long as the iterator is; the elements' data is not copied. `forEach` on a
`ListValue` or `MapValue` reads a window at a time instead (2 KB whatever the length) and gives the cursor
back however it ends, an exception included, and so does every walk the SDK makes for itself (`contains`,
`withBins`, `withNewer`, `putAll`, `addAll`, the `java.util` conversions). What holds a handle in a call is
what the code opened and has not closed: a `ListBuilder` or `MapBuilder` until `close`, a cursor from
`LowLevel.enter` until `LowLevel.close`.

## Which TeaVM, and why

**TeaVM 0.13.1, the `WEBASSEMBLY` target: the linear-memory backend.** It is the last release that has
it: 0.14 removed it, leaving the WebAssembly GC backend, whose modules need the exception-handling
proposal (which the server's runtime, WAMR, supports only in its classic interpreter, not ahead of time),
the JavaScript string builtins and an imported memory. The linear-memory backend emits what every other
SDK emits: one exported memory, a start function, plain imports, and functions that compile ahead of time
to native code. The version is therefore pinned, and the parent `pom.xml` says so. `teavm-core` is named
at compile scope on purpose: the backend's runtime classes live there and the class library lists it at
runtime scope only, which the plugin does not see.

The module asks its host for four `teavm` functions (its own logging and clock) and, when `Math` is used,
for `teavmMath` functions; `mod-wamr` provides both. Everything else a Java UDF does goes through the
`aerospike` imports like any other guest.

TeaVM collects `@Export` methods only from classes it reaches from the entry point, which is why the
generated class, and not the annotated one, is the `mainClass`: its exports reach the annotated methods.

## The one rule of this backend

The address of an array is valid only until the next allocation: the collector may move it, and this
backend has no pinning. So the SDK takes `Address.ofData(buf)` inside the expression that hands it to the
host, and never keeps one. Code that calls the natives directly must do the same.

## Exceptions

A generated export catches what the method throws and fails the call with the message; `UdfException`
is the one to throw on purpose, and the one the SDK throws: for a bin, key or element of another kind, a
required one that is absent, and a write the record refuses. It carries the host's status, `status()`:
`LowLevel.NOT_FOUND`, `TYPE_MISMATCH`, `REFUSED`, a too-small status at or below `NEEDED_BASE`, or `ERR`,
which is also the status of one made with `new UdfException(message)`. In the raw form an uncaught exception ends
the call the same way, and `Udf.fail(message)` is the status-code way to say why.

## The examples

- `examples/hello`: the bare contract without the SDK, raw `@Import`s, `@Export`s and `Address`; what
  `tests/src/udf_java.rs` runs first.
- `examples/vector`: the exact top-k over packed float vectors that the Rust, C and Go SDKs also carry,
  digest for digest and bit for bit, a `@Stream` class in the raw form on `LowLevel`;
  `tests/src/udf_vector.rs` holds the five engines (Lua included) to one answer and times them.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge`; read it first. The client
  test `generated_exports_decode_arguments` drives its generated exports through every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map;
  the `Table` is a class with one static instance reused for both bins.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two sorted lists merged by union) and a set with re-adds (two maps of `Udf.now()` stamps merged
  by the later stamp); both return a `ListBuilder` of members from `@Function` methods.
- `examples/registers`: a record whose bins are last-writer-wins registers stamped by the server, on a
  namespace with bin convergence; the handler reads `binLastUpdateTime` and `binSourceId` per bin, keeps
  a delete that is the later write, and merges the one bin that is not a register (`seen`) by the larger
  number.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held to
  the same nine dry-run scenarios by `tests/src/events.rs`; written on `ListValue`, `MapValue` and the
  builders, with plain Java collections for its own bookkeeping.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API (walks, typed readers,
  every write, integer keys, GeoJSON, metadata, TTL, save, remove, deletion, a list by index, the
  `java.util` bridge, loops left early), which `tests/src/udf_java.rs` holds to the Rust fixture's
  expectations. Not an example to copy.

## Building in a container

The toolchain (JDK 21, Maven, TeaVM from Maven Central) can live in a container, `asd-build-wasm` here.
From this directory:

```sh
podman cp . asd-build-wasm:/work/java/sdk
podman exec asd-build-wasm bash -c 'cd /work/java/sdk && mvn -q -B package'
podman cp asd-build-wasm:/work/java/sdk/examples/vector/target/wasm/vector_java.wasm .
```

The generated sources land in each example's `target/generated-sources/annotations/`, which is where to
look when a module does not export what you expect.

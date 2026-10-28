# aerospike-udf-sdk-wasm-java

Write Aerospike WASM UDFs in Java and compile them with [TeaVM](https://teavm.org), for the server's
`mod-wamr` engine. Package `com.aerospike.udf` is the Java face of the same ABI the Rust SDK
(`aerospike-udf-sdk-wasm`), the C header (`aerospike-udf-sdk-wasm-c`) and the Go package
(`aerospike-udf-sdk-wasm-go`) speak, and the Rust SDK's documentation is the reference for the contract.

```java
package com.example;

import com.aerospike.udf.Bin;
import com.aerospike.udf.Function;
import com.aerospike.udf.Udf;

public final class Counter {
    private static final Bin N = Bin.of("n");

    @Function("increment")
    static long increment(long by) {
        long n = N.getLong(0) + by;

        N.set(n);
        Udf.commit();

        return n;
    }
}
```

`@Function` is the whole export: the SDK's annotation processor generates `CounterExports` beside the
class, with the TeaVM `@Export` method that decodes the arguments into the parameters and delivers the
return as the result, the `__udf_sig_increment` export (`fn increment(by: int) -> value`, derived from the
parameter types), and the `main` TeaVM wants. `Bin` is a bin of the record by name, `Udf.commit()` writes
the staged bins, and anything that fails throws `UdfException`, which becomes the failure the client sees.
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

and register the `.wasm` with `UdfLang::Wasm`. `make` builds the examples; `make fixtures` copies them to
`tests/fixtures/wasm/` as `*_java.wasm`.

## The annotations

The three annotations are the Java face of the Rust SDK's `export!`, `stream!` and `event!`; the
processor (`processor/`, artifact `aerospike-udf-sdk-wasm-java-processor`) turns them into the exports
at compile time, inside `javac`, before TeaVM runs.

- **`@Function("name")`** on a method: a record UDF. The parameters are the arguments, decoded by type
  (`long`/`int` → `int`, `double`, `boolean` → `bool`, `String` → `str`, `byte[]` → `bytes`, `Value` →
  `any`, `Value[]` last → `any...`; `@Nullable` for one that may be absent or nil, boxed when scalar), and
  the return is the result (`void` nil, `long`/`int`, `double`, `boolean`, `String`, `byte[]`, `Value`,
  `UdfMap`, `UdfList`). A missing, mismatched or surplus argument fails the call before the method runs,
  naming the argument. Throw `UdfException` to fail the call with a message; any other exception fails it
  with its message.
- **`@Stream("name")`** on a class with a `record` method and an `end` method: a stream UDF, exported as
  `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`. The processor
  builds one instance, so the class's fields are the stream's state; `record` returns `void`, `end` the
  partial.
- **`@On.<Kind>("name")`** on a method: an event handler, one annotation per kind (`@On.BeforeWrite`,
  `@On.BeforeDelete`, ..., `@On.Timer`, `@On.XdrMerge`), whose parameters are the records and the info the
  kind hands over and whose return is the kind's answer: `Decision` for a write (`accept()`, `reject(code,
  message)`, `record(bins)`), `Gate` for a delete or truncate, `Expiry`, `Eviction`, `ReadResult`,
  `ShipDecision`, `Resolution` for the merge (`keepLocal()`, `takeRemote()`, `policy()`, `record(bins)`),
  and `void` for the after- and once-per-node kinds and timers. A handler with other parameters or another
  return is a compile error.
- **Instance methods.** An exported method may be an instance method; the generated class then builds one
  instance of the module's class (a no-argument constructor it can reach) and calls through it, so state
  is ordinary fields.
- **The raw form.** A method declared `(int args, int argc)` and returning `int` is exported as it is,
  under any of the three, and decodes its arguments itself with `Udf.args`; the annotation's `params` (and
  `endParams` for a stream) gives the signature's parameter text. This is for a hot path that reads its
  arguments once and keeps them, as `examples/vector` does.

The generated class is `<Class>Exports` in the same package: the `@Export` methods, one `__udf_sig_<name>`
per export, and `main`. Name it as the TeaVM `mainClass`; the annotated class needs no `main` of its own.

## Shape of the API

- **`Bin`**: a bin of the record by name, made once (`Bin.of("n")`) and kept as a constant. Typed getters
  (`getLong()`, `getDouble()`, `getBoolean()`, `getString()`, `getBytes()`, `getFloats(float[])`, each
  throwing when the bin is absent or of another kind unless given a default), `get()` as a `Value` or null,
  `exists()`, `asList()` / `asMap()` as a cursor; setters of every kind (`set(long)` ... `set(UdfMap)`,
  `setGeoJson`, `setNull`, `setInts(long[])`, `setDoubles(double[])`, `remove()`), staged until
  `Udf.commit()`. `Udf.delete()` deletes the record, `Udf.setTtl` sets its time to live.
- **`Value`**: the 16-byte value every read is, with `asLong()`, `asDouble()`, `asBoolean()`, `asString()`,
  `asBytes()`, the `is*` tests, and `asList()` / `asMap()` opening a **`Cursor`** over a stored collection:
  `for (Value v : bin.asList())` and `for (var e : bin.asMap())` with `e.key` and `e.value`, a fresh
  element each; or `while (c.next())` over the reused `c.value` and `c.key`, allocation-free. It closes
  itself at the end and is `AutoCloseable` for an early exit.
- **`UdfMap` and `UdfList`**: collections built on the host, with chainable `put` / `add` of every kind,
  string and integer keys, `putNull` / `addNull`, `putGeoJson` / `addGeoJson`, `putAll` / `addAll` from a
  stored collection by position or from a `Value[]` in one import; written to a bin, returned as the
  result, nested, or handed back from a handler.
- **Metadata**: `Udf.generation`, `ttl`, `lastUpdateTime`, `recordSize`, `binCount`, `setName`, `digest`,
  the user key, `exists`, `now`; `Udf.log` writes to the server log.
- **Events**: `EventRecord` for a record the event hands over (typed readers that answer null for absent,
  `list` / `map` cursors, `withBins(out)` to keep the rest of a record under a few decided bins), the
  `Events.*Info` classes for the kind's own entry, the answer types; the merge through `Conflict`
  (`pair()` into `Pair` and `Both` with `newer`, `older`, `max`, `min`, `merge`, `withNewer`;
  `involvesDelete`, `lost`, ...). `examples/guard` and `examples/resolver` are the worked examples, held to
  the same behaviour as the other languages' by the client tests.
- **The low-level tier**, under all of that and mirroring the natives one call at a time: `Udf.bin(name,
  value)` into a reused `Value`, `Udf.enter` / `next` / `close` over a caller-owned window, `Udf.newMap`
  and the `mapPut*` / `listPush*` calls over `int` handles, the `set*` and `result*` calls, `Udf.fail`,
  every name as a `byte[]` twin, and status codes instead of exceptions. It allocates nothing; the vector
  example is written on it. `UdfMap.wrap` / `UdfList.wrap` bridge a handle to the objects, and `Params`
  holds the decoders the generated exports call.

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
is the one to throw on purpose, and the one the SDK throws. In the raw form an uncaught exception ends
the call the same way, and `Udf.fail(message)` is the status-code way to say why.

## The examples

- `examples/hello`: the bare contract without the SDK, raw `@Import`s, `@Export`s and `Address`; what
  `tests/src/udf_java.rs` runs first.
- `examples/vector`: the exact top-k over packed float vectors that the Rust, C and Go SDKs also carry,
  digest for digest and bit for bit, a `@Stream` class in the raw form on the low-level tier;
  `tests/src/udf_vector.rs` holds the five engines (Lua included) to one answer and times them.
- `examples/guard`: before-write, before-delete, before-truncate and timer handlers.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge`; read it first. The client
  test `generated_exports_decode_arguments` drives its generated exports through every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map;
  the `Table` is a class with one static instance reused for both bins.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two sorted lists merged by union) and a set with re-adds (two maps of `Udf.now()` stamps merged
  by the later stamp); both return a `UdfList` of members from `@Function` methods.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held to
  the same nine dry-run scenarios by `tests/src/events.rs`; written on the low-level tier.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API (cursors, typed readers,
  every write, integer keys, GeoJSON, metadata, TTL, deletion), which `java_sdk_covers_the_rust_surface`
  in `tests/src/udf_java.rs` holds to the Rust fixture's expectations. Not an example to copy.

## Building in this repository

The toolchain (JDK 21, Maven, TeaVM from Maven Central) lives in the `asd-build-wasm` container, which
sees only the server trees:

```sh
podman cp aerospike-udf-sdk-wasm-java asd-build-wasm:/work/java/sdk
podman exec asd-build-wasm bash -c 'cd /work/java/sdk && mvn -q -B package'
podman cp asd-build-wasm:/work/java/sdk/examples/vector/target/wasm/vector_java.wasm tests/fixtures/wasm/
```

The generated sources land in each example's `target/generated-sources/annotations/`, which is where to
look when a module does not export what you expect.

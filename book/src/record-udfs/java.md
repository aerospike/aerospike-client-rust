# Java

The Java SDK, `aerospike-udf-sdk-wasm-java`, is a Maven library compiled by [TeaVM](https://teavm.org)
into a plain linear-memory WebAssembly module: one exported memory, a start function that runs the
runtime's initialisation, and functions that compile ahead of time like every other guest's. The module
imports the server's natives and four `teavm` functions (the runtime's own logging and clock) plus, when
`Math` is used, some `teavmMath` functions; the server provides all of them.

TeaVM is pinned at 0.13.1, the last release with that backend. Its successor keeps only a WebAssembly GC
backend, whose modules need the exception-handling proposal, which the server's runtime runs only in its
interpreter, and JavaScript string builtins, which nothing outside a browser provides. The SDK's README
spells this out.

## A module from nothing

```java
package com.example;

import com.aerospike.udf.Function;
import com.aerospike.udf.Record;

public final class Counter {
    @Function("increment")
    static long increment(Record rec, long by) {
        long n = rec.getLong("n", 0) + by;

        rec.set("n", n);
        rec.save();

        return n;
    }
}
```

That is the whole module. `@Function` is the Java face of the Rust SDK's `export!`: at compile time the
SDK's annotation processor generates a class beside this one, `CounterExports`, holding the exported
`increment(int args, int argc)` that decodes the call's arguments, calls the method and delivers its
return as the result; the `__udf_sig_increment` export, whose text `fn increment(by: int) -> value` it
derives from the parameter types; and the module's `main`, which TeaVM wants as an entry point. Nothing in
the generated class is yours to edit, and nothing in yours refers to it except the build.

`rec` is the record the call is about. `getLong("n", 0)` reads bin `n` as an integer, 0 when it is
absent. `set` stages a write and `save()` writes what is staged, creating the record if it does not
exist. Every call that can fail throws `UdfException`, which the generated export turns into the failure
the client sees.

Build with the TeaVM Maven plugin (`targetType` `WEBASSEMBLY`, `optimizationLevel` `FULL`, and
`mainClass` the generated class, `com.example.CounterExports`), with the processor beside the SDK as a
`provided` dependency so that `javac` finds it, as `examples/*/pom.xml` in the SDK do; then register the
`.wasm` with `UdfLang::Wasm`.

```xml
<dependency>
  <groupId>com.aerospike.udf</groupId>
  <artifactId>aerospike-udf-sdk-wasm-java</artifactId>
</dependency>
<dependency>
  <groupId>com.aerospike.udf</groupId>
  <artifactId>aerospike-udf-sdk-wasm-java-processor</artifactId>
  <scope>provided</scope>
</dependency>
```

## Parameters and results

The method's parameters, after the `Record` if it takes one, are the UDF's arguments, in order, and their
Java types decide both how an argument is decoded and what the signature says:

| Java | Signature | Takes |
| --- | --- | --- |
| `long`, `int` | `int` | an integer (`int` must fit 32 bits) |
| `double` | `double` | a double, or an integer widened |
| `boolean` | `bool` | a boolean |
| `String` | `str` | a string |
| `byte[]` | `bytes` | a string or a blob, as its bytes |
| `Value` | `any` | anything, nil included, as the 16-byte value |
| `Value[]`, last | `any...` | every remaining argument |

A parameter marked `@Nullable` may be absent or nil and is then `null`; it reads `name?: type` in the
signature, and a scalar one is boxed: `@Nullable Long n`. A call that leaves a required argument out,
passes one of another kind, or passes more arguments than there are parameters, fails before the method
runs, with a message that names the argument (`increment: argument 2 (n) is not an int`,
`increment takes at most 2 arguments, got 3`), the same way the Rust macro rejects a call.

The return type is the result: `void` is nil, `long` and `int` an integer, `double`, `boolean`, `String`,
`byte[]` a blob, `MapBuilder` and `ListBuilder` a built collection, `MapValue` and `ListValue` a stored
one, a `java.util.Map` or `java.util.List` of plain Java values, and `Value` any value (a scalar, guest
bytes, a stored value by position; `Value.ofGeoJson(doc)` is a GeoJSON result). To fail the call, throw
`UdfException` with the message; any other exception the method lets out fails the call with that
exception's message too.

A method may be an instance method: the generated class then builds one instance of the module's class
and calls through it, so a module's state can be ordinary fields rather than statics. The class needs a
constructor without arguments that the generated class can reach.

**The raw form.** A method declared `(int args, int argc)` and returning `int` is exported as it is: it
takes the handles and returns the status, exactly as the hand-written export of the other languages does,
and decodes its arguments itself with `LowLevel.args`. Since nothing can be derived, the annotation's `params`
gives the signature's parameter text: `@Function(value = "vec_distance", params = "(bin: str, query:
bytes, metric: str)")`. This is the form for a hot path that wants to read its arguments once and keep
them, as the vector example does.

## The record

The record a function was called on is its first parameter, of type `Record`. A function that does not
touch the record leaves the parameter out. It is not one of the call's arguments: a client does not pass
it, the arity check does not count it, and the signature the module exports does not list it.

Import it by name, `import com.aerospike.udf.Record;`. Java has a `java.lang.Record` of its own, and a
wildcard import of `com.aerospike.udf.*` leaves the name ambiguous between the two, which does not
compile.

Everything about the record is a method on it. What is left on `Udf` is what is not about the record:
`now()`, `log`, `fail`, the `list` and `map` builders, timers.

There is one `Record` type, writable or not. A stream's records are read-only, and a write to one is
refused when it is made: it throws `UdfException` saying the record is read-only.

`Record` and the `EventRecord` an event handler is given both implement `RecordReader`, so a helper that
takes a `RecordReader` reads a bin the same way in a UDF and in a handler:

```java
static long balanceOf(RecordReader rec) {
    return rec.getLong("balance", 0);
}
```

## Reading

`rec.get("name")` returns the bin as a `Value`, or `null` when the record has no such bin. The typed
readers say what the UDF expects: `getLong`, `getDouble`, `getBoolean`, `getString`, `getBytes` (a string
or blob's bytes), `getList`, `getMap`, and `getFloats(name, float[])` for a blob of 32-bit floats read
straight into the array. Each comes in two forms. Without a default the bin must be there:
`rec.getLong("n")` throws `UdfException` naming the bin when it is absent. With a default,
`rec.getLong("n", 0)` answers the default for an absent bin. Both throw for a bin of another kind.
`rec.hasBin("name")` says whether there is one.

A `Value` is one value, read-only. `kind()` gives its kind as one of `Value.NIL`, `BOOL`, `INT`,
`DOUBLE`, `STR`, `BLOB`, `LIST`, `MAP`, `GEOJSON`, `HLL`, `ABSENT`. The tests `isInt()`, `isString()`,
`isList()`, `isMap()`, `isNull()` and the rest say the same one at a time. The readers `asLong()`,
`asDouble()`, `asBoolean()`, `asString()`, `asBytes()`, `asList()`, `asMap()` each throw when the kind is
another. A stored string or collection is held by position and is not copied until it is asked for.

## Lists and maps

A list the server holds is a `ListValue` and a map is a `MapValue`. Both read the data where it is.

A `ListValue` has `size()`, `isEmpty()`, `get(index)`, the typed `getLong(index)`, `getString(index)` and
so on, and `contains(text)` or `contains(number)`. It is an `Iterable<Value>`. `get(index)` is one call
to the server, which steps over the elements before it without decoding them. An index past the end
throws.

A `MapValue` has `get(key)`, which answers `null` for a key the map does not have, `containsKey(key)`,
`size()`, `isEmpty()`, and the typed readers in the same two forms as the record's: `getLong(key)` throws
when there is no such key, `getLong(key, dflt)` answers the default. A key is a `String` or a `long`. It
is an `Iterable<MapValue.Entry>`, and an entry has `getKey()` and `getValue()`. A wrong kind throws
`UdfException` naming the key or the index.

```java
@Function("sum_list")
static long sumList(Record rec) {
    long total = 0;

    for (Value v : rec.getList("l")) {
        total += v.asLong();        // throws, and fails the call, on anything but an int
    }

    return total;
}

@Function("max_count")
static long maxCount(Record rec) {
    long max = 0;

    for (MapValue.Entry e : rec.getMap("counts")) {
        max = Math.max(max, e.getValue().asLong());   // e.getKey() is the site
    }

    return max;
}

@Function("limit_for")
static long limitFor(Record rec, String kind) {
    return rec.getMap("limits").getLong(kind, 0);     // 0 when the map has no such key
}
```

Every element a loop yields is a fresh `Value`, so one can be kept after the loop moves on. Loops nest:
an element that is itself a list or map is walked the same way.

**A loop may be left early.** A for-each reads the collection from the server a window at a time, 128
values per call, and gives the server's cursor back as soon as the last window is read. For a collection
that fits one window, which is 128 elements or 64 entries, that is before the loop sees its first
element. So `break` and `return` hold nothing:

```java
@Function("first_over")
static long firstOver(Record rec, long limit) {
    for (Value v : rec.getList("l")) {
        if (v.asLong() > limit) {
            return v.asLong();
        }
    }

    return -1;
}
```

One case still holds something. A collection longer than one window keeps its cursor between windows. A
loop over one that is left early leaves that cursor open until the call returns, when the server releases
every handle the call held. That costs one of the call's 256 handle slots until then. The thing to avoid
is leaving a loop over a long collection early hundreds of times in one call.

## Writing

`rec.set("name", value)` stages a write of any kind: a `long`, a `double`, a `boolean`, a `String`, a
`byte[]` (a blob), a built list or map, a stored `ListValue` or `MapValue`, a `java.util.List` or
`java.util.Map`, or a `Value` read from the record, which goes back by position and keeps its kind.
`rec.setGeoJson("name", doc)` writes a GeoJSON bin, kept as the geospatial kind and not a string.
`rec.remove("name")` stages the removal of a bin. Writing `Value.ofNil()` does the same, since a bin
cannot hold nil. `rec.setTtl(seconds)` sets the TTL of the write.

**Nothing staged reaches the server until `rec.save()`.** A function that returns without saving has
changed nothing, however plausible its result. `save()` creates the record when the key has none, so
there is one verb for both. A function that must not overwrite checks `rec.exists()` first. The record is
locked for the whole call, so nothing comes between the check and the save. Two details:

- A save with nothing staged is still a write. The server treats it as a touch: the generation and the
  last-update time move.
- A save with nothing staged on a key with no record makes nothing, since a record must have a bin, and
  the client is told "key not found".

`rec.delete()` deletes the record. It needs no save, and whatever was staged goes with the record. The
server carries it out when the call returns, so `rec.exists()` keeps its answer for the rest of the call.

`rec.setInts("name", long[])` and `rec.setDoubles("name", double[])` write an array straight into a list
bin, with no value built per element. They take effect at once, without a save.

## Building collections

`Udf.list()` and `Udf.map()` start a list or map on the server and return a `ListBuilder` or a
`MapBuilder`. A built collection is written to a bin, returned as the result, nested in another
collection, or handed back from an event handler. `add` and `put` take every kind (`long`, `double`,
`boolean`, `String`, `byte[]` as a blob, `Value`, another builder). `addNull` and `putNull` add a nil,
`addGeoJson` and `putGeoJson` a GeoJSON. A map's `put` takes a `String` or a `long` key. They chain, each
is one call to the server, and each throws when the server refuses it. `addAll(stored)` and
`putAll(stored)` copy a stored list or map by position. `MapBuilder.containsKey(key)` asks a map being
built whether it has a key, in one call.

They are builders, not collections. What went in is not read back from them.

```java
@Function("describe")
static MapBuilder describe(Record rec) {
    return Udf.map(4)
            .put("generation", rec.generation())
            .put("bins", rec.binCount())
            .put("tags", Udf.list(2).add("a").add("b"));
}
```

## Plain Java collections

The SDK bridges to `java.util` in both directions.

Out of the record: `ListValue.toList()` copies a stored list out as a `java.util.List<Object>` and
`MapValue.toMap()` copies a stored map out as a `java.util.Map<Object, Object>`. An integer comes out as
a `Long`, a double as a `Double`, a boolean as a `Boolean`, a string or a GeoJSON as a `String`, a blob as
a `byte[]`, nil as `null`, and a nested list or map as a nested `List` or `Map`.

Into the record: `rec.set(name, list)` and `rec.set(name, map)` take a `java.util.List` or `Map`, and so
do the builders' `add`, `put`, `addAll` and `putAll`. A `@Function` method may return one. The elements
are the same plain types, with `Integer` accepted for an integer. A map key is a `String` or an integer.
Anything else throws `UdfException`.

```java
@Function("double_all")
static List<Object> doubleAll(Record rec) {
    List<Object> doubled = new ArrayList<>();

    for (Object item : rec.getList("l").toList()) {
        doubled.add(((Long) item) * 2);
    }

    rec.set("doubled", doubled);
    rec.save();

    return doubled;
}
```

Copying out costs an object per element. The views read the same data without that cost, so prefer
`ListValue` and `MapValue` when the code only reads.

## Metadata

On the record: `rec.exists()`, `rec.generation()`, `rec.ttl()`, `rec.lastUpdateTime()`, `rec.size()`,
`rec.binCount()`, `rec.setName()` (`null` when the record is in no set), `rec.key()` (the user key as a
`Value` when the record was written with `send_key`, `null` otherwise) and `rec.digest()`. Not on the
record: `Udf.now()`, the server's clock, which an event handler may not read, and
`Udf.log(level, message)`.

## Bin names

A bin is addressed by its name, as a `String`. The name crosses to the server as UTF-8 bytes. The record
keeps the encoded form of the names it has seen, so a name passed as a string literal is encoded once and
a read allocates nothing. No constant has to be declared for a name.

The cost depends on how many names a call uses. The last eight distinct names are matched by identity,
which is a few comparisons. Measured on the test server, a read of an integer bin costs about 640 ns this
way, against about 690 ns for the pre-encoded name of the earlier API and about 610 ns for the low-level
call. A call that rotates through more than eight distinct names pays a hash lookup for the others,
measured at about 250 ns more per lookup. Map keys go through the same cache, so they count toward the
eight.

## The low-level tier

`LowLevel` is the server's calls one at a time, for code that must not allocate. Names are `byte[]`
constants. `LowLevel.bin(name, value)` reads into a `Value` the caller owns and reuses. `enter`, `next`
and `close` walk a collection over a window the caller owns. `newMap`, `newList` and the `mapPut*` and
`listPush*` calls build over `int` handles. The record's key and digest go into caller buffers. Every
call returns a status and none throws.

```java
private static final byte[] N = LowLevel.bytes("n");
private static final Value SLOT = new Value();

@Function("peek")
static long peek() {
    if (LowLevel.bin(N, SLOT) != LowLevel.OK || !SLOT.isInt()) {
        return 0;
    }

    return SLOT.asLong();
}
```

Nothing in it is needed to write a UDF. The vector example is written on it, because a scan runs its
`record` once per record and it scores from a `float[]` field. A cursor opened with `LowLevel.enter` is
the caller's to close, on every path out of the loop. `LowLevel.list(handle)` and `LowLevel.map(handle)`
bridge a handle built down there to the builders, and `Params` holds the decoders the generated exports
call, so a hand-written export can use them too.

## What is different about Java

- **Addresses.** The collector of this backend may move an array, and there is no pinning, so an array's
  address is valid only until the next allocation. The SDK takes `Address.ofData(buf)` inside the
  expression that hands it to the host and never keeps one; code calling the natives directly must do the
  same.
- **Strings.** Each `String` value crossing is one UTF-8 encoding and one allocation, and so is each
  `String` parameter. A bin name is encoded once and kept, and a `byte[]` parameter takes a string
  argument as its bytes.
- **Exceptions.** A generated export catches what the method throws and fails the call with the message;
  `UdfException` is the one to throw on purpose, and what the SDK throws.
- **The runtime.** A Java module is larger (200 to 330 KB for the examples) and its instance costs more
  to start. On the vector top-k over 20,000 records it took 131 ms where the Rust module took 80 ms and
  the Go module 61 ms, measured on 2026-11-18 on one aarch64 node.
- **Coverage.** The SDK carries the Rust SDK's whole record surface; `examples/udf` is its fixture, one
  function per corner, and the tests in `tests/src/udf_java.rs` hold it to the expectations of the Rust
  fixture's tests.

## What the processor generates

For the counter above, `CounterExports` reads, in short:

```java
@Export(name = "increment")
public static int udf_increment(int args, int argc) {
    try {
        int n = Params.args(args, argc, ARGS_udf_increment);
        Params.arity("increment", 1, argc);
        Udf.resultInt(Counter.increment(Params.record(),
                Params.int64(ARGS_udf_increment, n, 0, "increment", "by")));
        return 0;
    }
    catch (UdfException e) {
        return Udf.fail(e.getMessage());
    }
    catch (Throwable t) {
        return Params.uncaught("increment", t);
    }
}

private static final byte[] SIG_increment = Params.signatureBytes("fn increment(by: int) -> value");

@Export(name = "__udf_sig_increment")
public static int sig_increment() {
    return Params.signature(SIG_increment);
}

public static void main(String[] a) {
}
```

`Params.record()` is the record of the call. It is handed to the method and is no part of the arity
check or of the signature.

The processor runs inside `javac`, before TeaVM, depends on nothing but the JDK, and reports a method
that cannot be exported (private, a parameter of another type, a handler with the wrong parameters, a
`@Stream` class without its `record` or `end`) as a compile error on that method.

## A module to read: the vector top-k

`examples/vector/.../Vector.java` is the exact top-k over packed `f32` vectors, the same algorithm as the
Rust, C and Go modules, bit for bit: a `@Stream("topk")` class whose `record` and `end` are the two halves,
a `@Function` `vec_distance`, and four-lane kernels for the three metrics (no SIMD is emitted, but the fold
order is the one the others use). Its state is the instance's fields. It keeps the raw form on
`LowLevel`, since it reads its arguments once per stream and keeps them, and gives the signatures
through `params`. The record half:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/vector/src/main/java/com/aerospike/udf/examples/Vector.java:record}}
```

> **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save builds the
> bin, and fails if the text is not a document the server would accept as a write.
> And after any save that fails, a list, map or blob value read before it answers an error: read
> the bin again.

## API reference

Generated from the SDK's classes: every public item of the record UDF surface, with its comment. The
`@Import` declarations in `Natives.java` are the raw natives and are not part of the API.

{{#include ../api/java-record.md}}

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

That is the whole module. `@Function` is the Java face of the Rust SDK's `export!`: at compile time the
SDK's annotation processor generates a class beside this one, `CounterExports`, holding the exported
`increment(int args, int argc)` that decodes the call's arguments, calls the method and delivers its
return as the result; the `__udf_sig_increment` export, whose text `fn increment(by: int) -> value` it
derives from the parameter types; and the module's `main`, which TeaVM wants as an entry point. Nothing in
the generated class is yours to edit, and nothing in yours refers to it except the build.

`Bin` is a bin of the record by name, with typed getters (`getLong(0)` reads the bin as an integer, 0
when it is absent) and setters of every kind; the name is encoded once, when the `Bin` is made, so a module
keeps its bins as constants. `Udf.commit()` writes the staged bins, creating the record if it does not
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

The method's parameters are the UDF's arguments, in order, and their Java types decide both how an
argument is decoded and what the signature says:

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
`byte[]` a blob, `UdfMap` and `UdfList` a built collection, and `Value` any value (a scalar, guest bytes,
a stored value by position; `Value.ofGeoJson(doc)` is a GeoJSON result). To fail the call, throw
`UdfException` with the message; any other exception the method lets out fails the call with that
exception's message too.

A method may be an instance method: the generated class then builds one instance of the module's class
and calls through it, so a module's state can be ordinary fields rather than statics. The class needs a
constructor without arguments that the generated class can reach.

**The raw form.** A method declared `(int args, int argc)` and returning `int` is exported as it is: it
takes the handles and returns the status, exactly as the hand-written export of the other languages does,
and decodes its arguments itself with `Udf.args`. Since nothing can be derived, the annotation's `params`
gives the signature's parameter text: `@Function(value = "vec_distance", params = "(bin: str, query:
bytes, metric: str)")`. This is the form for a hot path that wants to read its arguments once and keep
them, as the vector example does.

## Bins

A `Bin` is a bin of the record in hand, by name. Its getters read the bin as the type the UDF expects:
`getLong()`, `getDouble()`, `getBoolean()`, `getString()`, `getBytes()` (a string or blob's bytes),
`getFloats(float[])` (a blob of 32-bit floats straight into the array), and each throws `UdfException`
naming the bin when it is absent or of another kind, unless given a default (`getLong(0)`), which covers
absent and nil. `get()` is the bin as a `Value`, or `null` when absent; `exists()` says whether there is
one; `asList()` and `asMap()` open a cursor over a collection bin. The setters stage a write of any kind:
`set(long)`, `set(double)`, `set(boolean)`, `set(String)`, `set(byte[])` (a blob), `set(Value)` (a value
read from the record, by position, keeping its kind), `set(UdfMap)`, `set(UdfList)`, `setGeoJson(doc)`
(kept as the geospatial kind, not a string), `setNull()`, `setInts(long[])` and `setDoubles(double[])` (a
list in one import, no value built per element), and `remove()`. `Udf.commit()` writes them; `Udf.delete()`
deletes the record, and `Udf.setTtl(seconds)` sets the time to live the commit carries.

## Values and cursors

Every read is a `Value`: `kind`, `len`, `payload`, with the accessors `asLong()`, `asDouble()`,
`asBoolean()`, `asString()`, `asBytes()`, each throwing when the kind is another, and `isInt()`,
`isString()`, `isList()`, `isMap()`, `isNull()`. The kinds are `Value.NIL`, `BOOL`, `INT`, `DOUBLE`,
`STR`, `BLOB`, `LIST`, `MAP`, `GEOJSON`, `HLL`, `ABSENT`, and a `switch` on `kind` is the fast form of
a dispatch. A stored string or collection is held by position, never copied until asked.

`asList()` and `asMap()` on a `Value`, or on a `Bin`, open a `Cursor`: the Rust SDK's `iter()` and
`entries()`. A list cursor is an `Iterable<Value>` and a map cursor an `Iterable<Cursor.Entry>`, an entry
being a `key` and a `value` (and a `java.util.Map.Entry`), so the natural form is a for-each:

```java
private static final Bin L = Bin.of("l");
private static final Bin COUNTS = Bin.of("counts");

@Function("sum_list")
static long sumList() {
    long total = 0;

    for (Value v : L.asList()) {
        total += v.asLong();        // throws, and fails the call, on anything but an int
    }

    return total;
}

@Function("max_count")
static long maxCount() {
    long max = 0;

    for (var e : COUNTS.asMap()) {
        max = Math.max(max, e.value.asLong());   // e.key is the site
    }

    return max;
}
```

The for-each hands out a fresh `Value` or `Entry` each step. The `while` form allocates nothing: `c.next()`
steps, refilling the reused `c.value` (and `c.key` for a map) from a window of 32 values per import, and
goes inside a try-with-resources when the loop may `break`:

```java
try (Cursor<Cursor.Entry> c = COUNTS.asMap()) {
    while (c.next()) {
        if (c.value.asLong() > limit) {
            break;                               // close() runs; at the end the cursor closes itself
        }
    }
}
```

Cursors nest, as long as the inner one is closed, by exhaustion or by hand, before the outer one steps
on.

## Building collections

`UdfMap.create()` and `UdfList.create()` build a map or list on the host, to be written to a bin, returned
as the result, nested in another collection, or handed back from an event handler. `put` and `add` take
every kind (`long`, `double`, `boolean`, `String`, `byte[]` as a blob, `Value`, `UdfMap`, `UdfList`),
`putNull` / `addNull` a nil, `putGeoJson` / `addGeoJson` a GeoJSON; a map's `put` takes a `String` or a
`long` key. They chain, each is one import, and each throws when the host refuses it. `putAll(stored)`
and `addAll(stored)` copy a stored map or list by position; `putAll(pairs, n)` and `addAll(values, n)`
take a `Value[]` in one import.

```java
@Function("describe")
static UdfMap describe() {
    return UdfMap.create(4)
            .put("generation", Udf.generation())
            .put("bins", Udf.binCount())
            .put("tags", UdfList.create(2).add("a").add("b"));
}
```

## The record's metadata

`Udf.generation()`, `ttl()`, `lastUpdateTime()`, `recordSize()`, `binCount()`, `setName()` (null when the
record is in no set), `exists()`, `digest(buf)`, `keyType()` / `keyInt()` / `keyBytes(buf)` for the user
key, and `now()`, the server's clock, which an event handler may not read.

## The low-level tier

Under the objects is the layer that mirrors the natives one call at a time: `Udf.bin(name, value)` into
a reused `Value`, `Udf.enter` / `next` / `close` over a window the caller owns, `Udf.newMap` and the
`mapPut*` / `listPush*` calls over `int` handles, the `set*` calls returning a status, and the `byte[]`
twins of every name. It allocates nothing and every call returns a status rather than throwing. The
vector example is written on it, because it reads its arguments once per stream and scores from a static
`float[]`; nothing else needs to be. `UdfMap.wrap(handle)` and `UdfList.wrap(handle)` bridge a handle built
down there to the objects, and `Params` holds the decoders the generated exports call, so a hand-written
export can use them too.

## What is different about Java

- **Addresses.** The collector of this backend may move an array, and there is no pinning, so an array's
  address is valid only until the next allocation. The SDK takes `Address.ofData(buf)` inside the
  expression that hands it to the host and never keeps one; code calling the natives directly must do the
  same.
- **Strings.** Each `String` crossing is one UTF-8 encoding and one allocation, and so is each `String`
  parameter; `Bin` encodes a name once, and a `byte[]` parameter takes a string argument as its bytes.
- **Exceptions.** A generated export catches what the method throws and fails the call with the message;
  `UdfException` is the one to throw on purpose, and what the SDK throws.
- **The runtime.** A Java module is larger (150 to 250 KB for the examples) and its instance costs a
  little more to start: on the vector top-k it runs within 10 percent of Rust and Go, and a single call
  costs some 40 µs more than theirs.
- **Coverage.** The SDK carries the Rust SDK's whole record surface; `examples/udf` is its fixture, one
  function per corner, and `java_sdk_covers_the_rust_surface` in `tests/src/udf_java.rs` holds it to the
  expectations of the Rust fixture's tests.

## What the processor generates

For the counter above, `CounterExports` reads, in short:

```java
@Export(name = "increment")
public static int udf_increment(int args, int argc) {
    try {
        int n = Udf.args(args, argc, ARGS_increment);
        Params.arity("increment", 1, argc);
        Udf.resultInt(Counter.increment(Params.int64(ARGS_increment, n, 0, "increment", "by")));
        return 0;
    }
    catch (UdfException e) {
        return Udf.fail(e.getMessage());
    }
    catch (Throwable t) {
        return Params.uncaught("increment", t);
    }
}

private static final byte[] SIG_increment = Udf.signatureBytes("fn increment(by: int) -> value");

@Export(name = "__udf_sig_increment")
public static int sig_increment() {
    return Udf.signature(SIG_increment);
}

public static void main(String[] a) {
}
```

The processor runs inside `javac`, before TeaVM, depends on nothing but the JDK, and reports a method
that cannot be exported (private, a parameter of another type, a handler with the wrong parameters, a
`@Stream` class without its `record` or `end`) as a compile error on that method.

## A module to read: the vector top-k

`examples/vector/.../Vector.java` is the exact top-k over packed `f32` vectors, the same algorithm as the
Rust, C and Go modules, bit for bit: a `@Stream("topk")` class whose `record` and `end` are the two halves,
a `@Function` `vec_distance`, and four-lane kernels for the three metrics (no SIMD is emitted, but the fold
order is the one the others use). Its state is the instance's fields. It keeps the raw form on the
low-level tier, since it reads its arguments once per stream and keeps them, and gives the signatures
through `params`. The record half:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/vector/src/main/java/com/aerospike/udf/examples/Vector.java:record}}
```

## API reference

Generated from the SDK's classes: every public item of the record UDF surface, with its comment. The
`@Import` declarations in `Natives.java` are the raw natives and are not part of the API.

{{#include ../api/java-record.md}}

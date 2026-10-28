# Java

A Java stream UDF is a class under `@Stream("name")` with a `record` method, called with every record a
query thread reads, and an `end` method, called once afterwards, whose return is the partial. The class's
fields are the stream's state: the processor builds one instance per module instance, which is the "one
instance sees the whole stream" the contract promises, and exports `name_record` and `name_end` with the
signature `stream name { record(...) end(...) }` derived from the two methods' parameters, decoded like a
record UDF's.

## The shape

```java
@Stream("sum_n")
final class SumN {
    private static final Bin N = Bin.of("n");
    private long total;
    private long count;

    void record() {
        Value v = N.get();

        if (v != null && v.isInt()) {
            total += v.payload;
            count++;
        }
    }

    UdfMap end() {
        return UdfMap.create(2).put("total", total).put("count", count);
    }
}
```

`record` returns `void`; throwing `UdfException` ends the stream with the message. A collection bin is
walked with a for-each, `for (Value v : bin.asList())` or `for (var e : bin.asMap())`, as in a record UDF. `end` returns the
partial, here a built map. Nothing resets the fields between records.

A stream in the raw form has `int record(int args, int argc)` and `int end(int args, int argc)`,
exported as they are, and the annotation's `params` and `endParams` give the signature's parameter text.
That is the form for a stream that reads its arguments once, on the first record, and keeps them: decoding
typed parameters on every record would cost an import and, for a `String`, an allocation per record.

## The vector top-k

`examples/vector/.../Vector.java` is the full-size example, in the raw form. Its stream half:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/vector/src/main/java/com/aerospike/udf/examples/Vector.java:stream}}
```

The record method configures itself from the arguments on the first call, reads the vector straight into
a `float[]` field with `Udf.floats`, scores it with a four-lane kernel, and offers it to a bounded top-k
of preallocated hits; the end method builds the partial with `mapPutEntries` for each hit.

## API reference

{{#include ../api/java-stream.md}}

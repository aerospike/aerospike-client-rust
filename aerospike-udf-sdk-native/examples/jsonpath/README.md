# A JSONPath native UDF

Query a record with [RFC 9535](https://www.rfc-editor.org/rfc/rfc9535.html) JSONPath, server-side, with no
conversion to JSON text and no conversion to `serde_json::Value`.

```text
$.address.city                    a key inside a map bin
$.tags[*]                         every element of a list bin
$.orders[?@.total > 100].id       a filter over a list of maps
$..price                          every price at any depth
```

A record is the root object and its bins are the root's keys, so `$.address.city` means bin `address`, key
`city`.

## Why `jsonpath-rust`

It is the only JSONPath crate that is **generic over the value it queries**. Its `Queryable` trait is the seam
that lets the engine walk a record directly:

```rust
pub trait Queryable: Default + Clone + Debug + PartialEq + From<i64> + … {
    fn get(&self, key: &str) -> Option<&Self>;
    fn as_array(&self)  -> Option<&Vec<Self>>;
    fn as_object(&self) -> Option<Vec<(&str, &Self)>>;
    …
}
```

Everything else is welded to `serde_json::Value` (`serde_json_path`, `jsonpath_lib`, `jsonpath-plus`) and would
force a full eager conversion of every record, or works on JSON *text* rather than a value tree (`rsonpath`).

It also splits parsing from execution — `parse_json_path` then `js_path_process` — so an aggregation compiles its
path **once per query thread** rather than once per record.

## What is lazy, and what is not

`node.rs` implements `Queryable` over the record, expanding one level per node on first touch. Each node holds a
`std::cell::OnceCell` of its expansion, which is what `Queryable` needs: its methods hand out references, so a
child cannot be built on the fly and returned. There is **no `unsafe`** and no arena crate.

**It does not save reads.** By the time a UDF runs the server has already loaded every bin into memory; `rec_get`
is an in-memory lookup. Nothing here defers I/O, because there is none left to defer.

What it saves is **construction** — and the numbers below put a size on that.

## What it costs

200,000 records, each with two integer bins and a list bin holding one map. Best of three,
`tests/bench.rs`, against a namespace the bench had to itself.

**The same records, the same 20,000 selected:**

| filter | ms | records/sec |
|---|---|---|
| hand-written: two bin reads and a compare | 207 | 964,000 |
| `jsonpath: $.orders[?@.total >= N]` | 208 | 960,000 |

**Identical.** At any realistic selectivity a JSONPath filter is free, because building and shipping 20,000
record messages costs far more than the engine does. That is the same conclusion the emitter row in
`NATIVE_UDF_DESIGN.md` reached about native filters versus Lua ones, arrived at from the other direction.

**The same paths selecting nothing, so the response path cannot hide the engine:**

| filter | ms | records/sec |
|---|---|---|
| `$.nosuchbin` — root expanded, list bin untouched | 55 | 3,608,000 |
| `$.orders[?@.total > 2N]` — walks the list and the map | 75 | 2,683,000 |

So the engine plus the adapter costs about **275 ns per record**, and walking one small list-of-one-map on top of
that costs about **100 ns more**. For scale, the hand-written count+sum aggregation in the main bench runs at
165 ns per record — so a full RFC 9535 query language is within a small multiple of a hand-written accumulator.

**The gap between those two rows is the laziness, measured.** `$.nosuchbin` still expands the root and still sees
the `orders` bin — it just keeps it as a handle instead of walking it. Here that is 20 ms of 75; on a record whose
list holds a thousand elements rather than one it would be nearly all of it.

## What it costs to load

| plugin | size |
|---|---|
| `stats` (no dependencies) | 0.39 MB |
| this one | **2.22 MB** |

`jsonpath-rust` brings `serde_json`, `regex`, `pest` and `thiserror`, with no feature flags to trim them, and all
of it is resident in `asd` for the life of the process. That is the price of RFC 9535 including regex filters, and
an operator is choosing it when they name this module in the configuration file.

## Embedded JSON: a string bin that holds a document

A string bin whose text is a JSON object or array can be drilled into, and the rule is one line: **descending is
what triggers the parse.**

```text
bin_map: { "A": [1, 2, 3, { "a": 1, "b": "{\"a\": 1, \"b\": 2}" }] }

$.bin_map.A[3].b        ->  "{\"a\": 1, \"b\": 2}"      the string, unchanged
$.bin_map.A[3].b.b      ->  2                          parsed, and the path keeps going
$.bin_map.A[3].b[?@>1]  ->  2                          filters work inside it too
```

So no existing path changes meaning — a path that stops at a string still gets a string. Nesting works to any
depth (`$.payload.outer.inner` through two layers of quoting), one parse per level and no deeper, because a parsed
string's own strings are left as strings until something asks.

Two guards make this safe enough to have on by default:

- **Only text starting with `{` or `[` is attempted.** One byte rejects almost every ordinary string, and more
  importantly it means a bin holding `"42"` or `"true"` is **never** silently a number or a boolean. Those are
  valid JSON scalars, and treating them as such would change what a filter means on data nobody intended as JSON.
  Only objects and arrays can be descended into anyway, so the restriction costs nothing.

  **Leading whitespace is skipped before that check — but only JSON's whitespace**, meaning space, tab, CR and LF
  (RFC 8259 §2). Not `char::is_whitespace`, which is the Unicode set: `"\u{00A0}{\"a\":1}"` is *not* a document,
  because no JSON parser would accept it either. The gate is a pre-filter that must never disagree with the
  parser, so it uses the parser's definition and hands the parser the original text rather than a trimmed copy.
  An earlier version trimmed Unicode whitespace and parsed the trimmed string, which silently accepted documents
  nothing else would — and was asymmetric, since a *trailing* no-break space still failed.
- **Each string is attempted at most once**, whatever the answer — the `OnceCell` caches "not a document" too.

One thing an embedded object does **not** keep: **key order through an `jsonpath_extract` partial.** A partial's
wire form is an Aerospike map, which a client decodes into a `HashMap`, so entry order is lost. JSON says object
order is not significant (RFC 8259 §4) so this is acceptable — but it means an extracted object is equal to what
was collected, not identical to it. A unit test pins that, and it caught the assertion that had been passing by
luck on `HashMap`'s randomised iteration order.

The cost to know about: `$..key` and `$.*` descend into everything, so on a record full of string bins that begin
with `{` they will each be parsed once. A path that names its way down parses only what it walks through.

## What a path that cannot match does

**Nothing is an error.** Every one of these answers empty:

| case | example |
|---|---|
| missing bin | `$.nosuchbin` |
| missing key in a map that exists | `$.address.country` |
| recursive descent finding nothing | `$..nosuchkey` |
| key asked of a scalar | `$.id.anything`, `$.name.x`, `$.active.x` |
| key asked of a list | `$.tags.anything` |
| index asked of a scalar or a map | `$.id[0]`, `$.address[0]` |
| index out of range, either direction | `$.tags[99]`, `$.tags[-99]` |
| filter on something with no members | `$.id[?@ > 0]` |
| filter matching none of the members | `$.orders[?@.total > 100000]` |

That is not this implementation being lenient — it is what JSONPath *is*. RFC 9535 defines a query as producing a
nodelist, and a selector that does not apply to a node simply selects nothing from it. There are no runtime type
errors in the language, which is why `Queryable`'s methods return `Option` and not `Result`. A slice that overruns
is clamped (`$.tags[1:99]` gives the last two of three); one entirely past the end is empty.

Per function: `jsonpath` and `jsonpath_paths` give `[]`, `jsonpath_count` gives `0`, `jsonpath_exists` gives
`false`, `jsonpath_one` gives nil — which the client collapses to no value at all.

**Only two things are errors:** a malformed path, rejected when it is compiled; and an ambiguous map key, below.

### In an aggregation the distinction is load-bearing

A miss means *this record does not match* and the scan continues. An error means *the query stops*. A set where
half the records hold an integer where the path expects a list returns the other half, with no failure — that is
the miss path doing its job over mixed-shape data.

An error, by contrast, abandons the whole query. **That is deliberate and it is the harsh choice**: skipping the
record instead would hand back a result set quietly missing rows, and a filter whose answer is silently incomplete
is worse than one that stops and says why. Worth knowing before pointing a path at a billion records.

Getting that second half right needed a **bug fix in the server**, found by the test that asserts it: `apply_stream`
was returning zero even on failure, and `aggr_query_job_slice` acts on the return code and nothing else — so the
failure message was written into a result nobody read, and the query completed as a success with the offending
records missing. mod-wamr had the same bug for the same reason, and is fixed too.

## The sharp edge: ambiguous map keys

Aerospike map keys may be integers; JSONPath names are strings. So the key `5` is addressed as `$.m['5']` — and
**if the same map also holds the string key `"5"`, the two are indistinguishable.**

This module detects that and **fails the call**, rather than picking one and returning a plausible wrong answer.

The check cannot live in the trait: `Queryable`'s methods return `Option`, not `Result`, so a map with ambiguous
keys has no way to refuse to be read. So the collision is recorded during expansion in a `Trouble` cell shared by
the document, and the UDF checks it *after* the query and before the answer is used.

Two consequences worth knowing:

- A blob map key has no string form worth inventing, so those entries are simply not addressable.
- Doubles render with `{:?}`, which always emits a decimal point, so `5.0` cannot collide with `5` by accident of
  formatting. The check does not rely on that — it compares whatever the keys rendered to — but it means the
  common case never trips it.

## Layout

| file | what |
|---|---|
| `src/node.rs` | the lazy `Queryable` adapter — the interesting part |
| `src/owned.rs` | a match once it is out of the record, and the one writer into the host's builders |
| `src/lib.rs` | the UDF surface: three record functions and two aggregations |
| `src/tests.rs` | unit tests, including the ambiguity contract |
| `tests/against_a_server.rs` | the half that needs a record |
| `tests/bench.rs` | the numbers above |

## The surface

Record functions:

Called on a single record by key, with no query or scan involved:

```rust
client.execute_udf(&policy, &key, "jsonpath", "jsonpath", Some(&[as_val!("$.address.city")])).await?
```

| function | answer |
|---|---|
| `jsonpath(path)` | every match, as a list |
| `jsonpath_one(path)` | the first match, or nil |
| `jsonpath_exists(path)` | whether anything matched — nothing is copied out of the record |
| `jsonpath_count(path)` | how many locations matched, without building any of them |
| `jsonpath_paths(path)` | the **normalized path** to each match: `$['orders'][0]['total']` |

`jsonpath_paths` earns its place twice. It answers *where* a path resolved, which is the question you ask when a
filter matched more or fewer things than you expected. And a normalized path is a **concrete** location — no
wildcards, no filters — which is exactly the shape a write needs, since that is what maps onto an Aerospike
context. It is the read-only half of a setter, useful on its own.

Aggregations:

| function | shape |
|---|---|
| `jsonpath_filter(path)` | zero state; matching **records** go straight to the response through `emit_record` |
| `jsonpath_extract(path, chunk)` | collects the matched values, dumping every `chunk` |

Both compile the path in `configure`, which runs once per query thread — and again after every dump, which is why
the path string is kept beside the parse tree.

Neither uses `partial!`: a compiled `JpQuery` is a parse tree, not a value, so it can never be a `PartialField`.
`AsPartial` is written out by hand instead, which is what that escape hatch is for.

**A record-level predicate is a path that matches something.** `$[?@.total > 100]` does *not* mean "this record,
if its total exceeds 100" — RFC 9535 applies a filter to the *members* of the object it follows, binding `@` to
each member in turn. So `jsonpath_filter` selects a record when the path matches anything in it, and a predicate
on a nested list is written `$.orders[?@.total > 100]`.

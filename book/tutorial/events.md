# Tutorial: handling records in events

This tutorial writes six event handlers for one set of account records. Each step is a job handlers are
used for, shown in all five languages so you can compare them. It follows on from the
[record tutorial](../record-udfs/tutorial.md) and uses the same imports and helpers.

A handler differs from a record UDF in three ways:

1. **The server calls it.** A handler is bound to an event on a namespace or set, and runs when the
   server reaches that point.
2. **Its records are read-only.** The event hands over one or two records. The handler reads their bins
   through the event, not through the record API, and writes nothing.
3. **Its answer is the effect.** The handler returns a verdict, and the server acts on it: let the write
   through, refuse it, or write a different record.

A binding connects a handler to an event:

```text
event-set:event=before-write;namespace=test;set=accounts;module=tutorial;function=no_overdraft
```

In Rust the handlers sit together inside one `event! { ... }` block, each under an `on <kind>` line. Go
marks a handler with a `//udf:on` comment, C and C++ with a macro per kind, Java with an annotation per
kind. The kind fixes the handler's parameters and its answer.

## Step 1: refuse a bad write

The event: `before-write`. It runs after the write has produced the record it would leave and before
anything is stored.

The handler gets two records: `before`, the record as it was, and `after`, the record the write would
leave. Reading a bin of either answers "no value" when the bin is absent or nil: `None` in Rust, `false`
in Go, an empty `Optional` in C++, `null` in Java, a status other than `AS_UDF_OK` in C.

Rejecting fails the client's write with the result code and message given. Code 22 is "forbidden".

{{snippet no_overdraft}}

## Step 2: stamp every write

The event: `before-write` again, this time changing what gets stored.

A `record` answer replaces the record the write would leave. Its bins are the whole record: a bin left
out is deleted. So a handler that decides two bins and keeps the rest starts from `after` and puts its
own bins on top, which is what `with_bins` does.

The third parameter describes the write: where it came from, the TTL it asked for, whether it creates
the record.

{{snippet stamp}}

C has no `with_bins`. The handler above uses this helper, which walks the record's bin names and copies
each bin the answer does not already have:

{{snippet with_bins: c}}

Both handlers can be bound to the same set. They run as a chain in `order`, and a `record` answer from
one is the `after` the next one sees:

```text
event-set:event=before-write;namespace=test;set=accounts;module=tutorial;function=no_overdraft;order=0
event-set:event=before-write;namespace=test;set=accounts;module=tutorial;function=stamp;order=1
```

## Step 3: protect a record from deletion

The event: `before-delete`. It gets the record about to be deleted and answers allow or refuse.

{{snippet keep_locked}}

## Step 4: hide a bin from readers

The event: `after-read`. It runs after the server has gathered the record for a read, and may answer the
read with a different record. The stored record is not changed.

The answer follows the same rule as in step 2: its bins are the whole response. A bin set to nil in the
answer is left out, so the handler keeps every bin and sets `secret` to nil.

{{snippet hide_secret}}

## Step 5: keep a record past its expiry

The event: `before-expire`. It runs when the server comes to remove a record whose time to live has
passed. The handler answers expire, or keep with a new TTL in seconds.

{{snippet keep_marked}}

## Step 6: run on a timer

The event: `timer`. A timer is set on one record, and its handler runs on that record when the timer
comes due.

A timer handler is the exception to rule 2. It runs as a record UDF, so it is handed the record as a
record UDF is, and the whole record API from the record tutorial works in it: it reads bins, stages
writes and saves. It can also set the next timer,
which is how a recurring job is written.

{{snippet tick}}

The first timer is set from outside, or from a record UDF with the same call the handler uses:

```text
event-set:event=timer;namespace=test;set=accounts;module=tutorial;function=tick
event-timer-set:namespace=test;set=accounts;key=alice;name=tick;in=1000
```

## Quick reference

| Task | Rust | Go | C | C++ | Java |
| --- | --- | --- | --- | --- | --- |
| Read an integer bin | `record.int(name)?` | `rec.Int(name)` | `as_udf_event_bin(&rec, name, &v)` | `record.as_int(name)` | `record.binInt(name)` |
| Read a bin of any kind | `record.bin(name)` | `rec.Get(name)` | `as_udf_event_bin(&rec, name, &v)` | `record.get(name)` | `record.binValue(name)` |
| Let a write through | `Decision::Accept` | `udf.Accept()` | `as_udf_answer_accept()` | `udf::Decision::accept()` | `Decision.accept()` |
| Refuse a write | `Decision::Reject { code, message }` | `udf.Reject(code, msg)` | `as_udf_answer_reject(code, msg)` | `udf::Decision::reject(code, msg)` | `Decision.reject(code, msg)` |
| Store a different record | `Decision::Record { bins, ttl }` | `udf.AcceptRecord(bins)` | `as_udf_answer_record(bins, ttl)` | `udf::Decision::record(bins)` | `Decision.record(bins)` |
| Keep the bins not decided | `after.with_bins(out)?` | `after.WithBins(out)` | the helper in step 2 | `after.with_bins(out)` | `after.withBins(out)` |
| Allow or refuse a delete | `Gate::Allow`, `Gate::Refuse { .. }` | `udf.Allow()`, `udf.Refuse(..)` | `as_udf_answer_allow()`, `as_udf_answer_refuse(..)` | `udf::Gate::allow()`, `udf::Gate::refuse(..)` | `Gate.allow()`, `Gate.refuse(..)` |
| Answer a read | `ReadResult::Unchanged`, `ReadResult::Record { bins }` | `udf.Unchanged()`, `udf.ReadRecord(bins)` | `as_udf_answer_unchanged()`, `as_udf_answer_record(bins, -1)` | `udf::ReadResult::unchanged()`, `udf::ReadResult::record(bins)` | `ReadResult.unchanged()`, `ReadResult.record(bins)` |
| Expire or keep | `Expiry::Expire`, `Expiry::Keep { ttl }` | `udf.Expire()`, `udf.Keep(ttl)` | `as_udf_answer_expire()`, `as_udf_answer_keep(ttl)` | `udf::Expiry::expire()`, `udf::Expiry::keep(ttl)` | `Expiry.expire()`, `Expiry.keep(ttl)` |
| Set a timer | `as_udf::timer_set(name, ms)` | `udf.TimerSet(name, ms)` | `as_udf_timer_set(name, len, ms)` | `udf::timer_set(name, ms)` | `Udf.timerSet(name, ms)` |

The code on this page is compiled: it comes from the sources under `book/tutorial/`, one file per
language. [The event system](README.md) covers every event kind, bindings, filters and timers, and the
language chapters list every call in full.

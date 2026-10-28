# CRDT Tutorial

Two Aerospike clusters that both accept writes to the same record will, sooner or later, both change it
at once. XDR ships each write to the other side, and each side has to decide what the record is now. The
default policies pick a side. This tutorial builds records that need no picking: data structures whose
merge is a function of the two copies alone, so that both clusters compute the same bytes whatever order
the writes arrive in and however many times a write is delivered. Those structures are **CRDTs**,
conflict-free replicated data types, and the `xdr-merge` event is where their merge runs.

The tutorial is five steps. Each step is one record layout, one merge handler, the write UDFs that keep
the record in the shape the merge expects, and the test that proves the laws on real bytes. Each step
lifts exactly one limit of the step before, and each is written in Rust, Java, Go and C++ on the same
page, so the shape of the idea stays in view while the language changes.

## The picture

Two timelines say what the tutorial is about. One lane per data centre, a dot for each write, an arrow
for each shipment, and the record's state after every event. First a plain integer:

{{#include diagrams/intro-plain-int.svg}}

Then the same two writes against step 1's structure, one count per site:

{{#include diagrams/intro-gcounter.svg}}

The second record converges because its merge is a maximum over entries that only grow. Every step of
this tutorial draws its race on these two lanes.

## What a merge has to be

A merge `⊕` that two clusters can both run on their own copies without ever disagreeing has to obey
three laws:

- **commutative**: `a ⊕ b = b ⊕ a`, so the cluster that merges the mirror pair gets the same answer;
- **idempotent**: `(a ⊕ b) ⊕ b = a ⊕ b`, so a write delivered twice changes nothing the second time;
- **associative**: `(a ⊕ b) ⊕ c = a ⊕ (b ⊕ c)`, so three or more sites agree whatever order the writes
  reach them in.

A maximum obeys all three. So does a union. Every step of this tutorial is, underneath, one of those two
applied entry by entry, and every design decision is about making the operations the application wants
(increment, decrement, add, remove, update) expressible as something that only grows, so that a maximum
or a union is the right merge. The one rule of the state-based CRDT: **never shrink, never overwrite;
add a second structure instead.**

Two things the merge must not do. It must not read a clock, a generation, a last-update time or a
source id, because each cluster would read a different one; the server withholds all four from an
`xdr-merge` handler, zeroing the metadata and refusing the clock import. And it must produce the same
bytes for the same logical state, because the server compares the merged records' stored bytes to check
the laws and to decide that a merge changed nothing. Sorted lists and key-ordered maps are what make
that true.

## How the server checks it

The `xdr-merge` event fires when a shipped write arrives for a record that already exists locally. The
bound handler is given the local record, the incoming one and a `Conflict`, and answers a `Resolution`:
keep the local record, take the remote one, defer to the policy, or write a record of its own. Every
step here answers with a record of its own, built from the two.

Every merge runs on both sides, because the merged record ships back. The loop ends because a merge that
reproduces the stored bytes is not written and is reported to the source as a success:

{{#include diagrams/intro-merge-loop.svg}}

One cluster is enough to check the laws. The dry run, `execute_udf` on the reserved package `__event__`
with the function `xdr-merge`, runs the bound handler against the real current record and an incoming
record you describe, writes nothing, and reports `symmetric`, `idempotent` and, given a third record,
`associative`, each checked on the stored bytes of the results. The test of every step runs the four
languages' modules through it, and `tests/src/xdr_merge.rs` then runs them between two live clusters.
The event system chapter has [the whole contract](../events/README.md), and the language chapters under
it show the handler form each SDK gives it.

## The steps

| Step | Structure | Merge | What it adds | What it still cannot do |
| --- | --- | --- | --- | --- |
| [1](counter.md) | G-Counter, one count per site | per-site maximum | the pattern: grow-only state, a merge on the data alone | decrement |
| [2](pncounter.md) | PN-Counter, two maps | per-site maximum, twice | a second map instead of subtraction | hold a floor |
| [3](twopset.md) | 2P-Set, two lists | union, twice | tombstones: a removal is data that beats an add | add a removed tag back; the tombstones grow forever |
| [4](lwwset.md) | LWW-Element-Set, two maps of stamps | per-tag maximum, twice | a stamp per element, so the later write wins and a tag can return; the tie bias | trust the clock; purge the tombstones |
| [5](device-map.md) | the device map | per-device versions, removal markers, a horizon, a byte tie-break | application-raised versions instead of a clock, a purge horizon, a version floor | |

Steps 1 and 2 are about ten lines of logic each and establish the shape: a table gathered from both
sides, a merge that keeps the larger entry, write UDFs that only ever add. Step 3 changes the merge from
a maximum to a union and introduces removal as data. Step 4 puts a timestamp on every element, which is
the first time two sides can disagree about who was later, and shows the bias a tie needs. Step 5 is
what the four steps were preparing: a structure the application can add to, change, remove from and add
back to, with removals that do not accumulate, and with the clock replaced by a version the application
raises.

Read them in order. Each step's chapter opens with the limit of the previous one and closes with its
own.

## The sources

Every step is `examples/<name>` in each SDK, built into `tests/fixtures/wasm/<name>*.wasm`, and the
chapter includes all four sources at the end. The names are `counter`, `pncounter`, `twopset`, `lwwset`
and `crdt`.

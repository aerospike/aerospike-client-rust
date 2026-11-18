# Design decisions

This page records the choices that shaped the UDF API and the engine's behaviour, each with the reason
it went the way it did and the page that describes the result. They are the ones a reader is most
likely to trip over or to ask "why not the other way" about. Where an alternative was considered and
turned down, it is named.

Dates are when a decision was taken. Nothing here has been released, so none of them had to keep an
older behaviour alive.

## The record in a UDF

Decided 2026-11-17. Described in [Record UDFs](record-udfs/README.md) and each language's page.

| Decision | Why |
| --- | --- |
| **The record is an object, handed to the UDF as its first parameter.** It is not a client argument, is not counted when arguments are checked, and does not appear in the signature. A function that does not touch the record leaves it out. | Record operations were free functions on the SDK's namespace, mixed in with the clock, logging and the value builders. Nothing in a function's signature said whether it touched the record at all. |
| **Everything about the record is a method on it.** What stays on the namespace is what is not about the record: the clock, logging, failure, results, builders, arguments. | One place to look, and the same place in every language. |
| **Writing is `save()`, and there is no `create`, `update` or `commit`.** `save()` creates the record when there is none and updates it when there is. | Two verbs made the author ask a question the server answers anyway. `commit` was turned down because it promises a transaction, which an AP namespace does not give. |
| **`delete()` deletes the record; `remove(name)` removes a bin.** There is no `set_nil`, and the verb is `remove`, not `remove_bin`. | `remove` sits beside `set` and the typed readers, all of which take a bin's name; saying "bin" in one of them and not the others was noise. Writing nil through `set` still works and means the same. |
| **There is no `Bin` object.** A bin is addressed by its name on each call. | The Go, C++ and Java SDKs had one. It held a name and nothing else, and gave two ways to do every read and write. |
| **Whether a function may write is in its parameter's type where the language can say so.** Rust takes `&mut Record` or `&Record`; C++ takes `Record&` or `const Record&`. **Go and Java have one `Record` type**, and a write where none is allowed fails at run time. | Go and Java have no way to say "read-only reference" that a caller cannot cast away, and a second type would have doubled the surface for no check the compiler could make. |
| **An event handler's records are `EventRecord`, read-only, sharing one read interface with `Record`.** | A helper that reads bins should work on both. An event's records are views of two sides of a write and have nothing to save. |
| **C keeps functions, with the prefix `as_udf_rec_`.** | C has no objects; a prefix is how it groups. The names of the imports behind them did not change. |

Three things the server does that the API is built around, each verified on a running node before the
names were settled:

- **A save with nothing staged is still a write.** The server treats it as a touch: the generation
  and the last-update time move. On a key with no record it creates nothing.
- **A delete is carried out when the call returns.** Inside the call, `exists()` keeps answering what
  it did before.
- **A stream's record cannot be written**, by any call.

## Lists and maps

Decided 2026-11-17. Described in each language's page.

| Decision | Why |
| --- | --- |
| **Each language uses its own collection verbs.** Rust: `insert`, `get` with a string or an integer key, `at(index)`, `contains_key`. Go: `Len`, `Contains`, `Put` with any key, `Index`. C++: `contains`, `size`, `empty`, `at`. | The SDKs had copied one set of names across languages, so `get` and `set` in Java did not behave as a Java programmer expects a `get` and a `set` to. |
| **Java separates building from reading.** `ListBuilder` and `MapBuilder` build; `ListValue` and `MapValue` read, and convert to and from `java.util`. | A stored collection is read where it lies and cannot be changed; a built one is written once and cannot be indexed cheaply. One type that claimed to be both had methods that failed depending on where the value came from. |
| **Two host calls were added: one element of a stored list by index, and whether a built map holds a key.** | A list could only be walked from its start, and a built map could only be asked by packing the whole of it first, which made a membership test in a loop quadratic. |

## Reading and writing bins

Decided 2026-11-18, each after a defect showed what the rule had to be. Described in
[Security](security.md) and the language pages.

| Decision | Why |
| --- | --- |
| **Removing a bin is writing nil to it.** | That is how the server's record removes one. The call written for it used a hook the server's record does not have, so it had never worked. |
| **A value read from a bin stays readable when the bin is written again, the record is deleted, or a save fails.** The handle holds its own reference. | It used to point into a value the record frees on all three, which was a read of freed memory. |
| **After any save that fails, a list, map or blob value read before it answers an error.** Read the bin again. | Those are read where they lie in the stored record, and a failed save takes back the buffer earlier writes went to. Telling which handles were safe was not possible from the module, so all of them end. |
| **A read sees what the call has staged.** | A list, map or blob bin was read from storage without asking whether a value was staged over it. |
| **A bin name with a zero byte in it is refused.** | The record takes a C string, so `"n\0x"` was the bin `n`. |
| **`set_ttl` is refused where there is no record that can take one**: a stream's record, and an event. | It answered success and did nothing. |
| **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save fails for text the server would not accept as a client's write. Lua UDFs share the rule. | The bin was built anyway, with nothing in it to index. |

## Limits

Decided 2026-11-17 and 2026-11-18. Described in [Security](security.md) and
[Configuration](configuration.md).

| Decision | Why |
| --- | --- |
| **What a guest gets the host to build is charged to the call**, against the per-instance `max-memory`, and reserved against `max-total-memory`. A value is charged in full when made and refunded in full when its last reference goes. | A guest's own memory has a ceiling; strings, lists and maps built through host calls live on the server's heap and had none. |
| **Packed copies of collections are charged too.** | Reading back a collection the call built makes a copy on the server's heap, one a handle. Uncharged, that was 255 times the budget. |
| **`max-memory` is capped at 2 GiB.** | A length of 4 GiB minus one wrapped in 32-bit arithmetic. |
| **The limits are read once, at start.** | There is no code path that changes them under running calls, and so none to get wrong. |

## The `xdr-merge` event

Decided 2026-11-18. Described in [The event system](events/README.md#xdr-merge).

| Decision | Why |
| --- | --- |
| **A handler's answer is checked before it is taken, and what is wrong with it is the handler's failure**, which the binding's `on-failure` decides. | Bad bin names, non-string keys and a non-numeric TTL were found out while the bins were being written, or not at all: some failed the write whatever the binding said, some were silently skipped or stored. |
| **A `record` answer with no bins left is a failure, and there is a `delete` verdict.** | An empty map, or one whose values were all nil, removed the record on the cluster that merged and no other. A merge that built nothing by mistake deleted what it merged. Treating it as `keep-local` was considered; a failure was chosen so that the operator's `on-failure` applies and the mistake is counted. |
| **A `delete` removes the record on the cluster that merged, and on no other.** | Every verdict is local: each cluster decides for itself. A merge that deletes has to reach the same answer on both sides, like any other merge. |
| **`on-failure=fail` is not offered for `xdr-merge`.** | A failed merge that fails the write does not get the write sent again: the source gives up on a write refused for a reason it does not know as temporary. So `fail` kept the local record exactly as `abandon` does and reported it lost. Making it retry was considered and turned down. |
| **An incoming XDR delete is offered to the handler.** | It was, but only on a path that a namespace shipping to another cluster does not take, so between two clusters the handler was never asked. |
| **A handler sees both records unmasked.** | It merges real values and what it returns is stored; masking what it reads would make it store masked values. Refusing to bind on a masked set, and skipping the handler there, were considered. Binding a handler to a masked set is the operator's decision to trust it. |
| **An invalid GeoJSON document, and a record whose values alone exceed `max-record-size`, are handler failures.** | Both used to be found out by the write, which then failed whatever the binding said, or were stored. |
| **A signature's text is cleaned before it is quoted to an operator, and its version is read whole.** | The text is the module's. A version of 4294967297 had bound as version 1. |

## One event

Decided 2026-11-18.

**`xdr-merge` is the only event.** The engine first had fifteen kinds: guards on writes and deletes,
hooks after them, on expiry, eviction, reads, truncation and shipping, cluster and node notices, and
timers. They were removed, with the timer host calls and their SDK support in every language. One kind
could be tested to the standard on this page - every answer a handler can give, under each failure
policy, between two clusters - and the others had not been. The registry and the commands are still
written for kinds in general, so a kind can be added back as a row and a hook, with its own tests.

## How the engine is tested

Decided 2026-11-17 and 2026-11-18. Described in [Security](security.md#fuzzing).

| Decision | Why |
| --- | --- |
| **Host calls are tested on a record that behaves as the server's does.** It frees a value when another replaces it and takes back what a failed write takes back. | The first test record kept everything it was handed. It hid a read of freed memory, a read through a buffer a failed save had freed, and two leaks, through millions of fuzz executions. |
| **The host-call test fails if a host call exists that it does not call.** | A call added later should not be able to go untested by being forgotten. |
| **Every input a fuzzer has broken a target with is kept and replayed by `make check`.** There is no scheduled fuzzing job. | Replay is cheap and needs no special compiler. A scheduled job was not asked for. |
| **The tier production runs is fuzzed too**, under emulation where there is no x86-64 machine. | The interpreter was the only tier that had been. Emulation rules out AddressSanitizer there, which is recorded as a limit. |
| **The whole module runs under AddressSanitizer and ThreadSanitizer, with real SDK-built modules.** | The unit test and the fuzz targets leave out the part several threads are in together. |
| **Findings in the runtime are written up for upstream, not patched around silently.** | The server pins a runtime version; a private fix is a fix that is lost at the next update. |

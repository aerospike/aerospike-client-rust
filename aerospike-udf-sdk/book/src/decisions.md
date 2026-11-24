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
| **`max-memory` covers the memory a module starts with**, checked by the server at registration and at every load. (2026-11-23) | The runtime caps growth only, and gave a module all the initial memory it declared: 128 MiB ran under a cap of 64. |
| **The limits are read once, at start.** | There is no code path that changes them under running calls, and so none to get wrong. |

## One vocabulary across the languages

Decided 2026-11-18. Described in each language's page.

| Decision | Why |
| --- | --- |
| **The same thing has the same words in every SDK**, in the casing and grammar the language asks for. | The SDKs had grown apart: a string bin was read with `text`, `Str`, `as_text` or `getString`, depending on the language, and a module ported from one to another had to be relearned. |
| **The type words are the server's**: `int`, `double`, `bool`, `string`, `blob`, `list`, `map`, `geojson`, `nil`. Rust reads `rec.int(..)`, Go `rec.Int(..)`, C++ `rec.as_int(..)`, Java `rec.getInt(..)`, C `as_udf_rec_int(..)`. | Each language's own words (`long`, `Float64`, `bytes`) were considered. They are consistent inside a language and different across them, and the integer is 64 bits whatever a language calls it. |
| **Record metadata is spelled out**: `generation`, `last_update_time`, `source_id`, `bin_last_update_time`, `bin_source_id`, `bin_count`, `bin_names`, `deleted_bins`. | `gen`, `lut` and `src` were the wire's abbreviations showing through. |
| **The two sides of a merge are `current` and `incoming` everywhere**, in `Pair` and `Both` as in the handler's parameters. **The verdicts stay `keep-local` and `take-remote`.** | Those two are the words the server checks. Renaming the helpers and not the wire would have made the SDK say one thing and send another. |
| **Every language has the same set of readers and helpers**, where it can express them. C has the record-level ones and no `Pair`. | A helper present in one language and not the next read as a missing feature. |
| **What a language cannot say, it says once, differently**: C++ has `delete_record()` because `delete` is a keyword; Java has `sumInt` and `sumDouble` and argument readers named `argInt`; Rust keeps the type names `Text` and `Blob` beside the method `string()`; `is_null` stays where it means nil or absent. | Each is forced by the language, and is the only departure of its kind. |
| **Collection verbs stay each language's own**, as decided above; **names on the wire did not change.** | A Java programmer expects `put` and a Rust one `insert`. Host imports, export names and signature text belong to the server's contract. |

## What a reader answers

Decided 2026-11-19, after the five SDKs were run on the same inputs and disagreed on one case in five.
Described in each language's page.

| Decision | Why |
| --- | --- |
| **A typed reader has three answers: nothing for an absent or nil bin, the value, or a wrong-type error for a bin of another kind.** The same on a record, an event record, a pair, and the elements of a stored list or map. No widening: an integer is not a double. | Go, C and C++ answered "nothing" for a bin of another kind, Rust answered an error, Java threw for both. A UDF that reads a counter stored as a string should not see an absent counter. "Nothing for both" was considered: simpler signatures, but it hides data that is not what the code expects. |
| **A default stands in for an absent or nil bin only.** `int_or` on a bin of another kind is the same wrong-type error. | Go, C and C++ answered the default, so a counter stored as a string quietly read as 0. |
| **The blob reader reads a blob and nothing else.** The bytes of a string, GeoJSON or HLL value are read with the reader named `bytes`. | Three SDKs handed back a string's bytes as a blob and two did not. A separate name says what it does, and loses nothing. |
| **"Not there" is nothing, not an error**, for an index past the end of a list and a key a map lacks. Java's list `get` keeps throwing, as `java.util` does. | Rust answered an error, the others nothing. |
| **Every error carries the host's status.** Java's `UdfException` has `status()`. | Java code could not tell "refused" from "wrong type" without reading the message. |
| **A call takes up to 128 arguments, and one with more fails.** | Each SDK silently dropped arguments past its own limit: 30, 126 or 200. |
| **Nothing is cut or skipped silently.** A buffer too small reports the size it needs; keys, names and compared strings of any length work. | The C++ map builder cut a key to 63 bytes, which merged two keys into one. Several helpers answered "no" for any string past 64 or 128 bytes. |
| **Building a collection element by element never runs out of the call's handles.** | Every add of a double, bool, nil or blob and every put under an integer key took a handle for the rest of the call, so an ordinary loop failed near 250 elements, with an error that said the value had the wrong type. |
| **A string argument that is not UTF-8 is left to the language**: Rust fails the call, Java replaces the bad bytes with U+FFFD, Go, C and C++ pass them through. | Recorded in the conformance harness's allow-list. Failing the call everywhere was considered. |

The conformance module `apitest` is written to one specification in all five SDKs, and the harness in
the WASM module (`make conform`, under each sanitizer, and the `sdk` fuzz target) holds them to these
rules. A difference not on its allow-list fails the run.

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
| **A handler takes the two records and nothing else.** There was a third parameter, a description of the conflict. What a merge can need from it is now asked of the record it is about: whether the incoming write is a delete or carries only changed bins, whether the current record is a tombstone, and whether per-bin timestamps mean anything. Whether the call is a dry run is no longer shown. | Six of the seven merges written for this engine ignored the parameter, and the seventh used it to get back the two records it already had. A merge that reads how it was called, and not only what it was given, is one that can answer differently for the same two records. An optional third parameter was considered and turned down: one shape to document, and one for the code generators to accept. The contract version stays 1, since what the server sends did not change. |
| **Under bin convergence a merged record is stamped bin by bin by what each bin is**: the incoming value keeps the incoming stamp and is not shipped back, this cluster's own value is left as it was, and a value the handler computed is a write made here, which ships. | A merge marked every bin as arrived over XDR. A local write that was still waiting to ship when a merge landed was then never shipped, and the two clusters stayed apart. Found by the first test that ran a handler between two clusters with bin convergence on. Keeping the mark only where the local stamp wins was considered: it leaves a value merged from both sides, such as a maximum, unshipped. |
| **That stamping applies in every mode, and the record is an XDR write only when everything in it came with the incoming write.** Decided 2026-11-24. A merge, or `take-remote` made into one, is written the same way whether or not the namespace converges bins: bins are stamped wherever the namespace keeps metadata per bin, and a record holding anything the other cluster lacks ships back. | Without bin convergence the whole merged record was marked as arrived over XDR. A local change that had not left when the merge landed was kept, then never shipped: with changed-bins shipping every bin took the record's mark and the ship sent nothing; with whole records the first attempt still shipped, but a retry or a recovery read the mark and skipped the record. Under bin convergence too, the ship of the held local write was dropped once the merge changed the record, and the merge itself was never submitted. The two clusters then differed until that bin was written again, and nothing said so. Found by a review and confirmed by two-cluster tests that hold one side's ships for three seconds and take a namespace out of the DC and back. Keeping metadata per bin for every bound record was considered: whole-record shipping reads only the record's mark, so the mark alone is enough there. A merge that does not converge now sends the record back and forth; `shipped-back` counts the rounds. |
| **Whether a merged value is one side's is decided by logical equality**, maps entry by entry in any order. | `as_val_cmp` is an ordering: it compares two maps by size and keys only. A merged counter `{dc1: 5, dc2: 5}` was taken for the incoming `{dc1: 3, dc2: 5}`, stamped as the other cluster's, and never shipped back. Found by the same tests. |
| **`trigger=conflict` is gone.** `event-set` refuses `trigger`; every incoming write to a bound record is offered. The description lost its `trigger` and `lost` entries, and the SDKs their lost-bins accessor (`lost_bins`, `LostBins`, `lostBins`). Nothing had shipped, so nothing was kept for compatibility. | It offered only the writes in which bin convergence would refuse a bin. When one cluster was behind, the other's write won every bin, the handler was never asked, and the cluster that was behind took the bins whole, unmerged. Without bin convergence it never fired at all. |
| **The dry run tells a handler what a live merge on that namespace would**, bin convergence included, and stamps the merged record before it merges it again. | It always said bin convergence was off, so a merge that reads per-bin stamps could not be checked at all, and its idempotence check handed the handler a record with no stamps. |
| **The dry run hands a handler what a live merge would**: bin convergence and per-bin times as the namespace keeps them, the current record's TTL, the TTL the incoming write asks for, and a list of deleted bins even when it is empty. | Each of these differed, and a merge could pass its dry run and answer otherwise between two clusters. A test now runs every handler in every language both ways and compares the records. |
| **A signature's text is cleaned before it is quoted to an operator, and its version is read whole.** | The text is the module's. A version of 4294967297 had bound as version 1. |
| **`abandon` is the default `on-failure`.** Decided 2026-11-24. | A handler that fails has merged nothing. Under `policy` the write went ahead as if no handler were bound, and the receiving cluster took the other's record whole: the loss the handler is there to prevent. Requiring `on-failure` was the other choice; a default that keeps the record does the same without a parameter every binding must repeat. |
| **An abandoned write is a lost conflict with a sub-code, and the source counts it as `merge_abandoned`.** | It had answered a plain `lost-conflict`, which the source counts as a success, so a handler failing on every write looked like a healthy link. A new result code would change what every client reads; the sub-code rides in the error-details field, which the source now asks for (sub-codes only) on every ship. |
| **The counters split a write taken as it came from a collision merged, and failures by cause.** | `record` counted both, so a merge that never met a collision could not be told from one that met nothing else. `take-remote` was counted as `record`. Failures were one number, a timeout and a bad answer alike. |
| **Each failure is logged with the record's namespace, set and digest, ten lines a second per binding at most.** | The log named the module and function only, so a failure could not be followed to the record. The rate limit keeps a handler that fails on every write from flooding the log; a line says how many were not logged. |
| **Every source with an `src-id` sends it, and a node whose own `src-id` is 0 merges nothing.** | The merge needs both ids, and `ship-src-id` was one more thing to remember on every source. A binding reaching a node with no `src-id` through the cluster's metadata is kept, since `src-id` may be set later, and every merge is refused meanwhile. |
| **The handler's input carries no time.** | `now()` is refused in a handler, and a time in the description let the two clusters answer differently all the same. |
| **A `record` answer to an incoming delete is the handler's failure.** | The delete path has no write to make a record with, so the answer was ignored and the delete went ahead: a silent delete of what the handler asked to keep. `take-remote` is the delete; `keep-local` keeps the record. |
| **A `take-remote` that leaves the record as it is counts as `keep-local` and answers `lost-conflict`.** | It wrote the same record again, and the source was told it had changed it. |
| **`module=none` on a set opts it out of the namespace's binding.** | A namespace-wide binding covered every set, with no way to leave one out short of binding it to something else. |
| **`event-set` accepts any contract version from the oldest the server serves to the newest.** | A payload only gains entries, so a handler built for an older version reads what it knows. Exactly one version was accepted, which would have refused every deployed module the day a second one came. |
| **A client cannot call a handler.** | A handler called as a record UDF was handed a UDF's arguments in place of a merge's. Refused before it runs; whether a function is a handler is read once from its signature export and remembered. |
| **The binding lookup takes no lock.** | Every bound XDR write took the registry's lock twice. Bindings are now published as a snapshot replaced whole, and a binding is never changed once published: setting it again makes a new one that keeps the old one's counters. |

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
| **The tier production runs is tested too**, on a real x86-64 machine: the module's tests, the sanitizers, the conformance targets and the fuzzers. (Emulation was used first, where AddressSanitizer does not run.) | The interpreter was the only tier that had been. The real machine found a loop the Fast JIT could not stop, which no emulated run had reached. |
| **The whole module runs under AddressSanitizer and ThreadSanitizer, with real SDK-built modules.** | The unit test and the fuzz targets leave out the part several threads are in together. |
| **Findings in the runtime are written up for upstream.** One that breaks a guarantee the server makes is also patched, in a copy of the runtime built from `patches/runtime/`, never in the pinned submodule. (2026-11-22) | The server pins a runtime version; a private fix is lost at the next update unless it is written down and sent. A loop that outlives its deadline could not wait for an upstream release. |

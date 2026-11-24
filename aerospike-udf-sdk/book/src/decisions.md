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
| **That stamping applies in every mode, and the record is an XDR write only when everything in it came with the incoming write.** Decided 2026-11-24. A merge, or `take-remote` made into one, is written the same way whether or not the namespace converges bins: bins are stamped wherever the namespace keeps metadata per bin, and a record holding anything the other cluster lacks ships back. The shipping back is replaced in the second round: the record's flag stays, and the merge itself ships nothing. | Without bin convergence the whole merged record was marked as arrived over XDR. A local change that had not left when the merge landed was kept, then never shipped: with changed-bins shipping every bin took the record's mark and the ship sent nothing; with whole records the first attempt still shipped, but a retry or a recovery read the mark and skipped the record. Under bin convergence too, the ship of the held local write was dropped once the merge changed the record, and the merge itself was never submitted. The two clusters then differed until that bin was written again, and nothing said so. Found by a review and confirmed by two-cluster tests that hold one side's ships for three seconds and take a namespace out of the DC and back. Keeping metadata per bin for every bound record was considered: whole-record shipping reads only the record's mark, so the mark alone is enough there. A merge that does not converge now sends the record back and forth; `shipped-back` counts the rounds. |
| **Whether a merged value is one side's is decided by logical equality**, maps entry by entry in any order. | `as_val_cmp` is an ordering: it compares two maps by size and keys only. A merged counter `{dc1: 5, dc2: 5}` was taken for the incoming `{dc1: 3, dc2: 5}`, stamped as the other cluster's, and never shipped back. Found by the same tests. |
| **`trigger=conflict` is gone.** `event-set` refuses `trigger`; every incoming write to a bound record is offered. The description lost its `trigger` and `lost` entries, and the SDKs their lost-bins accessor (`lost_bins`, `LostBins`, `lostBins`). Nothing had shipped, so nothing was kept for compatibility. | It offered only the writes in which bin convergence would refuse a bin. When one cluster was behind, the other's write won every bin, the handler was never asked, and the cluster that was behind took the bins whole, unmerged. Without bin convergence it never fired at all. |
| **The dry run tells a handler what a live merge on that namespace would**, bin convergence included, and stamps the merged record before it merges it again. | It always said bin convergence was off, so a merge that reads per-bin stamps could not be checked at all, and its idempotence check handed the handler a record with no stamps. |
| **The dry run hands a handler what a live merge would**: bin convergence and per-bin times as the namespace keeps them, the current record's TTL, the TTL the incoming write asks for, and a list of deleted bins even when it is empty. The TTLs and bin convergence are gone in the second round. | Each of these differed, and a merge could pass its dry run and answer otherwise between two clusters. A test now runs every handler in every language both ways and compares the records. |
| **A signature's text is cleaned before it is quoted to an operator, and its version is read whole.** | The text is the module's. A version of 4294967297 had bound as version 1. |
| **`abandon` is the default `on-failure`.** Decided 2026-11-24. | A handler that fails has merged nothing. Under `policy` the write went ahead as if no handler were bound, and the receiving cluster took the other's record whole: the loss the handler is there to prevent. Requiring `on-failure` was the other choice; a default that keeps the record does the same without a parameter every binding must repeat. |
| **An abandoned write is a lost conflict with a sub-code, and the source counts it as `merge_abandoned`.** Replaced in the second round by a result code of its own. | It had answered a plain `lost-conflict`, which the source counts as a success, so a handler failing on every write looked like a healthy link. A new result code would change what every client reads; the sub-code rides in the error-details field, which the source now asks for (sub-codes only) on every ship. |
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

### Second round

Decided 2026-11-24, from the second review against the PRD. Where a decision above is replaced, the row
here says so.

| Decision | Why |
| --- | --- |
| **A merge ships nothing with `forward` off (R38).** The merge is submitted to XDR as the XDR write it is; the record keeps its own flag, so the local change it folded in still ships with that local write's own ship, retry or recovery. `shipped-back` is gone. Replaces "a record holding anything the other cluster lacks ships back". | Shipping back cost two ships per collision, the whole record under `bin-policy all`, and a handler that breaks the contract sent the record back and forth until it reached the size limit. Without it such a handler leaves the clusters different, which the counters and the failure log show and a manual repair fixes. The one case shipping back healed by itself, a merge that failed on the other cluster, is the same: counted, logged and repaired. |
| **A merged record expires at the later of the two records' expirations, and never-expire wins (R84).** The handler neither sees nor sets an expiration: a `ttl` in its answer is a failure. Replaces the TTLs the handler was shown and could answer. | The write took the incoming expiration before the merge, so each cluster took the other's and they swapped on every exchange; one then expired first, and the other's next write recreated it with only the bins it changed. The TTL shown was read off this cluster's clock, an input a merge must not depend on. |
| **A set bound to a handler gets no bin convergence, whatever `conflict-resolve-writes` says (R28).** Sets with and without a handler share a namespace. A bound set keeps each bin's time and source, never refuses a local write for clock skew, and offers the handler a write that carries no source time. | Bin convergence refused, before the handler ran, an XDR write without a source time, and refused a local write to a bin that had arrived with a later time: under clock skew, a write right after a merge. A bound set behaved differently depending on a namespace setting the handler cannot see. Stamping local writes `max(now, bin time + 1)` instead of refusing them was considered, and turned down: clock skew is the handler's concern, and versions in the data are the answer to it. |
| **Every incoming bin of a bound set is stamped with the incoming time and source (R31, R33)**: the bin's own time, else the write's, else the time the write arrived here; and the handler is shown those times, whatever the namespace ships. | Without bin convergence an incoming bin was stamped with this cluster's merge time and no source, and the handler was shown no per-bin times at all: the two clusters held different times for the same value, and a merge deciding by time could not converge. |
| **Re-replication leaves a bound set's bin times as they were.** | A bin stored without a time of its own reads the record's, and re-replication advances the record's time: the bin's time moved with no write to it. |
| **A failed merge under `abandon` answers a result code of its own, `XDR_MERGE_ABANDONED` (33), with a sub-code for each cause (R42)**: module fault, time limit, memory limit, invalid answer, refused source. Replaces the `LOST_CONFLICT` sub-code. | The PRD asks for a dedicated code. A lost conflict with a sub-code needed the source to ask every destination for error details on every ship to tell the two apart. The source now counts the code as it reads it. |
| **A refused source id is a failure the binding's `on-failure` decides.** Replaces `forbidden` and the `rejected` counter. | The PRD counts it with the other causes. Under `abandon` the source counts it as abandoned, with the refused-source sub-code. |
| **Running out of memory is its own cause, told by the runtime refusing a `memory.grow`.** | A guest allocator that cannot grow aborts, a trap like any other, so it was counted with the traps. |
| **`fired` counts handler calls; `no-times` counts calls on writes that carried no time; `skipped` counts writes applied without a call.** | The PRD's observability asks for the writes a time-based merge cannot decide. |
| **The failure log names the cause and its sub-code, and the user key where the write carries it (R45).** | A digest alone cannot be looked up by the application. |
| **`ship-record-lut` sends the record's time with every write and delete (R32).** | The record's time travelled only with `ship-bin-luts`, which needs changed-bins shipping, or to a connector. |
| **A `delete` answer leaves a durable tombstone; a new `expunge` answer leaves none (R34).** An `expunge` where the namespace forbids non-durable deletes is an invalid answer. | A delete left whatever the incoming write's durability said. An expunge also leaves no tombstone for shipping the delete: a merge ships nothing, so that one would linger. |
| **A `policy` answer is the incoming write applied, as `take-remote` applies it.** A whole record replaces the current one; changed bins are applied over it. | A bound set has no bin convergence, so applying the write is all the policy can be. The old path loaded the current bins for the handler and then applied a whole-record write over them, keeping bins the other cluster no longer has. |
| **Each cluster warns about its own outbound XDR settings that a binding does not allow, and about a bound set without `disable-eviction` (R83).** Checked when the binding is made and when a setting changes; nothing is refused; `event-list` shows the count. | The settings decide whether a merge can converge, and are on the other cluster's side as often as this one's. Refusing them was considered; the PRD asks for warnings. |
| **A write that cannot conflict skips the handler (R86)**: everything the record holds came from the same source by XDR, at or before the incoming write's time, with no local change since. Only for writes that carry a time. | Any merge that keeps the contract returns the incoming write there, so calling it costs a WASM call and changes nothing. Without a time, a retry of an older write could not be told from a newer one. |
| **The dry run is told the incoming record's time (`lut`) as a live merge is, and stamps a value the handler computed as a live merge does.** Replaces "later than both sides". The stamp itself is replaced below (R33). | The dry run gave the incoming bins no time and stamped computed values past both sides, so a merge that is idempotent live failed its dry run once bin times were always there. |
| **A replace into a bound set is not refused because the namespace converges bins (R28).** A client's replace and a whole record XDR ships both reach the record. | The namespace refused every replace while `conflict-resolve-writes` was on, before the handler, so a cluster shipping whole records dropped every write into a bound set as an ordinary abandon, and only a settings warning said why. A bound set has no bin convergence, so the refusal had no reason there. |
| **An older write from the same source is answered as not touched without a call (R86).** The record holds nothing but that source's content, every incoming bin is already here at a later or equal time, and nothing local changed. | Any merge that keeps the contract keeps the current record there. XDR ships a record's current state, so such a write arrives only through delivery reordering; it is rare, and costs a WASM call when it does. |
| **No feature key.** The event system is part of Enterprise Edition, as XDR is. | A gate on `asdb-event-system` was built and never enforced, since no issued key carried it. The PRD decides there is none. |

### Against the PRD

Decided 2026-11-24, comparing the code with the PRD's xdr-merge and CRDT requirements. Where a decision
above is replaced, the row here says so.

| Decision | Why |
| --- | --- |
| **A value the handler computed takes the time of the later of the two inputs' stamps (R33)**: the two sides' stamps for that bin, a side without the bin left out, or, when neither side has it, the two sides' latest stamps. Its source is the merging cluster's own. The dry run stamps the same way. Replaces "a write made here, at the merge's time". | Stamped with the merge's own clock, the same merged value held a different time on each cluster, so a handler deciding by time compared different values on the two sides. Taking the later input's source as well was tried, and two-cluster tests showed why not: XDR carries one source id per write, the sender's, so a value one cluster computed and shipped on arrived under the sender's id while the sender stored the other's. Stamped with its own id, it arrives as stored; where both clusters compute it, the next ship of the bin settles both on the later stamp. |
| **`ship-record-lut` is the coarser of the two ways to ship times.** Every bin of a shipped write takes the record's time. | Any write to the record advances that time, a merge on the sender included, so the receiver holds a later time for a bin than its writer does. Found by the computed-value test; a handler that needs exact times runs with `ship-bin-luts`. |
| **A write that creates a record of a bound set has its source id checked as a merge's is (PRD open question 5).** From a refused source it is counted in `fail-source` and logged, and `on-failure` decides: under `abandon` the record is not created. | No handler runs for a create, so the check was skipped, and a misconfigured source could seed a bound set with records no merge accepted, which later merges took as this cluster's own. The same rule as for an existing record keeps one meaning for `fail-source` and `on-failure`. |
| **Bindings are made with an info command and kept in SMD**, not in the configuration file the PRD's mockup shows. | Modules are registered through SMD, so bindings kept there reach every node the way modules do and cannot name a module a node lacks; they change without a restart, and `event-list` shows them with their counters. The PRD's syntax is a placeholder. |
| **The failure policy that applies the incoming write is `on-failure=policy`**, the PRD's "apply-incoming". | `policy` is the event's one word for what the server would do without a handler: the description's entry, the answer and this setting. Renaming the setting alone would give one thing two names. |
| **An incoming bin with no shipped time shows the time it arrived, unmarked (PRD open question 5).** `no-times` counts the calls. | A handler that decides by time needs the source to ship times, which deployment requires; an arrival time means a link set up wrong, which the counter and the settings warnings report to the operator, who can fix it. A mark would reach only the handler, which cannot. |
| **The incoming write applied under `on-failure=policy` is stamped as a merge stamps (PRD open question 7).** | It is the same apply path. A later merge on either cluster then compares the same times whether the write was merged or applied after a failure. |
| **Handler latency is a mean and a maximum (`avg-us`, `max-us`), not a histogram (R44).** | Two atomic updates on the write path, and enough to see a slow handler. A distribution waits for a question the two cannot answer. |
| **Relays and more than two clusters stay out of the preview (PRD open question 8).** | At a relay with `forward` on and changed bins shipped, a merge's bins keep the sender's earlier times, below the relay's ship cutoff, so they are not forwarded; and a relay ships what it forwards under its own source id. Both change what a far merge compares, and need a three-cluster simulation first. |
| **Record and stream UDFs can read the clock; a handler cannot (R26).** | The PRD forbids the clock in every function kind. A record UDF works on one record on one cluster, as Lua UDFs always have, and removing the call would break modules ported from Lua; a handler must decide the same on both clusters, so it is refused there. A write UDF that stores the time hands it to the merge; the book points such merges to versions. Open for the record-UDF work. |
| **A write UDF can read its cluster's XDR source id, `cluster_src_id` (R25)**; a handler cannot. The CRDT counters key by it. | A per-origin structure needs to know which origin is writing. Passed in by the client as a site name, it could be wrong, and a wrong site corrupts the merge state. A handler is refused for the clock's reason: the answer depends on which cluster runs the merge. |
| **`cluster_src_id` in a handler is an error in Rust and C++, and -1 in C, Go and Java.** | Each SDK reports it as it reports the refused clock: Rust and C++ return a result type, the other three a number. Making Go and Java return an error too would make the five uniform at the cost of matching their own clock call. |
| **`sorted_union` takes integer, string and blob elements; `max_per_key` takes string and integer keys.** Anything else is a type mismatch. | The host sorts the union, since an element read from a list has no stored bytes of its own to compare, and it sorts those three kinds. Map keys are looked up by string or integer; there is no lookup by a blob key. Refusing the rest keeps every cluster to one order. |
| **The C SDK's `as_udf_map_find` is public.** | Every inline function in the C header is API. It is the lookup a merge of two maps needs, a key read from one map looked up in the other, and a merge a user writes needs it as the SDK's own do. |

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

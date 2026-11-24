# C

A C `xdr-merge` handler is declared with `AS_UDF_EVENT_XDR_MERGE(name)`, which emits the signature
`event xdr-merge/1 name(current: record, incoming: record) -> resolution` and the
export, and takes a body with `args` and `argc` in scope. The opening and the close of the account merge
in `examples/resolver/resolver.c`:

```c
AS_UDF_EVENT_XDR_MERGE(merge_account)
{
	as_udf_event_record current, incoming;
	if (as_udf_event_decode(args, argc, &current, &incoming) != AS_UDF_OK) return as_udf_fail_str("merge_account: not an xdr-merge");

	int32_t out = as_udf_map_new(8);
	if (out <= 0) return as_udf_fail_str("merge_account: could not build the record");

	/* visits: the larger of the two counts. */
	as_udf_value cv, iv;
	bool has_c = as_udf_event_bin(&current, "visits", &cv) == AS_UDF_OK && cv.kind == AS_UDF_INT;
	bool has_i = as_udf_event_bin(&incoming, "visits", &iv) == AS_UDF_OK && iv.kind == AS_UDF_INT;
	if (has_c || has_i) {
		int64_t visits = has_c && has_i ? (iv.payload > cv.payload ? iv.payload : cv.payload) : has_c ? cv.payload : iv.payload;
		as_udf_map_put_string_int(out, "visits", 6, visits);
	}

	/* ... the other bins: the whole module is below ... */

	return as_udf_resolve_record(out);
}
```

## The two records

`as_udf_event_decode(args, argc, &current, &incoming)` fills two `as_udf_event_record`s, the record as it
is on this cluster and the one that arrived over XDR, and returns `AS_UDF_ERR` when the arguments are not
those of an `xdr-merge`.

An `as_udf_event_record` carries one side's `generation`, `last_update_time` and `source_id`, and
four stored values: `bin_names` (the list of bin names), `deleted_bins` (the names this side deleted: a list
when there are some, and of kind `AS_UDF_ABSENT`, not an empty list, when there are none), and
`bin_last_update_times` and `bin_source_ids` (per-bin last-update times and source ids, which the server
gives for every bin of both sides). `as_udf_event_bin(&rec, name, &v)` reads a bin
through the event's own import, and the readers a record UDF has are there for a side too, taking the
record first: `as_udf_event_has`, `as_udf_event_get`, `as_udf_event_int` and the other typed readers, which
answer `AS_UDF_OK`, `AS_UDF_NOT_FOUND` or `AS_UDF_TYPE_MISMATCH` as a record UDF's do, and
`as_udf_event_int_or(&rec, name, dflt, &n)` and its family for a default. `as_udf_event_deleted_any(&rec)`
says whether the side deleted a bin, `as_udf_list_count(&rec.deleted_bins)` how many (0 for the absent
shape), and `as_udf_event_with_bins(&rec, out)` puts every bin of one side that the built map `out` lacks into
it, answering 0 (which `as_udf_resolve_record` fails on) when a bin could not be carried over. An event's records are read-only and are read with the `as_udf_event_`
family, never with `as_udf_rec_`, which is for the record of a call and has none in a handler.

What the server says of the merge is a field of the record it is about. On the other record the field is
false:

| Field | What it says |
| --- | --- |
| `incoming.is_delete` | the incoming write is a record delete, so `incoming` has no bins |
| `incoming.is_partial` | the incoming write carries only the bins that changed, so a bin it lacks may well exist on the other cluster |
| `current.is_tombstone` | the current record exists but has no bins |

`as_udf_event_bin_last_update_time` and `as_udf_event_bin_source_id` answer per bin, and fall back to the
record's values for a bin the side neither has nor deleted.
`as_udf_event_incoming_is_newer(&current, &incoming, name)` orders the two records on one bin by LUT, then
source id, so both clusters answer the same. When nothing but the data tells two
values apart, `as_udf_cmp_stored(&a, &b, &order)` compares their stored bytes without copying either: the
status is `AS_UDF_TYPE_MISMATCH` for a value that is not stored, and `order` is exactly -1, 0 or 1, and
`as_udf_stored_bytes` copies them out. `as_udf_max_per_key`, `as_udf_sorted_union`, `as_udf_counter_merge`,
`as_udf_counter_total` and `as_udf_lww_by_version` are the data-only merges of two map or list values.
They are built on `as_udf_map_find(&m, &key, &out)`, which looks up in a stored map a key read from another
map, a string by its bytes and an integer by its value. It is public because every inline function in the
header is API, and it is the one lookup a merge of two maps needs: the key in hand is a value, not a C
string.
`as_udf_list_has_string` searches a stored list of strings, and
`as_udf_map_put_value(out, name, &v)` puts a value read from either side into the map being built, by
position.

## The answers

| Answer | What the server does |
| --- | --- |
| `as_udf_resolve_keep_local()` | keeps the local record and drops the incoming write |
| `as_udf_resolve_take_remote()` | applies the incoming write whole |
| `as_udf_resolve_policy()` | what it would have done without the handler: a set bound to a handler never uses bin convergence, so it applies the incoming write |
| `as_udf_resolve_delete()` | removes the record on this cluster and leaves a durable tombstone, so a late write cannot bring it back; a merge that deletes has to delete on both sides for the two to agree |
| `as_udf_resolve_expunge()` | removes the record on this cluster outright, with no tombstone |
| `as_udf_resolve_record(bins)` | writes the final record: `bins` is a built map in which a bin absent or nil is deleted |

Each returns the status the handler returns. A record answer with no bins left in it is the handler's
failure, not a delete. Neither removal is shipped to the other cluster. No answer carries a TTL: the
merged record takes the later of the two records' expirations, and a record that never expires wins.

## The module

`examples/resolver/resolver.c`, the account merge with `as_udf_event_incoming_is_newer` ordering the two sides
bin by bin:

```c
{{#include ../../../wasm-c/examples/resolver/resolver.c}}
```

## API reference

Generated from `aerospike-udf-sdk/wasm-c/include/aerospike_udf.h`: the event surface.

{{#include ../api/c-events.md}}

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

	return as_udf_resolve_record(out, -1);
}
```

## The two records

`as_udf_event_decode(args, argc, &current, &incoming)` fills two `as_udf_event_record`s, the record as it
is on this cluster and the one that arrived over XDR, and returns `AS_UDF_ERR` when the arguments are not
those of an `xdr-merge`.

An `as_udf_event_record` carries one side's `generation`, `last_update_time`, `ttl` and `source_id`, and
four stored values: `bin_names` (the list of bin names), `deleted_bins` (the names this side deleted: a list
when there are some, and of kind `AS_UDF_ABSENT`, not an empty list, when there are none), and
`bin_last_update_times` and `bin_source_ids` (per-bin last-update times and source ids, where the server
keeps them). `as_udf_event_bin(&rec, name, &v)` reads a bin
through the event's own import, and the readers a record UDF has are there for a side too, taking the
record first: `as_udf_event_has`, `as_udf_event_get`, `as_udf_event_int` and the other typed readers, which
answer `AS_UDF_OK`, `AS_UDF_NOT_FOUND` or `AS_UDF_TYPE_MISMATCH` as a record UDF's do, and
`as_udf_event_int_or(&rec, name, dflt, &n)` and its family for a default. `as_udf_event_deleted_any(&rec)`
says whether the side deleted a bin, `as_udf_list_count(&rec.deleted_bins)` how many (0 for the absent
shape), and `as_udf_event_with_bins(&rec, out)` puts every bin of one side that the built map `out` lacks into
it, answering 0 (which `as_udf_resolve_record` fails on) when a bin could not be carried over. An event's records are read-only and are read with the `as_udf_event_`
family, never with `as_udf_rec_`, which is for the record of a call and has none in a handler.

What the server says of the merge is a field of the record it is about. On the other record the field is
false, and `lost_bins` is `AS_UDF_ABSENT`:

| Field | What it says |
| --- | --- |
| `incoming.is_delete` | the incoming write is a record delete, so `incoming` has no bins |
| `incoming.is_partial` | the incoming write carries only the bins that changed, so a bin it lacks may well exist on the other cluster |
| `incoming.lost_bins` | a stored list: the names of the incoming bins that would lose under bin convergence; `AS_UDF_ABSENT` when there are none (`as_udf_list_count` answers either shape) |
| `current.is_tombstone` | the current record exists but has no bins |
| `uses_bin_convergence` | on either record, the same answer: per-bin LUTs are meaningful, for a time-based merge to check before it trusts `as_udf_event_bin_last_update_time` |

`as_udf_event_bin_last_update_time` and `as_udf_event_bin_source_id` answer per bin and fall back to the record's values.
`as_udf_event_incoming_is_newer(&current, &incoming, name)` orders the two records on one bin by LUT, then
source id: the order the server's bin convergence uses, so both clusters answer the same. When nothing but the data tells two
values apart, `as_udf_cmp_stored(&a, &b, &order)` compares their stored bytes without copying either: the
status is `AS_UDF_TYPE_MISMATCH` for a value that is not stored, and `order` is exactly -1, 0 or 1, and
`as_udf_stored_bytes` copies them out. `as_udf_list_has_string` searches a stored list of strings, and
`as_udf_map_put_value(out, name, &v)` puts a value read from either side into the map being built, by
position.

## The answers

| Answer | What the server does |
| --- | --- |
| `as_udf_resolve_keep_local()` | keeps the local record and drops the incoming write |
| `as_udf_resolve_take_remote()` | applies the incoming write whole |
| `as_udf_resolve_policy()` | lets the namespace's policy decide, as it would have without the handler |
| `as_udf_resolve_delete()` | removes the record on this cluster; a merge that deletes has to delete on both sides for the two to agree |
| `as_udf_resolve_record(bins, ttl)` | writes the final record: `bins` is a built map in which a bin absent or nil is deleted, `ttl` a TTL with a write's meaning, or -1 to keep the one the incoming write asked for |

Each returns the status the handler returns. A record answer with no bins left in it is the handler's
failure, not a delete.

## The module

`examples/resolver/resolver.c`, the account merge with `as_udf_event_incoming_is_newer` ordering the two sides
the way the server's bin convergence does:

```c
{{#include ../../../aerospike-udf-sdk/wasm-c/examples/resolver/resolver.c}}
```

## API reference

Generated from `aerospike-udf-sdk/wasm-c/include/aerospike_udf.h`: the event surface.

{{#include ../api/c-events.md}}

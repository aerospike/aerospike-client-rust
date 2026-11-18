# C

A C `xdr-merge` handler is declared with `AS_UDF_EVENT_XDR_MERGE(name)`, which emits the signature
`event xdr-merge/1 name(current: record, incoming: record, conflict: conflict) -> resolution` and the
export, and takes a body with `args` and `argc` in scope. The opening and the close of the account merge
in `examples/resolver/resolver.c`:

```c
AS_UDF_EVENT_XDR_MERGE(merge_account)
{
	as_udf_conflict c;
	if (as_udf_event_decode(args, argc, &c) != AS_UDF_OK) return as_udf_failf("merge_account: not a conflict");

	int32_t out = as_udf_map_new(8);
	if (out <= 0) return as_udf_failf("merge_account: could not build the record");

	/* visits: the larger of the two counts. */
	as_udf_value lv, rv;
	bool has_l = as_udf_event_bin(&c.local, "visits", &lv) == AS_UDF_OK && lv.kind == AS_UDF_INT;
	bool has_r = as_udf_event_bin(&c.remote, "visits", &rv) == AS_UDF_OK && rv.kind == AS_UDF_INT;
	if (has_l || has_r) {
		int64_t visits = has_l && has_r ? (rv.payload > lv.payload ? rv.payload : lv.payload) : has_l ? lv.payload : rv.payload;
		as_udf_map_put_str_int(out, "visits", 6, visits);
	}

	/* ... the other bins: the whole module is below ... */

	return as_udf_resolve_record(out, -1);
}
```

## The conflict

`as_udf_event_decode(args, argc, &c)` fills an `as_udf_conflict`, and returns `AS_UDF_ERR` when the
arguments are not a conflict:

| Field | What it is |
| --- | --- |
| `c.local`, `c.remote` | the current record and the incoming one, each an `as_udf_event_record` |
| `c.lost` | a stored list: the incoming bins that would lose |
| `c.dry_run` | the binding runs in dry-run mode |
| `c.conflict_trigger` | the binding has `trigger=conflict`: a bin would lose under bin convergence |
| `c.bin_convergence` | the policy the handler stands in for is `bin-lut` rather than a plain apply |
| `c.remote_partial` | the incoming write carries only the bins that changed |
| `c.remote_deleted_record` | the incoming write is a record delete; the remote side has no bins |
| `c.local_tombstone` | the current record is a tombstone: it exists and has no bins |
| `c.description` | the whole description map, for anything not decoded above |

`as_udf_map_get`, `as_udf_map_int`, `as_udf_map_str`, `as_udf_map_bool` and `as_udf_str_is` read the
description's maps.

## Records

An `as_udf_event_record` carries one side's `gen`, `lut`, `ttl` and `src_id`, and four stored values from
the description: `bins` (the list of bin names), `deleted` (the names this side deleted), and `bin_luts`
and `bin_srcs` (per-bin LUTs and source ids, where the server keeps them). `as_udf_event_bin(&rec, name,
&v)` reads a bin through the event's own import. An event's records are read-only and are read with the
`as_udf_event_` family, never with `as_udf_rec_`, which is for the record of a call and has none in a
handler.

`as_udf_event_bin_lut` and `as_udf_event_bin_src` answer per bin and fall back to the record's values.
`as_udf_event_remote_newer(&c, name)` orders the two sides of one bin by LUT, then source id: the order
the server's bin convergence uses, so both clusters answer the same. When nothing but the data tells two
values apart, `as_udf_cmp_stored(&a, &b)` compares their stored bytes without copying either, and
`as_udf_stored_bytes` copies them out. `as_udf_list_has_str` searches a stored list of strings, and
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

`examples/resolver/resolver.c`, the account merge with `as_udf_event_remote_newer` ordering the two sides
the way the server's bin convergence does:

```c
{{#include ../../../aerospike-udf-sdk-wasm-c/examples/resolver/resolver.c}}
```

## API reference

Generated from `aerospike-udf-sdk-wasm-c/include/aerospike_udf.h`: the event surface.

{{#include ../api/c-events.md}}

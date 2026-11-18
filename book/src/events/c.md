# C

A C event handler is declared with one of the `AS_UDF_EVENT_<KIND>(name)` macros, which emits the
signature `event <kind>/1 name(...)` and the export, and takes a body with `args` and `argc` in scope:

```c
AS_UDF_EVENT_BEFORE_WRITE(guard_account)
{
	as_udf_value desc;
	as_udf_event_record before, after;
	as_udf_write_info write;
	if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK ||
			as_udf_event_get_record(&desc, "before", 0, &before) != AS_UDF_OK ||
			as_udf_event_get_record(&desc, "after", 1, &after) != AS_UDF_OK ||
			as_udf_event_write_info(&desc, &write) != AS_UDF_OK) {
		return as_udf_failf("guard_account: not a before-write event");
	}

	as_udf_value balance;
	if (as_udf_event_bin(&after, "balance", &balance) == AS_UDF_OK && balance.kind == AS_UDF_INT && balance.payload < 0) {
		return as_udf_answer_reject(22, "balance may not go negative");
	}

	int32_t out = as_udf_map_new(4);
	as_udf_map_put_str_str(out, "stamped", 7, write.source, as_udf_strlen(write.source));
	with_bins(&after, out); /* every bin of `after` not already in `out`, by position */
	return as_udf_answer_record(out, -1);
}
```

`with_bins` is the module's own helper; `examples/guard/guard.c` below has it. It asks
`as_udf_map_has(out, name, len)` for each bin of the record, so what the handler put in the map wins.

## Records

`as_udf_event_desc(args, argc, &desc)` reads the description; `as_udf_event_get_record(&desc, key, which,
&rec)` fills an `as_udf_event_record` from its entry (`before`, `after`, `record`; `local` and `remote`
for the conflict); `which` is the index the record is read by. `as_udf_event_bin(&rec, name, &v)` reads a
bin through the event's own import. An event's records are read-only and are read with the
`as_udf_event_` family, never with `as_udf_rec_`, which is for the record of a call and has none in a
handler. The timer is the exception, below. `rec.bins`, `rec.deleted`, `rec.bin_luts`, `rec.bin_srcs` are the
lists and maps from the description, and `as_udf_event_bin_lut` / `as_udf_event_bin_src` fall back to
the record's values. `as_udf_map_get`, `as_udf_map_int`, `as_udf_map_str`, `as_udf_map_bool` and
`as_udf_str_is` read the description's maps; `as_udf_list_has_str` searches a stored list of strings.

## The kinds

| Kind | Declaration | Decoder | Answer helpers |
| --- | --- | --- | --- |
| `before-write` | `AS_UDF_EVENT_BEFORE_WRITE` | `as_udf_event_get_record` ×2, `as_udf_event_write_info` | `as_udf_answer_accept`, `_reject`, `_record` |
| `before-delete` | `AS_UDF_EVENT_BEFORE_DELETE` | `as_udf_event_get_record`, `as_udf_event_delete_info` | `_allow`, `_refuse` |
| `after-write`, `after-delete` | `AS_UDF_EVENT_AFTER_WRITE`, `_DELETE` | as above | `_done` |
| `before-expire` | `AS_UDF_EVENT_BEFORE_EXPIRE` | `as_udf_event_expire_info` | `_expire`, `_keep(ttl)` |
| `before-evict` | `AS_UDF_EVENT_BEFORE_EVICT` | `as_udf_event_expire_info` | `_evict`, `_protect` |
| `after-read` | `AS_UDF_EVENT_AFTER_READ` | `as_udf_event_read_info` | `_unchanged`, `_record` |
| `before-truncate`, `after-truncate` | `AS_UDF_EVENT_BEFORE_TRUNCATE`, `_AFTER_TRUNCATE` | `as_udf_event_truncate_info` | `_allow`, `_refuse` / `_done` |
| `before-ship` | `AS_UDF_EVENT_BEFORE_SHIP` | `as_udf_event_ship_info` | `_ship`, `_skip`, `_record` |
| `partition-transfer` | `AS_UDF_EVENT_PARTITION_TRANSFER` | `as_udf_event_transfer_info` | `_done` |
| `cluster-change` | `AS_UDF_EVENT_CLUSTER_CHANGE` | `as_udf_event_cluster_info` | `_done` |
| `node-lifecycle` | `AS_UDF_EVENT_NODE_LIFECYCLE` | `as_udf_event_lifecycle_info` | `_done` |
| `timer` | `AS_UDF_EVENT_TIMER` | `as_udf_event_timer_info(args, argc, &t)` | `_done`; runs as a record UDF |
| `xdr-merge` | `AS_UDF_EVENT_XDR_MERGE` | `as_udf_event_decode` (`conflict_trigger` for `trigger=conflict`); `as_udf_cmp_stored`, `as_udf_stored_bytes` for a data-only tie-break | `as_udf_resolve_keep_local`, `_take_remote`, `_policy`, `_record` |

`as_udf_answer(verdict, code, message, bins, ttl)` is the general form behind the helpers;
`AS_UDF_EVENT_HANDLER(name, "kind", "(params) -> answer")` the general form behind the declarations.

## Timers

A timer handler runs as a record UDF, on the record the timer was set on. So it is the one handler in
which the `as_udf_rec_` family works: it reads with `as_udf_rec_bin`, stages with `as_udf_rec_set_int`
and the rest, and saves with `as_udf_rec_save()` like any record UDF. `as_udf_timer_set("tick", 4, 200)`
re-arms it; `as_udf_timer_cancel` cancels. Both are refused in any other handler.

```c
AS_UDF_EVENT_TIMER(tick)
{
	as_udf_timer_info t;
	if (as_udf_event_timer_info(args, argc, &t) != AS_UDF_OK) return as_udf_failf("tick: not a timer");

	as_udf_value ticks;
	int64_t n = as_udf_rec_bin("ticks", &ticks) == AS_UDF_OK && ticks.kind == AS_UDF_INT ? ticks.payload : 0;
	n++;

	as_udf_rec_set_int("ticks", 5, n);
	as_udf_rec_save();

	if (n < 3) as_udf_timer_set("tick", 4, 200);

	return as_udf_answer_done();
}
```

## The modules

`examples/guard/guard.c`:

```c
{{#include ../../../aerospike-udf-sdk-wasm-c/examples/guard/guard.c}}
```

`examples/resolver/resolver.c`, the account merge with `as_udf_event_remote_newer` ordering the two sides
the way the server's bin convergence does:

```c
{{#include ../../../aerospike-udf-sdk-wasm-c/examples/resolver/resolver.c}}
```

## API reference

Generated from `aerospike-udf-sdk-wasm-c/include/aerospike_udf.h`: the event surface.

{{#include ../api/c-events.md}}

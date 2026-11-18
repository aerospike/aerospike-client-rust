/*
 * Copyright 2015-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Guards in C: the same handlers as aerospike-udf-sdk-wasm/examples/guard - a before-write that refuses a
 * negative balance and stamps every write that gets through, a before-delete lock, a truncate fence, and
 * a timer that counts ticks and re-arms itself up to three.
 */

#include "aerospike_udf.h"

/* Copy the second record's bins, with `out`'s entries on top, into `out`. */
static void with_bins(const as_udf_event_record* rec, int32_t out)
{
	as_udf_value w[32];
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&rec->bins), as_udf_value_at(&rec->bins));
	if (cur <= 0) return;
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			char name[32];
			int32_t len = as_udf_bytes(&w[i], name, (int32_t)sizeof(name) - 1);
			if (len < 0) continue;
			name[len] = '\0';
			/* What the handler put there wins. The host answers for the map it is building, so nothing
			 * is packed to ask. */
			if (as_udf_map_has(out, name, len) > 0) continue;
			as_udf_value v;
			if (as_udf_event_bin(rec, name, &v) == AS_UDF_OK) as_udf_map_put_value(out, name, &v);
		}
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
}

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

	as_udf_value writes;
	int64_t n = as_udf_event_bin(&before, "writes", &writes) == AS_UDF_OK && writes.kind == AS_UDF_INT ? writes.payload : 0;

	int32_t out = as_udf_map_new(4);
	if (out <= 0) return as_udf_failf("guard_account: could not build the record");
	as_udf_map_put_str_str(out, "stamped", 7, write.source, as_udf_strlen(write.source));
	as_udf_map_put_str_int(out, "writes", 6, n + 1);
	with_bins(&after, out);
	return as_udf_answer_record(out, -1);
}

AS_UDF_EVENT_BEFORE_DELETE(guard_delete)
{
	as_udf_value desc;
	as_udf_event_record rec;
	if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK || as_udf_event_get_record(&desc, "record", 0, &rec) != AS_UDF_OK) {
		return as_udf_failf("guard_delete: not a before-delete event");
	}
	as_udf_value locked;
	if (as_udf_event_bin(&rec, "locked", &locked) == AS_UDF_OK && locked.kind == AS_UDF_BOOL && locked.payload != 0) {
		return as_udf_answer_refuse(22, "record is locked");
	}
	return as_udf_answer_allow();
}

AS_UDF_EVENT_BEFORE_TRUNCATE(guard_truncate)
{
	as_udf_value desc;
	as_udf_truncate_info t;
	if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK || as_udf_event_truncate_info(&desc, &t) != AS_UDF_OK) {
		return as_udf_failf("guard_truncate: not a before-truncate event");
	}
	if (t.set[0] == 'k' && t.set[1] == 'e' && t.set[2] == 'e' && t.set[3] == 'p') {
		return as_udf_answer_refuse(22, "this set is protected from truncation");
	}
	return as_udf_answer_allow();
}

/* The timer handler runs as a record UDF: the as_udf_rec_* functions work on the record the timer is on. */
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

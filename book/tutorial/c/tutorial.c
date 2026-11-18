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
 * The C side of the book's tutorials. The page record-udfs/tutorial.md includes the code between the ANCHOR
 * comments, so what the book shows is what compiles here.
 */

// ANCHOR: setup
#include "aerospike_udf.h"
// ANCHOR_END: setup

// ANCHOR: visit
AS_UDF_EXPORT(visit) int32_t
visit(int32_t args, int32_t argc)
{
	as_udf_value by;
	if (as_udf_args(args, argc, &by, 1) < 1 || by.kind != AS_UDF_INT) return as_udf_failf("visit: by must be an integer");

	as_udf_value v;
	int64_t n = as_udf_rec_bin("visits", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;   /* 0 when absent */
	n += by.payload;

	as_udf_rec_set_int("visits", 6, n);                                    /* staged, not yet written */
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("visit: could not save");   /* creates the record when there is none */

	as_udf_result_int(n);
	return 0;
}
AS_UDF_SIGNATURE(visit, "fn visit(by: int) -> value")
// ANCHOR_END: visit

// ANCHOR: withdraw
AS_UDF_EXPORT(withdraw) int32_t
withdraw(int32_t args, int32_t argc)
{
	as_udf_value amount;
	if (as_udf_args(args, argc, &amount, 1) < 1 || amount.kind != AS_UDF_INT) return as_udf_failf("withdraw: amount must be an integer");

	as_udf_value balance;
	if (as_udf_rec_bin("balance", &balance) != AS_UDF_OK) return as_udf_failf("withdraw: bin balance is absent");
	if (balance.kind != AS_UDF_INT) return as_udf_failf("withdraw: bin balance is not an integer");

	if (amount.payload > balance.payload) return as_udf_failf("insufficient funds");

	as_udf_rec_set_int("balance", 7, balance.payload - amount.payload);
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("withdraw: could not save");

	as_udf_result_int(balance.payload - amount.payload);
	return 0;
}
AS_UDF_SIGNATURE(withdraw, "fn withdraw(amount: int) -> value")
// ANCHOR_END: withdraw

// ANCHOR: rename
AS_UDF_EXPORT(rename) int32_t
rename(int32_t args, int32_t argc)
{
	as_udf_value arg;
	char name[64];
	int32_t len = -1;

	if (as_udf_args(args, argc, &arg, 1) == 1 && arg.kind == AS_UDF_STR) {
		len = as_udf_bytes(&arg, name, (int32_t)sizeof(name) - 1);     /* the string's bytes, copied out */
	}
	if (len < 0) return as_udf_failf("rename: name must be a string of at most 63 bytes");
	name[len] = '\0';

	as_udf_value owner;
	if (as_udf_rec_bin("owner", &owner) != AS_UDF_OK || owner.kind != AS_UDF_STR) return as_udf_failf("rename: no string bin owner");

	if (as_udf_str_is(&owner, name)) {
		as_udf_result_bool(0);
		return 0;
	}

	as_udf_rec_set_str("owner", 5, name, len);
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("rename: could not save");

	as_udf_result_bool(1);
	return 0;
}
AS_UDF_SIGNATURE(rename, "fn rename(name: str) -> value")
// ANCHOR_END: rename

// ANCHOR: total
AS_UDF_EXPORT(total) int32_t
total(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value history;
	if (as_udf_rec_bin("history", &history) != AS_UDF_OK || history.kind != AS_UDF_LIST) return as_udf_failf("total: no list bin history");

	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&history), as_udf_value_at(&history));
	if (cur <= 0) return as_udf_failf("total: could not open the list");

	as_udf_value w[128];                                               /* 128 values per import */
	int64_t sum = 0;
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			if (w[i].kind != AS_UDF_INT) { as_udf_cursor_close(cur); return as_udf_failf("total: not an integer"); }
			sum += w[i].payload;
		}
		if (n < 128) break;
	}
	as_udf_cursor_close(cur);

	as_udf_result_int(sum);
	return 0;
}
AS_UDF_SIGNATURE(total, "fn total() -> value")
// ANCHOR_END: total

// ANCHOR: limit_for
AS_UDF_EXPORT(limit_for) int32_t
limit_for(int32_t args, int32_t argc)
{
	as_udf_value arg;
	char kind[64];
	int32_t len = -1;

	if (as_udf_args(args, argc, &arg, 1) == 1 && arg.kind == AS_UDF_STR) len = as_udf_bytes(&arg, kind, (int32_t)sizeof(kind));
	if (len < 0) return as_udf_failf("limit_for: kind must be a string of at most 64 bytes");

	as_udf_value limits;
	if (as_udf_rec_bin("limits", &limits) != AS_UDF_OK || limits.kind != AS_UDF_MAP) return as_udf_failf("limit_for: no map bin limits");

	as_udf_value limit;
	int32_t rc = as_udf_coll_get(as_udf_value_src(&limits), as_udf_value_at(&limits), kind, len, &limit, (int32_t)sizeof(limit));

	if (rc == AS_UDF_NOT_FOUND) {                                      /* no such key */
		as_udf_result_int(0);
		return 0;
	}
	if (rc != AS_UDF_OK || limit.kind != AS_UDF_INT) return as_udf_failf("limit_for: the limit is not an integer");

	as_udf_result_int(limit.payload);
	return 0;
}
AS_UDF_SIGNATURE(limit_for, "fn limit_for(kind: str) -> value")
// ANCHOR_END: limit_for

// ANCHOR: limits_total
AS_UDF_EXPORT(limits_total) int32_t
limits_total(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value limits;
	if (as_udf_rec_bin("limits", &limits) != AS_UDF_OK || limits.kind != AS_UDF_MAP) return as_udf_failf("limits_total: no map bin limits");

	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&limits), as_udf_value_at(&limits));
	if (cur <= 0) return as_udf_failf("limits_total: could not open the map");

	as_udf_value w[128];                                               /* a map yields key, value, key, value */
	int64_t sum = 0;
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 1; i < n; i += 2) {
			if (w[i].kind == AS_UDF_INT) sum += w[i].payload;
		}
		if (n < 128) break;
	}
	as_udf_cursor_close(cur);

	as_udf_result_int(sum);
	return 0;
}
AS_UDF_SIGNATURE(limits_total, "fn limits_total() -> value")
// ANCHOR_END: limits_total

// ANCHOR: log_payment
AS_UDF_EXPORT(log_payment) int32_t
log_payment(int32_t args, int32_t argc)
{
	as_udf_value amount;
	if (as_udf_args(args, argc, &amount, 1) < 1 || amount.kind != AS_UDF_INT) return as_udf_failf("log_payment: amount must be an integer");

	as_udf_value history;
	if (as_udf_rec_bin("history", &history) != AS_UDF_OK || history.kind != AS_UDF_LIST) return as_udf_failf("log_payment: no list bin history");

	int32_t count = (int32_t)history.len + 1;
	int32_t out = as_udf_list_new(count);
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&history), as_udf_value_at(&history));
	if (out <= 0 || cur <= 0) return as_udf_failf("log_payment: could not build the list");

	as_udf_value w[128];
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		as_udf_list_push_values(out, w, n * (int32_t)sizeof(as_udf_value));   /* the stored elements, a batch at a time */
		if (n < 128) break;
	}
	as_udf_cursor_close(cur);
	as_udf_list_push_int(out, amount.payload);                         /* then the new one */

	as_udf_rec_set_handle("history", 7, out);
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("log_payment: could not save");

	as_udf_result_int(count);
	return 0;
}
AS_UDF_SIGNATURE(log_payment, "fn log_payment(amount: int) -> value")
// ANCHOR_END: log_payment

// ANCHOR: summary
AS_UDF_EXPORT(summary) int32_t
summary(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value owner, balance;
	if (as_udf_rec_bin("owner", &owner) != AS_UDF_OK || as_udf_rec_bin("balance", &balance) != AS_UDF_OK) return as_udf_failf("summary: no owner or balance");

	int32_t out = as_udf_map_new(5);
	if (out <= 0) return as_udf_failf("summary: could not build the map");

	as_udf_map_put_value(out, "owner", &owner);                        /* a stored value goes in as it is */
	as_udf_map_put_value(out, "balance", &balance);
	as_udf_map_put_str_int(out, "generation", 10, as_udf_rec_gen());
	as_udf_map_put_str_int(out, "bins", 4, as_udf_rec_numbins());
	as_udf_map_put_str_int(out, "ttl", 3, as_udf_rec_ttl());

	as_udf_result_handle(out);
	return 0;
}
AS_UDF_SIGNATURE(summary, "fn summary() -> value")
// ANCHOR_END: summary

// ANCHOR: renew
AS_UDF_EXPORT(renew) int32_t
renew(int32_t args, int32_t argc)
{
	as_udf_value seconds;
	if (as_udf_args(args, argc, &seconds, 1) < 1 || seconds.kind != AS_UDF_INT) return as_udf_failf("renew: seconds must be an integer");

	as_udf_value v;
	int64_t n = as_udf_rec_bin("renewals", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;
	n++;

	as_udf_rec_set_int("renewals", 8, n);
	if (as_udf_rec_set_ttl((int32_t)seconds.payload) != AS_UDF_OK) return as_udf_failf("renew: the TTL was refused");   /* takes effect with the save */
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("renew: could not save");

	as_udf_result_int(n);
	return 0;
}
AS_UDF_SIGNATURE(renew, "fn renew(seconds: int) -> value")
// ANCHOR_END: renew

// ANCHOR: clear_note
AS_UDF_EXPORT(clear_note) int32_t
clear_note(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_rec_remove("note", 4);                                      /* staged, like a set */
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_failf("clear_note: could not save");

	as_udf_result_nil();
	return 0;
}
AS_UDF_SIGNATURE(clear_note, "fn clear_note() -> value")
// ANCHOR_END: clear_note

// ANCHOR: close
AS_UDF_EXPORT(close) int32_t
close(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	if (as_udf_rec_delete() != AS_UDF_OK) return as_udf_failf("close: could not delete the record");   /* no save: the delete is the write */

	as_udf_result_nil();
	return 0;
}
AS_UDF_SIGNATURE(close, "fn close() -> value")
// ANCHOR_END: close

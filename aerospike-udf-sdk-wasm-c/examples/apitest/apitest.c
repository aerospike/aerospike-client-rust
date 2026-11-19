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
 * The conformance module of the C SDK: the same `apitest` every guest SDK has, written to one specification
 * so that a harness can call each export of all of them with the same record and the same arguments and
 * compare the answers. Nothing here is an example to copy; it is the header's public API used the way a UDF
 * would use it, one export per corner.
 *
 * Almost every export answers a list whose first element is a tag: "some", "none", "wrong", or a status named
 * after the host's code ("ok", "err", "absent", "refused", "small").
 *
 * What C does its own way, because the header has no more to offer:
 *   - There is no declared signature: every export reads its arguments with as_udf_args and checks them by
 *     hand, and the AS_UDF_SIGNATURE texts below are written out, not derived.
 *   - A string argument that names a bin is copied into a 64 KiB buffer and NUL-terminated, because the
 *     readers (as_udf_rec_int and the rest) take a NUL-terminated name.
 *   - A typed reader answers a status and fills an out-parameter: AS_UDF_NOT_FOUND is "none" and
 *     AS_UDF_TYPE_MISMATCH is "wrong".
 *   - The arguments are read into an array the export sizes, so every export checks `argc` against what it
 *     declares and fails when the client passed more.
 *   - There is no Pair and no Both: the merge handler leaves out the bins that need them.
 *
 * Build: clang --target=wasm32 -O3 -nostdlib -ffreestanding -Wl,--no-entry -Wl,--export-dynamic \
 *            -I../../include -o apitest_c.wasm apitest.c
 */

#include "aerospike_udf.h"

/* The longest string argument the module copies out: a bin name, the text of `txt`, GeoJSON text. */
#define STR_MAX 65536
/* The largest string or blob the module reads from a bin or builds for `mk`. */
#define BIG_MAX (1 << 20)
/* How many arguments `ar` reads, the first two and the rest: the most a UDF takes. */
#define ARGS_MAX AS_UDF_MAX_ARGS
/* The largest `n` that `mk` builds for; anything over, or negative, is refused before a builder is called. */
#define MK_MAX 1000000
/* How many elements `wr` hands to as_udf_rec_set_ints and as_udf_rec_set_doubles. */
#define BATCH_MAX 256

static char S1[STR_MAX + 1];
static char S2[STR_MAX + 1];
static uint8_t BIG[BIG_MAX];
static int64_t INTS[BATCH_MAX];
static double DOUBLES[BATCH_MAX];

static const char GEO[] = "{\"type\":\"Point\",\"coordinates\":[1,2]}";

/*==========================================================
 * Small things.
 */

/* The decimal text of `n` into `out`, which holds at least 21 bytes. Returns the length; no terminator. */
static int32_t fmt_int(char* out, int64_t n)
{
	char tmp[20];
	int32_t i = 0, len = 0;
	uint64_t u = n < 0 ? 0 - (uint64_t)n : (uint64_t)n;
	if (n < 0) out[len++] = '-';
	do {
		tmp[i++] = (char)('0' + u % 10);
		u /= 10;
	} while (u != 0);
	while (i > 0) out[len++] = tmp[--i];
	return len;
}

/* A sum that wraps: signed overflow is undefined in C, and the stored integers are anything. */
static int64_t wrap_add(int64_t a, int64_t b)
{
	return (int64_t)((uint64_t)a + (uint64_t)b);
}

/* The tag of a status, named after the host's code. */
static const char* tag_of(int32_t rc)
{
	if (rc == AS_UDF_OK) return "ok";
	if (rc == AS_UDF_NOT_FOUND) return "absent";
	if (rc == AS_UDF_TYPE_MISMATCH) return "wrong";
	if (rc == AS_UDF_REFUSED) return "refused";
	if (rc <= AS_UDF_NEEDED_BASE) return "small";
	return "err";
}

/* The kind of a value, as `val` and `ar` answer it. */
static const char* kind_name(const as_udf_value* v)
{
	switch (v->kind) {
	case AS_UDF_NIL: return "nil";
	case AS_UDF_BOOL: return "bool";
	case AS_UDF_INT: return "int";
	case AS_UDF_DOUBLE: return "double";
	case AS_UDF_STR: return "string";
	case AS_UDF_BLOB: return "blob";
	case AS_UDF_LIST: return "list";
	case AS_UDF_MAP: return "map";
	case AS_UDF_GEOJSON: return "geojson";
	case AS_UDF_HLL: return "hll";
	default: return "other";
	}
}

/* Fail the call with a message made of parts: "<fn>: argument <i + 1> (<param>) <what>", or, with `i` negative,
 * "<fn>: <what>" followed by `n` when `with_n`. Returns the status the export returns. */
static int32_t fail_parts(const char* fn, int32_t i, const char* param, const char* what, bool with_n, int64_t n)
{
	char msg[256];
	int32_t len = 0;
	const char* parts[4] = { fn, ": ", "", "" };
	char index[24], number[24];
	if (i >= 0) {
		index[fmt_int(index, (int64_t)i + 1)] = '\0';
		parts[2] = "argument ";
		parts[3] = index;
	}
	for (int32_t p = 0; p < 4; p++) {
		for (const char* s = parts[p]; *s != '\0' && len < 200; s++) msg[len++] = *s;
	}
	if (i >= 0) {
		msg[len++] = ' ';
		msg[len++] = '(';
		for (const char* s = param; *s != '\0' && len < 220; s++) msg[len++] = *s;
		msg[len++] = ')';
		msg[len++] = ' ';
	}
	for (const char* s = what; *s != '\0' && len < 230; s++) msg[len++] = *s;
	if (with_n) {
		number[fmt_int(number, n)] = '\0';
		for (const char* s = number; *s != '\0'; s++) msg[len++] = *s;
	}
	as_udf_fail(msg, len);
	return 1;
}

/* Whether the client passed no more arguments than the export declares; fails the call, naming the limit and
 * the count, when it passed more. */
static bool arity(const char* fn, int32_t argc, int32_t max)
{
	if (argc <= max) return true;
	char msg[96];
	int32_t len = 0;
	for (const char* s = fn; *s != '\0' && len < 32; s++) msg[len++] = *s;
	for (const char* s = ": takes at most "; *s != '\0'; s++) msg[len++] = *s;
	len += fmt_int(msg + len, max);
	for (const char* s = " arguments, got "; *s != '\0'; s++) msg[len++] = *s;
	len += fmt_int(msg + len, argc);
	as_udf_fail(msg, len);
	return false;
}

/* Fail because an argument is not what the export takes. */
static int32_t arg_fail(const char* fn, int32_t i, const char* param, const char* what)
{
	return fail_parts(fn, i, param, what, false, 0);
}

/* Whether argument `i` is there and a string; fails the call when it is not. */
static bool arg_is_string(const char* fn, const as_udf_value* a, int32_t n, int32_t i, const char* param)
{
	if (i >= n) {
		arg_fail(fn, i, param, "is missing");
		return false;
	}
	if (a[i].kind != AS_UDF_STR) {
		arg_fail(fn, i, param, "is not a string");
		return false;
	}
	return true;
}

/* Argument `i` as a string of at most STR_MAX bytes, copied into `out` (STR_MAX + 1 bytes) and NUL-terminated,
 * with its length in `len`. Fails the call, and returns false, when it is missing, not a string or longer. */
static bool arg_string(const char* fn, const as_udf_value* a, int32_t n, int32_t i, const char* param, char* out, int32_t* len)
{
	if (! arg_is_string(fn, a, n, i, param)) return false;
	int32_t got = as_udf_as_string(&a[i], out, STR_MAX);
	if (got <= AS_UDF_NEEDED_BASE) {
		fail_parts(fn, i, param, "is longer than 65536 bytes: ", true, (int64_t)a[i].len);
		return false;
	}
	if (got < 0) {
		fail_parts(fn, i, param, "could not be read: ", true, got);
		return false;
	}
	out[got] = '\0';
	*len = got;
	return true;
}

/*==========================================================
 * The answer: a list built on the host, with the first refusal kept, as the C++ SDK's List keeps it.
 */

static int32_t OUT;
static int32_t OUT_RC;

static void out_rc(int32_t rc)
{
	if (rc != AS_UDF_OK && OUT_RC == AS_UDF_OK) OUT_RC = rc;
}

static void out_new(void)
{
	OUT = as_udf_list_new(8);
	OUT_RC = OUT > 0 ? AS_UDF_OK : AS_UDF_ERR;
}

static void out_text(const char* s) { out_rc(as_udf_list_push_string(OUT, s, as_udf_strlen(s))); }
static void out_string(const char* s, int32_t len) { out_rc(as_udf_list_push_string(OUT, s, len)); }
static void out_blob(const void* b, int32_t len) { out_rc(as_udf_list_push_blob(OUT, b, len)); }
static void out_int(int64_t n) { out_rc(as_udf_list_push_int(OUT, n)); }
static void out_double(double d) { out_rc(as_udf_list_push_double(OUT, d)); }
static void out_bool(bool b) { out_rc(as_udf_list_push_bool(OUT, b)); }
static void out_nil(void) { out_rc(as_udf_list_push_nil(OUT)); }
static void out_value(const as_udf_value* v) { out_rc(as_udf_list_push_value(OUT, v)); }

/* A value that may not be there: nil when it is absent. */
static void out_value_or_nil(const as_udf_value* v)
{
	if (v->kind == AS_UDF_ABSENT) {
		out_nil();
	}
	else {
		out_value(v);
	}
}

/* Set the answer as the result, or fail the call when it could not be built. */
static int32_t out_done(const char* fn)
{
	if (OUT_RC == AS_UDF_OK) OUT_RC = as_udf_result_handle(OUT);
	if (OUT_RC != AS_UDF_OK) return fail_parts(fn, -1, "", "could not build the answer: ", true, OUT_RC);
	return 0;
}

/*==========================================================
 * rd(name, how): one reader of the record.
 */

/* The tag of a typed reader's status: nothing there is "none", another kind is "wrong". */
static const char* read_tag(int32_t rc)
{
	return rc == AS_UDF_NOT_FOUND ? "none" : tag_of(rc);
}

AS_UDF_EXPORT(rd) int32_t
rd(int32_t args, int32_t argc)
{
	as_udf_value a[2], v;
	if (! arity("rd", argc, 2)) return 1;
	int32_t n = as_udf_args(args, argc, a, 2);
	int32_t name_len, rc;
	if (! arg_string("rd", a, n, 0, "name", S1, &name_len)) return 1;
	if (! arg_is_string("rd", a, n, 1, "how")) return 1;
	const char* name = S1;
	const as_udf_value* how = &a[1];

	out_new();

	if (as_udf_string_is(how, "has")) {
		out_text("some");
		out_bool(as_udf_rec_has(name));
	}
	else if (as_udf_string_is(how, "get")) {
		if (as_udf_rec_get(name, &v)) {
			out_text("some");
			out_value(&v);
		}
		else {
			out_text("none");
		}
	}
	else if (as_udf_string_is(how, "bin")) {
		rc = as_udf_rec_bin(name, &v);
		if (rc == AS_UDF_OK) {
			out_text("some");
			out_value(&v);
		}
		else {
			out_text(tag_of(rc));
		}
	}
	else if (as_udf_string_is(how, "int")) {
		int64_t x;
		rc = as_udf_rec_int(name, &x);
		out_text(rc == AS_UDF_OK ? "some" : read_tag(rc));
		if (rc == AS_UDF_OK) out_int(x);
	}
	else if (as_udf_string_is(how, "double")) {
		double x;
		rc = as_udf_rec_double(name, &x);
		out_text(rc == AS_UDF_OK ? "some" : read_tag(rc));
		if (rc == AS_UDF_OK) out_double(x);
	}
	else if (as_udf_string_is(how, "bool")) {
		bool x;
		rc = as_udf_rec_bool(name, &x);
		out_text(rc == AS_UDF_OK ? "some" : read_tag(rc));
		if (rc == AS_UDF_OK) out_bool(x);
	}
	else if (as_udf_string_is(how, "string")) {
		int32_t got = as_udf_rec_string(name, (char*)BIG, BIG_MAX);
		if (got >= 0) {
			out_text("some");
			out_string((const char*)BIG, got);
		}
		else {
			out_text(read_tag(got));
		}
	}
	else if (as_udf_string_is(how, "blob")) {
		int32_t got = as_udf_rec_blob(name, BIG, BIG_MAX);
		if (got >= 0) {
			out_text("some");
			out_blob(BIG, got);
		}
		else {
			out_text(read_tag(got));
		}
	}
	else if (as_udf_string_is(how, "list")) {
		rc = as_udf_rec_list(name, &v);
		out_text(rc == AS_UDF_OK ? "some" : read_tag(rc));
		if (rc == AS_UDF_OK) out_value(&v);
	}
	else if (as_udf_string_is(how, "map")) {
		rc = as_udf_rec_map(name, &v);
		out_text(rc == AS_UDF_OK ? "some" : read_tag(rc));
		if (rc == AS_UDF_OK) out_value(&v);
	}
	else if (as_udf_string_is(how, "int_or")) {
		int64_t x;
		rc = as_udf_rec_int_or(name, 7, &x);
		out_text(rc == AS_UDF_OK ? "some" : tag_of(rc));
		if (rc == AS_UDF_OK) out_int(x);
	}
	else if (as_udf_string_is(how, "double_or")) {
		double x;
		rc = as_udf_rec_double_or(name, 7.5, &x);
		out_text(rc == AS_UDF_OK ? "some" : tag_of(rc));
		if (rc == AS_UDF_OK) out_double(x);
	}
	else if (as_udf_string_is(how, "bool_or")) {
		bool x;
		rc = as_udf_rec_bool_or(name, true, &x);
		out_text(rc == AS_UDF_OK ? "some" : tag_of(rc));
		if (rc == AS_UDF_OK) out_bool(x);
	}
	else {
		return as_udf_fail_str("rd: unknown how");
	}

	return out_done("rd");
}
AS_UDF_SIGNATURE(rd, "fn rd(name: str[65536], how: str) -> value")

/*==========================================================
 * wr(name, how, v?): one write, staged and saved.
 */

/* The integers of a list into INTS. The count, or AS_UDF_TYPE_MISMATCH when the value is not a list, an element
 * is not an integer or there are more than BATCH_MAX of them. */
static int32_t ints_of(const as_udf_value* list)
{
	int32_t count = 0;
	if (list->kind != AS_UDF_LIST || list->len > BATCH_MAX) return AS_UDF_TYPE_MISMATCH;
	if (list->len == 0) return 0;
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(list), as_udf_value_at(list));
	if (cur <= 0) return AS_UDF_ERR;
	as_udf_value w[32];
	for (int32_t n; count >= 0 && (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			if (count == BATCH_MAX || as_udf_as_int(&w[i], &INTS[count]) != AS_UDF_OK) {
				count = AS_UDF_TYPE_MISMATCH;
				break;
			}
			count++;
		}
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
	return count;
}

/* The doubles of a list into DOUBLES, as ints_of. An integer element is not a double. */
static int32_t doubles_of(const as_udf_value* list)
{
	int32_t count = 0;
	if (list->kind != AS_UDF_LIST || list->len > BATCH_MAX) return AS_UDF_TYPE_MISMATCH;
	if (list->len == 0) return 0;
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(list), as_udf_value_at(list));
	if (cur <= 0) return AS_UDF_ERR;
	as_udf_value w[32];
	for (int32_t n; count >= 0 && (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			if (count == BATCH_MAX || as_udf_as_double(&w[i], &DOUBLES[count]) != AS_UDF_OK) {
				count = AS_UDF_TYPE_MISMATCH;
				break;
			}
			count++;
		}
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
	return count;
}

AS_UDF_EXPORT(wr) int32_t
wr(int32_t args, int32_t argc)
{
	as_udf_value a[3];
	if (! arity("wr", argc, 3)) return 1;
	int32_t n = as_udf_args(args, argc, a, 3);
	int32_t name_len;
	if (! arg_string("wr", a, n, 0, "name", S1, &name_len)) return 1;
	if (! arg_is_string("wr", a, n, 1, "how")) return 1;
	const char* name = S1;
	const as_udf_value* how = &a[1];
	/* An argument that is not there is told from one that is nil by the count: `set` of nothing is
	 * "wrong", while `set` of a nil removes the bin. */
	bool missing = n <= 2;
	as_udf_value v = as_udf_v_nil();
	if (! missing) v = a[2];

	int32_t stage;

	if (as_udf_string_is(how, "set")) {
		stage = missing ? AS_UDF_TYPE_MISMATCH : as_udf_rec_set_value(name, name_len, &v);
	}
	else if (as_udf_string_is(how, "blob")) {
		/* A string or a blob, nothing else: as_udf_bytes would take GeoJSON and an HLL too. */
		int32_t got = v.kind == AS_UDF_STR ? as_udf_as_string(&v, (char*)BIG, BIG_MAX) :
				v.kind == AS_UDF_BLOB ? as_udf_as_blob(&v, BIG, BIG_MAX) : AS_UDF_TYPE_MISMATCH;
		stage = got < 0 ? got : as_udf_rec_set_blob(name, name_len, BIG, got);
	}
	else if (as_udf_string_is(how, "geojson")) {
		int32_t got = v.kind == AS_UDF_STR ? as_udf_as_string(&v, S2, STR_MAX) : AS_UDF_TYPE_MISMATCH;
		stage = got < 0 ? got : as_udf_rec_set_geojson(name, name_len, S2, got);
	}
	else if (as_udf_string_is(how, "ints")) {
		int32_t count = ints_of(&v);
		stage = count < 0 ? count : as_udf_rec_set_ints(name, name_len, INTS, count * 8);
	}
	else if (as_udf_string_is(how, "doubles")) {
		int32_t count = doubles_of(&v);
		stage = count < 0 ? count : as_udf_rec_set_doubles(name, name_len, DOUBLES, count * 8);
	}
	else if (as_udf_string_is(how, "remove")) {
		stage = as_udf_rec_remove(name, name_len);
	}
	else {
		return as_udf_fail_str("wr: unknown how");
	}

	out_new();
	out_text(tag_of(stage));
	out_text(stage == AS_UDF_OK ? tag_of(as_udf_rec_save()) : "skipped");
	out_int(as_udf_rec_bin_count());

	return out_done("wr");
}
AS_UDF_SIGNATURE(wr, "fn wr(name: str[65536], how: str, v?: any) -> value")

/*==========================================================
 * meta(): what the record says of itself.
 */

AS_UDF_EXPORT(meta) int32_t
meta(int32_t args, int32_t argc)
{
	(void)args;
	if (! arity("meta", argc, 0)) return 1;

	int32_t out = as_udf_map_new(8);
	if (out <= 0) return as_udf_fail_str("meta: could not build the answer");

	uint8_t digest[20];
	int64_t ttl = as_udf_rec_ttl();
	int32_t set_len = as_udf_rec_set_name(S1, STR_MAX);
	if (set_len == AS_UDF_NOT_FOUND) set_len = 0;
	if (set_len < 0) return fail_parts("meta", -1, "", "could not read the set name: ", true, set_len);
	as_udf_value key, flag;
	int32_t rc = as_udf_rec_key(BIG, BIG_MAX, &key), put;
	if (rc == AS_UDF_NOT_FOUND) {
		key = as_udf_v_nil();
		rc = AS_UDF_OK;
	}
	if (rc != AS_UDF_OK) return fail_parts("meta", -1, "", "could not read the key: ", true, rc);

#define PUT(call) do { put = (call); if (put != AS_UDF_OK && rc == AS_UDF_OK) rc = put; } while (0)
	flag = as_udf_v_bool(as_udf_rec_exists() != 0);
	PUT(as_udf_map_put_value(out, "exists", &flag));
	PUT(as_udf_map_put_string_int(out, "generation", 10, as_udf_rec_generation()));
	PUT(as_udf_map_put_string_int(out, "ttl", 3, ttl == 0 ? -1 : ttl));
	PUT(as_udf_map_put_string_int(out, "bins", 4, as_udf_rec_bin_count()));
	flag = as_udf_v_bool(as_udf_rec_size() > 0);
	PUT(as_udf_map_put_value(out, "sized", &flag));
	PUT(as_udf_map_put_string_string(out, "set", 3, S1, set_len));
	PUT(as_udf_map_put_value(out, "key", &key));
	PUT(as_udf_map_put_string_int(out, "digest", 6, as_udf_rec_digest(digest, (int32_t)sizeof(digest))));
#undef PUT

	if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	if (rc != AS_UDF_OK) return fail_parts("meta", -1, "", "could not build the answer: ", true, rc);
	return 0;
}
AS_UDF_SIGNATURE(meta, "fn meta() -> value")

/*==========================================================
 * val(v): the kind of a value, and the value handed back.
 */

AS_UDF_EXPORT(val) int32_t
val(int32_t args, int32_t argc)
{
	as_udf_value a[1];
	if (! arity("val", argc, 1)) return 1;
	if (as_udf_args(args, argc, a, 1) < 1) return arg_fail("val", 0, "v", "is missing");

	out_new();
	out_text(kind_name(&a[0]));
	out_value(&a[0]);
	if (a[0].kind == AS_UDF_LIST || a[0].kind == AS_UDF_MAP) out_int((int64_t)a[0].len);

	return out_done("val");
}
AS_UDF_SIGNATURE(val, "fn val(v: any) -> value")

/*==========================================================
 * mk(what, n): a value built with the builders, one call an element.
 */

/* Fail with the tag of the status a builder gave. */
static int32_t mk_fail(int32_t rc)
{
	return fail_parts("mk", -1, "", tag_of(rc), false, 0);
}

/* Element `i` of the mixed list, as a value of the guest's. `s` (24 bytes) and `b` hold its bytes until the call
 * that takes it. */
static as_udf_value mixed(int64_t i, char* s, uint8_t* b)
{
	switch (i % 6) {
	case 0: return as_udf_v_int(i);
	case 1: return as_udf_v_double((double)i + 0.5);
	case 2: return as_udf_v_bool(i % 4 == 2);
	case 3:
		s[0] = 's';
		return as_udf_v_string(s, 1 + fmt_int(s + 1, i));
	case 4:
		*b = (uint8_t)(i & 0xff);
		return as_udf_v_blob(b, 1);
	default: return as_udf_v_nil();
	}
}

/* Element `i` of the mixed list, appended with the push of its kind. */
static int32_t push_mixed(int32_t list, int64_t i)
{
	char s[24];
	uint8_t b;
	switch (i % 6) {
	case 0: return as_udf_list_push_int(list, i);
	case 1: return as_udf_list_push_double(list, (double)i + 0.5);
	case 2: return as_udf_list_push_bool(list, i % 4 == 2);
	case 3:
		s[0] = 's';
		return as_udf_list_push_string(list, s, 1 + fmt_int(s + 1, i));
	case 4:
		b = (uint8_t)(i & 0xff);
		return as_udf_list_push_blob(list, &b, 1);
	default: return as_udf_list_push_nil(list);
	}
}

AS_UDF_EXPORT(mk) int32_t
mk(int32_t args, int32_t argc)
{
	as_udf_value a[2];
	if (! arity("mk", argc, 2)) return 1;
	int32_t count = as_udf_args(args, argc, a, 2);
	if (! arg_is_string("mk", a, count, 0, "what")) return 1;
	if (count < 2) return arg_fail("mk", 1, "n", "is missing");
	if (a[1].kind != AS_UDF_INT) return arg_fail("mk", 1, "n", "is not an int");
	const as_udf_value* what = &a[0];
	int64_t n = a[1].payload;
	if (n < 0 || n > MK_MAX) return mk_fail(AS_UDF_REFUSED);
	int32_t rc = AS_UDF_OK;
	char label[24];

	if (as_udf_string_is(what, "list_int")) {
		int32_t out = as_udf_list_new(8);
		if (out <= 0) return mk_fail(AS_UDF_ERR);
		for (int64_t i = 0; i < n && rc == AS_UDF_OK; i++) rc = as_udf_list_push_int(out, i);
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	}
	else if (as_udf_string_is(what, "list_mixed")) {
		int32_t out = as_udf_list_new(8);
		if (out <= 0) return mk_fail(AS_UDF_ERR);
		for (int64_t i = 0; i < n && rc == AS_UDF_OK; i++) rc = push_mixed(out, i);
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	}
	else if (as_udf_string_is(what, "map_str")) {
		int32_t out = as_udf_map_new(8);
		if (out <= 0) return mk_fail(AS_UDF_ERR);
		label[0] = 'k';
		for (int64_t i = 0; i < n && rc == AS_UDF_OK; i++) {
			rc = as_udf_map_put_string_int(out, label, 1 + fmt_int(label + 1, i), i);
		}
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	}
	else if (as_udf_string_is(what, "map_int")) {
		int32_t out = as_udf_map_new(8);
		if (out <= 0) return mk_fail(AS_UDF_ERR);
		label[0] = 'v';
		for (int64_t i = 0; i < n && rc == AS_UDF_OK; i++) {
			as_udf_value v = as_udf_v_string(label, 1 + fmt_int(label + 1, i));
			rc = as_udf_map_put_int_value(out, i, &v);
		}
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	}
	else if (as_udf_string_is(what, "map_mixed")) {
		int32_t out = as_udf_map_new(8);
		if (out <= 0) return mk_fail(AS_UDF_ERR);
		label[0] = 'k';
		for (int64_t i = 0; i < n && rc == AS_UDF_OK; i++) {
			char s[24];
			uint8_t b;
			as_udf_value v = mixed(i, s, &b);
			rc = as_udf_map_put_string_value(out, label, 1 + fmt_int(label + 1, i), &v);
		}
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(out);
	}
	else if (as_udf_string_is(what, "nested")) {
		int32_t inner = as_udf_list_new(1);
		if (inner <= 0) return mk_fail(AS_UDF_ERR);
		rc = as_udf_list_push_int(inner, n);
		for (int64_t i = 1; i < n && rc == AS_UDF_OK; i++) {
			int32_t outer = as_udf_list_new(1);
			if (outer <= 0) return mk_fail(AS_UDF_ERR);
			rc = as_udf_list_push(outer, inner);
			inner = outer;
		}
		if (rc == AS_UDF_OK) rc = as_udf_result_handle(inner);
	}
	else if (as_udf_string_is(what, "string")) {
		for (int64_t i = 0; i < n; i++) BIG[i] = 'x';
		as_udf_result_string((const char*)BIG, (int32_t)n);
	}
	else if (as_udf_string_is(what, "blob")) {
		for (int64_t i = 0; i < n; i++) BIG[i] = (uint8_t)(i & 0xff);
		as_udf_result_blob(BIG, (int32_t)n);
	}
	else {
		return as_udf_fail_str("mk: unknown what");
	}

	return rc == AS_UDF_OK ? 0 : mk_fail(rc);
}
AS_UDF_SIGNATURE(mk, "fn mk(what: str, n: int) -> value")

/*==========================================================
 * walk(name): a stored list or map, with a cursor and with the lookups.
 */

AS_UDF_EXPORT(walk) int32_t
walk(int32_t args, int32_t argc)
{
	as_udf_value a[1], bin, w[32];
	if (! arity("walk", argc, 1)) return 1;
	int32_t n = as_udf_args(args, argc, a, 1);
	int32_t name_len;
	if (! arg_string("walk", a, n, 0, "name", S1, &name_len)) return 1;

	out_new();

	if (! as_udf_rec_get(S1, &bin)) {
		out_text("none");
	}
	else if (bin.kind == AS_UDF_LIST) {
		int64_t sum = 0, strings = 0;
		int32_t len = (int32_t)bin.len;
		int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&bin), as_udf_value_at(&bin));
		for (int32_t got; cur > 0 && (got = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
			for (int32_t i = 0; i < got; i++) {
				if (w[i].kind == AS_UDF_INT) sum = wrap_add(sum, w[i].payload);
				if (w[i].kind == AS_UDF_STR) strings++;
			}
			if (got < 32) break;
		}
		as_udf_cursor_close(cur);

		as_udf_value first = as_udf_list_at(&bin, 0);
		as_udf_value last = as_udf_list_at(&bin, len - 1);
		as_udf_value past = as_udf_list_at(&bin, len);

		out_text("list");
		out_int(len);
		out_int(sum);
		out_int(strings);
		out_value_or_nil(&first);
		out_value_or_nil(&last);
		out_text(past.kind == AS_UDF_ABSENT ? "none" : "some");
	}
	else if (bin.kind == AS_UDF_MAP) {
		int64_t sum = 0;
		int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&bin), as_udf_value_at(&bin));
		/* A map yields key, value, key, value: the values are the odd ones. */
		for (int32_t got; cur > 0 && (got = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
			for (int32_t i = 1; i < got; i += 2) {
				if (w[i].kind == AS_UDF_INT) sum = wrap_add(sum, w[i].payload);
			}
			if (got < 32) break;
		}
		as_udf_cursor_close(cur);

		as_udf_value at_k1 = as_udf_map_get(&bin, "k1");
		as_udf_value at_1 = as_udf_map_get_int(&bin, 1);

		out_text("map");
		out_int((int64_t)bin.len);
		out_int(sum);
		out_bool(at_k1.kind != AS_UDF_ABSENT);
		out_bool(at_1.kind != AS_UDF_ABSENT);
		out_value_or_nil(&at_k1);
		out_value_or_nil(&at_1);
	}
	else {
		out_text("wrong");
	}

	return out_done("walk");
}
AS_UDF_SIGNATURE(walk, "fn walk(name: str[65536]) -> value")

/*==========================================================
 * ar(a, b?, rest...): the arguments as they arrive.
 */

static as_udf_value AR[ARGS_MAX];

AS_UDF_EXPORT(ar) int32_t
ar(int32_t args, int32_t argc)
{
	if (! arity("ar", argc, ARGS_MAX)) return 1;
	int32_t n = as_udf_args(args, argc, AR, ARGS_MAX);
	if (n < 1) return arg_fail("ar", 0, "a", "is missing");
	if (AR[0].kind != AS_UDF_INT) return arg_fail("ar", 0, "a", "is not an int");
	bool has_b = n > 1 && AR[1].kind != AS_UDF_NIL;
	if (has_b && AR[1].kind != AS_UDF_STR) return arg_fail("ar", 1, "b", "is not a string");

	out_new();
	out_int(AR[0].payload);
	if (has_b) {
		out_value(&AR[1]);
	}
	else {
		out_nil();
	}
	out_int(n > 2 ? n - 2 : 0);
	for (int32_t i = 2; i < n; i++) out_text(kind_name(&AR[i]));

	return out_done("ar");
}
AS_UDF_SIGNATURE(ar, "fn ar(a: int, b?: str, rest: any...) -> value")

/*==========================================================
 * life(what): staging, saving, deleting, the TTL, and failing after a save.
 */

AS_UDF_EXPORT(life) int32_t
life(int32_t args, int32_t argc)
{
	as_udf_value a[1];
	if (! arity("life", argc, 1)) return 1;
	int32_t n = as_udf_args(args, argc, a, 1);
	if (! arg_is_string("life", a, n, 0, "what")) return 1;
	const as_udf_value* what = &a[0];
	int32_t rc;

	out_new();

	/* A staging call that fails is the whole answer: its tag, and nothing after it. */
	if (as_udf_string_is(what, "staged")) {
		int64_t t;
		if ((rc = as_udf_rec_set_int("t", 1, 1)) != AS_UDF_OK || (rc = as_udf_rec_int_or("t", -1, &t)) != AS_UDF_OK) {
			out_text(tag_of(rc));
		}
		else {
			out_text("some");
			out_int(t);
			out_int(as_udf_rec_bin_count());
		}
	}
	else if (as_udf_string_is(what, "save_twice")) {
		if ((rc = as_udf_rec_set_int("t", 1, 1)) != AS_UDF_OK) {
			out_text(tag_of(rc));
		}
		else {
			int32_t first = as_udf_rec_save();
			if ((rc = as_udf_rec_set_int("t", 1, 2)) != AS_UDF_OK) {
				out_text(tag_of(rc));
			}
			else {
				int32_t second = as_udf_rec_save();
				out_text(tag_of(first));
				out_text(tag_of(second));
			}
		}
	}
	else if (as_udf_string_is(what, "delete")) {
		out_text(tag_of(as_udf_rec_delete()));
		out_bool(as_udf_rec_exists() != 0);
	}
	else if (as_udf_string_is(what, "ttl")) {
		int32_t ttl = as_udf_rec_set_ttl(100);

		// A refused set_ttl is a failed staging call: its status is the answer.
		out_text(tag_of(ttl));

		if (ttl == AS_UDF_OK) {
			out_text(tag_of(as_udf_rec_save()));
		}
	}
	else if (as_udf_string_is(what, "fail_after_save")) {
		if ((rc = as_udf_rec_set_int("t", 1, 9)) != AS_UDF_OK) {
			out_text(tag_of(rc));
		}
		else {
			as_udf_rec_save();
			return as_udf_fail_str("life: failing");
		}
	}
	else {
		return as_udf_fail_str("life: unknown what");
	}

	return out_done("life");
}
AS_UDF_SIGNATURE(life, "fn life(what: str) -> value")

/*==========================================================
 * res(kind): each result call.
 */

AS_UDF_EXPORT(res) int32_t
res(int32_t args, int32_t argc)
{
	as_udf_value a[1];
	if (! arity("res", argc, 1)) return 1;
	int32_t n = as_udf_args(args, argc, a, 1);
	if (! arg_is_string("res", a, n, 0, "kind")) return 1;
	const as_udf_value* kind = &a[0];

	if (as_udf_string_is(kind, "nil")) {
		as_udf_result_nil();
	}
	else if (as_udf_string_is(kind, "int")) {
		as_udf_result_int(INT64_MIN);
	}
	else if (as_udf_string_is(kind, "double")) {
		as_udf_result_double(1e308);
	}
	else if (as_udf_string_is(kind, "bool")) {
		as_udf_result_bool(1);
	}
	else if (as_udf_string_is(kind, "string")) {
		as_udf_result_string("", 0);
	}
	else if (as_udf_string_is(kind, "blob")) {
		as_udf_result_blob("", 0);
	}
	else if (as_udf_string_is(kind, "geojson")) {
		as_udf_result_geojson(GEO, as_udf_strlen(GEO));
	}
	else if (as_udf_string_is(kind, "list")) {
		int32_t rc = as_udf_result_handle(as_udf_list_new(0));
		if (rc != AS_UDF_OK) return fail_parts("res", -1, "", "could not return the list: ", true, rc);
	}
	else if (as_udf_string_is(kind, "map")) {
		int32_t rc = as_udf_result_handle(as_udf_map_new(0));
		if (rc != AS_UDF_OK) return fail_parts("res", -1, "", "could not return the map: ", true, rc);
	}
	else if (as_udf_string_is(kind, "fail_long")) {
		for (int32_t i = 0; i < 600; i++) S1[i] = 'x';
		as_udf_fail(S1, 600);
		return 1;
	}
	else {
		return as_udf_fail_str("res: unknown kind");
	}

	return 0;
}
AS_UDF_SIGNATURE(res, "fn res(kind: str) -> value")

/*==========================================================
 * txt(s): a string argument, copied out and handed back.
 */

AS_UDF_EXPORT(txt) int32_t
txt(int32_t args, int32_t argc)
{
	as_udf_value a[1];
	if (! arity("txt", argc, 1)) return 1;
	int32_t n = as_udf_args(args, argc, a, 1);
	int32_t len;
	if (! arg_string("txt", a, n, 0, "s", S1, &len)) return 1;

	out_new();
	out_int(len);
	out_string(S1, len);
	out_int(len > 0 ? (int64_t)(uint8_t)S1[0] : -1);

	return out_done("txt");
}
AS_UDF_SIGNATURE(txt, "fn txt(s: str[65536]) -> value")

/*==========================================================
 * cmp(a, b): the stored-value comparison of two bins.
 */

AS_UDF_EXPORT(cmp) int32_t
cmp(int32_t args, int32_t argc)
{
	as_udf_value a[2], va, vb;
	if (! arity("cmp", argc, 2)) return 1;
	int32_t n = as_udf_args(args, argc, a, 2);
	int32_t a_len, b_len, order;
	if (! arg_string("cmp", a, n, 0, "a", S1, &a_len)) return 1;
	if (! arg_string("cmp", a, n, 1, "b", S2, &b_len)) return 1;

	out_new();

	bool has_a = as_udf_rec_get(S1, &va);
	bool has_b = as_udf_rec_get(S2, &vb);

	if (! has_a || ! has_b) {
		out_text("none");
	}
	else {
		int32_t rc = as_udf_cmp_stored(&va, &vb, &order);
		out_text(rc == AS_UDF_OK ? "some" : tag_of(rc));
		if (rc == AS_UDF_OK) out_int(order);
	}

	return out_done("cmp");
}
AS_UDF_SIGNATURE(cmp, "fn cmp(a: str[65536], b: str[65536]) -> value")

/*==========================================================
 * agg: a stream UDF. The whole stream shares one instance, so the state is two statics.
 */

static int64_t AGG_COUNT;
static int64_t AGG_SUM;

AS_UDF_EXPORT(agg_record) int32_t
agg_record(int32_t args, int32_t argc)
{
	(void)args;
	if (! arity("agg", argc, 0)) return 1;

	int64_t n;
	if (as_udf_rec_int("n", &n) == AS_UDF_OK) {
		AGG_COUNT++;
		AGG_SUM = wrap_add(AGG_SUM, n);
	}

	return 0;
}

AS_UDF_EXPORT(agg_end) int32_t
agg_end(int32_t args, int32_t argc)
{
	(void)args;
	if (! arity("agg", argc, 0)) return 1;

	out_new();
	out_int(AGG_COUNT);
	out_int(AGG_SUM);

	return out_done("agg");
}
AS_UDF_SIGNATURE(agg, "stream agg { record() end() }")

/*==========================================================
 * merge_api: an xdr-merge handler that answers what the two records say of themselves. C has no Pair and no
 * Both, so `max`, `min`, `sum`, `newer_s`, `older_s`, `c_or_i` and `i_or_c` are not here.
 */

/* Put a list field of an event record under `key`. A field the server sent none of is AS_UDF_ABSENT, not an
 * empty list: the bin is then left out. */
static int32_t put_names(int32_t out, const char* key, const as_udf_value* names)
{
	return names->kind == AS_UDF_ABSENT ? AS_UDF_OK : as_udf_map_put_value(out, key, names);
}

/* Put a list of four integers and two booleans under `key`. */
static int32_t put_meta(int32_t out, const char* key, const as_udf_event_record* r, bool flag1, bool flag2)
{
	int32_t list = as_udf_list_new(6);
	if (list <= 0) return AS_UDF_ERR;
	int32_t rc = as_udf_list_push_int(list, r->generation);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_int(list, r->last_update_time);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_int(list, r->ttl);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_int(list, r->source_id);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_bool(list, flag1);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_bool(list, flag2);
	if (rc == AS_UDF_OK) rc = as_udf_map_put_string_handle(out, key, as_udf_strlen(key), list);
	return rc;
}

/* Put a list of two integers under `key`. */
static int32_t put_two(int32_t out, const char* key, int64_t first, int64_t second)
{
	int32_t list = as_udf_list_new(2);
	if (list <= 0) return AS_UDF_ERR;
	int32_t rc = as_udf_list_push_int(list, first);
	if (rc == AS_UDF_OK) rc = as_udf_list_push_int(list, second);
	if (rc == AS_UDF_OK) rc = as_udf_map_put_string_handle(out, key, as_udf_strlen(key), list);
	return rc;
}

AS_UDF_EVENT_XDR_MERGE(merge_api)
{
	as_udf_event_record current, incoming;
	if (as_udf_event_decode(args, argc, &current, &incoming) != AS_UDF_OK) return as_udf_fail_str("merge_api: not an xdr-merge");

	int32_t out = as_udf_map_new(16);
	if (out <= 0) return as_udf_fail_str("merge_api: could not build the record");

	/* The bins that need `n` and `s` are not here, but the handler still holds the two to their kinds, as
	 * every SDK's does: an `n` that is not an integer or an `s` that is not a string fails the merge. */
	const as_udf_event_record* sides[2] = { &current, &incoming };
	for (int32_t i = 0; i < 2; i++) {
		int64_t n;
		as_udf_value s;
		if (as_udf_event_int(sides[i], "n", &n) == AS_UDF_TYPE_MISMATCH) return as_udf_fail_str("merge_api: n is not an integer");
		if (as_udf_event_get(sides[i], "s", &s) && as_udf_expect(&s, AS_UDF_STR) != AS_UDF_OK) {
			return as_udf_fail_str("merge_api: s is not a string");
		}
	}

	as_udf_value newer = as_udf_v_bool(as_udf_event_incoming_is_newer(&current, &incoming, "s"));
	int32_t rc = as_udf_map_put_value(out, "i_newer", &newer);

	if (rc == AS_UDF_OK) rc = put_names(out, "c_names", &current.bin_names);
	if (rc == AS_UDF_OK) rc = put_names(out, "i_names", &incoming.bin_names);
	if (rc == AS_UDF_OK) rc = put_names(out, "c_del", &current.deleted_bins);
	if (rc == AS_UDF_OK) rc = put_names(out, "i_del", &incoming.deleted_bins);
	if (rc == AS_UDF_OK) rc = put_names(out, "lost", &incoming.lost_bins);
	if (rc == AS_UDF_OK) rc = put_meta(out, "c_meta", &current, current.is_tombstone, current.uses_bin_convergence);
	if (rc == AS_UDF_OK) rc = put_meta(out, "i_meta", &incoming, incoming.is_delete, incoming.is_partial);
	if (rc == AS_UDF_OK) {
		rc = put_two(out, "blut", as_udf_event_bin_last_update_time(&current, "s"),
				as_udf_event_bin_last_update_time(&incoming, "s"));
	}
	if (rc == AS_UDF_OK) {
		rc = put_two(out, "bsrc", as_udf_event_bin_source_id(&current, "s"), as_udf_event_bin_source_id(&incoming, "s"));
	}
	if (rc != AS_UDF_OK) return fail_parts("merge_api", -1, "", "could not build the record: ", true, rc);

	return as_udf_resolve_record(out, -1);
}

/*==========================================================
 * stop_probe(what): not part of the specification, and not called by the conformance harness. The bin reads
 * that cannot answer when the call has no handle left: as_udf_rec_has and as_udf_rec_get stop the call with
 * the header's message rather than say "no such bin". tests/src/udf_cpp.rs calls it.
 */

/* Takes every handle the call has left, by building empty lists until the host refuses one. Nothing gives
 * them back before the call ends. */
static void take_every_handle(void)
{
	while (as_udf_list_new(0) > 0) {
	}
}

/* Reads the string bin s, saves a bin x, takes every handle, and then: "has" asks whether the list bin l is
 * there and "get" reads it, both of which need a handle and must stop the call; "reread" reads s again, which
 * reuses the handle of the first read and must succeed. The saved x is undone with the rest of a call that
 * fails. */
AS_UDF_EXPORT(stop_probe) int32_t
stop_probe(int32_t args, int32_t argc)
{
	as_udf_value a[1], s, v;
	if (! arity("stop_probe", argc, 1)) return 1;
	int32_t n = as_udf_args(args, argc, a, 1);
	if (! arg_is_string("stop_probe", a, n, 0, "what")) return 1;
	if (as_udf_rec_bin("s", &s) != AS_UDF_OK || s.kind != AS_UDF_STR) return as_udf_fail_str("stop_probe: no string bin s");
	if (as_udf_rec_set_int("x", 1, 1) != AS_UDF_OK || as_udf_rec_save() != AS_UDF_OK) {
		return as_udf_fail_str("stop_probe: could not save x");
	}

	take_every_handle();

	if (as_udf_string_is(&a[0], "has")) {
		as_udf_result_bool(as_udf_rec_has("l"));
		return 0;
	}
	if (as_udf_string_is(&a[0], "get")) {
		as_udf_result_bool(as_udf_rec_get("l", &v));
		return 0;
	}
	if (as_udf_string_is(&a[0], "reread")) {
		if (! as_udf_rec_get("s", &v) || v.kind != AS_UDF_STR) return as_udf_fail_str("stop_probe: s is gone");
		as_udf_result_int((int64_t)v.len);
		return 0;
	}
	return as_udf_fail_str("stop_probe: no such probe");
}
AS_UDF_SIGNATURE(stop_probe, "fn stop_probe(what: str) -> value")

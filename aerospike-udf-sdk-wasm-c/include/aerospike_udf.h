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
#pragma once

/*
 * Writing an Aerospike WASM UDF in C, for the server's mod-wamr engine.
 *
 * Header-only. The module's imports are declared with their wasm import names, so a UDF is a freestanding
 * C file compiled with clang for wasm32 and no libc:
 *
 *   clang --target=wasm32 -O2 -nostdlib -ffreestanding -Wl,--no-entry -Wl,--export-dynamic \
 *       -I<this directory> -o my_udf.wasm my_udf.c
 *
 * The contract is the one `aerospike-udf-sdk-wasm` (Rust) documents: every read is a 16-byte value
 * (`as_udf_value`), a string or collection is read by position from a source the host holds, nothing is
 * msgpack and nothing is allocated unless the UDF does it. A UDF is an export
 * `int32_t name(int32_t args, int32_t argc)` returning 0 on success; it sets its result with `result_*` and
 * fails with `as_udf_fail`. A stream (aggregation) UDF exports `<name>_record` and `<name>_end`.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*==========================================================
 * The imports.
 */

#define AS_UDF_IMPORT(name) __attribute__((import_module("aerospike"), import_name(#name)))

/* Bins - reading. One import for any kind: the host writes a 16-byte value. */
AS_UDF_IMPORT(bin_value) int32_t as_udf_bin_value(const char* name, int32_t name_len, void* out, int32_t cap);

/* Bins - writing. Staged until rec_update / rec_create. */
AS_UDF_IMPORT(set_int) int32_t as_udf_set_int(const char* name, int32_t name_len, int64_t value);
AS_UDF_IMPORT(set_double) int32_t as_udf_set_double(const char* name, int32_t name_len, double value);
AS_UDF_IMPORT(set_bool) int32_t as_udf_set_bool(const char* name, int32_t name_len, int32_t value);
AS_UDF_IMPORT(set_str) int32_t as_udf_set_str(const char* name, int32_t name_len, const char* value, int32_t value_len);
AS_UDF_IMPORT(set_blob) int32_t as_udf_set_blob(const char* name, int32_t name_len, const void* value, int32_t value_len);
AS_UDF_IMPORT(set_geojson) int32_t as_udf_set_geojson(const char* name, int32_t name_len, const char* value, int32_t value_len);
AS_UDF_IMPORT(set_nil) int32_t as_udf_set_nil(const char* name, int32_t name_len);
AS_UDF_IMPORT(set_handle) int32_t as_udf_set_handle(const char* name, int32_t name_len, int32_t handle);
AS_UDF_IMPORT(set_at) int32_t as_udf_set_at(const char* name, int32_t name_len, int32_t src, int32_t at);
AS_UDF_IMPORT(bin_remove) int32_t as_udf_bin_remove(const char* name, int32_t name_len);
AS_UDF_IMPORT(set_list_ints) int32_t as_udf_set_list_ints(const char* name, int32_t name_len, const int64_t* vals, int32_t vals_len);
AS_UDF_IMPORT(set_list_doubles) int32_t as_udf_set_list_doubles(const char* name, int32_t name_len, const double* vals, int32_t vals_len);

/* Collections, by position in a source. */
AS_UDF_IMPORT(cursor_values) int32_t as_udf_cursor_values(int32_t cursor, void* out, int32_t cap);
AS_UDF_IMPORT(cursor_bytes_at) int32_t as_udf_cursor_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
AS_UDF_IMPORT(coll_bytes_at) int32_t as_udf_coll_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
AS_UDF_IMPORT(coll_cmp_at) int32_t as_udf_coll_cmp_at(int32_t src1, int32_t at1, int32_t src2, int32_t at2);
AS_UDF_IMPORT(cursor_enter_at) int32_t as_udf_cursor_enter_at(int32_t src, int32_t at);
AS_UDF_IMPORT(coll_get) int32_t as_udf_coll_get(int32_t src, int32_t at, const char* key, int32_t key_len, void* out, int32_t cap);
AS_UDF_IMPORT(coll_get_i) int32_t as_udf_coll_get_i(int32_t src, int32_t at, int64_t key, void* out, int32_t cap);
AS_UDF_IMPORT(cursor_close) void as_udf_cursor_close(int32_t cursor);
AS_UDF_IMPORT(pack_handle) int32_t as_udf_pack_handle(int32_t handle);
AS_UDF_IMPORT(args_values) int32_t as_udf_args_values(int32_t handle, void* out, int32_t cap);
AS_UDF_IMPORT(coll_size) int32_t as_udf_coll_size(int32_t handle);

/* Collections - building, on the host, as handles. */
AS_UDF_IMPORT(list_new) int32_t as_udf_list_new(int32_t capacity);
AS_UDF_IMPORT(map_new) int32_t as_udf_map_new(int32_t capacity);
AS_UDF_IMPORT(val_int) int32_t as_udf_val_int(int64_t value);
AS_UDF_IMPORT(val_double) int32_t as_udf_val_double(double value);
AS_UDF_IMPORT(val_bool) int32_t as_udf_val_bool(int32_t value);
AS_UDF_IMPORT(val_nil) int32_t as_udf_val_nil(void);
AS_UDF_IMPORT(val_str) int32_t as_udf_val_str(const char* value, int32_t value_len);
AS_UDF_IMPORT(val_blob) int32_t as_udf_val_blob(const void* value, int32_t value_len);
AS_UDF_IMPORT(val_geojson) int32_t as_udf_val_geojson(const char* value, int32_t value_len);
AS_UDF_IMPORT(list_push) int32_t as_udf_list_push(int32_t handle, int32_t value);
AS_UDF_IMPORT(list_push_int) int32_t as_udf_list_push_int(int32_t handle, int64_t value);
AS_UDF_IMPORT(list_push_str) int32_t as_udf_list_push_str(int32_t handle, const char* value, int32_t value_len);
AS_UDF_IMPORT(map_put) int32_t as_udf_map_put(int32_t handle, int32_t key, int32_t value);
AS_UDF_IMPORT(map_put_str_int) int32_t as_udf_map_put_str_int(int32_t handle, const char* key, int32_t key_len, int64_t value);
AS_UDF_IMPORT(map_put_str_str) int32_t as_udf_map_put_str_str(int32_t handle, const char* key, int32_t key_len, const char* value, int32_t value_len);
AS_UDF_IMPORT(map_put_str_handle) int32_t as_udf_map_put_str_handle(int32_t handle, const char* key, int32_t key_len, int32_t value);
AS_UDF_IMPORT(list_push_at) int32_t as_udf_list_push_at(int32_t handle, int32_t src, int32_t at);
AS_UDF_IMPORT(map_put_str_at) int32_t as_udf_map_put_str_at(int32_t handle, const char* key, int32_t key_len, int32_t src, int32_t at);
AS_UDF_IMPORT(map_put_i_at) int32_t as_udf_map_put_i_at(int32_t handle, int64_t key, int32_t src, int32_t at);
/* A batch: 16-byte values appended to a list, 32-byte key-value pairs put into a map. Returns how many went in. */
AS_UDF_IMPORT(list_push_values) int32_t as_udf_list_push_values(int32_t handle, const void* vals, int32_t vals_len);
AS_UDF_IMPORT(map_put_entries) int32_t as_udf_map_put_entries(int32_t handle, const void* pairs, int32_t pairs_len);

/* Results and failure. */
AS_UDF_IMPORT(result_nil) void as_udf_result_nil(void);
AS_UDF_IMPORT(result_int) void as_udf_result_int(int64_t value);
AS_UDF_IMPORT(result_double) void as_udf_result_double(double value);
AS_UDF_IMPORT(result_bool) void as_udf_result_bool(int32_t value);
AS_UDF_IMPORT(result_str) void as_udf_result_str(const char* value, int32_t value_len);
AS_UDF_IMPORT(result_blob) void as_udf_result_blob(const void* value, int32_t value_len);
AS_UDF_IMPORT(result_geojson) void as_udf_result_geojson(const char* value, int32_t value_len);
AS_UDF_IMPORT(result_handle) int32_t as_udf_result_handle(int32_t handle);
AS_UDF_IMPORT(result_at) int32_t as_udf_result_at(int32_t src, int32_t at);
AS_UDF_IMPORT(fail) void as_udf_fail(const char* message, int32_t message_len);

/* Metadata. */
AS_UDF_IMPORT(gen) int64_t as_udf_gen(void);
AS_UDF_IMPORT(ttl) int64_t as_udf_ttl(void);
AS_UDF_IMPORT(last_update) int64_t as_udf_last_update(void);
AS_UDF_IMPORT(record_size) int64_t as_udf_record_size(void);
AS_UDF_IMPORT(numbins) int64_t as_udf_numbins(void);
AS_UDF_IMPORT(current_time) int64_t as_udf_current_time(void);
AS_UDF_IMPORT(set_ttl) int32_t as_udf_set_ttl(int32_t seconds);
AS_UDF_IMPORT(set_name) int32_t as_udf_set_name(void* out, int32_t cap);
AS_UDF_IMPORT(key_type) int32_t as_udf_key_type(void);
AS_UDF_IMPORT(key_int) int64_t as_udf_key_int(void);
AS_UDF_IMPORT(key_bytes) int32_t as_udf_key_bytes(void* out, int32_t cap);
AS_UDF_IMPORT(digest) int32_t as_udf_digest(void* out, int32_t cap);
AS_UDF_IMPORT(rec_create) int32_t as_udf_rec_create(void);
AS_UDF_IMPORT(rec_update) int32_t as_udf_rec_update(void);
AS_UDF_IMPORT(rec_remove) int32_t as_udf_rec_remove(void);
AS_UDF_IMPORT(rec_exists) int32_t as_udf_rec_exists(void);
AS_UDF_IMPORT(log) void as_udf_log(int32_t level, const char* message, int32_t message_len);

/*==========================================================
 * The contract's constants.
 */

/* Status codes. Anything at or below AS_UDF_NEEDED_BASE is "the buffer was too small": -(rc) - 16 bytes needed. */
#define AS_UDF_OK 0
#define AS_UDF_ERR (-1)
#define AS_UDF_NOT_FOUND (-2)
#define AS_UDF_TYPE_MISMATCH (-3)
#define AS_UDF_REFUSED (-4)
#define AS_UDF_NEEDED_BASE (-16)

/* Value kinds, as the host hands them out. */
#define AS_UDF_ABSENT (-1)
#define AS_UDF_NIL 0
#define AS_UDF_BOOL 1
#define AS_UDF_INT 2
#define AS_UDF_DOUBLE 3
#define AS_UDF_STR 4
#define AS_UDF_BLOB 5
#define AS_UDF_LIST 6
#define AS_UDF_MAP 7
#define AS_UDF_OTHER 8
#define AS_UDF_GEOJSON 9
#define AS_UDF_HLL 10

/* Kinds a guest uses only in a batch it hands the host: bytes of its own by address, a built collection by handle. */
#define AS_UDF_GUEST_STR 16
#define AS_UDF_GUEST_BLOB 17
#define AS_UDF_GUEST_GEOJSON 18
#define AS_UDF_HANDLE 19

#define AS_UDF_LOG_WARN 0
#define AS_UDF_LOG_INFO 1
#define AS_UDF_LOG_DEBUG 2

/* The 16-byte value: kind, length (bytes, elements or entries), payload (the scalar, or source << 32 | offset). */
typedef struct as_udf_value_s {
	int32_t kind;
	uint32_t len;
	int64_t payload;
} as_udf_value;

/* Where a string, blob or collection sits: the source handle and the offset in it. */
static inline int32_t as_udf_value_src(const as_udf_value* v) { return (int32_t)(v->payload >> 32); }
static inline int32_t as_udf_value_at(const as_udf_value* v) { return (int32_t)(v->payload & 0xffffffff); }
static inline double as_udf_value_double(const as_udf_value* v) { double d; __builtin_memcpy(&d, &v->payload, 8); return d; }

/*==========================================================
 * Conveniences.
 */

#define AS_UDF_EXPORT(name) __attribute__((export_name(#name)))

/* A signature export beside a UDF, as the Rust macros emit: `fn name(a: int, b: str[256]) -> value`. */
#define AS_UDF_SIGNATURE(name, text)                                                   \
	static const char as_udf_sig_##name[] = text;                                    \
	__attribute__((export_name("__udf_sig_" #name))) int32_t as_udf_sig_fn_##name(void) \
	{                                                                                  \
		return (int32_t)(uintptr_t)as_udf_sig_##name;                                  \
	}

static inline int32_t as_udf_strlen(const char* s)
{
	int32_t n = 0;
	while (s[n] != '\0') n++;
	return n;
}

/* A bin by name, as a value. Returns the status; AS_UDF_NOT_FOUND for no such bin. */
static inline int32_t as_udf_bin(const char* name, as_udf_value* out)
{
	return as_udf_bin_value(name, as_udf_strlen(name), out, (int32_t)sizeof(*out));
}

/* A stored value's bytes as the server stores them, msgpack header included, into a buffer: the encoding a
 * convergent merge compares when nothing else tells two candidates apart. Returns the length, or a status. */
static inline int32_t as_udf_stored_bytes(const as_udf_value* v, void* out, int32_t cap)
{
	return as_udf_coll_bytes_at(as_udf_value_src(v), as_udf_value_at(v), out, cap);
}

/* Compare two stored values byte by byte, as memcmp would on their stored encodings, without copying
 * either: negative, zero or positive; AS_UDF_TYPE_MISMATCH when a value is not a stored one. */
static inline int32_t as_udf_cmp_stored(const as_udf_value* a, const as_udf_value* b)
{
	return as_udf_coll_cmp_at(as_udf_value_src(a), as_udf_value_at(a), as_udf_value_src(b), as_udf_value_at(b));
}

/* The bytes of a string, blob, GeoJson or HLL value into a buffer. Returns the length, or a status. */
static inline int32_t as_udf_bytes(const as_udf_value* v, void* out, int32_t cap)
{
	return as_udf_cursor_bytes_at(as_udf_value_src(v), as_udf_value_at(v), out, cap);
}

/* The call's arguments into `out`, at most `cap` of them. Returns how many there are (bounded by cap). */
static inline int32_t as_udf_args(int32_t handle, int32_t argc, as_udf_value* out, int32_t cap)
{
	if (handle <= 0 || argc <= 0) return 0;
	int32_t n = as_udf_args_values(handle, out, cap * (int32_t)sizeof(as_udf_value));
	return n < 0 ? 0 : n;
}

/* A 16-byte value of the guest's, for a batch. */
static inline as_udf_value as_udf_v_int(int64_t i) { as_udf_value v = { AS_UDF_INT, 0, i }; return v; }
static inline as_udf_value as_udf_v_double(double d) { as_udf_value v = { AS_UDF_DOUBLE, 0, 0 }; __builtin_memcpy(&v.payload, &d, 8); return v; }
static inline as_udf_value as_udf_v_str(const char* s, int32_t len) { as_udf_value v = { AS_UDF_GUEST_STR, (uint32_t)len, (int64_t)(uintptr_t)s }; return v; }
static inline as_udf_value as_udf_v_blob(const void* b, int32_t len) { as_udf_value v = { AS_UDF_GUEST_BLOB, (uint32_t)len, (int64_t)(uintptr_t)b }; return v; }
static inline as_udf_value as_udf_v_handle(int32_t h) { as_udf_value v = { AS_UDF_HANDLE, 0, h }; return v; }

/* Fail the call with a NUL-terminated message, and return the non-zero status a UDF then returns. */
static inline int32_t as_udf_failf(const char* message)
{
	as_udf_fail(message, as_udf_strlen(message));
	return 1;
}

static inline void as_udf_logf(int32_t level, const char* message)
{
	as_udf_log(level, message, as_udf_strlen(message));
}

/*==========================================================
 * Events.
 *
 * An event handler is a function the server calls at a point in its own processing. The first event,
 * `xdr-merge`, hands it the local record and the incoming XDR write as two records (`which` 0 and 1, read
 * through as_udf_event_bin, never the record imports) and a description of the conflict, and takes back a
 * verdict or the final record. Declare one with AS_UDF_EVENT_XDR_MERGE, which also emits the signature
 * `event xdr-merge/1 ...` that `event-set` checks before binding. A handler must be pure and symmetric:
 * both DCs resolve the same pair with the sides swapped and must land on the same record. The order helpers
 * (as_udf_event_remote_newer) are symmetric; "mine first" is not.
 */

AS_UDF_IMPORT(event_bin_value) int32_t as_udf_event_bin_value(int32_t which, const char* name, int32_t name_len, void* out, int32_t cap);

/* One side of the conflict. */
typedef struct as_udf_event_record_s {
	int32_t which;            /* 0 local, 1 remote */
	int64_t gen;
	int64_t lut;
	int64_t ttl;
	int64_t src_id;
	as_udf_value bins;        /* list of bin names */
	as_udf_value deleted;     /* list of bin names this side deleted (AS_UDF_ABSENT when none) */
	as_udf_value bin_luts;    /* map name -> LUT, per bin, when the server keeps one (else AS_UDF_ABSENT) */
	as_udf_value bin_srcs;    /* map name -> source id, likewise */
} as_udf_event_record;

typedef struct as_udf_conflict_s {
	as_udf_value description; /* the whole map, for anything not decoded below */
	as_udf_value lost;        /* list: the incoming bins that would lose */
	bool dry_run;
	bool conflict_trigger;    /* bound with trigger=conflict: a bin would lose under bin convergence */
	bool bin_convergence;     /* the policy stood in for is bin-lut rather than a plain apply */
	bool remote_partial;      /* the incoming write carries only changed bins */
	bool remote_deleted_record; /* the incoming write is a record delete; remote has no bins */
	bool local_tombstone;     /* the current record is a tombstone: exists, no bins */
	as_udf_event_record local;  /* the current record */
	as_udf_event_record remote; /* the incoming record */
} as_udf_conflict;

/* A map entry by string key; AS_UDF_ABSENT kind when there is none or `m` is not a map. */
static inline as_udf_value as_udf_map_get(const as_udf_value* m, const char* key)
{
	as_udf_value v = { AS_UDF_ABSENT, 0, 0 };
	if (m->kind == AS_UDF_MAP) {
		as_udf_coll_get(as_udf_value_src(m), as_udf_value_at(m), key, as_udf_strlen(key), &v, (int32_t)sizeof(v));
	}
	return v;
}

static inline int64_t as_udf_map_int(const as_udf_value* m, const char* key, int64_t dflt)
{
	as_udf_value v = as_udf_map_get(m, key);
	return v.kind == AS_UDF_INT ? v.payload : dflt;
}

/* Whether a string value reads as `text`. */
static inline bool as_udf_str_is(const as_udf_value* v, const char* text)
{
	char buf[64];
	int32_t n = v->kind == AS_UDF_STR ? as_udf_bytes(v, buf, (int32_t)sizeof(buf)) : -1;
	if (n < 0 || n != as_udf_strlen(text)) return false;
	for (int32_t i = 0; i < n; i++) if (buf[i] != text[i]) return false;
	return true;
}

static inline bool as_udf_event_side(const as_udf_value* conflict, const char* key, int32_t which, as_udf_event_record* out)
{
	as_udf_value m = as_udf_map_get(conflict, key);
	if (m.kind != AS_UDF_MAP) return false;
	out->which = which;
	out->gen = as_udf_map_int(&m, "gen", 0);
	out->lut = as_udf_map_int(&m, "lut", 0);
	out->ttl = as_udf_map_int(&m, "ttl", 0);
	out->src_id = as_udf_map_int(&m, "src-id", 0);
	out->bins = as_udf_map_get(&m, "bins");
	out->deleted = as_udf_map_get(&m, "deleted");
	out->bin_luts = as_udf_map_get(&m, "bin-luts");
	out->bin_srcs = as_udf_map_get(&m, "bin-srcs");
	return out->bins.kind == AS_UDF_LIST;
}

/* Decode an event call's arguments. Returns AS_UDF_OK, or AS_UDF_ERR when they are not a conflict. */
static inline int32_t as_udf_event_decode(int32_t args, int32_t argc, as_udf_conflict* out)
{
	as_udf_value a[1];
	if (as_udf_args(args, argc, a, 1) < 1 || a[0].kind != AS_UDF_MAP) return AS_UDF_ERR;
	out->description = a[0];
	out->lost = as_udf_map_get(&a[0], "lost");
	as_udf_value mode = as_udf_map_get(&a[0], "mode");
	as_udf_value trigger = as_udf_map_get(&a[0], "trigger");
	as_udf_value policy = as_udf_map_get(&a[0], "policy");
	out->dry_run = as_udf_str_is(&mode, "dry-run");
	out->conflict_trigger = as_udf_str_is(&trigger, "conflict");
	out->bin_convergence = as_udf_str_is(&policy, "bin-lut");
	if (! as_udf_event_side(&a[0], "current", 0, &out->local) || ! as_udf_event_side(&a[0], "incoming", 1, &out->remote)) {
		return AS_UDF_ERR;
	}
	as_udf_value remote = as_udf_map_get(&a[0], "incoming");
	as_udf_value current = as_udf_map_get(&a[0], "current");
	as_udf_value v;
	v = as_udf_map_get(&remote, "partial");
	out->remote_partial = v.kind == AS_UDF_BOOL && v.payload != 0;
	v = as_udf_map_get(&remote, "deleted-record");
	out->remote_deleted_record = v.kind == AS_UDF_BOOL && v.payload != 0;
	v = as_udf_map_get(&current, "tombstone");
	out->local_tombstone = v.kind == AS_UDF_BOOL && v.payload != 0;
	return AS_UDF_OK;
}

/* A bin of one side. AS_UDF_NOT_FOUND when that side has no such bin. */
static inline int32_t as_udf_event_bin(const as_udf_event_record* r, const char* name, as_udf_value* out)
{
	return as_udf_event_bin_value(r->which, name, as_udf_strlen(name), out, (int32_t)sizeof(*out));
}

/* When this side last wrote the bin: per bin where the server keeps one, the record's LUT otherwise. */
static inline int64_t as_udf_event_bin_lut(const as_udf_event_record* r, const char* name)
{
	return as_udf_map_int(&r->bin_luts, name, r->lut);
}

static inline int64_t as_udf_event_bin_src(const as_udf_event_record* r, const char* name)
{
	return as_udf_map_int(&r->bin_srcs, name, r->src_id);
}

/* Whether the remote side wrote the bin later: by LUT, then source id - the order the server's bin
 * convergence uses, so both DCs answer the same. */
static inline bool as_udf_event_remote_newer(const as_udf_conflict* c, const char* name)
{
	int64_t ll = as_udf_event_bin_lut(&c->local, name);
	int64_t rl = as_udf_event_bin_lut(&c->remote, name);
	return rl > ll || (rl == ll && as_udf_event_bin_src(&c->remote, name) > as_udf_event_bin_src(&c->local, name));
}

/* Whether a list of strings (by position) holds `text`. */
static inline bool as_udf_list_has_str(const as_udf_value* list, const char* text)
{
	if (list->kind != AS_UDF_LIST) return false;
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(list), as_udf_value_at(list));
	if (cur <= 0) return false;
	as_udf_value w[32];
	bool found = false;
	for (int32_t n; ! found && (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) if (as_udf_str_is(&w[i], text)) { found = true; break; }
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
	return found;
}

/* Put any value under a string key: a scalar by value, guest bytes by address, a stored value by position, a
 * built collection by handle. */
static inline int32_t as_udf_map_put_value(int32_t handle, const char* key, const as_udf_value* v)
{
	int32_t key_len = as_udf_strlen(key);
	switch (v->kind) {
	case AS_UDF_INT: return as_udf_map_put_str_int(handle, key, key_len, v->payload);
	case AS_UDF_DOUBLE: return as_udf_map_put_str_handle(handle, key, key_len, as_udf_val_double(as_udf_value_double(v)));
	case AS_UDF_BOOL: return as_udf_map_put_str_handle(handle, key, key_len, as_udf_val_bool((int32_t)v->payload));
	case AS_UDF_NIL: return as_udf_map_put_str_handle(handle, key, key_len, as_udf_val_nil());
	case AS_UDF_GUEST_STR: return as_udf_map_put_str_str(handle, key, key_len, (const char*)(uintptr_t)v->payload, (int32_t)v->len);
	case AS_UDF_GUEST_BLOB: return as_udf_map_put_str_handle(handle, key, key_len, as_udf_val_blob((const void*)(uintptr_t)v->payload, (int32_t)v->len));
	case AS_UDF_GUEST_GEOJSON: return as_udf_map_put_str_handle(handle, key, key_len, as_udf_val_geojson((const char*)(uintptr_t)v->payload, (int32_t)v->len));
	case AS_UDF_HANDLE: return as_udf_map_put_str_handle(handle, key, key_len, (int32_t)v->payload);
	case AS_UDF_ABSENT: return AS_UDF_NOT_FOUND;
	default: return as_udf_map_put_str_at(handle, key, key_len, as_udf_value_src(v), as_udf_value_at(v));
	}
}

/* The verdicts. Each returns the status the handler returns. */
static inline int32_t as_udf_resolve_keep_local(void) { as_udf_result_str("keep-local", 10); return 0; }
static inline int32_t as_udf_resolve_take_remote(void) { as_udf_result_str("take-remote", 11); return 0; }
static inline int32_t as_udf_resolve_policy(void) { as_udf_result_str("policy", 6); return 0; }

/* The final record: `bins` (a built map; a bin absent or nil is deleted) and a TTL with a write's meaning,
 * or -1 to keep the one the incoming write asked for. */
static inline int32_t as_udf_resolve_record(int32_t bins, int64_t ttl)
{
	int32_t out = as_udf_map_new(3);
	if (out <= 0) return as_udf_failf("resolve: could not build the resolution");
	as_udf_map_put_str_str(out, "verdict", 7, "record", 6);
	as_udf_map_put_str_handle(out, "bins", 4, bins);
	if (ttl >= 0) as_udf_map_put_str_int(out, "ttl", 3, ttl);
	return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_failf("resolve: could not return the record");
}

/*----------------------------------------------------------
 * The other events. Each handler is `int32_t name(int32_t args, int32_t argc)`; as_udf_event_desc reads
 * the description, the as_udf_event_*_info helpers read its parts, as_udf_event_bin reads a record's bins
 * (`which` 0 for the first record the event hands over, 1 for the second), and the as_udf_answer_* helpers
 * deliver the answer. Declare a handler with AS_UDF_EVENT_HANDLER, which also emits its signature.
 */

AS_UDF_IMPORT(timer_set) int32_t as_udf_timer_set(const char* name, int32_t name_len, int64_t delay_ms);
AS_UDF_IMPORT(timer_cancel) int32_t as_udf_timer_cancel(const char* name, int32_t name_len);

/* The description map of an event call. AS_UDF_ERR when the arguments are not one. */
static inline int32_t as_udf_event_desc(int32_t args, int32_t argc, as_udf_value* desc)
{
	as_udf_value a[1];
	if (as_udf_args(args, argc, a, 1) < 1 || a[0].kind != AS_UDF_MAP) return AS_UDF_ERR;
	*desc = a[0];
	return AS_UDF_OK;
}

/* A record of the description (`before`, `after`, `record`), readable with as_udf_event_bin. */
static inline int32_t as_udf_event_get_record(const as_udf_value* desc, const char* key, int32_t which, as_udf_event_record* out)
{
	return as_udf_event_side(desc, key, which, out) ? AS_UDF_OK : AS_UDF_ERR;
}

/* A short string entry of a map into `out` (NUL-terminated); "" when absent. */
static inline void as_udf_map_str(const as_udf_value* m, const char* key, char* out, int32_t cap)
{
	as_udf_value v = as_udf_map_get(m, key);
	int32_t n = v.kind == AS_UDF_STR ? as_udf_bytes(&v, out, cap - 1) : -1;
	out[n < 0 ? 0 : n] = '\0';
}

static inline bool as_udf_map_bool(const as_udf_value* m, const char* key)
{
	as_udf_value v = as_udf_map_get(m, key);
	return v.kind == AS_UDF_BOOL && v.payload != 0;
}

typedef struct as_udf_write_info_s {
	char source[8];      /* client, batch, udf, ops, xdr */
	int64_t generation;  /* the generation the client asked to match, 0 for none */
	int64_t ttl;         /* the TTL the write asked for, with a write's meaning */
	bool create;         /* the record did not exist */
	bool durable_delete;
} as_udf_write_info;

static inline int32_t as_udf_event_write_info(const as_udf_value* desc, as_udf_write_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "write");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "source", out->source, (int32_t)sizeof(out->source));
	out->generation = as_udf_map_int(&m, "generation", 0);
	out->ttl = as_udf_map_int(&m, "ttl", 0);
	out->create = as_udf_map_bool(&m, "create");
	out->durable_delete = as_udf_map_bool(&m, "durable-delete");
	return AS_UDF_OK;
}

typedef struct as_udf_delete_info_s {
	char source[8];
	bool durable;
} as_udf_delete_info;

static inline int32_t as_udf_event_delete_info(const as_udf_value* desc, as_udf_delete_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "delete");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "source", out->source, (int32_t)sizeof(out->source));
	out->durable = as_udf_map_bool(&m, "durable");
	return AS_UDF_OK;
}

typedef struct as_udf_truncate_info_s {
	char ns[32];
	char set[64]; /* "" for the whole namespace */
	int64_t lut;  /* records last updated before this (ms since the epoch) go */
} as_udf_truncate_info;

static inline int32_t as_udf_event_truncate_info(const as_udf_value* desc, as_udf_truncate_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "truncate");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "namespace", out->ns, (int32_t)sizeof(out->ns));
	as_udf_map_str(&m, "set", out->set, (int32_t)sizeof(out->set));
	out->lut = as_udf_map_int(&m, "lut", 0);
	return AS_UDF_OK;
}

typedef struct as_udf_expire_info_s {
	int64_t void_time;       /* when the record was due, seconds since the Aerospike epoch */
	int64_t now;
	int64_t evict_void_time; /* before-evict only: records due before this are being evicted */
} as_udf_expire_info;

static inline int32_t as_udf_event_expire_info(const as_udf_value* desc, as_udf_expire_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "expire");
	if (m.kind != AS_UDF_MAP) m = as_udf_map_get(desc, "evict");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	out->void_time = as_udf_map_int(&m, "void-time", 0);
	out->now = as_udf_map_int(&m, "now", 0);
	out->evict_void_time = as_udf_map_int(&m, "evict-void-time", 0);
	return AS_UDF_OK;
}

typedef struct as_udf_read_info_s {
	char source[8];
	bool all; /* every bin was asked for, not named ones */
} as_udf_read_info;

static inline int32_t as_udf_event_read_info(const as_udf_value* desc, as_udf_read_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "read");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "source", out->source, (int32_t)sizeof(out->source));
	out->all = as_udf_map_bool(&m, "all");
	return AS_UDF_OK;
}

typedef struct as_udf_ship_info_s {
	char dc[32];
} as_udf_ship_info;

static inline int32_t as_udf_event_ship_info(const as_udf_value* desc, as_udf_ship_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "ship");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "dc", out->dc, (int32_t)sizeof(out->dc));
	return AS_UDF_OK;
}

typedef struct as_udf_lifecycle_info_s {
	char phase[32]; /* service-ready, shutdown, binding-deployed */
	char node[17];  /* this node's id, hex */
	as_udf_value binding; /* binding-deployed: a map of event, namespace, set, name, module, function; else AS_UDF_ABSENT */
} as_udf_lifecycle_info;

static inline int32_t as_udf_event_lifecycle_info(const as_udf_value* desc, as_udf_lifecycle_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "lifecycle");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "phase", out->phase, (int32_t)sizeof(out->phase));
	as_udf_map_str(&m, "node", out->node, (int32_t)sizeof(out->node));
	out->binding = as_udf_map_get(&m, "binding");
	return AS_UDF_OK;
}

typedef struct as_udf_cluster_info_s {
	char key[17];
	int64_t size;
	char self[17];
	as_udf_value nodes; /* list of hex node ids in succession order */
} as_udf_cluster_info;

static inline int32_t as_udf_event_cluster_info(const as_udf_value* desc, as_udf_cluster_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "cluster");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "key", out->key, (int32_t)sizeof(out->key));
	as_udf_map_str(&m, "self", out->self, (int32_t)sizeof(out->self));
	out->size = as_udf_map_int(&m, "size", 0);
	out->nodes = as_udf_map_get(&m, "nodes");
	return AS_UDF_OK;
}

typedef struct as_udf_transfer_info_s {
	char ns[32];
	int64_t partition;
	bool arrived; /* true: arrived here; false: departed */
	char peer[17];
} as_udf_transfer_info;

static inline int32_t as_udf_event_transfer_info(const as_udf_value* desc, as_udf_transfer_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "transfer");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	char dir[16];
	as_udf_map_str(&m, "namespace", out->ns, (int32_t)sizeof(out->ns));
	as_udf_map_str(&m, "direction", dir, (int32_t)sizeof(dir));
	as_udf_map_str(&m, "peer", out->peer, (int32_t)sizeof(out->peer));
	out->partition = as_udf_map_int(&m, "partition", 0);
	out->arrived = dir[0] == 'a';
	return AS_UDF_OK;
}

/* A timer handler's argument: the timer that came due. The handler runs as a record UDF on the record. */
typedef struct as_udf_timer_info_s {
	char name[64];
	int64_t due;
	int64_t now;
} as_udf_timer_info;

static inline int32_t as_udf_event_timer_info(int32_t args, int32_t argc, as_udf_timer_info* out)
{
	as_udf_value a[1];
	if (as_udf_args(args, argc, a, 1) < 1 || a[0].kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&a[0], "name", out->name, (int32_t)sizeof(out->name));
	out->due = as_udf_map_int(&a[0], "due", 0);
	out->now = as_udf_map_int(&a[0], "now", 0);
	return AS_UDF_OK;
}

/* The generic answer: a verdict word, with a code, a message, a bins map and a TTL where the kind takes
 * them (0, NULL, 0 and -1 for none). Returns the status the handler returns. */
static inline int32_t as_udf_answer(const char* verdict, int32_t code, const char* message, int32_t bins, int64_t ttl)
{
	if (code == 0 && message == NULL && bins == 0 && ttl < 0) {
		as_udf_result_str(verdict, as_udf_strlen(verdict));
		return 0;
	}
	int32_t out = as_udf_map_new(5);
	if (out <= 0) return as_udf_failf("answer: could not build the answer");
	as_udf_map_put_str_str(out, "verdict", 7, verdict, as_udf_strlen(verdict));
	if (code != 0) as_udf_map_put_str_int(out, "code", 4, code);
	if (message != NULL) as_udf_map_put_str_str(out, "message", 7, message, as_udf_strlen(message));
	if (bins != 0) as_udf_map_put_str_handle(out, "bins", 4, bins);
	if (ttl >= 0) as_udf_map_put_str_int(out, "ttl", 3, ttl);
	return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_failf("answer: could not return the answer");
}

/* The words each kind takes. before-write: accept / reject / record; before-delete, before-truncate: allow /
 * refuse; before-expire: expire / keep (ttl); before-evict: evict / protect; after-read: unchanged / record;
 * before-ship: ship / skip / record; the after- and once-per-node events and timers: ok. */
static inline int32_t as_udf_answer_accept(void) { return as_udf_answer("accept", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_reject(int32_t code, const char* message) { return as_udf_answer("reject", code, message, 0, -1); }
static inline int32_t as_udf_answer_record(int32_t bins, int64_t ttl) { return as_udf_answer("record", 0, NULL, bins, ttl); }
static inline int32_t as_udf_answer_allow(void) { return as_udf_answer("allow", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_refuse(int32_t code, const char* message) { return as_udf_answer("refuse", code, message, 0, -1); }
static inline int32_t as_udf_answer_expire(void) { return as_udf_answer("expire", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_keep(int64_t ttl) { return as_udf_answer("keep", 0, NULL, 0, ttl); }
static inline int32_t as_udf_answer_evict(void) { return as_udf_answer("evict", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_protect(void) { return as_udf_answer("protect", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_unchanged(void) { return as_udf_answer("unchanged", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_ship(void) { return as_udf_answer("ship", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_skip(void) { return as_udf_answer("skip", 0, NULL, 0, -1); }
static inline int32_t as_udf_answer_done(void) { return as_udf_answer("ok", 0, NULL, 0, -1); }

/* Declare an event handler of any kind: `AS_UDF_EVENT_HANDLER(name, "before-write",
 * "(before: record, after: record, write: write) -> decision") { ... }`. The kind and version go into the
 * signature `event <kind>/1 name(...)` that `event-set` checks. */
#define AS_UDF_EVENT_HANDLER(name, kind, params)                                                        \
	AS_UDF_SIGNATURE(name, "event " kind "/1 " #name params)                                             \
	AS_UDF_EXPORT(name) int32_t name(int32_t args, int32_t argc)

#define AS_UDF_EVENT_BEFORE_WRITE(name) AS_UDF_EVENT_HANDLER(name, "before-write", "(before: record, after: record, write: write) -> decision")
#define AS_UDF_EVENT_BEFORE_DELETE(name) AS_UDF_EVENT_HANDLER(name, "before-delete", "(record: record, delete: delete) -> gate")
#define AS_UDF_EVENT_AFTER_WRITE(name) AS_UDF_EVENT_HANDLER(name, "after-write", "(before: record, after: record, write: write) -> ()")
#define AS_UDF_EVENT_AFTER_DELETE(name) AS_UDF_EVENT_HANDLER(name, "after-delete", "(record: record, delete: delete) -> ()")
#define AS_UDF_EVENT_BEFORE_EXPIRE(name) AS_UDF_EVENT_HANDLER(name, "before-expire", "(record: record, expire: expire) -> expiry")
#define AS_UDF_EVENT_BEFORE_EVICT(name) AS_UDF_EVENT_HANDLER(name, "before-evict", "(record: record, evict: evict) -> eviction")
#define AS_UDF_EVENT_AFTER_READ(name) AS_UDF_EVENT_HANDLER(name, "after-read", "(record: record, read: read) -> read-result")
#define AS_UDF_EVENT_BEFORE_TRUNCATE(name) AS_UDF_EVENT_HANDLER(name, "before-truncate", "(truncate: truncate) -> gate")
#define AS_UDF_EVENT_AFTER_TRUNCATE(name) AS_UDF_EVENT_HANDLER(name, "after-truncate", "(truncate: truncate) -> ()")
#define AS_UDF_EVENT_BEFORE_SHIP(name) AS_UDF_EVENT_HANDLER(name, "before-ship", "(record: record, ship: ship) -> ship-decision")
#define AS_UDF_EVENT_PARTITION_TRANSFER(name) AS_UDF_EVENT_HANDLER(name, "partition-transfer", "(transfer: transfer) -> ()")
#define AS_UDF_EVENT_CLUSTER_CHANGE(name) AS_UDF_EVENT_HANDLER(name, "cluster-change", "(cluster: cluster) -> ()")
#define AS_UDF_EVENT_NODE_LIFECYCLE(name) AS_UDF_EVENT_HANDLER(name, "node-lifecycle", "(lifecycle: lifecycle) -> ()")
#define AS_UDF_EVENT_TIMER(name) AS_UDF_EVENT_HANDLER(name, "timer", "(timer: timer) -> ()")

/* Declare an xdr-merge handler: `AS_UDF_EVENT_XDR_MERGE(name) { ... }` with `args` and `argc` in
 * scope for as_udf_event_decode. */
#define AS_UDF_EVENT_XDR_MERGE(name)                                                                       \
	AS_UDF_SIGNATURE(name, "event xdr-merge/1 " #name "(current: record, incoming: record, conflict: conflict) -> resolution") \
	AS_UDF_EXPORT(name) int32_t name(int32_t args, int32_t argc)

/*==========================================================
 * Freestanding shims.
 *
 * clang emits calls to memcpy and memset for struct copies and array fills even with -nostdlib, so a module
 * needs definitions of them. Weak, so a UDF that brings its own wins.
 */

#ifndef AS_UDF_NO_LIBC_SHIMS
__attribute__((weak)) void* memcpy(void* dst, const void* src, unsigned long n)
{
	unsigned char* d = (unsigned char*)dst;
	const unsigned char* s = (const unsigned char*)src;
	for (unsigned long i = 0; i < n; i++) d[i] = s[i];
	return dst;
}

__attribute__((weak)) void* memset(void* dst, int c, unsigned long n)
{
	unsigned char* d = (unsigned char*)dst;
	for (unsigned long i = 0; i < n; i++) d[i] = (unsigned char)c;
	return dst;
}
#endif

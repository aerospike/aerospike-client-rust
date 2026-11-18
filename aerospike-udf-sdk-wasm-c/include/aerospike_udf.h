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
 *
 * Everything that acts on the record of the call is named `as_udf_rec_*`: reading a bin (`as_udf_rec_bin`),
 * staging one (`as_udf_rec_set_int` and the rest of the family), removing one (`as_udf_rec_remove`), the
 * metadata (`as_udf_rec_gen`, `as_udf_rec_ttl`, ...), and the two calls that end a write:
 * `as_udf_rec_save`, which writes what was staged and creates the record when it is not there, and
 * `as_udf_rec_delete`, which deletes the record. The records an event hands over are read through the
 * `as_udf_event_*` family instead. What is left without either prefix is not about a record: the clock,
 * the log, results, failure, and the list and map builders.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*==========================================================
 * The imports.
 */

/* Declares a host import: the function that follows is resolved from the `aerospike` import module under
 * `name`. */
#define AS_UDF_IMPORT(name) __attribute__((import_module("aerospike"), import_name(#name)))

/* The record's bins - reading. One import for any kind: the host writes a 16-byte value. */
AS_UDF_IMPORT(bin_value) int32_t as_udf_rec_bin_value(const char* name, int32_t name_len, void* out, int32_t cap);

/* The record's bins - writing. Staged until as_udf_rec_save. */

/* Stage an integer into a bin. Returns AS_UDF_OK, AS_UDF_ERR for a name that is empty or longer than 15
 * bytes, or AS_UDF_REFUSED when the record does not take the write. */
AS_UDF_IMPORT(set_int) int32_t as_udf_rec_set_int(const char* name, int32_t name_len, int64_t value);
/* Stage a double into a bin. Statuses as as_udf_rec_set_int. */
AS_UDF_IMPORT(set_double) int32_t as_udf_rec_set_double(const char* name, int32_t name_len, double value);
/* Stage a boolean into a bin: any non-zero `value` is true. Statuses as as_udf_rec_set_int. */
AS_UDF_IMPORT(set_bool) int32_t as_udf_rec_set_bool(const char* name, int32_t name_len, int32_t value);
/* Stage a string into a bin: `value_len` bytes, no terminator needed, copied by the host. Statuses as
 * as_udf_rec_set_int. */
AS_UDF_IMPORT(set_str) int32_t as_udf_rec_set_str(const char* name, int32_t name_len, const char* value, int32_t value_len);
/* Stage a blob into a bin, copied by the host. Statuses as as_udf_rec_set_int, and AS_UDF_TYPE_MISMATCH when the
 * bin holds bytes of another subtype (an HLL, say), which a plain blob would silently relabel. */
AS_UDF_IMPORT(set_blob) int32_t as_udf_rec_set_blob(const char* name, int32_t name_len, const void* value, int32_t value_len);
/* Stage GeoJSON text into a bin as a geospatial value rather than a string. The text is copied by the host and
 * not validated by this call. Statuses as as_udf_rec_set_int. */
AS_UDF_IMPORT(set_geojson) int32_t as_udf_rec_set_geojson(const char* name, int32_t name_len, const char* value, int32_t value_len);
/* Stage a value built on the host (a list, a map or a val_* value) into a bin, which takes it over. Returns
 * AS_UDF_OK, AS_UDF_ERR for an unusable handle or a bad name, AS_UDF_TYPE_MISMATCH when the handle holds bytes
 * and the bin holds bytes of another subtype, or AS_UDF_REFUSED. */
AS_UDF_IMPORT(set_handle) int32_t as_udf_rec_set_handle(const char* name, int32_t name_len, int32_t handle);
/* Stage the stored value at offset `at` of source `src` into a bin, copied on the host without the guest
 * decoding it. Returns AS_UDF_OK, AS_UDF_ERR for a bad source, offset or name, or AS_UDF_REFUSED when the record
 * does not take the write or a plain blob would replace bytes of another subtype. */
AS_UDF_IMPORT(set_at) int32_t as_udf_rec_set_at(const char* name, int32_t name_len, int32_t src, int32_t at);
/* Remove a bin from the record: staged like a write, and applied by as_udf_rec_save. This is the one way to
 * drop a bin; deleting the whole record is as_udf_rec_delete. Returns AS_UDF_OK, AS_UDF_ERR for a name that is
 * empty or longer than 15 bytes, or AS_UDF_REFUSED when the record does not take the removal. */
AS_UDF_IMPORT(bin_remove) int32_t as_udf_rec_remove(const char* name, int32_t name_len);
/* Write an array of 64-bit integers to a bin as a list, with `vals_len` the size of the array in bytes. Applied
 * at once, not staged: as_udf_rec_save is not needed. Returns AS_UDF_OK, AS_UDF_ERR for a bad name,
 * AS_UDF_REFUSED where there is no writable record (a stream UDF), or another non-zero status when the server
 * rejects the write. */
AS_UDF_IMPORT(set_list_ints) int32_t as_udf_rec_set_list_ints(const char* name, int32_t name_len, const int64_t* vals, int32_t vals_len);
/* Write an array of doubles to a bin as a list, with `vals_len` the size of the array in bytes. Applied at
 * once and with the statuses of as_udf_rec_set_list_ints. */
AS_UDF_IMPORT(set_list_doubles) int32_t as_udf_rec_set_list_doubles(const char* name, int32_t name_len, const double* vals, int32_t vals_len);

/* Collections, by position in a source. */

/* Read the next values of a cursor into `out`, as many 16-byte values as `cap` bytes hold, and step past them.
 * Returns the count, 0 at the end, or AS_UDF_ERR. A map yields its keys and values in turn; a nested list or
 * map comes out as one value, by position. */
AS_UDF_IMPORT(cursor_values) int32_t as_udf_cursor_values(int32_t cursor, void* out, int32_t cap);
/* Copy the bytes of the string, blob, GeoJSON or HLL value at offset `at` of source `src` into `out`. Returns
 * the length, a needed-length status when `cap` is too small (nothing is written), AS_UDF_TYPE_MISMATCH when
 * the value is of another kind, or AS_UDF_ERR for a bad source or offset. */
AS_UDF_IMPORT(cursor_bytes_at) int32_t as_udf_cursor_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
/* Copy the value at offset `at` of source `src` as the server stores it, msgpack header included, into `out`.
 * Returns the length, a needed-length status when `cap` is too small (nothing is written), or AS_UDF_ERR for a
 * bad source or offset. */
AS_UDF_IMPORT(coll_bytes_at) int32_t as_udf_coll_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
/* Compare the stored encodings of two values byte by byte, as memcmp would, a shorter one that is a prefix of
 * the other first. Returns -1, 0 or 1, or AS_UDF_TYPE_MISMATCH when either is not a stored value. */
AS_UDF_IMPORT(coll_cmp_at) int32_t as_udf_coll_cmp_at(int32_t src1, int32_t at1, int32_t src2, int32_t at2);
/* Open a cursor over the list or map at offset `at` of source `src`, positioned at its first element. Returns
 * the cursor's handle, or 0 when the value is not a list or map or no handle is free. */
AS_UDF_IMPORT(cursor_enter_at) int32_t as_udf_cursor_enter_at(int32_t src, int32_t at);
/* Look up a string key in the map at offset `at` of source `src` and write the entry's 16-byte value to `out`
 * (`cap` at least 16). Returns AS_UDF_OK, AS_UDF_NOT_FOUND for no such key or not a map (`out` is then
 * AS_UDF_ABSENT), or AS_UDF_ERR for a bad source, offset or `cap`. The search is linear in the map's entries. */
AS_UDF_IMPORT(coll_get) int32_t as_udf_coll_get(int32_t src, int32_t at, const char* key, int32_t key_len, void* out, int32_t cap);
/* Look up an integer key in the map at offset `at` of source `src`. Output and statuses as as_udf_coll_get. */
AS_UDF_IMPORT(coll_get_i) int32_t as_udf_coll_get_i(int32_t src, int32_t at, int64_t key, void* out, int32_t cap);
/* The element at `index` of the list at offset `at` of source `src`, written to `out` as a 16-byte value
 * (`cap` at least 16), without opening a cursor. Returns AS_UDF_OK, AS_UDF_NOT_FOUND past the end or when the
 * value is not a list (`out` is then AS_UDF_ABSENT), or AS_UDF_ERR for a bad source, offset or `cap`. */
AS_UDF_IMPORT(list_get) int32_t as_udf_list_get(int32_t src, int32_t at, int32_t index, void* out, int32_t cap);
/* Give a cursor's handle back. Only the most recently created handle is released; any other stays until the
 * call ends, when the host releases them all. */
AS_UDF_IMPORT(cursor_close) void as_udf_cursor_close(int32_t cursor);
/* Make a list or map built on the host readable by position: returns a source handle over a packed copy of it,
 * or 0 when `handle` is not a list or map or no handle is free. */
AS_UDF_IMPORT(pack_handle) int32_t as_udf_pack_handle(int32_t handle);
/* Read the call's argument list into `out` as 16-byte values, as many as `cap` bytes hold. Returns the count
 * written, or AS_UDF_ERR when `handle` is not a list's. */
AS_UDF_IMPORT(args_values) int32_t as_udf_args_values(int32_t handle, void* out, int32_t cap);
/* The number of elements of a built list, of entries of a built map, or of the collection a cursor was opened
 * over. AS_UDF_ERR for any other handle. */
AS_UDF_IMPORT(coll_size) int32_t as_udf_coll_size(int32_t handle);

/* Collections - building, on the host, as handles. */

/* Start an empty list on the host, sized for `capacity` elements; it grows past that. Returns its handle, or 0
 * when none is free: a call holds at most 256 handles. */
AS_UDF_IMPORT(list_new) int32_t as_udf_list_new(int32_t capacity);
/* Start an empty map on the host, sized for `capacity` entries. The map is ordered by key, whatever order the
 * entries are put in. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(map_new) int32_t as_udf_map_new(int32_t capacity);
/* Whether a map the call built (a handle from as_udf_map_new) has a string key: 1 or 0, or a negative status
 * when `handle` is not a built map. Asks the host directly, so the map is not packed to answer it. */
AS_UDF_IMPORT(map_has) int32_t as_udf_map_has(int32_t handle, const char* key, int32_t key_len);
/* Whether a map the call built has an integer key. Answers as as_udf_map_has. */
AS_UDF_IMPORT(map_has_i) int32_t as_udf_map_has_i(int32_t handle, int64_t key);
/* An integer as a value of its own, for as_udf_list_push, as_udf_map_put or as_udf_rec_set_handle. Returns its
 * handle, or 0 when none is free. */
AS_UDF_IMPORT(val_int) int32_t as_udf_val_int(int64_t value);
/* A double as a value of its own. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(val_double) int32_t as_udf_val_double(double value);
/* A boolean as a value of its own: any non-zero `value` is true. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(val_bool) int32_t as_udf_val_bool(int32_t value);
/* Nil as a value of its own. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(val_nil) int32_t as_udf_val_nil(void);
/* A string as a value of its own: `value_len` bytes, copied by the host. Returns its handle, or 0 when none is
 * free. */
AS_UDF_IMPORT(val_str) int32_t as_udf_val_str(const char* value, int32_t value_len);
/* A blob as a value of its own, copied by the host. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(val_blob) int32_t as_udf_val_blob(const void* value, int32_t value_len);
/* GeoJSON text as a value of its own, so that it enters a built collection as GeoJSON rather than as a string.
 * Copied by the host. Returns its handle, or 0 when none is free. */
AS_UDF_IMPORT(val_geojson) int32_t as_udf_val_geojson(const char* value, int32_t value_len);
/* Append the value behind handle `value` to a built list, which takes it over. Returns AS_UDF_OK, or AS_UDF_ERR
 * when `handle` is not a built list or `value` is not usable. */
AS_UDF_IMPORT(list_push) int32_t as_udf_list_push(int32_t handle, int32_t value);
/* Append an integer to a built list without a value handle. Returns AS_UDF_OK, or AS_UDF_ERR when `handle` is
 * not a built list. */
AS_UDF_IMPORT(list_push_int) int32_t as_udf_list_push_int(int32_t handle, int64_t value);
/* Append a string to a built list without a value handle: `value_len` bytes, copied by the host. Returns
 * AS_UDF_OK, or AS_UDF_ERR when `handle` is not a built list. */
AS_UDF_IMPORT(list_push_str) int32_t as_udf_list_push_str(int32_t handle, const char* value, int32_t value_len);
/* Put an entry into a built map, key and value both by handle; the map takes them over. Returns AS_UDF_OK, or
 * AS_UDF_ERR when `handle` is not a built map or either handle is not usable. */
AS_UDF_IMPORT(map_put) int32_t as_udf_map_put(int32_t handle, int32_t key, int32_t value);
/* Put an integer under a string key of a built map. Returns AS_UDF_OK, or AS_UDF_ERR when `handle` is not a
 * built map. */
AS_UDF_IMPORT(map_put_str_int) int32_t as_udf_map_put_str_int(int32_t handle, const char* key, int32_t key_len, int64_t value);
/* Put a string under a string key of a built map; both are copied by the host. Returns AS_UDF_OK, or AS_UDF_ERR
 * when `handle` is not a built map. */
AS_UDF_IMPORT(map_put_str_str) int32_t as_udf_map_put_str_str(int32_t handle, const char* key, int32_t key_len, const char* value, int32_t value_len);
/* Put the value behind handle `value` under a string key of a built map, which takes it over. Returns
 * AS_UDF_OK, or AS_UDF_ERR when `handle` is not a built map or `value` is not usable. */
AS_UDF_IMPORT(map_put_str_handle) int32_t as_udf_map_put_str_handle(int32_t handle, const char* key, int32_t key_len, int32_t value);
/* Append the stored value at offset `at` of source `src` to a built list, copied on the host without the guest
 * decoding it. Returns AS_UDF_OK, or AS_UDF_ERR when `handle` is not a built list or the position is bad. */
AS_UDF_IMPORT(list_push_at) int32_t as_udf_list_push_at(int32_t handle, int32_t src, int32_t at);
/* Put the stored value at offset `at` of source `src` under a string key of a built map. Returns AS_UDF_OK, or
 * AS_UDF_ERR when `handle` is not a built map or the position is bad. */
AS_UDF_IMPORT(map_put_str_at) int32_t as_udf_map_put_str_at(int32_t handle, const char* key, int32_t key_len, int32_t src, int32_t at);
/* Put the stored value at offset `at` of source `src` under an integer key of a built map. Returns AS_UDF_OK,
 * or AS_UDF_ERR when `handle` is not a built map or the position is bad. */
AS_UDF_IMPORT(map_put_i_at) int32_t as_udf_map_put_i_at(int32_t handle, int64_t key, int32_t src, int32_t at);
/* Append a batch of values to a built list: `vals` is an array of 16-byte values and `vals_len` its size in
 * bytes. Returns how many went in, fewer than were given when one could not be taken, or AS_UDF_ERR when
 * `handle` is not a built list. */
AS_UDF_IMPORT(list_push_values) int32_t as_udf_list_push_values(int32_t handle, const void* vals, int32_t vals_len);
/* Put a batch of entries into a built map: `pairs` is an array of 32-byte pairs, a 16-byte key then a 16-byte
 * value, and `pairs_len` its size in bytes. Returns how many entries went in, fewer than were given when one
 * could not be taken, or AS_UDF_ERR when `handle` is not a built map. */
AS_UDF_IMPORT(map_put_entries) int32_t as_udf_map_put_entries(int32_t handle, const void* pairs, int32_t pairs_len);

/* Results and failure. */

/* Set the call's result to nil. Of several result calls, the last one wins. */
AS_UDF_IMPORT(result_nil) void as_udf_result_nil(void);
/* Set the call's result to an integer. */
AS_UDF_IMPORT(result_int) void as_udf_result_int(int64_t value);
/* Set the call's result to a double. */
AS_UDF_IMPORT(result_double) void as_udf_result_double(double value);
/* Set the call's result to a boolean: any non-zero `value` is true. */
AS_UDF_IMPORT(result_bool) void as_udf_result_bool(int32_t value);
/* Set the call's result to a string: `value_len` bytes, copied by the host. */
AS_UDF_IMPORT(result_str) void as_udf_result_str(const char* value, int32_t value_len);
/* Set the call's result to a blob, copied by the host. */
AS_UDF_IMPORT(result_blob) void as_udf_result_blob(const void* value, int32_t value_len);
/* Set the call's result to GeoJSON text, copied by the host. */
AS_UDF_IMPORT(result_geojson) void as_udf_result_geojson(const char* value, int32_t value_len);
/* Set the call's result to a value built on the host, which leaves its handle. Returns AS_UDF_OK, or AS_UDF_ERR
 * for an unusable handle. */
AS_UDF_IMPORT(result_handle) int32_t as_udf_result_handle(int32_t handle);
/* Set the call's result to the stored value at offset `at` of source `src`. Returns AS_UDF_OK, or AS_UDF_ERR
 * for a bad source or offset. */
AS_UDF_IMPORT(result_at) int32_t as_udf_result_at(int32_t src, int32_t at);
/* Mark the call as failed with a message for the client: `message_len` bytes, of which the host keeps the first
 * 511. The UDF still has to return; as_udf_failf does both. */
AS_UDF_IMPORT(fail) void as_udf_fail(const char* message, int32_t message_len);

/* The record's metadata. */

/* The record's generation. */
AS_UDF_IMPORT(gen) int64_t as_udf_rec_gen(void);
/* The record's remaining lifetime in seconds; 0 for a record that does not expire. */
AS_UDF_IMPORT(ttl) int64_t as_udf_rec_ttl(void);
/* When the record was last written, in milliseconds, as the server stores it: counted from the Aerospike epoch
 * (2010-01-01 UTC), not the Unix one. */
AS_UDF_IMPORT(last_update) int64_t as_udf_rec_last_update(void);
/* The record's stored size in bytes. */
AS_UDF_IMPORT(record_size) int64_t as_udf_rec_size(void);
/* How many bins the record has. */
AS_UDF_IMPORT(numbins) int64_t as_udf_rec_numbins(void);
/* The server's wall clock, in milliseconds since the Unix epoch. Returns -1 in an event handler, where the
 * clock is refused: a decision that reads it cannot converge across clusters. */
AS_UDF_IMPORT(current_time) int64_t as_udf_current_time(void);
/* Set the TTL the record's write carries, in seconds, with a write's meaning. Returns AS_UDF_OK. */
AS_UDF_IMPORT(set_ttl) int32_t as_udf_rec_set_ttl(int32_t seconds);
/* Copy the record's set name into `out`, without a terminator. Returns the length, a needed-length status when
 * `cap` is too small, or AS_UDF_NOT_FOUND when the record is in no set. */
AS_UDF_IMPORT(set_name) int32_t as_udf_rec_set_name(void* out, int32_t cap);
/* The kind of the record's user key (AS_UDF_INT, AS_UDF_STR or AS_UDF_BLOB), or AS_UDF_ABSENT when the record
 * was stored without one. */
AS_UDF_IMPORT(key_type) int32_t as_udf_rec_key_type(void);
/* The record's user key when it is an integer; 0 when it is not, so check as_udf_rec_key_type first. */
AS_UDF_IMPORT(key_int) int64_t as_udf_rec_key_int(void);
/* Copy the record's string or blob user key into `out`. Returns the length, a needed-length status when `cap`
 * is too small, AS_UDF_NOT_FOUND when the record was stored without a key, or AS_UDF_ERR for an integer key. */
AS_UDF_IMPORT(key_bytes) int32_t as_udf_rec_key_bytes(void* out, int32_t cap);
/* Copy the record's digest into `out`: 20 bytes. Returns the length, or a needed-length status when `cap` is
 * too small. */
AS_UDF_IMPORT(digest) int32_t as_udf_rec_digest(void* out, int32_t cap);
/* The two host calls as_udf_rec_save chooses between: the host has one for a record that is not there yet and
 * one for a record that is, and refuses each on the other. Not for a UDF to call: as_udf_rec_save is the
 * record's one write. */
AS_UDF_IMPORT(rec_create) int32_t as_udf_host_rec_create(void);
AS_UDF_IMPORT(rec_update) int32_t as_udf_host_rec_update(void);
/* Delete the record; nothing has to be saved first. The delete is carried out when the call returns, so
 * as_udf_rec_exists keeps answering what it did until then. Returns AS_UDF_OK, or AS_UDF_REFUSED when the
 * server does not delete it. */
AS_UDF_IMPORT(rec_remove) int32_t as_udf_rec_delete(void);
/* Whether the record exists: 1 or 0, not a status. */
AS_UDF_IMPORT(rec_exists) int32_t as_udf_rec_exists(void);
/* Write a line to the server log at an AS_UDF_LOG_* level; any other level logs as debug. `message_len` bytes,
 * of which the host keeps the first 511. */
AS_UDF_IMPORT(log) void as_udf_log(int32_t level, const char* message, int32_t message_len);

/*==========================================================
 * The contract's constants.
 */

/* Status codes. Anything at or below AS_UDF_NEEDED_BASE is "the buffer was too small": -(rc) - 16 bytes needed. */

/* Success. */
#define AS_UDF_OK 0
/* The call could not be carried out: a bad handle, position, name or buffer. */
#define AS_UDF_ERR (-1)
/* No such bin, map key or value. */
#define AS_UDF_NOT_FOUND (-2)
/* The value is not of the kind the call works on. */
#define AS_UDF_TYPE_MISMATCH (-3)
/* The server refused the operation. */
#define AS_UDF_REFUSED (-4)
/* Where needed-length answers start. A status at or below this means only that the buffer was too small:
 * `-(rc) - 16` bytes are needed. */
#define AS_UDF_NEEDED_BASE (-16)

/* Value kinds, as the host hands them out. */

/* No value: a bin, map entry or key that is not there. */
#define AS_UDF_ABSENT (-1)
/* Nil. */
#define AS_UDF_NIL 0
/* A boolean: the payload is 0 or 1. */
#define AS_UDF_BOOL 1
/* A 64-bit integer, in the payload. */
#define AS_UDF_INT 2
/* A double: its bits are in the payload, read with as_udf_value_double. */
#define AS_UDF_DOUBLE 3
/* A string the host holds: `len` bytes, read by position with as_udf_bytes. */
#define AS_UDF_STR 4
/* A blob the host holds: `len` bytes, read by position with as_udf_bytes. */
#define AS_UDF_BLOB 5
/* A list the host holds: `len` elements, walked by position with a cursor. */
#define AS_UDF_LIST 6
/* A map the host holds: `len` entries, read by position with a cursor or as_udf_map_get. */
#define AS_UDF_MAP 7
/* A value a UDF cannot interpret. It carries no length and no payload. */
#define AS_UDF_OTHER 8
/* GeoJSON the host holds: `len` bytes of text, read by position with as_udf_bytes. */
#define AS_UDF_GEOJSON 9
/* A HyperLogLog the host holds: `len` bytes, read with as_udf_bytes. Write one back by position
 * (as_udf_rec_set_at), which keeps it an HLL. */
#define AS_UDF_HLL 10

/* Kinds a guest uses only in a batch it hands the host: bytes of its own by address, a built collection by handle. */

/* In a batch only: a string of the guest's, the payload its address in guest memory and `len` its length. */
#define AS_UDF_GUEST_STR 16
/* In a batch only: a blob of the guest's, the payload its address in guest memory and `len` its length. */
#define AS_UDF_GUEST_BLOB 17
/* In a batch only: GeoJSON text of the guest's, the payload its address in guest memory and `len` its
 * length. */
#define AS_UDF_GUEST_GEOJSON 18
/* In a batch only: a value built on the host, the payload its handle. */
#define AS_UDF_HANDLE 19

/* Log level for as_udf_log: warning, the most severe. */
#define AS_UDF_LOG_WARN 0
/* Log level for as_udf_log: info. */
#define AS_UDF_LOG_INFO 1
/* Log level for as_udf_log: debug. */
#define AS_UDF_LOG_DEBUG 2

/* The 16-byte value: kind, length (bytes, elements or entries), payload (the scalar, or source << 32 | offset). */
typedef struct as_udf_value_s {
	int32_t kind;
	uint32_t len;
	int64_t payload;
} as_udf_value;

/* Where a string, blob or collection sits: the source handle and the offset in it. */
static inline int32_t as_udf_value_src(const as_udf_value* v) { return (int32_t)(v->payload >> 32); }
/* The offset of a string, blob or collection value within its source. */
static inline int32_t as_udf_value_at(const as_udf_value* v) { return (int32_t)(v->payload & 0xffffffff); }
/* The double an AS_UDF_DOUBLE value carries in its payload. */
static inline double as_udf_value_double(const as_udf_value* v) { double d; __builtin_memcpy(&d, &v->payload, 8); return d; }

/*==========================================================
 * Conveniences.
 */

/* Exports the function that follows from the module under `name`, which is what makes it callable as a UDF. */
#define AS_UDF_EXPORT(name) __attribute__((export_name(#name)))

/* A signature export beside a UDF, as the Rust macros emit: `fn name(a: int, b: str[256]) -> value`. */
#define AS_UDF_SIGNATURE(name, text)                                                   \
	static const char as_udf_sig_##name[] = text;                                    \
	__attribute__((export_name("__udf_sig_" #name))) int32_t as_udf_sig_fn_##name(void) \
	{                                                                                  \
		return (int32_t)(uintptr_t)as_udf_sig_##name;                                  \
	}

/* The length of a NUL-terminated string; a freestanding module has no libc strlen. */
static inline int32_t as_udf_strlen(const char* s)
{
	int32_t n = 0;
	while (s[n] != '\0') n++;
	return n;
}

/* A bin by name, as a value. Returns the status; AS_UDF_NOT_FOUND for no such bin. */
static inline int32_t as_udf_rec_bin(const char* name, as_udf_value* out)
{
	return as_udf_rec_bin_value(name, as_udf_strlen(name), out, (int32_t)sizeof(*out));
}

/* Write the record: the staged bins and removals go to it, and it is created when it does not exist. With
 * nothing staged it is still a write to a record that exists (the server treats it as a touch: the generation
 * and the last-update time move); on a key with no record it creates nothing. Returns AS_UDF_OK, or
 * AS_UDF_REFUSED when the server does not take the write, as in a stream UDF. */
static inline int32_t as_udf_rec_save(void)
{
	return as_udf_rec_exists() ? as_udf_host_rec_update() : as_udf_host_rec_create();
}

/* The element at `index` of a stored list; AS_UDF_ABSENT kind past the end or when `list` is not a list. */
static inline as_udf_value as_udf_list_at(const as_udf_value* list, int32_t index)
{
	as_udf_value v = { AS_UDF_ABSENT, 0, 0 };
	if (list->kind == AS_UDF_LIST && index >= 0) {
		as_udf_list_get(as_udf_value_src(list), as_udf_value_at(list), index, &v, (int32_t)sizeof(v));
	}
	return v;
}

/* Whether a map the call built has the NUL-terminated string key. False for a handle that is not a built
 * map. */
static inline bool as_udf_map_contains(int32_t handle, const char* key)
{
	return as_udf_map_has(handle, key, as_udf_strlen(key)) > 0;
}

/* Whether a map the call built has the integer key. False for a handle that is not a built map. */
static inline bool as_udf_map_contains_i(int32_t handle, int64_t key)
{
	return as_udf_map_has_i(handle, key) > 0;
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
/* A double as a 16-byte value, for a batch. */
static inline as_udf_value as_udf_v_double(double d) { as_udf_value v = { AS_UDF_DOUBLE, 0, 0 }; __builtin_memcpy(&v.payload, &d, 8); return v; }
/* A string of the guest's as a 16-byte value, for a batch. Only the address travels, so the bytes must stay
 * valid until the batch call. */
static inline as_udf_value as_udf_v_str(const char* s, int32_t len) { as_udf_value v = { AS_UDF_GUEST_STR, (uint32_t)len, (int64_t)(uintptr_t)s }; return v; }
/* A blob of the guest's as a 16-byte value, for a batch. Only the address travels, so the bytes must stay
 * valid until the batch call. */
static inline as_udf_value as_udf_v_blob(const void* b, int32_t len) { as_udf_value v = { AS_UDF_GUEST_BLOB, (uint32_t)len, (int64_t)(uintptr_t)b }; return v; }
/* A value built on the host as a 16-byte value, for a batch: a list or map nested in another. */
static inline as_udf_value as_udf_v_handle(int32_t h) { as_udf_value v = { AS_UDF_HANDLE, 0, h }; return v; }

/* Fail the call with a NUL-terminated message, and return the non-zero status a UDF then returns. */
static inline int32_t as_udf_failf(const char* message)
{
	as_udf_fail(message, as_udf_strlen(message));
	return 1;
}

/* Log a NUL-terminated message at an AS_UDF_LOG_* level. */
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

/* A bin of one of an event's records, as a 16-byte value in `out` (`cap` at least 16): `which` is 0 for the
 * first record the event hands over, 1 for the second. Returns AS_UDF_OK, AS_UDF_NOT_FOUND when that record or
 * bin is not there, or AS_UDF_ERR when `cap` is too small. */
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

/* An xdr-merge call, decoded by as_udf_event_decode: the two records and what the description says of the
 * conflict. */
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

/* An integer entry of a map by string key; `dflt` when there is none or it is not an integer. */
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

/* Decode the record under `key` of a description into `out`, marked as record `which` for as_udf_event_bin.
 * False when the entry is not a map or carries no list of bins. */
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

/* The id of the source that last wrote the bin on this side: per bin where the server keeps one, the record's
 * source id otherwise. */
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

/* Answer `keep-local`: the local record stays and the incoming write is dropped. Returns the status the handler
 * returns. */
static inline int32_t as_udf_resolve_keep_local(void) { as_udf_result_str("keep-local", 10); return 0; }
/* Answer `take-remote`: the incoming write is applied whole. Returns the status the handler returns. */
static inline int32_t as_udf_resolve_take_remote(void) { as_udf_result_str("take-remote", 11); return 0; }
/* Answer `policy`: the namespace's policy decides, as it would have without the event. Returns the status the
 * handler returns. */
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

/* Set a timer on the record in hand: the set's `timer` handler runs on this record `delay_ms` milliseconds from
 * now, under `name` (1 to 63 bytes). Returns AS_UDF_OK, or AS_UDF_REFUSED when the name is out of range, there
 * is no record in hand, or the server does not take the timer. */
AS_UDF_IMPORT(timer_set) int32_t as_udf_timer_set(const char* name, int32_t name_len, int64_t delay_ms);
/* Cancel the timer `name` on the record in hand. Returns AS_UDF_OK, or AS_UDF_REFUSED when the name is out of
 * range, there is no record in hand, or the server does not cancel it. */
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

/* A boolean entry of a map by string key; false when there is none or it is not a boolean. */
static inline bool as_udf_map_bool(const as_udf_value* m, const char* key)
{
	as_udf_value v = as_udf_map_get(m, key);
	return v.kind == AS_UDF_BOOL && v.payload != 0;
}

/* The `write` part of a before-write or after-write description. */
typedef struct as_udf_write_info_s {
	char source[8];      /* client, batch, udf, ops, xdr */
	int64_t generation;  /* the generation the client asked to match, 0 for none */
	int64_t ttl;         /* the TTL the write asked for, with a write's meaning */
	bool create;         /* the record did not exist */
	bool durable_delete;
} as_udf_write_info;

/* Read the `write` part of a description into `out`. AS_UDF_ERR when the description has none; a field that is
 * missing, or a string too long for its buffer, reads as 0, false or "". */
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

/* The `delete` part of a before-delete or after-delete description: where the delete came from, and whether it
 * is durable. */
typedef struct as_udf_delete_info_s {
	char source[8];
	bool durable;
} as_udf_delete_info;

/* Read the `delete` part of a description into `out`. AS_UDF_ERR when the description has none; missing fields
 * read as in as_udf_event_write_info. */
static inline int32_t as_udf_event_delete_info(const as_udf_value* desc, as_udf_delete_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "delete");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "source", out->source, (int32_t)sizeof(out->source));
	out->durable = as_udf_map_bool(&m, "durable");
	return AS_UDF_OK;
}

/* The `truncate` part of a before-truncate or after-truncate description. */
typedef struct as_udf_truncate_info_s {
	char ns[32];
	char set[64]; /* "" for the whole namespace */
	int64_t lut;  /* records last updated before this (ms since the epoch) go */
} as_udf_truncate_info;

/* Read the `truncate` part of a description into `out`. AS_UDF_ERR when the description has none; missing
 * fields read as in as_udf_event_write_info. */
static inline int32_t as_udf_event_truncate_info(const as_udf_value* desc, as_udf_truncate_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "truncate");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "namespace", out->ns, (int32_t)sizeof(out->ns));
	as_udf_map_str(&m, "set", out->set, (int32_t)sizeof(out->set));
	out->lut = as_udf_map_int(&m, "lut", 0);
	return AS_UDF_OK;
}

/* The `expire` part of a before-expire description, or the `evict` part of a before-evict one. */
typedef struct as_udf_expire_info_s {
	int64_t void_time;       /* when the record was due, seconds since the Aerospike epoch */
	int64_t now;
	int64_t evict_void_time; /* before-evict only: records due before this are being evicted */
} as_udf_expire_info;

/* Read the `expire` part of a description, or its `evict` part when there is no `expire`, into `out`.
 * AS_UDF_ERR when the description has neither; a missing field reads as 0. */
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

/* The `read` part of an after-read description: where the read came from, and what it asked for. */
typedef struct as_udf_read_info_s {
	char source[8];
	bool all; /* every bin was asked for, not named ones */
} as_udf_read_info;

/* Read the `read` part of a description into `out`. AS_UDF_ERR when the description has none; missing fields
 * read as in as_udf_event_write_info. */
static inline int32_t as_udf_event_read_info(const as_udf_value* desc, as_udf_read_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "read");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "source", out->source, (int32_t)sizeof(out->source));
	out->all = as_udf_map_bool(&m, "all");
	return AS_UDF_OK;
}

/* The `ship` part of a before-ship description: the name of the DC. */
typedef struct as_udf_ship_info_s {
	char dc[32];
} as_udf_ship_info;

/* Read the `ship` part of a description into `out`. AS_UDF_ERR when the description has none. */
static inline int32_t as_udf_event_ship_info(const as_udf_value* desc, as_udf_ship_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "ship");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "dc", out->dc, (int32_t)sizeof(out->dc));
	return AS_UDF_OK;
}

/* The `lifecycle` part of a node-lifecycle description. */
typedef struct as_udf_lifecycle_info_s {
	char phase[32]; /* service-ready, shutdown, binding-deployed */
	char node[17];  /* this node's id, hex */
	as_udf_value binding; /* binding-deployed: a map of event, namespace, set, name, module, function; else AS_UDF_ABSENT */
} as_udf_lifecycle_info;

/* Read the `lifecycle` part of a description into `out`. AS_UDF_ERR when the description has none; missing
 * fields read as in as_udf_event_write_info. */
static inline int32_t as_udf_event_lifecycle_info(const as_udf_value* desc, as_udf_lifecycle_info* out)
{
	as_udf_value m = as_udf_map_get(desc, "lifecycle");
	if (m.kind != AS_UDF_MAP) return AS_UDF_ERR;
	as_udf_map_str(&m, "phase", out->phase, (int32_t)sizeof(out->phase));
	as_udf_map_str(&m, "node", out->node, (int32_t)sizeof(out->node));
	out->binding = as_udf_map_get(&m, "binding");
	return AS_UDF_OK;
}

/* The `cluster` part of a cluster-change description: the description's `key`, `size` and `self` entries and
 * its list of nodes. */
typedef struct as_udf_cluster_info_s {
	char key[17];
	int64_t size;
	char self[17];
	as_udf_value nodes; /* list of hex node ids in succession order */
} as_udf_cluster_info;

/* Read the `cluster` part of a description into `out`. AS_UDF_ERR when the description has none; missing fields
 * read as in as_udf_event_write_info, and `nodes` as AS_UDF_ABSENT. */
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

/* The `transfer` part of a partition-transfer description: the namespace, the partition, the direction and the
 * peer. */
typedef struct as_udf_transfer_info_s {
	char ns[32];
	int64_t partition;
	bool arrived; /* true: arrived here; false: departed */
	char peer[17];
} as_udf_transfer_info;

/* Read the `transfer` part of a description into `out`. AS_UDF_ERR when the description has none; missing
 * fields read as in as_udf_event_write_info. */
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

/* Decode a timer handler's arguments into `out`. AS_UDF_ERR when they are not a timer's description; a missing
 * field reads as 0 or "". */
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

/* before-write: let the write proceed as it is. Returns the status the handler returns. */
static inline int32_t as_udf_answer_accept(void) { return as_udf_answer("accept", 0, NULL, 0, -1); }
/* before-write: fail the write with a result code and a NUL-terminated message (0 and NULL for none). Returns
 * the status the handler returns. */
static inline int32_t as_udf_answer_reject(int32_t code, const char* message) { return as_udf_answer("reject", code, message, 0, -1); }
/* before-write, after-read, before-ship: answer with this record instead. `bins` is the handle of a built map;
 * `ttl` is a TTL with a write's meaning, or -1 for none. Returns the status the handler returns. */
static inline int32_t as_udf_answer_record(int32_t bins, int64_t ttl) { return as_udf_answer("record", 0, NULL, bins, ttl); }
/* before-delete, before-truncate: let it proceed. Returns the status the handler returns. */
static inline int32_t as_udf_answer_allow(void) { return as_udf_answer("allow", 0, NULL, 0, -1); }
/* before-delete, before-truncate: refuse with a result code and a NUL-terminated message (0 and NULL for none).
 * Returns the status the handler returns. */
static inline int32_t as_udf_answer_refuse(int32_t code, const char* message) { return as_udf_answer("refuse", code, message, 0, -1); }
/* before-expire: let the record expire. Returns the status the handler returns. */
static inline int32_t as_udf_answer_expire(void) { return as_udf_answer("expire", 0, NULL, 0, -1); }
/* before-expire: keep the record, for `ttl` seconds more or, with -1, without naming a TTL. Returns the status
 * the handler returns. */
static inline int32_t as_udf_answer_keep(int64_t ttl) { return as_udf_answer("keep", 0, NULL, 0, ttl); }
/* before-evict: let the record be evicted. Returns the status the handler returns. */
static inline int32_t as_udf_answer_evict(void) { return as_udf_answer("evict", 0, NULL, 0, -1); }
/* before-evict: keep the record. Returns the status the handler returns. */
static inline int32_t as_udf_answer_protect(void) { return as_udf_answer("protect", 0, NULL, 0, -1); }
/* after-read: answer the read with the record as it is. Returns the status the handler returns. */
static inline int32_t as_udf_answer_unchanged(void) { return as_udf_answer("unchanged", 0, NULL, 0, -1); }
/* before-ship: ship the record as it is. Returns the status the handler returns. */
static inline int32_t as_udf_answer_ship(void) { return as_udf_answer("ship", 0, NULL, 0, -1); }
/* before-ship: do not ship the record. Returns the status the handler returns. */
static inline int32_t as_udf_answer_skip(void) { return as_udf_answer("skip", 0, NULL, 0, -1); }
/* The after- events, the once-per-node events and timers: answer `ok`. Returns the status the handler returns. */
static inline int32_t as_udf_answer_done(void) { return as_udf_answer("ok", 0, NULL, 0, -1); }

/* Declare an event handler of any kind: `AS_UDF_EVENT_HANDLER(name, "before-write",
 * "(before: record, after: record, write: write) -> decision") { ... }`. The kind and version go into the
 * signature `event <kind>/1 name(...)` that `event-set` checks. */
#define AS_UDF_EVENT_HANDLER(name, kind, params)                                                        \
	AS_UDF_SIGNATURE(name, "event " kind "/1 " #name params)                                             \
	AS_UDF_EXPORT(name) int32_t name(int32_t args, int32_t argc)

/* Declare a before-write handler: `AS_UDF_EVENT_BEFORE_WRITE(name) { ... }`, with `args` and `argc` in scope.
 * Also emits its signature. Answer with as_udf_answer_accept, as_udf_answer_reject or as_udf_answer_record. */
#define AS_UDF_EVENT_BEFORE_WRITE(name) AS_UDF_EVENT_HANDLER(name, "before-write", "(before: record, after: record, write: write) -> decision")
/* Declare a before-delete handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_allow or
 * as_udf_answer_refuse. */
#define AS_UDF_EVENT_BEFORE_DELETE(name) AS_UDF_EVENT_HANDLER(name, "before-delete", "(record: record, delete: delete) -> gate")
/* Declare a after-write handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_AFTER_WRITE(name) AS_UDF_EVENT_HANDLER(name, "after-write", "(before: record, after: record, write: write) -> ()")
/* Declare a after-delete handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_AFTER_DELETE(name) AS_UDF_EVENT_HANDLER(name, "after-delete", "(record: record, delete: delete) -> ()")
/* Declare a before-expire handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_expire or
 * as_udf_answer_keep. */
#define AS_UDF_EVENT_BEFORE_EXPIRE(name) AS_UDF_EVENT_HANDLER(name, "before-expire", "(record: record, expire: expire) -> expiry")
/* Declare a before-evict handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_evict or
 * as_udf_answer_protect. */
#define AS_UDF_EVENT_BEFORE_EVICT(name) AS_UDF_EVENT_HANDLER(name, "before-evict", "(record: record, evict: evict) -> eviction")
/* Declare a after-read handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_unchanged or
 * as_udf_answer_record. */
#define AS_UDF_EVENT_AFTER_READ(name) AS_UDF_EVENT_HANDLER(name, "after-read", "(record: record, read: read) -> read-result")
/* Declare a before-truncate handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_allow or
 * as_udf_answer_refuse. */
#define AS_UDF_EVENT_BEFORE_TRUNCATE(name) AS_UDF_EVENT_HANDLER(name, "before-truncate", "(truncate: truncate) -> gate")
/* Declare a after-truncate handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_AFTER_TRUNCATE(name) AS_UDF_EVENT_HANDLER(name, "after-truncate", "(truncate: truncate) -> ()")
/* Declare a before-ship handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_ship,
 * as_udf_answer_skip or as_udf_answer_record. */
#define AS_UDF_EVENT_BEFORE_SHIP(name) AS_UDF_EVENT_HANDLER(name, "before-ship", "(record: record, ship: ship) -> ship-decision")
/* Declare a partition-transfer handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_PARTITION_TRANSFER(name) AS_UDF_EVENT_HANDLER(name, "partition-transfer", "(transfer: transfer) -> ()")
/* Declare a cluster-change handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_CLUSTER_CHANGE(name) AS_UDF_EVENT_HANDLER(name, "cluster-change", "(cluster: cluster) -> ()")
/* Declare a node-lifecycle handler, as AS_UDF_EVENT_BEFORE_WRITE does. Answer with as_udf_answer_done. */
#define AS_UDF_EVENT_NODE_LIFECYCLE(name) AS_UDF_EVENT_HANDLER(name, "node-lifecycle", "(lifecycle: lifecycle) -> ()")
/* Declare a timer handler, as AS_UDF_EVENT_BEFORE_WRITE does. Read the timer with as_udf_event_timer_info and
 * answer with as_udf_answer_done. */
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

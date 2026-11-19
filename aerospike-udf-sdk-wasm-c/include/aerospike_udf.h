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
 * metadata (`as_udf_rec_generation`, `as_udf_rec_ttl`, ...), and the two calls that end a write:
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
 * `name`. The C function is named beside it and need not match: the import keeps the host's word (`set_str`,
 * `gen`, `numbins`) while the function takes the SDK's (`as_udf_rec_set_string`, `as_udf_rec_generation`,
 * `as_udf_rec_bin_count`). */
#define AS_UDF_IMPORT(name) __attribute__((import_module("aerospike"), import_name(#name)))

/* Read a bin of the record of the call into `out` as a 16-byte value: `out` points at an as_udf_value and `cap`
 * is its size in bytes, at least 16. `name` is the bin name, `name_len` bytes with no terminator needed: 1 to 15
 * bytes, none of them zero. Returns AS_UDF_OK; AS_UDF_NOT_FOUND, with `out` of kind AS_UDF_ABSENT, when the
 * record has no such bin, the name is not a bin name or the call has no record (an event handler reads with
 * as_udf_event_bin_value); AS_UDF_ERR when `cap` is under 16 or no handle is free. A bin never reads as nil: a
 * record has no nil bins. A scalar comes in the value itself. A string, blob, GeoJSON, HLL, list or map stays on
 * the host and the value says where it is; each such read takes one of the call's 256 handles, so read a bin once
 * and keep the value rather than reading it in a loop. The read sees what the call has staged: a bin set earlier
 * reads as set, a bin removed earlier as not found. A list, map or blob value read before a save that failed
 * answers AS_UDF_ERR afterwards: read the bin again. as_udf_rec_bin is this call for a NUL-terminated name. */
AS_UDF_IMPORT(bin_value) int32_t as_udf_rec_bin_value(const char* name, int32_t name_len, void* out, int32_t cap);

/* The record's bins - writing. Staged until as_udf_rec_save. */

/* Stage a 64-bit integer into a bin of the record of the call. `name` is the bin name, `name_len` bytes with no
 * terminator needed: 1 to 15 bytes, none of them zero. Staged: nothing is stored until as_udf_rec_save, and a
 * read of the bin later in the call sees the staged value. Returns AS_UDF_OK, AS_UDF_ERR for a bad name, or
 * AS_UDF_REFUSED when the record does not take the write: a stream UDF's record is read-only and an event handler
 * has no record to write. */
AS_UDF_IMPORT(set_int) int32_t as_udf_rec_set_int(const char* name, int32_t name_len, int64_t value);
/* Stage a double into a bin. Name, staging and statuses as as_udf_rec_set_int. */
AS_UDF_IMPORT(set_double) int32_t as_udf_rec_set_double(const char* name, int32_t name_len, double value);
/* Stage a boolean into a bin: any non-zero `value` is true. Name, staging and statuses as as_udf_rec_set_int. */
AS_UDF_IMPORT(set_bool) int32_t as_udf_rec_set_bool(const char* name, int32_t name_len, int32_t value);
/* Stage a string into a bin: `value_len` bytes at `value`, no terminator needed, copied by the host, so the
 * buffer may be reused at once. An empty string is a value, not a removal. Name, staging and statuses as
 * as_udf_rec_set_int. AS_UDF_REFUSED also when the copy would exceed the memory the call may build on the host.
 */
AS_UDF_IMPORT(set_str) int32_t as_udf_rec_set_string(const char* name, int32_t name_len, const char* value, int32_t value_len);
/* Stage a blob into a bin: `value_len` bytes at `value`, copied by the host. Name, staging and statuses as
 * as_udf_rec_set_int. Also AS_UDF_TYPE_MISMATCH when the bin holds bytes of another subtype (an HLL, say), which
 * a plain blob would silently relabel: write such a value back by position with as_udf_rec_set_at, which keeps
 * its subtype. */
AS_UDF_IMPORT(set_blob) int32_t as_udf_rec_set_blob(const char* name, int32_t name_len, const void* value, int32_t value_len);
/* Stage GeoJSON text into a bin as a geospatial value rather than a string: `value_len` bytes at `value`, copied
 * by the host. The text is not validated by this call: staging takes any text, and it is as_udf_rec_save that
 * fails for text the server would not accept as GeoJSON from a client. Name, staging and statuses as
 * as_udf_rec_set_int. */
AS_UDF_IMPORT(set_geojson) int32_t as_udf_rec_set_geojson(const char* name, int32_t name_len, const char* value, int32_t value_len);
/* Stage a value built on the host into a bin: `handle` is one from as_udf_list_new, as_udf_map_new or an
 * `as_udf_val_` call. The bin and the handle then share the one value, and the handle stays usable. `name` is the
 * bin name, `name_len` bytes with no terminator needed: 1 to 15 bytes, none of them zero. Staged: nothing is
 * stored until as_udf_rec_save, and a read of the bin later in the call sees the staged value. Returns AS_UDF_OK;
 * AS_UDF_ERR for a handle that holds no built value (0, a cursor, a source) or a bad name; AS_UDF_TYPE_MISMATCH
 * when the handle holds bytes and the bin holds bytes of another subtype; AS_UDF_REFUSED when the record does not
 * take the write (a stream UDF, an event handler) or the value nests collections more than 64 levels deep. */
AS_UDF_IMPORT(set_handle) int32_t as_udf_rec_set_handle(const char* name, int32_t name_len, int32_t handle);
/* Stage a value the call read into a bin, copied on the host without the guest decoding it: a way to move a list,
 * a map, an HLL or any other value from one bin to another, or out of a collection into a bin, with its kind
 * kept. `src` and `at` are as_udf_value_src and as_udf_value_at of a value the call read: a bin, an argument, an
 * element of a collection. `name` is the bin name, `name_len` bytes with no terminator needed: 1 to 15 bytes,
 * none of them zero. Staged: nothing is stored until as_udf_rec_save, and a read of the bin later in the call
 * sees the staged value. Returns AS_UDF_OK, AS_UDF_ERR for a bad source, offset or name, or AS_UDF_REFUSED when
 * the record does not take the write (a stream UDF, an event handler) or a plain blob would replace bytes of
 * another subtype. */
AS_UDF_IMPORT(set_at) int32_t as_udf_rec_set_at(const char* name, int32_t name_len, int32_t src, int32_t at);
/* Remove a bin from the record: staged like a write, applied by as_udf_rec_save, and read as not found for the
 * rest of the call. This is the one way to drop a bin, and there is no call that sets a bin to nil: a bin cannot
 * hold nil, and removing it is the same thing. Removing a bin that is not there is not an error: it answers
 * AS_UDF_OK and changes nothing. Deleting the whole record is as_udf_rec_delete. `name` is the bin name,
 * `name_len` bytes with no terminator needed: 1 to 15 bytes, none of them zero. Returns AS_UDF_OK, AS_UDF_ERR
 * for a bad name, or AS_UDF_REFUSED when the record does not take the write: a stream UDF's record is read-only
 * and an event handler has no record to write. */
AS_UDF_IMPORT(bin_remove) int32_t as_udf_rec_remove(const char* name, int32_t name_len);
/* Write an array of 64-bit integers to a bin as a list, in one call and with no value built per element.
 * `vals_len` is the size of the array in bytes, 8 times the number of elements. `name` is the bin name,
 * `name_len` bytes with no terminator needed: 1 to 15 bytes, none of them zero. Applied at once, not staged:
 * as_udf_rec_save is not needed, and the write is not undone by leaving it out. It is undone when the call then
 * fails: a call that fails writes nothing. Returns AS_UDF_OK, AS_UDF_ERR for a bad name, or AS_UDF_REFUSED where
 * there is no writable record (a stream UDF, an event handler), on a key with no record, which this call does
 * not create, or when the server rejects the write; after a rejection, as after a failed save, a list, map or
 * blob value read from a bin earlier in the call has to be read again. */
AS_UDF_IMPORT(set_list_ints) int32_t as_udf_rec_set_ints(const char* name, int32_t name_len, const int64_t* vals, int32_t vals_len);
/* Write an array of doubles to a bin as a list, with `vals_len` the size of the array in bytes, 8 times the
 * number of elements. Applied at once and with the statuses of as_udf_rec_set_ints: refused on a key with no
 * record, and undone when the call then fails. */
AS_UDF_IMPORT(set_list_doubles) int32_t as_udf_rec_set_doubles(const char* name, int32_t name_len, const double* vals, int32_t vals_len);

/* Collections, by position in a source. */

/* Read the next values of a cursor into `out` and step past them. `cursor` is a handle from
 * as_udf_cursor_enter_at, `out` an array of as_udf_value and `cap` its size in bytes: as many values are written
 * as fit, so an array of 32 is filled 32 at a time. Returns how many were written, 0 at the end, or AS_UDF_ERR
 * for a handle that is not a cursor or a value that cannot be read. A list yields its elements in order. A map
 * yields key, value, key, value, so give it room for an even number. A scalar element is in the value; a string,
 * blob, nested list or nested map comes by position and takes no handle until it is entered. The values stay
 * readable after the cursor is closed.
 * ```c
 * as_udf_value list, w[32];
 * int64_t total = 0;
 * if (as_udf_rec_list("scores", &list) != AS_UDF_OK) return as_udf_fail_str("no list bin scores");
 * int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&list), as_udf_value_at(&list));
 * for (int32_t n; cur > 0 && (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
 * 	for (int32_t i = 0; i < n; i++) if (w[i].kind == AS_UDF_INT) total += w[i].payload;
 * }
 * as_udf_cursor_close(cur);
 * ```
 */
AS_UDF_IMPORT(cursor_values) int32_t as_udf_cursor_values(int32_t cursor, void* out, int32_t cap);
/* Copy the bytes of the string, blob, GeoJSON or HLL value at offset `at` of source `src` into `out`, which holds
 * `cap` bytes. No terminator is written. Returns the length, which may be 0; a needed-length status when `cap` is
 * too small, in which case nothing is written and `-(rc) - 16` is the size to retry with (the value's `len` says
 * it beforehand); AS_UDF_TYPE_MISMATCH when the value is of another kind; or AS_UDF_ERR for a bad source or
 * offset. as_udf_bytes is this call on a value. */
AS_UDF_IMPORT(cursor_bytes_at) int32_t as_udf_cursor_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
/* Copy the value at offset `at` of source `src` as the server stores it, msgpack header included, into `out`,
 * which holds `cap` bytes. For a string, blob or HLL bin read as a whole these are the bytes themselves. Returns
 * the length, a needed-length status when `cap` is too small (nothing is written), or AS_UDF_ERR for a bad source
 * or offset. as_udf_stored_bytes is this call on a value. */
AS_UDF_IMPORT(coll_bytes_at) int32_t as_udf_coll_bytes_at(int32_t src, int32_t at, void* out, int32_t cap);
/* Compare the stored encodings of two values byte by byte, as memcmp would, without copying either into the
 * guest; a shorter one that is a prefix of the other comes first. Each value is given by its source and offset.
 * Returns -1, 0 or 1, or AS_UDF_TYPE_MISMATCH (-3) when either position is not a stored value: the status shares
 * the return with the order, so this import is not for a UDF to call. as_udf_cmp_stored is this call on two
 * values, with their kinds checked and the order kept apart from the status. The order is one of encodings, not
 * of meaning: it is total and the same on every cluster, which is what a merge needs of a tie-break. */
AS_UDF_IMPORT(coll_cmp_at) int32_t as_udf_coll_cmp_at(int32_t src1, int32_t at1, int32_t src2, int32_t at2);
/* Open a cursor over the list or map at offset `at` of source `src`, positioned at its first element, to read
 * with as_udf_cursor_values. `src` and `at` are as_udf_value_src and as_udf_value_at of a value the call read: a
 * bin, an argument, an element of a collection. Returns the cursor's handle, or 0 when the value is not a list or
 * map or no handle is free: a call holds at most 256 handles. Close it with as_udf_cursor_close when the walk is
 * done; whatever is left open is released when the call ends. */
AS_UDF_IMPORT(cursor_enter_at) int32_t as_udf_cursor_enter_at(int32_t src, int32_t at);
/* Look up a string key in the map at offset `at` of source `src` and write the entry's value to `out` as a
 * 16-byte value (`cap` at least 16). `key` is `key_len` bytes, no terminator needed. Returns AS_UDF_OK;
 * AS_UDF_NOT_FOUND, with `out` of kind AS_UDF_ABSENT, for no such key or when the value is not a map; or
 * AS_UDF_ERR for a bad source, offset or `cap`. An entry that holds nil is found, with kind AS_UDF_NIL. The
 * search is linear in the map's entries. as_udf_map_get is this call on a value. */
AS_UDF_IMPORT(coll_get) int32_t as_udf_coll_get(int32_t src, int32_t at, const char* key, int32_t key_len, void* out, int32_t cap);
/* Look up an integer key in the map at offset `at` of source `src`. Output and statuses as as_udf_coll_get. */
AS_UDF_IMPORT(coll_get_i) int32_t as_udf_coll_get_int(int32_t src, int32_t at, int64_t key, void* out, int32_t cap);
/* The element at `index`, counted from 0, of the list at offset `at` of source `src`, written to `out` as a
 * 16-byte value (`cap` at least 16) without opening a cursor. The host steps over the elements before it, so the
 * cost grows with `index`; walk with a cursor to read them all. Returns AS_UDF_OK; AS_UDF_NOT_FOUND, with `out`
 * of kind AS_UDF_ABSENT, past the end or when the value is not a list; or AS_UDF_ERR for a bad source, offset or
 * `cap`. as_udf_list_at is this call on a value. */
AS_UDF_IMPORT(list_get) int32_t as_udf_list_get(int32_t src, int32_t at, int32_t index, void* out, int32_t cap);
/* Give a cursor's handle back. Only the most recently created handle is released, so close cursors in the reverse
 * of the order they were opened in; any other handle stays taken until the call ends, when the host releases them
 * all. Closing 0 or a handle that is not valid does nothing. */
AS_UDF_IMPORT(cursor_close) void as_udf_cursor_close(int32_t cursor);
/* Make a list or map built on the host readable by position: returns a source handle over a packed copy of it, to
 * read at offset 0 with as_udf_cursor_enter_at, as_udf_coll_get or as_udf_list_get. The copy is of the collection
 * as it is now: what is added afterwards is not in it. Returns 0 when `handle` is not a list or map or no handle
 * is free. The copy counts against the memory the call may build on the host. */
AS_UDF_IMPORT(pack_handle) int32_t as_udf_pack_handle(int32_t handle);
/* Read the call's arguments into `out` as 16-byte values, as many as `cap` bytes hold. `handle` is the `args`
 * parameter the export was called with. Returns the count written, which is less than the export's `argc` when
 * `out` is too small, or AS_UDF_ERR when `handle` is not the argument list's. Nothing says that arguments were
 * left unread but `argc`: compare the two (see as_udf_args). A nil argument is of kind AS_UDF_NIL. Read them
 * once: each string, blob, list or map argument takes a handle every time. as_udf_args is this call with the
 * count in values. */
AS_UDF_IMPORT(args_values) int32_t as_udf_args_values(int32_t handle, void* out, int32_t cap);
/* The number of elements of a built list, of entries of a built map, or of the collection a cursor was opened
 * over (a map counts its entries, not its keys and values). AS_UDF_ERR for any other handle. A stored list or map
 * needs no call: its value's `len` is the count. */
AS_UDF_IMPORT(coll_size) int32_t as_udf_coll_size(int32_t handle);

/* Collections - building, on the host, as handles. */

/* Start an empty list on the host. `capacity` is a hint of how many elements it will hold: the list grows past
 * it, and 0 asks for a default. Returns its handle, or 0 when none is free: a call holds at most 256 handles, and
 * 0 is never a valid one. Fill it with the `as_udf_list_push` calls, then store it with as_udf_rec_set_handle,
 * nest it with as_udf_list_push or as_udf_map_put_string_handle, or return it with as_udf_result_handle. What it
 * holds lives on the host and is charged to the call's memory limit.
 * ```c
 * int32_t tags = as_udf_list_new(2);
 * if (tags <= 0) return as_udf_fail_str("could not build the list");
 * as_udf_list_push_string(tags, "new", 3);
 * as_udf_list_push_int(tags, 7);
 * if (as_udf_rec_set_handle("tags", 4, tags) != AS_UDF_OK) return as_udf_fail_str("could not set tags");
 * if (as_udf_rec_save() != AS_UDF_OK) return as_udf_fail_str("could not save");
 * ```
 */
AS_UDF_IMPORT(list_new) int32_t as_udf_list_new(int32_t capacity);
/* Start an empty map on the host. `capacity` is a hint of how many entries it will hold: the map grows past it,
 * and 0 asks for a default. The map is ordered by key, whatever order the entries are put in, and that is the
 * order the client receives it in. Returns its handle, or 0 when none is free: a call holds at most 256 handles,
 * and 0 is never a valid one. Fill it with the `as_udf_map_put` calls; a put under a key that is already there
 * replaces its value.
 * ```c
 * int32_t out = as_udf_map_new(2);
 * if (out <= 0) return as_udf_fail_str("could not build the map");
 * as_udf_map_put_string_int(out, "visits", 6, 3);
 * as_udf_map_put_string_string(out, "site", 4, "home", 4);
 * return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_fail_str("could not return the map");
 * ```
 */
AS_UDF_IMPORT(map_new) int32_t as_udf_map_new(int32_t capacity);
/* Whether a map held by handle has a string key (`key_len` bytes, no terminator needed): 1 or 0, or AS_UDF_ERR
 * when `handle` is not a map's. For a map from as_udf_map_new; a stored map is asked with as_udf_coll_get. The
 * host answers for the map it holds, so nothing is packed and the check is fit for a loop. as_udf_map_contains is
 * this call for a NUL-terminated key. */
AS_UDF_IMPORT(map_has) int32_t as_udf_map_has(int32_t handle, const char* key, int32_t key_len);
/* Whether a map held by handle has an integer key: 1 or 0, or AS_UDF_ERR when `handle` is not a map's. Otherwise
 * as as_udf_map_has. */
AS_UDF_IMPORT(map_has_i) int32_t as_udf_map_has_int(int32_t handle, int64_t key);
/* An integer as a value of its own on the host, for the calls that take a value by handle: as_udf_list_push,
 * as_udf_map_put (as a key or as a value), as_udf_rec_set_handle and as_udf_result_handle. Returns its handle, or
 * 0 when none is free: a call holds at most 256 handles, and 0 is never a valid one. Every such value takes a
 * handle for the rest of the call, so none of the `as_udf_val_` calls belongs in a loop: a loop that makes one
 * value an element stops near 250 elements. An element of a collection needs no handle: as_udf_list_push_value
 * and its typed forms (as_udf_list_push_double and the rest), as_udf_map_put_string_value and
 * as_udf_map_put_int_value hand the value over as it is, and take any number of elements. */
AS_UDF_IMPORT(val_int) int32_t as_udf_val_int(int64_t value);
/* A double as a value of its own, by handle. Returns its handle, or 0 when none is free. Not for a loop: see
 * as_udf_val_int, and as_udf_list_push_double for a double that enters a list without a handle. */
AS_UDF_IMPORT(val_double) int32_t as_udf_val_double(double value);
/* A boolean as a value of its own, by handle: any non-zero `value` is true. Returns its handle, or 0 when none is
 * free. Not for a loop: see as_udf_val_int. */
AS_UDF_IMPORT(val_bool) int32_t as_udf_val_bool(int32_t value);
/* Nil as a value of its own, by handle: a nil element of a list or a nil entry of a map, which in an xdr-merge
 * record answer means "delete this bin". Returns its handle, or 0 when none is free. Not for a loop: see
 * as_udf_val_int. */
AS_UDF_IMPORT(val_nil) int32_t as_udf_val_nil(void);
/* A string as a value of its own, by handle: `value_len` bytes, no terminator needed, copied by the host. Returns
 * its handle, or 0 when none is free or the copy would exceed the memory the call may build on the host. Not for
 * a loop: see as_udf_val_int. */
AS_UDF_IMPORT(val_str) int32_t as_udf_val_string(const char* value, int32_t value_len);
/* A blob as a value of its own, by handle: `value_len` bytes, copied by the host. Returns its handle, or 0 when
 * none is free or the copy would exceed the memory the call may build on the host. Not for a loop: see
 * as_udf_val_int. */
AS_UDF_IMPORT(val_blob) int32_t as_udf_val_blob(const void* value, int32_t value_len);
/* GeoJSON text as a value of its own, by handle, so that it enters a built collection or a bin as GeoJSON rather
 * than as a string. `value_len` bytes, copied by the host and not validated here. Returns its handle, or 0 when
 * none is free. Not for a loop: see as_udf_val_int. */
AS_UDF_IMPORT(val_geojson) int32_t as_udf_val_geojson(const char* value, int32_t value_len);
/* Append the value behind handle `value` to a built list: a value from an `as_udf_val_` call, or another built
 * list or map, which nests it. The list and the handle share the value and the handle stays usable. Returns
 * AS_UDF_OK; AS_UDF_ERR when `handle` is not a list the call built or `value` holds no value (0 included, so the
 * result of a failed `as_udf_val_` call can be passed straight in); AS_UDF_REFUSED when the collection does not
 * take the value: it would come to contain itself, nest more than 64 levels deep, or exceed the memory the call
 * may build on the host. */
AS_UDF_IMPORT(list_push) int32_t as_udf_list_push(int32_t handle, int32_t value);
/* Append an integer to a built list without a value handle. Returns AS_UDF_OK, AS_UDF_ERR when `handle` is not a
 * list the call built, or AS_UDF_REFUSED when the call's host memory is used up. */
AS_UDF_IMPORT(list_push_int) int32_t as_udf_list_push_int(int32_t handle, int64_t value);
/* Append a string to a built list without a value handle: `value_len` bytes, no terminator needed, copied by the
 * host. Returns AS_UDF_OK, AS_UDF_ERR when `handle` is not a list the call built, or AS_UDF_REFUSED when the
 * call's host memory is used up. */
AS_UDF_IMPORT(list_push_str) int32_t as_udf_list_push_string(int32_t handle, const char* value, int32_t value_len);
/* Put an entry into a built map, key and value both by handle (see as_udf_val_int). A key that is already there
 * has its value replaced. The map shares both values with their handles, which stay usable. Each handle made for
 * the call is taken for good, so this is not the form for a loop: as_udf_map_put_int_value puts under an integer
 * key, and as_udf_map_put_string_value under a string key, with no handle at all. Returns AS_UDF_OK; AS_UDF_ERR
 * when `handle` is not a map the call built or either handle holds no value; AS_UDF_REFUSED when the collection
 * does not take the value: it would come to contain itself, nest more than 64 levels deep, or exceed the memory
 * the call may build on the host. */
AS_UDF_IMPORT(map_put) int32_t as_udf_map_put(int32_t handle, int32_t key, int32_t value);
/* Put an integer under a string key of a built map: `key_len` bytes of key, no terminator needed, copied by the
 * host. A key that is already there has its value replaced. Returns AS_UDF_OK, AS_UDF_ERR when `handle` is not a
 * map the call built, or AS_UDF_REFUSED when the call's host memory is used up. */
AS_UDF_IMPORT(map_put_str_int) int32_t as_udf_map_put_string_int(int32_t handle, const char* key, int32_t key_len, int64_t value);
/* Put a string under a string key of a built map; both are given with their lengths and copied by the host.
 * Replacement and statuses as as_udf_map_put_string_int. */
AS_UDF_IMPORT(map_put_str_str) int32_t as_udf_map_put_string_string(int32_t handle, const char* key, int32_t key_len, const char* value, int32_t value_len);
/* Put the value behind handle `value` under a string key of a built map: a value from an `as_udf_val_` call, or a
 * built list or map, which nests it. The map and the handle share the value and the handle stays usable. Returns
 * AS_UDF_OK; AS_UDF_ERR when `handle` is not a map the call built or `value` holds no value; AS_UDF_REFUSED when
 * the collection does not take the value: it would come to contain itself, nest more than 64 levels deep, or
 * exceed the memory the call may build on the host. */
AS_UDF_IMPORT(map_put_str_handle) int32_t as_udf_map_put_string_handle(int32_t handle, const char* key, int32_t key_len, int32_t value);
/* Append a value the call read to a built list, copied on the host without the guest decoding it and with its
 * kind kept. `src` and `at` are as_udf_value_src and as_udf_value_at of a value the call read: a bin, an
 * argument, an element of a collection. Returns AS_UDF_OK, AS_UDF_ERR when `handle` is not a list the call built
 * or the position is bad, or AS_UDF_REFUSED when the list does not take the value (nesting past 64 levels, or the
 * call's host memory used up). */
AS_UDF_IMPORT(list_push_at) int32_t as_udf_list_push_at(int32_t handle, int32_t src, int32_t at);
/* Put a value the call read under a string key of a built map, copied on the host with its kind kept. `src` and
 * `at` are the value's source and offset; `key` is `key_len` bytes. Returns AS_UDF_OK, or AS_UDF_ERR when
 * `handle` is not a map the call built, the position is bad or the map does not take the entry. */
AS_UDF_IMPORT(map_put_str_at) int32_t as_udf_map_put_string_at(int32_t handle, const char* key, int32_t key_len, int32_t src, int32_t at);
/* Put a value the call read under an integer key of a built map, copied on the host with its kind kept. Returns
 * AS_UDF_OK, or AS_UDF_ERR when `handle` is not a map the call built, the position is bad or the map does not
 * take the entry. */
AS_UDF_IMPORT(map_put_i_at) int32_t as_udf_map_put_int_at(int32_t handle, int64_t key, int32_t src, int32_t at);
/* Append a batch of values to a built list in one call: `vals` is an array of as_udf_value and `vals_len` its
 * size in bytes, 16 a value. Make the values with as_udf_v_int, as_udf_v_double, as_udf_v_bool, as_udf_v_nil,
 * as_udf_v_string, as_udf_v_blob, as_udf_v_geojson and as_udf_v_handle, or pass on values the call read. Bytes of
 * the guest's travel by address and are copied during the call. No value in a batch takes a handle. Returns how
 * many went in: fewer than were given when one could not be taken, the ones before it having been appended; or
 * AS_UDF_ERR when `handle` is not a list the call built. as_udf_list_push_value is this call for one value, with
 * a status for an answer.
 * ```c
 * as_udf_value row[3] = { as_udf_v_int(42), as_udf_v_double(0.5), as_udf_v_string("ok", 2) };
 * int32_t out = as_udf_list_new(3);
 * if (as_udf_list_push_values(out, row, (int32_t)sizeof(row)) != 3) return as_udf_fail_str("could not build the row");
 * ```
 */
AS_UDF_IMPORT(list_push_values) int32_t as_udf_list_push_values(int32_t handle, const void* vals, int32_t vals_len);
/* Put a batch of entries into a built map in one call: `pairs` is an array of as_udf_value, a key then its value,
 * and `pairs_len` its size in bytes, 32 an entry. Keys and values are made as for as_udf_list_push_values; a key
 * is a string or an integer. Returns how many entries went in: fewer than were given when one could not be taken,
 * the ones before it having been put; or AS_UDF_ERR when `handle` is not a map the call built. */
AS_UDF_IMPORT(map_put_entries) int32_t as_udf_map_put_entries(int32_t handle, const void* pairs, int32_t pairs_len);

/* Results and failure. */

/* Set the call's result to nil. The result is what the client receives when the UDF returns 0. Of several result
 * calls the last one wins, and a UDF that sets none returns nil. */
AS_UDF_IMPORT(result_nil) void as_udf_result_nil(void);
/* Set the call's result to a 64-bit integer. The result is what the client receives when the UDF returns 0. Of
 * several result calls the last one wins, and a UDF that sets none returns nil.
 * ```c
 * int64_t n;
 * if (as_udf_rec_int_or("n", 0, &n) != AS_UDF_OK) return as_udf_fail_str("n is not an integer");
 * as_udf_result_int(n);
 * return 0;
 * ```
 */
AS_UDF_IMPORT(result_int) void as_udf_result_int(int64_t value);
/* Set the call's result to a double. See as_udf_result_int. */
AS_UDF_IMPORT(result_double) void as_udf_result_double(double value);
/* Set the call's result to a boolean: any non-zero `value` is true. See as_udf_result_int. */
AS_UDF_IMPORT(result_bool) void as_udf_result_bool(int32_t value);
/* Set the call's result to a string: `value_len` bytes, no terminator needed, copied by the host, so a buffer on
 * the stack will do. A copy larger than the call may build on the host fails the call. See as_udf_result_int. */
AS_UDF_IMPORT(result_str) void as_udf_result_string(const char* value, int32_t value_len);
/* Set the call's result to a blob: `value_len` bytes, copied by the host. See as_udf_result_string. */
AS_UDF_IMPORT(result_blob) void as_udf_result_blob(const void* value, int32_t value_len);
/* Set the call's result to GeoJSON text, which the client receives as GeoJSON rather than as a string:
 * `value_len` bytes, copied by the host. */
AS_UDF_IMPORT(result_geojson) void as_udf_result_geojson(const char* value, int32_t value_len);
/* Set the call's result to a value built on the host: a list or map, or a value from an `as_udf_val_` call. The
 * handle stays usable. Returns AS_UDF_OK, or AS_UDF_ERR when the handle holds no built value. A result that nests
 * collections more than 64 levels deep is not an error here: it fails the call, with a message saying so. */
AS_UDF_IMPORT(result_handle) int32_t as_udf_result_handle(int32_t handle);
/* Set the call's result to a value the call read, by position and with its kind kept: a bin handed back, or one
 * element of it. `src` and `at` are as_udf_value_src and as_udf_value_at of a value the call read: a bin, an
 * argument, an element of a collection. Returns AS_UDF_OK, or AS_UDF_ERR for a bad source or offset. */
AS_UDF_IMPORT(result_at) int32_t as_udf_result_at(int32_t src, int32_t at);
/* Mark the call as failed with a message for the client, which sees a UDF error carrying it: `message_len` bytes,
 * no terminator needed, of which the host keeps the first 511. The call is failed whatever the UDF then returns
 * and whatever result it set, but the UDF still has to return: this call comes back. A call that fails writes
 * nothing, even what it had already saved: an as_udf_rec_save that succeeded, and an as_udf_rec_set_ints or
 * as_udf_rec_set_doubles, are undone. as_udf_fail_str does both for a NUL-terminated message. A UDF that returns
 * non-zero without calling this fails with a message that gives only the module, the function and the number. */
AS_UDF_IMPORT(fail) void as_udf_fail(const char* message, int32_t message_len);

/* The record's metadata. */

/* The record's generation: the count of writes it has taken, which the server raises on each one; 0 on a key
 * with no record. Of the record of the call: not for an event handler, whose records carry their own metadata. */
AS_UDF_IMPORT(gen) int64_t as_udf_rec_generation(void);
/* The record's remaining lifetime in seconds; 0 for a record that does not expire. A key with no record has no
 * TTL. Of the record of the call: not for an event handler, whose records carry their own metadata. */
AS_UDF_IMPORT(ttl) int64_t as_udf_rec_ttl(void);
/* When the record was last written, as the server stores it: milliseconds since the Citrusleaf epoch,
 * 2010-01-01T00:00:00Z, not the Unix one, so it is not on the scale of as_udf_now (it is Unix milliseconds minus
 * 1262304000000). 0 on a key with no record. Of the record of the call: not for an event handler, whose records
 * carry their own metadata. */
AS_UDF_IMPORT(last_update) int64_t as_udf_rec_last_update_time(void);
/* The record's stored size in bytes; 0 on a key with no record. Of the record of the call: not for an event
 * handler, whose records carry their own metadata. */
AS_UDF_IMPORT(record_size) int64_t as_udf_rec_size(void);
/* How many bins the record has, as it is stored: a bin the call has staged is not counted until as_udf_rec_save
 * has written it, and the count is 0 on a key with no record. An event record's bins are counted by the `len` of
 * its `bin_names`. */
AS_UDF_IMPORT(numbins) int64_t as_udf_rec_bin_count(void);
/* The server's wall clock, in milliseconds since the Unix epoch. Returns -1 in an event handler, where the clock
 * is refused: a decision that reads it cannot converge across clusters. Not about the record, and not on the
 * scale of as_udf_rec_last_update_time, which counts from 2010: subtract 1262304000000 from this to compare. */
AS_UDF_IMPORT(current_time) int64_t as_udf_now(void);
/* Set the time to live the record's write will carry, in seconds, with a client write's meaning: 0 for the
 * namespace default, and -1 (all 32 bits set) for a record that never expires. It takes effect with the write,
 * so follow it with as_udf_rec_save. On a key with no record it creates nothing by itself: a save with a TTL set
 * and no bin staged fails the call with "key not found". Returns AS_UDF_OK, or AS_UDF_REFUSED where there is no
 * record that can take one: a stream UDF's record and an event handler. */
AS_UDF_IMPORT(set_ttl) int32_t as_udf_rec_set_ttl(int32_t seconds);
/* Copy the name of the record's set into `out`, which holds `cap` bytes; no terminator is written. Returns the
 * length, a needed-length status when `cap` is too small (nothing is written), or AS_UDF_NOT_FOUND when the
 * record is in no set. Not a setter: `set` here is the set a record belongs to. */
AS_UDF_IMPORT(set_name) int32_t as_udf_rec_set_name(void* out, int32_t cap);
/* The kind of the record's user key (AS_UDF_INT, AS_UDF_STR or AS_UDF_BLOB), or AS_UDF_ABSENT when the record was
 * stored without one: the server keeps the key only when the client wrote the record with `send_key`, and it
 * cannot be recovered from the digest. */
AS_UDF_IMPORT(key_type) int32_t as_udf_rec_key_type(void);
/* The record's user key when it is an integer; 0 when it is not or the record stores none, so check
 * as_udf_rec_key_type first. */
AS_UDF_IMPORT(key_int) int64_t as_udf_rec_key_int(void);
/* Copy the record's string or blob user key into `out`, which holds `cap` bytes; no terminator is written.
 * Returns the length, a needed-length status when `cap` is too small (nothing is written), AS_UDF_NOT_FOUND when
 * the record was stored without a key, or AS_UDF_ERR for an integer key: read that with as_udf_rec_key_int.
 * as_udf_rec_key gives the key of any kind as one value. */
AS_UDF_IMPORT(key_bytes) int32_t as_udf_rec_key_bytes(void* out, int32_t cap);
/* Copy the record's digest into `out`: 20 bytes, the hash of set and key that identifies the record, there
 * whether or not the key was stored. Returns the length, or a needed-length status when `cap` is under 20
 * (nothing is written). */
AS_UDF_IMPORT(digest) int32_t as_udf_rec_digest(void* out, int32_t cap);
/* The host call as_udf_rec_save makes for a key with no record yet: creates the record from what was staged.
 * Returns AS_UDF_OK, or AS_UDF_REFUSED when the server does not create it, which includes a record that already
 * exists. Not for a UDF to call: as_udf_rec_save chooses between this and `rec_update` and is the record's one
 * write. */
AS_UDF_IMPORT(rec_create) int32_t as_udf_host_rec_create(void);
/* The host call as_udf_rec_save makes for a record that exists: applies the staged bins and removals to it.
 * Returns AS_UDF_OK, or AS_UDF_REFUSED when the server does not take the write, which includes a key with no
 * record. Not for a UDF to call: as_udf_rec_save chooses between this and `rec_create`. */
AS_UDF_IMPORT(rec_update) int32_t as_udf_host_rec_update(void);
/* Delete the record of the call. Nothing has to be saved first, and whatever was staged goes with the record. The
 * delete is carried out when the call returns, so as_udf_rec_exists keeps answering what it did until then.
 * Returns AS_UDF_OK, or AS_UDF_REFUSED when the server does not delete it: in a stream UDF or an event handler,
 * and on a key with no record, where there is nothing to delete and nothing is created. Removing one bin is
 * as_udf_rec_remove.
 * ```c
 * if (as_udf_rec_delete() != AS_UDF_OK) return as_udf_fail_str("could not delete the record");
 * as_udf_result_nil();
 * return 0;
 * ```
 */
AS_UDF_IMPORT(rec_remove) int32_t as_udf_rec_delete(void);
/* Whether the record of the call exists: 1 or 0, not a status. A record UDF may be called on a key that has no
 * record, which is how it creates one; check this first in a function that must not create, or must not
 * overwrite. On such a key the generation, the last-update time, the size and the bin count are 0, there is no
 * TTL, every bin reads as not found, and reading creates nothing: only an as_udf_rec_save with a bin staged
 * creates the record. The record is locked for the whole call, so nothing comes between the check and the save.
 * 0 in an event handler. */
AS_UDF_IMPORT(rec_exists) int32_t as_udf_rec_exists(void);
/* Write a line to the server log at an AS_UDF_LOG_* level; any other level logs as debug. `message_len` bytes, no
 * terminator needed, of which the host keeps the first 511, with control characters escaped. A call may write 16
 * lines and a module 100 a second; what is over is dropped and counted. A line from an event handler is dropped.
 * as_udf_log_str is this call for a NUL-terminated message. */
AS_UDF_IMPORT(log) void as_udf_log(int32_t level, const char* message, int32_t message_len);

/*==========================================================
 * The contract's constants.
 */

/* Status codes. Anything at or below AS_UDF_NEEDED_BASE is "the buffer was too small": -(rc) - 16 bytes needed. */

/* The call did what was asked. The status every import that returns one uses for success. */
#define AS_UDF_OK 0
/* The call could not be carried out as it was made: a bad handle, position, name or buffer. A mistake in the
 * module rather than a state of the record. */
#define AS_UDF_ERR (-1)
/* What was asked for is not there: no such bin, map key or list element, no stored user key, no set. What every
 * typed reader answers for a bin, an element or an entry that is absent or nil. */
#define AS_UDF_NOT_FOUND (-2)
/* The value is not of the kind the call works on: every typed reader answers it for a value of another kind (an
 * integer is not a double, a string is not a blob), as_udf_bytes and as_udf_cmp_stored for a value that is not
 * held by position, and a blob write for a bin that holds bytes of another subtype. */
#define AS_UDF_TYPE_MISMATCH (-3)
/* The server refused the operation: a write to a record that takes none (a stream UDF's, or none at all in an
 * event handler), a save or delete the server rejected, or a value over the limits on what a call may build. */
#define AS_UDF_REFUSED (-4)
/* Where needed-length answers start. A status at or below this, from a call that copies bytes into a buffer,
 * means only that the buffer was too small: nothing was written and `-(rc) - 16` bytes are needed.
 * ```c
 * char small[8];
 * int32_t n = as_udf_rec_string("name", small, (int32_t)sizeof(small));
 * int32_t needed = n <= AS_UDF_NEEDED_BASE ? -n - 16 : n;
 * ```
 */
#define AS_UDF_NEEDED_BASE (-16)

/* Value kinds, as the host hands them out. */

/* The kind of no value at all: a bin, map entry, list element or user key that is not there. What the lookups
 * leave in `out` when they answer AS_UDF_NOT_FOUND. */
#define AS_UDF_ABSENT (-1)
/* The kind of a nil: a nil argument, list element or map entry. A bin is never nil; in a batch it puts a nil. */
#define AS_UDF_NIL 0
/* The kind of a boolean: the payload is 0 or 1. */
#define AS_UDF_BOOL 1
/* The kind of a 64-bit signed integer: the payload is the integer. */
#define AS_UDF_INT 2
/* The kind of a double: its bits are in the payload, read with as_udf_value_double, not by casting the payload.
 */
#define AS_UDF_DOUBLE 3
/* The kind of a string the host holds: `len` bytes, copied out with as_udf_as_string or as_udf_bytes. The value
 * carries no characters, only where they are. */
#define AS_UDF_STR 4
/* The kind of a blob the host holds: `len` bytes, copied out with as_udf_as_blob or as_udf_bytes. */
#define AS_UDF_BLOB 5
/* The kind of a list the host holds: `len` elements, walked with a cursor (as_udf_cursor_enter_at) or read one at
 * a time with as_udf_list_at. */
#define AS_UDF_LIST 6
/* The kind of a map the host holds: `len` entries, walked with a cursor or looked up with as_udf_map_get and
 * as_udf_map_get_int. */
#define AS_UDF_MAP 7
/* The kind of a value a UDF cannot interpret. It carries no length and no payload, and can only be recognised. */
#define AS_UDF_OTHER 8
/* The kind of GeoJSON the host holds: `len` bytes of text, copied out with as_udf_bytes. Not a string: the
 * string readers answer AS_UDF_TYPE_MISMATCH for it. */
#define AS_UDF_GEOJSON 9
/* The kind of a HyperLogLog the host holds: `len` bytes, copied out with as_udf_bytes. Not a blob: the blob
 * readers answer AS_UDF_TYPE_MISMATCH for it. Write one back by position (as_udf_rec_set_at or
 * as_udf_rec_set_value), which keeps it an HLL; as_udf_rec_set_blob would store a plain blob and is refused over
 * an HLL bin. */
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

/* Log level for as_udf_log and as_udf_log_str: warning, the most severe a UDF can write. */
#define AS_UDF_LOG_WARN 0
/* Log level for as_udf_log and as_udf_log_str: info, the server log's default level. */
#define AS_UDF_LOG_INFO 1
/* Log level for as_udf_log and as_udf_log_str: debug, which the server writes only when its log is configured
 * to. What any unknown level is logged as. */
#define AS_UDF_LOG_DEBUG 2

/* The most arguments a UDF takes, in every SDK: a call with more has to fail, saying so. See as_udf_args. */
#define AS_UDF_MAX_ARGS 128

/* The 16-byte value every read fills and every batch is made of. `kind` is one of the AS_UDF_* kinds. For a
 * boolean or integer the payload is the value, for a double its bits (as_udf_value_double). For a string, blob,
 * GeoJSON, HLL, list or map the bytes stay on the host: `len` is the count of bytes, elements or entries, and the
 * payload says where the value is, source handle in the high half and offset in the low (as_udf_value_src,
 * as_udf_value_at). Such a value is valid until the call ends; copying the struct copies the reference, not the
 * data.
 * ```c
 * as_udf_value v;
 * char name[65];
 * if (as_udf_rec_bin("name", &v) != AS_UDF_OK) return as_udf_fail_str("no bin name");
 * int32_t n = as_udf_as_string(&v, name, 64);
 * if (n < 0) return as_udf_fail_str("name is not a string of at most 64 bytes");
 * name[n] = 0;
 * ```
 */
typedef struct as_udf_value_s {
	int32_t kind;
	uint32_t len;
	int64_t payload;
} as_udf_value;

/* The handle of the source a string, blob, GeoJSON, HLL, list or map value sits in: the `src` the by-position
 * imports take. Meaningless for a scalar, whose payload is the value. */
static inline int32_t as_udf_value_src(const as_udf_value* v) { return (int32_t)(v->payload >> 32); }
/* The offset of a string, blob, GeoJSON, HLL, list or map value within its source: the `at` the by-position
 * imports take. */
static inline int32_t as_udf_value_at(const as_udf_value* v) { return (int32_t)(v->payload & 0xffffffff); }
/* The double an AS_UDF_DOUBLE value carries as bits in its payload. No check of the kind: for an integer value it
 * reinterprets the integer's bits, so test `kind` first and widen an integer with a cast. */
static inline double as_udf_value_double(const as_udf_value* v) { double d; __builtin_memcpy(&d, &v->payload, 8); return d; }

/*==========================================================
 * Conveniences.
 */

/* Exports the function that follows from the module under `name`, which is what makes it callable as a UDF. A UDF
 * is `int32_t name(int32_t args, int32_t argc)`: `args` is the handle of the argument list (for as_udf_args) and
 * `argc` how many arguments the client passed. It returns 0 for success and non-zero for failure. Every call of a
 * record UDF runs in a fresh instance of the module, so a static does not carry from one call to the next.
 * ```c
 * AS_UDF_EXPORT(add) int32_t
 * add(int32_t args, int32_t argc)
 * {
 * 	as_udf_value a[1];
 * 	int64_t n;
 * 	if (argc != 1 || as_udf_args(args, argc, a, 1) != 1 || a[0].kind != AS_UDF_INT) return as_udf_fail_str("add: one integer");
 * 	if (as_udf_rec_int_or("n", 0, &n) != AS_UDF_OK) return as_udf_fail_str("add: n is not an integer");
 * 	n += a[0].payload;
 * 	as_udf_rec_set_int("n", 1, n);
 * 	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_fail_str("add: could not save");
 * 	as_udf_result_int(n);
 * 	return 0;
 * }
 * AS_UDF_SIGNATURE(add, "fn add(by: int) -> value")
 * ```
 */
#define AS_UDF_EXPORT(name) __attribute__((export_name(#name)))

/* Exports the signature of the UDF `name` beside it, as the export `__udf_sig_<name>` a client reads to learn
 * what the function takes. `text` is a string literal in the form the Rust macros emit: `fn name(a: int, b?:
 * str[256], rest: any...) -> value`, with the types `int`, `double`, `bool`, `str`, `bytes` and `any`, a `?`
 * after an optional parameter and a capacity in brackets. A stream UDF's reads `stream name { record(...)
 * end(...) }`. Written at file scope, once per function; the text is not checked against the function. */
#define AS_UDF_SIGNATURE(name, text)                                                   \
	static const char as_udf_sig_##name[] = text;                                    \
	__attribute__((export_name("__udf_sig_" #name))) int32_t as_udf_sig_fn_##name(void) \
	{                                                                                  \
		return (int32_t)(uintptr_t)as_udf_sig_##name;                                  \
	}

/* The length of a NUL-terminated string, for the imports that take a length; a freestanding module has no libc
 * strlen. `s` must not be NULL. */
static inline int32_t as_udf_strlen(const char* s)
{
	int32_t n = 0;
	while (s[n] != '\0') n++;
	return n;
}

/* Read a bin of the record of the call, of any kind, into `out`; `name` is NUL-terminated and at most 15 bytes.
 * Returns AS_UDF_OK, or AS_UDF_NOT_FOUND, with `out` of kind AS_UDF_ABSENT, when the record has no such bin. Then
 * switch on `out->kind`: a scalar is in the value, anything else is read by position. The typed readers
 * (as_udf_rec_int and the rest) are shorter where the kind is known. See as_udf_rec_bin_value for what each read
 * costs and what it sees. */
static inline int32_t as_udf_rec_bin(const char* name, as_udf_value* out)
{
	return as_udf_rec_bin_value(name, as_udf_strlen(name), out, (int32_t)sizeof(*out));
}

/* Write the record: the staged bins and removals go to it, and it is created when it does not exist. Nothing a
 * UDF stages reaches the server without this call, and a UDF that returns without it has changed nothing. With
 * nothing staged it is still a write to a record that exists (the server treats it as a touch: the generation and
 * the last-update time move). On a key with no record, a save with a bin staged creates the record; a save with
 * no bin staged (nothing, or only an as_udf_rec_remove or an as_udf_rec_set_ttl) creates nothing and the call
 * fails with "key not found". What a save wrote is not final until the call succeeds: a call that fails after a
 * save that succeeded writes nothing. Returns AS_UDF_OK, or AS_UDF_REFUSED when the server does not take the
 * write: a stream UDF, an event handler, or a write the server rejects, such as a GeoJSON bin whose text is not
 * GeoJSON. After a refusal a list, map or blob value read from a bin earlier in the call answers AS_UDF_ERR: read
 * the bin again. It may be called more than once in a call.
 * ```c
 * int64_t visits;
 * if (as_udf_rec_int_or("visits", 0, &visits) != AS_UDF_OK) return as_udf_fail_str("visits is not an integer");
 * as_udf_rec_set_int("visits", 6, visits + 1);
 * as_udf_rec_set_string("site", 4, "home", 4);
 * as_udf_rec_remove("draft", 5);
 * if (as_udf_rec_save() != AS_UDF_OK) return as_udf_fail_str("could not save the record");
 */
static inline int32_t as_udf_rec_save(void)
{
	return as_udf_rec_exists() ? as_udf_host_rec_update() : as_udf_host_rec_create();
}

/* The element at `index`, counted from 0, of a stored list, in one host call and without a cursor. Its kind is
 * AS_UDF_ABSENT past the end, for a negative index or when `list` is not of kind AS_UDF_LIST. The host steps over
 * the elements before it, so reading every element this way is quadratic: walk with a cursor for that. */
static inline as_udf_value as_udf_list_at(const as_udf_value* list, int32_t index)
{
	as_udf_value v = { AS_UDF_ABSENT, 0, 0 };
	if (list->kind == AS_UDF_LIST && index >= 0) {
		as_udf_list_get(as_udf_value_src(list), as_udf_value_at(list), index, &v, (int32_t)sizeof(v));
	}
	return v;
}

/* Whether a map held by handle (one from as_udf_map_new) has the NUL-terminated string key. False for a handle
 * that is not a map's. A stored map, which is a value and not a handle, is asked with as_udf_map_get. */
static inline bool as_udf_map_contains(int32_t handle, const char* key)
{
	return as_udf_map_has(handle, key, as_udf_strlen(key)) > 0;
}

/* Whether a map held by handle (one from as_udf_map_new) has the integer key. False for a handle that is not a
 * map's. */
static inline bool as_udf_map_contains_int(int32_t handle, int64_t key)
{
	return as_udf_map_has_int(handle, key) > 0;
}

/* What a typed reader answers for a value that is to be of kind `kind` (an AS_UDF_* kind): AS_UDF_OK when it is,
 * AS_UDF_NOT_FOUND when it is absent or nil, AS_UDF_TYPE_MISMATCH when it is of another kind. The three outcomes
 * every typed reader of this header has, on a bin, on an event record's bin and on an element of a collection:
 * nothing there, the value, or a value of another kind, told apart by the status alone. Use it for the kinds that
 * have no reader of their own: `as_udf_expect(&v, AS_UDF_LIST)`. */
static inline int32_t as_udf_expect(const as_udf_value* v, int32_t kind)
{
	if (v->kind == kind) return AS_UDF_OK;
	return v->kind == AS_UDF_ABSENT || v->kind == AS_UDF_NIL ? AS_UDF_NOT_FOUND : AS_UDF_TYPE_MISMATCH;
}

/* Whether a value is held on the host and read by position: a string, blob, GeoJSON, HLL, list or map. What
 * as_udf_cmp_stored and as_udf_stored_bytes work on. A scalar is not, and neither is a value of the guest's (an
 * `as_udf_v_` value). */
static inline bool as_udf_is_stored(const as_udf_value* v)
{
	return v->kind == AS_UDF_STR || v->kind == AS_UDF_BLOB || v->kind == AS_UDF_GEOJSON || v->kind == AS_UDF_HLL ||
			v->kind == AS_UDF_LIST || v->kind == AS_UDF_MAP;
}

/* Copy a stored value's bytes as the server stores them, msgpack header included, into `out`, which holds `cap`
 * bytes: the encoding a convergent merge compares when nothing else tells two candidates apart. `v` is a value
 * read by position (a string, blob, GeoJSON, HLL, list or map). Returns the length; a needed-length status when
 * `cap` is too small (nothing is written); AS_UDF_NOT_FOUND for an absent or nil value; AS_UDF_TYPE_MISMATCH for
 * a scalar or a value of the guest's, whose payload is not a position; or AS_UDF_ERR when the value is no longer
 * readable. To compare two values without copying either, use as_udf_cmp_stored. */
static inline int32_t as_udf_stored_bytes(const as_udf_value* v, void* out, int32_t cap)
{
	if (! as_udf_is_stored(v)) return v->kind == AS_UDF_ABSENT || v->kind == AS_UDF_NIL ? AS_UDF_NOT_FOUND : AS_UDF_TYPE_MISMATCH;
	return as_udf_coll_bytes_at(as_udf_value_src(v), as_udf_value_at(v), out, cap);
}

/* Compare two stored values byte by byte, as memcmp would on their stored encodings, without copying either. The
 * order is written to `order`: exactly -1, 0 or 1, and nothing else. Returns AS_UDF_OK; AS_UDF_TYPE_MISMATCH,
 * with `order` untouched, when either value is not held by position (a scalar, nil, an absent value, a value of
 * the guest's) or the host cannot compare the two. The status is never an order, so test it before reading
 * `order`. The order is of encodings, not of meaning, and is the same on every cluster.
 * ```c
 * int32_t order;
 * if (as_udf_cmp_stored(&mine, &theirs, &order) != AS_UDF_OK) return as_udf_fail_str("not two stored values");
 * const as_udf_value* winner = order >= 0 ? &mine : &theirs;
 * ```
 */
static inline int32_t as_udf_cmp_stored(const as_udf_value* a, const as_udf_value* b, int32_t* order)
{
	if (! as_udf_is_stored(a) || ! as_udf_is_stored(b)) return AS_UDF_TYPE_MISMATCH;
	int32_t rc = as_udf_coll_cmp_at(as_udf_value_src(a), as_udf_value_at(a), as_udf_value_src(b), as_udf_value_at(b));
	if (rc < -1 || rc > 1) return AS_UDF_TYPE_MISMATCH;
	*order = rc;
	return AS_UDF_OK;
}

/* Copy the raw bytes of a string, blob, GeoJSON or HLL value into `out`, which holds `cap` bytes: the one reader
 * that takes all four kinds, for a caller that wants the bytes whatever they are labelled. No terminator is
 * written, so leave room and add one to use the bytes as a C string. Returns the length; a needed-length status
 * when `cap` is too small, in which case nothing is written (the value's `len` is the size, so the buffer can be
 * sized first); AS_UDF_NOT_FOUND for an absent or nil value; AS_UDF_TYPE_MISMATCH for any other kind (a scalar, a
 * list, a map, a value of the guest's); AS_UDF_ERR when the value is no longer readable. The strict readers are
 * as_udf_as_string, which takes only a string, and as_udf_as_blob, which takes only a blob. */
static inline int32_t as_udf_bytes(const as_udf_value* v, void* out, int32_t cap)
{
	if (v->kind != AS_UDF_STR && v->kind != AS_UDF_BLOB && v->kind != AS_UDF_GEOJSON && v->kind != AS_UDF_HLL) {
		return v->kind == AS_UDF_ABSENT || v->kind == AS_UDF_NIL ? AS_UDF_NOT_FOUND : AS_UDF_TYPE_MISMATCH;
	}
	return as_udf_cursor_bytes_at(as_udf_value_src(v), as_udf_value_at(v), out, cap);
}

/* A value as an integer, into `out`: AS_UDF_OK; AS_UDF_NOT_FOUND, with `out` untouched, when the value is absent
 * or nil; AS_UDF_TYPE_MISMATCH, with `out` untouched, when it is of another kind. A double is not truncated. For
 * an element of a list, an entry of a map, an argument: any value the call read. */
static inline int32_t as_udf_as_int(const as_udf_value* v, int64_t* out)
{
	int32_t rc = as_udf_expect(v, AS_UDF_INT);
	if (rc == AS_UDF_OK) *out = v->payload;
	return rc;
}

/* A value as a double, into `out`. Statuses as as_udf_as_int. An integer is not widened: it is another kind. */
static inline int32_t as_udf_as_double(const as_udf_value* v, double* out)
{
	int32_t rc = as_udf_expect(v, AS_UDF_DOUBLE);
	if (rc == AS_UDF_OK) *out = as_udf_value_double(v);
	return rc;
}

/* A value as a boolean, into `out`. Statuses as as_udf_as_int. An integer is not a boolean. The return says
 * whether the value was read, not what it holds. */
static inline int32_t as_udf_as_bool(const as_udf_value* v, bool* out)
{
	int32_t rc = as_udf_expect(v, AS_UDF_BOOL);
	if (rc == AS_UDF_OK) *out = v->payload != 0;
	return rc;
}

/* Copy a string value into `out`, which holds `cap` bytes; no terminator is written, so pass one less than the
 * buffer and add it. Returns the length; a needed-length status when `cap` is too small (nothing is written;
 * `-(rc) - 16` bytes are needed); AS_UDF_NOT_FOUND when the value is absent or nil; AS_UDF_TYPE_MISMATCH when it
 * is not a string: GeoJSON is not one. Every answer but the length is negative. */
static inline int32_t as_udf_as_string(const as_udf_value* v, char* out, int32_t cap)
{
	int32_t rc = as_udf_expect(v, AS_UDF_STR);
	return rc == AS_UDF_OK ? as_udf_bytes(v, out, cap) : rc;
}

/* Copy a blob value into `out`, which holds `cap` bytes. Strict: only a plain blob. Returns the length; a
 * needed-length status when `cap` is too small (nothing is written); AS_UDF_NOT_FOUND when the value is absent or
 * nil; AS_UDF_TYPE_MISMATCH when it is not a blob: a string, GeoJSON or an HLL is not one, and as_udf_bytes reads
 * the raw bytes of any of them. */
static inline int32_t as_udf_as_blob(const as_udf_value* v, void* out, int32_t cap)
{
	int32_t rc = as_udf_expect(v, AS_UDF_BLOB);
	return rc == AS_UDF_OK ? as_udf_bytes(v, out, cap) : rc;
}

/* The call's arguments into `out`, an array of `cap` values: `handle` and `argc` are the two parameters the
 * export was called with. Returns how many were written, which is the smaller of `cap` and the number the client
 * passed; 0 when the client passed none; AS_UDF_ERR when they cannot be read. An argument the client left out is
 * simply not there; one passed as nil is of kind AS_UDF_NIL. Call it once and keep the array: each string, blob,
 * list or map argument takes a handle every time it is read.
 *
 * Nothing is dropped behind the caller's back, but nothing stops the client passing more than the array holds
 * either: `argc` is the number passed, whatever `cap` is. A UDF takes at most AS_UDF_MAX_ARGS (128) arguments, in
 * every SDK, and a call with more than the function declares has to fail rather than run on the first few. So
 * check `argc` against what the function takes, before or after the read:
 * ```c
 * as_udf_value a[2];
 * if (argc > 2) return as_udf_fail_str("add_to takes at most 2 arguments");
 * int32_t n = as_udf_args(args, argc, a, 2);
 * if (n < 1 || a[0].kind != AS_UDF_STR) return as_udf_fail_str("the first argument is a bin name");
 * int64_t by = n > 1 && a[1].kind == AS_UDF_INT ? a[1].payload : 1;
 * ```
 * A function that takes any number reads into an array of AS_UDF_MAX_ARGS and fails when `argc` is over it.
 */
static inline int32_t as_udf_args(int32_t handle, int32_t argc, as_udf_value* out, int32_t cap)
{
	if (argc <= 0) return 0;
	if (handle <= 0) return AS_UDF_ERR;
	return as_udf_args_values(handle, out, cap * (int32_t)sizeof(as_udf_value));
}

/* An integer as a 16-byte value of the guest's, for a batch (as_udf_list_push_values, as_udf_map_put_entries) or
 * one of the `_value` calls (as_udf_list_push_value, as_udf_map_put_value, as_udf_rec_set_value). Nothing is sent
 * to the host until the call that takes the value. */
static inline as_udf_value as_udf_v_int(int64_t i) { as_udf_value v = { AS_UDF_INT, 0, i }; return v; }
/* A double as a 16-byte value, for a batch or a `_value` call. */
static inline as_udf_value as_udf_v_double(double d) { as_udf_value v = { AS_UDF_DOUBLE, 0, 0 }; __builtin_memcpy(&v.payload, &d, 8); return v; }
/* A boolean as a 16-byte value, for a batch or a `_value` call. */
static inline as_udf_value as_udf_v_bool(bool b) { as_udf_value v = { AS_UDF_BOOL, 0, b ? 1 : 0 }; return v; }
/* Nil as a 16-byte value, for a batch or a `_value` call: a nil element or entry. In as_udf_rec_set_value it
 * removes the bin, since a bin cannot hold nil. */
static inline as_udf_value as_udf_v_nil(void) { as_udf_value v = { AS_UDF_NIL, 0, 0 }; return v; }
/* A string of the guest's as a 16-byte value (kind AS_UDF_GUEST_STR), for a batch or a `_value` call: `len`
 * bytes at `s`, no terminator needed. Only the address travels, so the bytes must stay valid and unchanged until
 * the call that hands the value to the host, which copies them. It is not a stored string: as_udf_bytes and the
 * typed readers answer AS_UDF_TYPE_MISMATCH for it. */
static inline as_udf_value as_udf_v_string(const char* s, int32_t len) { as_udf_value v = { AS_UDF_GUEST_STR, (uint32_t)len, (int64_t)(uintptr_t)s }; return v; }
/* A blob of the guest's as a 16-byte value (kind AS_UDF_GUEST_BLOB), for a batch or a `_value` call. Only the
 * address travels, so the bytes must stay valid until the call that hands the value to the host, which copies
 * them. */
static inline as_udf_value as_udf_v_blob(const void* b, int32_t len) { as_udf_value v = { AS_UDF_GUEST_BLOB, (uint32_t)len, (int64_t)(uintptr_t)b }; return v; }
/* GeoJSON text of the guest's as a 16-byte value (kind AS_UDF_GUEST_GEOJSON), for a batch or a `_value` call, so
 * that it arrives as GeoJSON and not as a string. Only the address travels, as for as_udf_v_string. */
static inline as_udf_value as_udf_v_geojson(const char* s, int32_t len) { as_udf_value v = { AS_UDF_GUEST_GEOJSON, (uint32_t)len, (int64_t)(uintptr_t)s }; return v; }
/* A value built on the host as a 16-byte value (kind AS_UDF_HANDLE), for a batch or a `_value` call: how a built
 * list or map is nested in another without a call of its own. */
static inline as_udf_value as_udf_v_handle(int32_t h) { as_udf_value v = { AS_UDF_HANDLE, 0, h }; return v; }

/* Append a value of any kind to a built list: a scalar or nil by value, bytes of the guest's (an `as_udf_v_`
 * value) by address, a value the call read by position and with its kind kept, a built collection by handle. The
 * general push, and the one for a loop: the value travels as its 16 bytes and takes no handle, so a list takes
 * as many elements as the call's memory allows. Returns AS_UDF_OK; AS_UDF_NOT_FOUND for a value of kind
 * AS_UDF_ABSENT, which appends nothing; AS_UDF_ERR when `handle` is not a list the call built; AS_UDF_REFUSED
 * when the list does not take the value: it cannot be read, the list would come to contain itself or nest more
 * than 64 levels deep, or the memory the call may build on the host is used up.
 * ```c
 * int32_t out = as_udf_list_new(0);
 * for (int32_t i = 0; i < count; i++) {
 * 	as_udf_value v = as_udf_v_double(samples[i]);
 * 	if (as_udf_list_push_value(out, &v) != AS_UDF_OK) return as_udf_fail_str("could not build the list");
 * }
 * ```
 */
static inline int32_t as_udf_list_push_value(int32_t handle, const as_udf_value* v)
{
	if (v->kind == AS_UDF_ABSENT) return AS_UDF_NOT_FOUND;
	int32_t n = as_udf_list_push_values(handle, v, (int32_t)sizeof(*v));
	return n == 1 ? AS_UDF_OK : (n < 0 ? n : AS_UDF_REFUSED);
}

/* Append a double to a built list, with no value handle. Statuses as as_udf_list_push_value. */
static inline int32_t as_udf_list_push_double(int32_t handle, double value)
{
	as_udf_value v = as_udf_v_double(value);
	return as_udf_list_push_value(handle, &v);
}

/* Append a boolean to a built list, with no value handle. Statuses as as_udf_list_push_value. */
static inline int32_t as_udf_list_push_bool(int32_t handle, bool value)
{
	as_udf_value v = as_udf_v_bool(value);
	return as_udf_list_push_value(handle, &v);
}

/* Append a nil to a built list, with no value handle. Statuses as as_udf_list_push_value. */
static inline int32_t as_udf_list_push_nil(int32_t handle)
{
	as_udf_value v = as_udf_v_nil();
	return as_udf_list_push_value(handle, &v);
}

/* Append a blob to a built list, with no value handle: `value_len` bytes, copied by the host. Statuses as
 * as_udf_list_push_value. */
static inline int32_t as_udf_list_push_blob(int32_t handle, const void* value, int32_t value_len)
{
	as_udf_value v = as_udf_v_blob(value, value_len);
	return as_udf_list_push_value(handle, &v);
}

/* Append GeoJSON text to a built list as GeoJSON, with no value handle: `value_len` bytes, copied by the host
 * and not validated here. Statuses as as_udf_list_push_value. */
static inline int32_t as_udf_list_push_geojson(int32_t handle, const char* value, int32_t value_len)
{
	as_udf_value v = as_udf_v_geojson(value, value_len);
	return as_udf_list_push_value(handle, &v);
}

/* Put a value of any kind under a string key of a built map: `key_len` bytes of key, of any length and with no
 * terminator needed, copied by the host. The value is a scalar or nil by value, bytes of the guest's (an
 * `as_udf_v_` value) by address, a value the call read by position and with its kind kept, or a built collection
 * by handle. The general put, and the one for a loop: neither the key nor the value takes a handle. A key that is
 * already there has its value replaced. Returns AS_UDF_OK; AS_UDF_NOT_FOUND for a value of kind AS_UDF_ABSENT,
 * which puts nothing; AS_UDF_ERR when `handle` is not a map the call built; AS_UDF_REFUSED when the map does not
 * take the entry: the value cannot be read, the map would come to contain itself or nest more than 64 levels
 * deep, or the memory the call may build on the host is used up. */
static inline int32_t as_udf_map_put_string_value(int32_t handle, const char* key, int32_t key_len, const as_udf_value* v)
{
	if (v->kind == AS_UDF_ABSENT) return AS_UDF_NOT_FOUND;
	as_udf_value pair[2] = { as_udf_v_string(key, key_len), *v };
	int32_t n = as_udf_map_put_entries(handle, pair, (int32_t)sizeof(pair));
	return n == 1 ? AS_UDF_OK : (n < 0 ? n : AS_UDF_REFUSED);
}

/* as_udf_map_put_string_value for a NUL-terminated key, of any length. The way to carry a bin read from an event
 * record into a record answer. */
static inline int32_t as_udf_map_put_value(int32_t handle, const char* key, const as_udf_value* v)
{
	return as_udf_map_put_string_value(handle, key, as_udf_strlen(key), v);
}

/* Put a value of any kind under an integer key of a built map, with no handle for the key or the value: the put
 * for a loop over integer keys. The value and the statuses are those of as_udf_map_put_string_value. */
static inline int32_t as_udf_map_put_int_value(int32_t handle, int64_t key, const as_udf_value* v)
{
	if (v->kind == AS_UDF_ABSENT) return AS_UDF_NOT_FOUND;
	as_udf_value pair[2] = { as_udf_v_int(key), *v };
	int32_t n = as_udf_map_put_entries(handle, pair, (int32_t)sizeof(pair));
	return n == 1 ? AS_UDF_OK : (n < 0 ? n : AS_UDF_REFUSED);
}

/* Stage a value of any kind into a bin of the record of the call: the general setter. A scalar goes by value,
 * bytes of the guest's (an `as_udf_v_` value) by address, a value the call read (a bin, an argument, an element
 * of a collection) by position and with its kind kept, a built collection by handle. A nil removes the bin, since
 * a bin cannot hold nil. `name` is the bin name, `name_len` bytes with no terminator needed: 1 to 15 bytes, none
 * of them zero. Staged until as_udf_rec_save. Returns AS_UDF_OK; AS_UDF_NOT_FOUND for a value of kind
 * AS_UDF_ABSENT, which stages nothing; or the status of the setter of that kind (as_udf_rec_set_int and the
 * rest, as_udf_rec_set_handle, as_udf_rec_set_at). */
static inline int32_t as_udf_rec_set_value(const char* name, int32_t name_len, const as_udf_value* v)
{
	switch (v->kind) {
	case AS_UDF_INT: return as_udf_rec_set_int(name, name_len, v->payload);
	case AS_UDF_DOUBLE: return as_udf_rec_set_double(name, name_len, as_udf_value_double(v));
	case AS_UDF_BOOL: return as_udf_rec_set_bool(name, name_len, (int32_t)v->payload);
	case AS_UDF_NIL: return as_udf_rec_remove(name, name_len);
	case AS_UDF_GUEST_STR: return as_udf_rec_set_string(name, name_len, (const char*)(uintptr_t)v->payload, (int32_t)v->len);
	case AS_UDF_GUEST_BLOB: return as_udf_rec_set_blob(name, name_len, (const void*)(uintptr_t)v->payload, (int32_t)v->len);
	case AS_UDF_GUEST_GEOJSON: return as_udf_rec_set_geojson(name, name_len, (const char*)(uintptr_t)v->payload, (int32_t)v->len);
	case AS_UDF_HANDLE: return as_udf_rec_set_handle(name, name_len, (int32_t)v->payload);
	case AS_UDF_ABSENT: return AS_UDF_NOT_FOUND;
	default: return as_udf_rec_set_at(name, name_len, as_udf_value_src(v), as_udf_value_at(v));
	}
}

/* Whether the record of the call has the bin. A bin the call has staged counts, and one it has removed does not.
 * `name` is NUL-terminated. */
static inline bool as_udf_rec_has(const char* name)
{
	as_udf_value v;
	return as_udf_rec_bin(name, &v) == AS_UDF_OK;
}

/* A bin as a value of any kind, for a caller that wants one test for "there is something to read". False, with
 * `out` of kind AS_UDF_ABSENT, when the record has no such bin; true with the value otherwise. `name` is
 * NUL-terminated. as_udf_rec_bin is the same read with the status. */
static inline bool as_udf_rec_get(const char* name, as_udf_value* out)
{
	if (as_udf_rec_bin(name, out) == AS_UDF_OK && out->kind != AS_UDF_NIL) return true;
	out->kind = AS_UDF_ABSENT;
	out->len = 0;
	out->payload = 0;
	return false;
}

/* The bin as an integer, into `out`. Returns AS_UDF_OK with the value; AS_UDF_NOT_FOUND, with `out` untouched,
 * when the record has no such bin; AS_UDF_TYPE_MISMATCH, with `out` untouched, when the bin is of another kind: a
 * double is not truncated. The typed readers all answer these three, so a missing bin and a bin of the wrong kind
 * are told apart by the status. (AS_UDF_ERR is the read itself failing: no handle free, a value no longer
 * readable.) `name` is NUL-terminated.
 * ```c
 * int64_t balance;
 * double rate;
 * char owner[33];
 * int32_t rc = as_udf_rec_int("balance", &balance);
 * if (rc == AS_UDF_NOT_FOUND) return as_udf_fail_str("no bin balance");
 * if (rc != AS_UDF_OK) return as_udf_fail_str("balance is not an integer");
 * if (as_udf_rec_double_or("rate", 0.0, &rate) != AS_UDF_OK) return as_udf_fail_str("rate is not a double");
 * int32_t n = as_udf_rec_string("owner", owner, 32);
 * if (n < 0) return as_udf_fail_str("no string bin owner of at most 32 bytes");
 * owner[n] = 0;
 * ```
 */
static inline int32_t as_udf_rec_int(const char* name, int64_t* out)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_as_int(&v, out) : rc;
}

/* The bin as a double, into `out`. Statuses as as_udf_rec_int. An integer bin is not widened: it is another
 * kind. */
static inline int32_t as_udf_rec_double(const char* name, double* out)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_as_double(&v, out) : rc;
}

/* The bin as a boolean, into `out`. Statuses as as_udf_rec_int. An integer bin is not a boolean. The return says
 * whether the bin was read, not what it holds. */
static inline int32_t as_udf_rec_bool(const char* name, bool* out)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_as_bool(&v, out) : rc;
}

/* Copy a string bin into `out`, which holds `cap` bytes; no terminator is written, so pass one less than the
 * buffer and add it. Returns the length, a needed-length status when `cap` is too small (nothing is written;
 * `-(rc) - 16` bytes are needed), AS_UDF_NOT_FOUND for no such bin, or AS_UDF_TYPE_MISMATCH when the bin is not a
 * string: a GeoJSON bin is not one. Every answer but the length is negative. */
static inline int32_t as_udf_rec_string(const char* name, char* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_as_string(&v, out, cap) : rc;
}

/* Copy a blob bin into `out`, which holds `cap` bytes. Strict: only a plain blob. Returns the length, a
 * needed-length status when `cap` is too small (nothing is written), AS_UDF_NOT_FOUND for no such bin, or
 * AS_UDF_TYPE_MISMATCH when the bin is not a plain blob: a string, a GeoJSON or an HLL bin is not one, and is
 * read with as_udf_rec_bytes. */
static inline int32_t as_udf_rec_blob(const char* name, void* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_as_blob(&v, out, cap) : rc;
}

/* Copy the raw bytes of a string, blob, GeoJSON or HLL bin into `out`, which holds `cap` bytes: the reader for a
 * caller that wants the bytes whatever the bin is labelled. Returns the length, a needed-length status when `cap`
 * is too small (nothing is written), AS_UDF_NOT_FOUND for no such bin, or AS_UDF_TYPE_MISMATCH for a bin of any
 * other kind. as_udf_bytes is the same reader on a value. */
static inline int32_t as_udf_rec_bytes(const char* name, void* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_rec_bin(name, &v);
	return rc == AS_UDF_OK ? as_udf_bytes(&v, out, cap) : rc;
}

/* A list bin, as a value to walk by position: `out->len` is its element count, as_udf_list_at reads one element
 * and a cursor (as_udf_cursor_enter_at) reads them all. Returns AS_UDF_OK, with a list that may be empty;
 * AS_UDF_NOT_FOUND when the record has no such bin; AS_UDF_TYPE_MISMATCH when the bin is not a list. On any
 * answer but AS_UDF_OK, `out` is not a list and should not be read as one. */
static inline int32_t as_udf_rec_list(const char* name, as_udf_value* out)
{
	int32_t rc = as_udf_rec_bin(name, out);
	return rc == AS_UDF_OK ? as_udf_expect(out, AS_UDF_LIST) : rc;
}

/* A map bin, as a value to read by position: `out->len` is its entry count, as_udf_map_get looks a key up and a
 * cursor walks the entries. Returns AS_UDF_OK, with a map that may be empty; AS_UDF_NOT_FOUND when the record has
 * no such bin; AS_UDF_TYPE_MISMATCH when the bin is not a map. On any answer but AS_UDF_OK, `out` is not a map
 * and should not be read as one. */
static inline int32_t as_udf_rec_map(const char* name, as_udf_value* out)
{
	int32_t rc = as_udf_rec_bin(name, out);
	return rc == AS_UDF_OK ? as_udf_expect(out, AS_UDF_MAP) : rc;
}

/* The bin's integer into `out`, or `dflt` when the record has no such bin: the default stands in for an absent
 * bin only. Returns AS_UDF_OK either way, or AS_UDF_TYPE_MISMATCH, with `out` untouched, when the bin is of
 * another kind, which is never read as the default. The form for a counter that starts at zero on a record that
 * does not have it yet. */
static inline int32_t as_udf_rec_int_or(const char* name, int64_t dflt, int64_t* out)
{
	int32_t rc = as_udf_rec_int(name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* The bin's double into `out`, or `dflt` when the record has no such bin. Statuses as as_udf_rec_int_or: an
 * integer bin is not widened and does not give the default, it is AS_UDF_TYPE_MISMATCH. */
static inline int32_t as_udf_rec_double_or(const char* name, double dflt, double* out)
{
	int32_t rc = as_udf_rec_double(name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* The bin's boolean into `out`, or `dflt` when the record has no such bin. Statuses as as_udf_rec_int_or. */
static inline int32_t as_udf_rec_bool_or(const char* name, bool dflt, bool* out)
{
	int32_t rc = as_udf_rec_bool(name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* The record's user key as one value, into `key`. An integer key is in the value itself (kind AS_UDF_INT). A
 * string or blob key is copied into `out`, which holds `cap` bytes, and the value views it there with kind
 * AS_UDF_GUEST_STR or AS_UDF_GUEST_BLOB: its payload is the address of `out` and its `len` the length, so `out`
 * has to outlive the value, and the bytes are read from `out`, not with as_udf_bytes. Returns AS_UDF_OK;
 * AS_UDF_NOT_FOUND, with `key` of kind AS_UDF_ABSENT, when the record was stored without a key (the client did
 * not send one); or a needed-length status, with `key` of kind AS_UDF_ABSENT, when a string or blob key does not
 * fit in `cap` bytes: `-(rc) - 16` bytes are needed. The two are different answers: a key that does not fit is
 * not a missing key. */
static inline int32_t as_udf_rec_key(void* out, int32_t cap, as_udf_value* key)
{
	int32_t kind = as_udf_rec_key_type();
	key->kind = AS_UDF_ABSENT;
	key->len = 0;
	key->payload = 0;
	if (kind == AS_UDF_INT) {
		*key = as_udf_v_int(as_udf_rec_key_int());
		return AS_UDF_OK;
	}
	if (kind != AS_UDF_STR && kind != AS_UDF_BLOB) return AS_UDF_NOT_FOUND;
	int32_t n = as_udf_rec_key_bytes(out, cap);
	if (n < 0) return n;
	*key = kind == AS_UDF_STR ? as_udf_v_string((const char*)out, n) : as_udf_v_blob(out, n);
	return AS_UDF_OK;
}

/* Fail the call with a NUL-terminated message, of which the client receives the first 511 bytes, and return the
 * non-zero status a UDF then returns, so a failure is one statement. The message is taken as it is: there is no
 * format and there are no format arguments.
 * ```c
 * int64_t balance;
 * if (as_udf_rec_int("balance", &balance) != AS_UDF_OK) return as_udf_fail_str("withdraw: no integer bin balance");
 * ```
 */
static inline int32_t as_udf_fail_str(const char* message)
{
	as_udf_fail(message, as_udf_strlen(message));
	return 1;
}

/* Log a NUL-terminated message at an AS_UDF_LOG_* level. The message is taken as it is: there is no format.
 * Limits as as_udf_log: 511 bytes a line, 16 lines a call. */
static inline void as_udf_log_str(int32_t level, const char* message)
{
	as_udf_log(level, message, as_udf_strlen(message));
}

/*==========================================================
 * Events.
 *
 * An event handler is a function the server calls at a point in its own processing. There is one event,
 * `xdr-merge`: the server hands the handler the record as it is on this cluster and the incoming XDR write
 * as two records, `current` and `incoming` (read through as_udf_event_bin, never the record imports), and
 * takes back a verdict or the final record. Declare one with AS_UDF_EVENT_XDR_MERGE, which also emits the
 * signature `event xdr-merge/1 ...` that `event-set` checks before binding, and get the two records with
 * as_udf_event_decode. A handler must be pure and symmetric: both DCs resolve the same pair with the sides
 * swapped and must land on the same record. The order helper (as_udf_event_incoming_is_newer) is symmetric;
 * "mine first" is not.
 *
 * In an xdr-merge handler the record-level `generation` and `last_update_time` are 0 on both sides, there is no
 * clock (as_udf_now answers -1) and a line given to as_udf_log is dropped. The times a handler does get, the
 * per-bin last-update times, are milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z (Unix
 * milliseconds minus 1262304000000).
 */

/* Read a bin of one of an event's records into `out` as a 16-byte value (`cap` at least 16): `which` is 0 for the
 * current record, 1 for the incoming one, and `name` is `name_len` bytes. Returns AS_UDF_OK; AS_UDF_NOT_FOUND
 * when that record has no such bin, when there is no such record (the call is not an event, or the side is
 * absent) or the name is not a bin name; AS_UDF_ERR when `cap` is under 16. Unlike as_udf_rec_bin_value, `out` is
 * left as it was on AS_UDF_NOT_FOUND. as_udf_event_bin is this call on a decoded record. */
AS_UDF_IMPORT(event_bin_value) int32_t as_udf_event_bin_value(int32_t which, const char* name, int32_t name_len, void* out, int32_t cap);

/* One of the two records of an xdr-merge, as as_udf_event_decode fills it: its metadata, its bin names, and the
 * facts of the merge that are about this record. Read-only: its bins are read with the `as_udf_event_` functions
 * and nothing writes it; the handler's answer is what changes the record. `ttl` is in seconds. `source_id` is the
 * XDR source id of the cluster that wrote this version. `generation` and `last_update_time` are 0 on both records
 * of an xdr-merge: each cluster has its own, so a merge that read them could not converge, and it is the per-bin
 * times that order the two sides. A metadata field the server did not send is 0. The list and map fields are
 * values to read by position, of kind AS_UDF_ABSENT when the server sent none; read the per-bin maps through
 * as_udf_event_bin_last_update_time and as_udf_event_bin_source_id, which fall back to the record's. A fact that
 * is about the other record is false here, and `lost_bins` is AS_UDF_ABSENT on the current record.
 *
 * `deleted_bins` and `lost_bins` have one of two shapes, and never a third: a list of names (kind AS_UDF_LIST)
 * when there are some, and no value at all (kind AS_UDF_ABSENT) when there are none. The server does not send an
 * empty list for "none". So do not read `.len` without testing the kind: as_udf_list_count answers the count for
 * either shape, as_udf_list_has_string answers false for the absent one, and as_udf_event_deleted_any is the
 * test for `deleted_bins`. */
typedef struct as_udf_event_record_s {
	int32_t which;            /* 0 current, 1 incoming */
	int64_t generation;
	int64_t last_update_time;
	int64_t ttl;
	int64_t source_id;
	as_udf_value bin_names;   /* list of bin names */
	as_udf_value deleted_bins; /* list of bin names this side deleted; AS_UDF_ABSENT, not an empty list, when none */
	as_udf_value bin_last_update_times; /* map name -> last-update time, per bin, when the server keeps one (else AS_UDF_ABSENT) */
	as_udf_value bin_source_ids; /* map name -> source id, likewise */
	as_udf_value lost_bins;   /* incoming: list of the names of its bins that would lose under bin convergence; AS_UDF_ABSENT when none */
	bool is_delete;           /* incoming: the write is a record delete, so this record has no bins */
	bool is_partial;          /* incoming: the write carries only the bins that changed */
	bool is_tombstone;        /* current: the record exists but has no bins */
	bool uses_bin_convergence; /* either, the same on both: per-bin LUTs are meaningful */
} as_udf_event_record;

/* The entry under a NUL-terminated string key of a stored map, as a value. Of kind AS_UDF_ABSENT when there is no
 * such key or `m` is not of kind AS_UDF_MAP: a key the map does not have is nothing, not an error. An entry that
 * holds nil is of kind AS_UDF_NIL. The lookup is linear in the map's entries. For a map bin, a nested map, or a
 * map argument; a map being built is asked with as_udf_map_contains. */
static inline as_udf_value as_udf_map_get(const as_udf_value* m, const char* key)
{
	as_udf_value v = { AS_UDF_ABSENT, 0, 0 };
	if (m->kind == AS_UDF_MAP) {
		as_udf_coll_get(as_udf_value_src(m), as_udf_value_at(m), key, as_udf_strlen(key), &v, (int32_t)sizeof(v));
	}
	return v;
}

/* The entry under an integer key of a stored map, as a value. Of kind AS_UDF_ABSENT when there is no such key or
 * `m` is not of kind AS_UDF_MAP; otherwise as as_udf_map_get. */
static inline as_udf_value as_udf_map_get_int(const as_udf_value* m, int64_t key)
{
	as_udf_value v = { AS_UDF_ABSENT, 0, 0 };
	if (m->kind == AS_UDF_MAP) {
		as_udf_coll_get_int(as_udf_value_src(m), as_udf_value_at(m), key, &v, (int32_t)sizeof(v));
	}
	return v;
}

/* The integer under a NUL-terminated string key of a stored map, into `out`. Returns AS_UDF_OK; AS_UDF_NOT_FOUND,
 * with `out` untouched, when there is no such key, the entry is nil or `m` is absent or nil;
 * AS_UDF_TYPE_MISMATCH, with `out` untouched, when the entry is not an integer or `m` is a value of another kind
 * than a map. */
static inline int32_t as_udf_map_int(const as_udf_value* m, const char* key, int64_t* out)
{
	int32_t rc = as_udf_expect(m, AS_UDF_MAP);
	if (rc != AS_UDF_OK) return rc;
	as_udf_value v = as_udf_map_get(m, key);
	return as_udf_as_int(&v, out);
}

/* The integer under a NUL-terminated string key of a stored map into `out`, or `dflt` when there is nothing
 * there (no such key, a nil entry, no map). Returns AS_UDF_OK either way, or AS_UDF_TYPE_MISMATCH, with `out`
 * untouched, when the entry is of another kind or `m` is not a map: the default stands in for nothing else. */
static inline int32_t as_udf_map_int_or(const as_udf_value* m, const char* key, int64_t dflt, int64_t* out)
{
	int32_t rc = as_udf_map_int(m, key, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* Whether a stored string value is exactly the `n` bytes at `text`, read through `buf`, which holds at least `n`
 * bytes. Plumbing of as_udf_string_is and as_udf_list_has_string, which size the buffer. */
static inline bool as_udf_string_eq(const as_udf_value* v, const char* text, int32_t n, char* buf)
{
	if (v->kind != AS_UDF_STR || (int64_t)v->len != (int64_t)n) return false;
	if (as_udf_bytes(v, buf, n) != n) return false;
	for (int32_t i = 0; i < n; i++) if (buf[i] != text[i]) return false;
	return true;
}

/* Whether a stored string value reads as the NUL-terminated `text`, of any length. False when `v` is not of kind
 * AS_UDF_STR: GeoJSON text is not a string. The lengths are compared first, and only a string as long as `text`
 * is copied out, into as many bytes of stack as `text` is long; so a `text` of many kilobytes needs a stack that
 * size (clang's default for wasm32 is 64 KB; `-Wl,-z,stack-size=N` raises it). */
static inline bool as_udf_string_is(const as_udf_value* v, const char* text)
{
	int32_t n = as_udf_strlen(text);
	if (v->kind != AS_UDF_STR || (int64_t)v->len != (int64_t)n) return false;
	char small[64];
	char* buf = n <= (int32_t)sizeof(small) ? small : (char*)__builtin_alloca((unsigned long)n);
	return as_udf_string_eq(v, text, n, buf);
}

/* Whether a stored map has a true boolean under `key`: false when the key is missing or holds anything else.
 * Plumbing of as_udf_event_decode, which reads the event's flags with it. */
static inline bool as_udf_event_flag(const as_udf_value* m, const char* key)
{
	as_udf_value v = as_udf_map_get(m, key);
	return v.kind == AS_UDF_BOOL && v.payload != 0;
}

/* Decode the record under `key` ("current" or "incoming") of the event's description map into `out`, marked as
 * record `which` for as_udf_event_bin, with the facts that are about that record. False when the entry is not a
 * map or carries no list of bins; `out` is then partly filled and not to be used. Plumbing of
 * as_udf_event_decode, which is what a handler calls. */
static inline bool as_udf_event_side(const as_udf_value* description, const char* key, int32_t which, as_udf_event_record* out)
{
	as_udf_value m = as_udf_map_get(description, key);
	if (m.kind != AS_UDF_MAP) return false;
	as_udf_value policy = as_udf_map_get(description, "policy");
	as_udf_value none = { AS_UDF_ABSENT, 0, 0 };
	out->which = which;
	as_udf_map_int_or(&m, "gen", 0, &out->generation);
	as_udf_map_int_or(&m, "lut", 0, &out->last_update_time);
	as_udf_map_int_or(&m, "ttl", 0, &out->ttl);
	as_udf_map_int_or(&m, "src-id", 0, &out->source_id);
	out->bin_names = as_udf_map_get(&m, "bins");
	out->deleted_bins = as_udf_map_get(&m, "deleted");
	out->bin_last_update_times = as_udf_map_get(&m, "bin-luts");
	out->bin_source_ids = as_udf_map_get(&m, "bin-srcs");
	out->lost_bins = which == 1 ? as_udf_map_get(description, "lost") : none;
	out->is_delete = which == 1 && as_udf_event_flag(&m, "deleted-record");
	out->is_partial = which == 1 && as_udf_event_flag(&m, "partial");
	out->is_tombstone = which == 0 && as_udf_event_flag(&m, "tombstone");
	out->uses_bin_convergence = as_udf_string_is(&policy, "bin-lut");
	return out->bin_names.kind == AS_UDF_LIST;
}

/* Decode an xdr-merge call's arguments into its two records: `current`, the record as it is on this cluster, and
 * `incoming`, the one that arrived over XDR. `args` and `argc` are the handler's own parameters. Returns
 * AS_UDF_OK, or AS_UDF_ERR when the arguments are not those of an xdr-merge, in which case neither record is to
 * be used and the handler should fail. Call it once, first: the records hold values that stay valid for the whole
 * call. */
static inline int32_t as_udf_event_decode(int32_t args, int32_t argc, as_udf_event_record* current, as_udf_event_record* incoming)
{
	as_udf_value a[1];
	if (as_udf_args(args, argc, a, 1) < 1 || a[0].kind != AS_UDF_MAP) return AS_UDF_ERR;
	if (! as_udf_event_side(&a[0], "current", 0, current) || ! as_udf_event_side(&a[0], "incoming", 1, incoming)) {
		return AS_UDF_ERR;
	}
	return AS_UDF_OK;
}

/* Read a bin of one of the event's records, of any kind, into `out`; `name` is NUL-terminated. Returns AS_UDF_OK,
 * or AS_UDF_NOT_FOUND when that record has no such bin, which leaves `out` as it was. A bin the incoming write
 * does not carry is not found even if the other cluster has it (see `is_partial`), and so is a bin that side
 * deleted (see `deleted_bins`). */
static inline int32_t as_udf_event_bin(const as_udf_event_record* r, const char* name, as_udf_value* out)
{
	return as_udf_event_bin_value(r->which, name, as_udf_strlen(name), out, (int32_t)sizeof(*out));
}

/* Whether one of the event's records has the bin. False for a bin that side deleted or that a partial incoming
 * write does not carry. */
static inline bool as_udf_event_has(const as_udf_event_record* r, const char* name)
{
	as_udf_value v;
	return as_udf_event_bin(r, name, &v) == AS_UDF_OK;
}

/* A bin of one of the event's records as a value of any kind. False, with `out` of kind AS_UDF_ABSENT, when that
 * record has no such bin; true with the value otherwise. */
static inline bool as_udf_event_get(const as_udf_event_record* r, const char* name, as_udf_value* out)
{
	if (as_udf_event_bin(r, name, out) == AS_UDF_OK && out->kind != AS_UDF_NIL) return true;
	out->kind = AS_UDF_ABSENT;
	out->len = 0;
	out->payload = 0;
	return false;
}

/* A bin of one of the event's records as an integer, into `out`. Returns AS_UDF_OK with the value;
 * AS_UDF_NOT_FOUND, with `out` untouched, when that record has no such bin or it is nil; AS_UDF_TYPE_MISMATCH,
 * with `out` untouched, when the bin is of another kind. The three outcomes of as_udf_rec_int, on an event
 * record.
 * ```c
 * int64_t mine, theirs;
 * if (as_udf_event_int_or(&current, "visits", 0, &mine) != AS_UDF_OK) return as_udf_fail_str("visits is not an integer");
 * if (as_udf_event_int_or(&incoming, "visits", 0, &theirs) != AS_UDF_OK) return as_udf_fail_str("visits is not an integer");
 * as_udf_map_put_string_int(out, "visits", 6, mine > theirs ? mine : theirs);
 * ```
 */
static inline int32_t as_udf_event_int(const as_udf_event_record* r, const char* name, int64_t* out)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_as_int(&v, out) : rc;
}

/* A bin of one of the event's records as a double, into `out`. Statuses as as_udf_event_int. An integer bin is
 * not widened: it is another kind. */
static inline int32_t as_udf_event_double(const as_udf_event_record* r, const char* name, double* out)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_as_double(&v, out) : rc;
}

/* A bin of one of the event's records as a boolean, into `out`. Statuses as as_udf_event_int. The return says
 * whether the bin was read, not what it holds. */
static inline int32_t as_udf_event_bool(const as_udf_event_record* r, const char* name, bool* out)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_as_bool(&v, out) : rc;
}

/* Copy a string bin of one of the event's records into `out`, which holds `cap` bytes; no terminator is written.
 * Returns the length, a needed-length status when `cap` is too small (nothing is written), AS_UDF_NOT_FOUND for
 * no such bin, or AS_UDF_TYPE_MISMATCH when it is not a string. */
static inline int32_t as_udf_event_string(const as_udf_event_record* r, const char* name, char* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_as_string(&v, out, cap) : rc;
}

/* Copy a blob bin of one of the event's records into `out`, which holds `cap` bytes. Strict: only a plain blob.
 * Returns the length, a needed-length status when `cap` is too small (nothing is written), AS_UDF_NOT_FOUND for
 * no such bin, or AS_UDF_TYPE_MISMATCH when it is not a plain blob; as_udf_event_bytes reads the raw bytes of a
 * string, GeoJSON or HLL bin too. */
static inline int32_t as_udf_event_blob(const as_udf_event_record* r, const char* name, void* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_as_blob(&v, out, cap) : rc;
}

/* Copy the raw bytes of a string, blob, GeoJSON or HLL bin of one of the event's records into `out`, which holds
 * `cap` bytes. Returns the length, a needed-length status when `cap` is too small (nothing is written),
 * AS_UDF_NOT_FOUND for no such bin, or AS_UDF_TYPE_MISMATCH for a bin of any other kind. */
static inline int32_t as_udf_event_bytes(const as_udf_event_record* r, const char* name, void* out, int32_t cap)
{
	as_udf_value v;
	int32_t rc = as_udf_event_bin(r, name, &v);
	return rc == AS_UDF_OK ? as_udf_bytes(&v, out, cap) : rc;
}

/* A list bin of one of the event's records, as a value to walk by position. Returns AS_UDF_OK, with a list that
 * may be empty; AS_UDF_NOT_FOUND when that record has no such bin; AS_UDF_TYPE_MISMATCH when the bin is not a
 * list. On any answer but AS_UDF_OK, `out` is not to be read as a list. */
static inline int32_t as_udf_event_list(const as_udf_event_record* r, const char* name, as_udf_value* out)
{
	int32_t rc = as_udf_event_bin(r, name, out);
	return rc == AS_UDF_OK ? as_udf_expect(out, AS_UDF_LIST) : rc;
}

/* A map bin of one of the event's records, as a value to read by position. Returns AS_UDF_OK, with a map that may
 * be empty; AS_UDF_NOT_FOUND when that record has no such bin; AS_UDF_TYPE_MISMATCH when the bin is not a map. On
 * any answer but AS_UDF_OK, `out` is not to be read as a map. */
static inline int32_t as_udf_event_map(const as_udf_event_record* r, const char* name, as_udf_value* out)
{
	int32_t rc = as_udf_event_bin(r, name, out);
	return rc == AS_UDF_OK ? as_udf_expect(out, AS_UDF_MAP) : rc;
}

/* The integer of a bin of one of the event's records into `out`, or `dflt` when that record has no such bin: the
 * default stands in for an absent or nil bin only. Returns AS_UDF_OK either way, or AS_UDF_TYPE_MISMATCH, with
 * `out` untouched, when the bin is of another kind. */
static inline int32_t as_udf_event_int_or(const as_udf_event_record* r, const char* name, int64_t dflt, int64_t* out)
{
	int32_t rc = as_udf_event_int(r, name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* The double of a bin of one of the event's records into `out`, or `dflt` when that record has no such bin.
 * Statuses as as_udf_event_int_or: an integer bin is not widened and does not give the default. */
static inline int32_t as_udf_event_double_or(const as_udf_event_record* r, const char* name, double dflt, double* out)
{
	int32_t rc = as_udf_event_double(r, name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* The boolean of a bin of one of the event's records into `out`, or `dflt` when that record has no such bin.
 * Statuses as as_udf_event_int_or. */
static inline int32_t as_udf_event_bool_or(const as_udf_event_record* r, const char* name, bool dflt, bool* out)
{
	int32_t rc = as_udf_event_bool(r, name, out);
	if (rc != AS_UDF_NOT_FOUND) return rc;
	*out = dflt;
	return AS_UDF_OK;
}

/* How many elements a list value has, and 0 for a value that is not a list. The count for the fields of an event
 * record that are a list or nothing at all: `deleted_bins` and `lost_bins` are of kind AS_UDF_LIST when the
 * server sent the names and of kind AS_UDF_ABSENT when it sent none, and never an empty list for "none". This
 * answers the count either way, so a handler need not test the kind first:
 * `as_udf_list_count(&incoming.lost_bins)`. */
static inline int32_t as_udf_list_count(const as_udf_value* list)
{
	return list->kind == AS_UDF_LIST ? (int32_t)list->len : 0;
}

/* Whether this side deleted any bin: its `deleted_bins` list is there and not empty (the field is a list of
 * names, or of kind AS_UDF_ABSENT when this side deleted none; as_udf_list_count answers its length either
 * way). The case every merge has to guard against, since a deleted bin reads as absent and taking the other
 * side's copy would bring it back; as_udf_list_has_string on `deleted_bins` says whether a given bin was one of
 * them. */
static inline bool as_udf_event_deleted_any(const as_udf_event_record* r)
{
	return as_udf_list_count(&r->deleted_bins) > 0;
}

/* When this side last wrote the bin, as the server's timestamp for it, in milliseconds since the Citrusleaf
 * epoch, 2010-01-01T00:00:00Z (Unix milliseconds minus 1262304000000): the bin's own where the server keeps one
 * (see `uses_bin_convergence`), the record's `last_update_time` otherwise, also for a bin the record does not
 * have; in an xdr-merge the record's is 0. `name` is NUL-terminated. A number to order the two sides by, not to
 * compare with a clock, which a handler does not have. */
static inline int64_t as_udf_event_bin_last_update_time(const as_udf_event_record* r, const char* name)
{
	int64_t t = r->last_update_time;
	as_udf_map_int_or(&r->bin_last_update_times, name, r->last_update_time, &t);
	return t;
}

/* The id of the XDR source that last wrote the bin on this side: the bin's own where the server keeps one, the
 * record's source id otherwise. What breaks a tie between equal last-update times. */
static inline int64_t as_udf_event_bin_source_id(const as_udf_event_record* r, const char* name)
{
	int64_t id = r->source_id;
	as_udf_map_int_or(&r->bin_source_ids, name, r->source_id, &id);
	return id;
}

/* Whether the incoming record wrote the bin later than the current one: by last-update time, then by source id,
 * the order the server's bin convergence uses. Both clusters see the same two times and ids, so both give the
 * same side as newer whichever of them is `current`: a rule built on this is symmetric. False when the two are
 * equal in both. */
static inline bool as_udf_event_incoming_is_newer(const as_udf_event_record* current, const as_udf_event_record* incoming, const char* name)
{
	int64_t ct = as_udf_event_bin_last_update_time(current, name);
	int64_t it = as_udf_event_bin_last_update_time(incoming, name);
	return it > ct || (it == ct && as_udf_event_bin_source_id(incoming, name) > as_udf_event_bin_source_id(current, name));
}

/* Whether a stored list holds the string `text` (NUL-terminated, of any length) among its elements; elements
 * that are not strings are not it. False when `list` is not of kind AS_UDF_LIST, which includes the
 * AS_UDF_ABSENT of an event record's `deleted_bins` or `lost_bins` when there are none. Linear in the list. Only
 * elements as long as `text` are copied out, into as many bytes of stack as `text` is long (see
 * as_udf_string_is). The walk takes one handle for its cursor and gives it back; when no handle is free it
 * cannot be made, and the answer is found the slow way, an element at a time, rather than given as false. For an
 * event record's `bin_names`, `deleted_bins` and `lost_bins`, and any list of strings. */
static inline bool as_udf_list_has_string(const as_udf_value* list, const char* text)
{
	if (list->kind != AS_UDF_LIST) return false;
	int32_t len = as_udf_strlen(text);
	char small[64];
	char* buf = len <= (int32_t)sizeof(small) ? small : (char*)__builtin_alloca((unsigned long)len);
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(list), as_udf_value_at(list));
	if (cur <= 0) {
		for (int32_t i = 0; i < (int32_t)list->len; i++) {
			as_udf_value v = as_udf_list_at(list, i);
			if (as_udf_string_eq(&v, text, len, buf)) return true;
		}
		return false;
	}
	as_udf_value w[32];
	bool found = false;
	for (int32_t n; ! found && (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) if (as_udf_string_eq(&w[i], text, len, buf)) { found = true; break; }
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
	return found;
}

/* Put every bin of one of the event's records that the built map `overrides` lacks into it, by position, and
 * return the map's handle: that record with `overrides` on top. For a handler that decides a few bins and keeps
 * the rest of one record. A bin this side deleted is not among its bins, so it is not put and the answer leaves
 * it out: nothing is brought back. Returns `overrides`, or 0 when a bin could not be carried over (its name or
 * value could not be read, or the map did not take it): 0 is no handle, and as_udf_resolve_record fails the call
 * for it, so a record answer is never built short of a bin. It prefers a side, so two clusters that each use it
 * on their own current record do not converge; use it on a side both agree on, such as the one
 * as_udf_event_incoming_is_newer names.
 * ```c
 * int32_t out = as_udf_map_new(8);
 * as_udf_map_put_string_int(out, "visits", 6, visits);
 * return as_udf_resolve_record(as_udf_event_with_bins(&incoming, out), -1);
 * ```
 */
static inline int32_t as_udf_event_with_bins(const as_udf_event_record* r, int32_t overrides)
{
	if (r->bin_names.kind != AS_UDF_LIST) return overrides;
	bool ok = true;
	for (int32_t i = 0; ok && i < (int32_t)r->bin_names.len; i++) {
		/* A bin name is at most 15 bytes; the buffer takes any the server could send. */
		char name[64];
		as_udf_value entry = as_udf_list_at(&r->bin_names, i), v;
		int32_t len = as_udf_as_string(&entry, name, (int32_t)sizeof(name) - 1);
		if (len < 0) {
			ok = false;
			break;
		}
		name[len] = '\0';
		if (as_udf_map_has(overrides, name, len) > 0) continue;
		int32_t rc = as_udf_event_bin(r, name, &v);
		if (rc == AS_UDF_NOT_FOUND) continue;
		ok = rc == AS_UDF_OK && as_udf_map_put_string_value(overrides, name, len, &v) == AS_UDF_OK;
	}
	return ok ? overrides : 0;
}

/* The verdicts. Each returns the status the handler returns. */

/* Answer `keep-local`: the record on this cluster stays as it is and the incoming write is dropped. Sets the
 * call's result and returns 0, the status the handler returns: `return as_udf_resolve_keep_local();`. */
static inline int32_t as_udf_resolve_keep_local(void) { as_udf_result_string("keep-local", 10); return 0; }
/* Answer `take-remote`: the incoming write is applied, its bins over the current record. Sets the call's result
 * and returns 0, the status the handler returns: `return as_udf_resolve_take_remote();`. */
static inline int32_t as_udf_resolve_take_remote(void) { as_udf_result_string("take-remote", 11); return 0; }
/* Answer `policy`: the namespace's conflict policy decides, as it would have without a handler. The answer for
 * the cases a handler has no opinion on. Sets the call's result and returns 0, the status the handler returns:
 * `return as_udf_resolve_policy();`. */
static inline int32_t as_udf_resolve_policy(void) { as_udf_result_string("policy", 6); return 0; }
/* Answer `delete`: the record is removed on this cluster. The other cluster decides for itself, so a merge that
 * deletes has to delete on both sides for the two to agree. A record answer with no bins left in it is not this:
 * it is the handler's failure. Sets the call's result and returns 0, the status the handler returns: `return
 * as_udf_resolve_delete();`. */
static inline int32_t as_udf_resolve_delete(void) { as_udf_result_string("delete", 6); return 0; }

/* Answer with the final record. `bins` is the handle of a built map from bin name to value: the record is left
 * with exactly these bins, a bin that is absent from the map or nil in it is deleted, and at least one bin must
 * remain. `ttl` is the record's time to live in seconds with a write's meaning (0 for the namespace default), or
 * -1 to keep the one the incoming write asked for. Returns the status the handler returns: 0, or non-zero with
 * the call failed when the answer could not be built.
 * ```c
 * int32_t out = as_udf_map_new(4);
 * int64_t a, b;
 * if (as_udf_event_int_or(&current, "visits", 0, &a) != AS_UDF_OK) return as_udf_fail_str("visits is not an integer");
 * if (as_udf_event_int_or(&incoming, "visits", 0, &b) != AS_UDF_OK) return as_udf_fail_str("visits is not an integer");
 * as_udf_map_put_string_int(out, "visits", 6, a > b ? a : b);
 * return as_udf_resolve_record(out, -1);
 * ```
 */
static inline int32_t as_udf_resolve_record(int32_t bins, int64_t ttl)
{
	int32_t out = as_udf_map_new(3);
	if (out <= 0) return as_udf_fail_str("resolve: could not build the resolution");
	int32_t rc = as_udf_map_put_string_string(out, "verdict", 7, "record", 6);
	if (rc == AS_UDF_OK) rc = as_udf_map_put_string_handle(out, "bins", 4, bins);
	if (rc == AS_UDF_OK && ttl >= 0) rc = as_udf_map_put_string_int(out, "ttl", 3, ttl);
	if (rc != AS_UDF_OK) return as_udf_fail_str("resolve: could not build the record answer");
	return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_fail_str("resolve: could not return the record");
}

/* Declare an xdr-merge handler: `AS_UDF_EVENT_XDR_MERGE(name) { ... }` defines and exports `int32_t name(int32_t
 * args, int32_t argc)` and the signature `event-set` checks before binding it. The body decodes the two records
 * with as_udf_event_decode and returns one of the `as_udf_resolve_` answers. A handler cannot write a record,
 * read the clock or log, and must be symmetric: both clusters resolve the same pair with the sides swapped and
 * have to land on the same record.
 * ```c
 * AS_UDF_EVENT_XDR_MERGE(newest_wins)
 * {
 * 	as_udf_event_record current, incoming;
 * 	if (as_udf_event_decode(args, argc, &current, &incoming) != AS_UDF_OK) return as_udf_fail_str("not an xdr-merge");
 * 	if (incoming.is_delete) return as_udf_resolve_policy();
 * 	return as_udf_event_incoming_is_newer(&current, &incoming, "profile") ?
 * 		as_udf_resolve_take_remote() : as_udf_resolve_keep_local();
 * }
 * ```
 */
#define AS_UDF_EVENT_XDR_MERGE(name)                                                                       \
	AS_UDF_SIGNATURE(name, "event xdr-merge/1 " #name "(current: record, incoming: record) -> resolution") \
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

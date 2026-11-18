// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.aerospike.udf;

import org.teavm.interop.Address;
import org.teavm.interop.Import;

/**
 * The module's imports: the server's natives under the {@code aerospike} import module, one static native
 * method each. An {@link Address} parameter crosses as the i32 offset into linear memory the host reads or
 * writes; the host validates every pointer and length. Nothing here is public: {@link LowLevel} is these
 * calls for user code, one at a time, and the rest of the SDK is built on them.
 */
final class Natives {
    private Natives() {
    }

    // Reading.

    @Import(module = "aerospike", name = "bin_value")
    static native int binValue(Address name, int nameLen, Address out, int cap);

    @Import(module = "aerospike", name = "cursor_values")
    static native int cursorValues(int cursor, Address out, int cap);

    @Import(module = "aerospike", name = "cursor_bytes_at")
    static native int cursorBytesAt(int src, int at, Address out, int cap);

    @Import(module = "aerospike", name = "coll_bytes_at")
    static native int collBytesAt(int src, int at, Address out, int cap);

    @Import(module = "aerospike", name = "coll_cmp_at")
    static native int collCmpAt(int src1, int at1, int src2, int at2);

    @Import(module = "aerospike", name = "cursor_enter_at")
    static native int cursorEnterAt(int src, int at);

    @Import(module = "aerospike", name = "coll_get")
    static native int collGet(int src, int at, Address key, int keyLen, Address out, int cap);

    @Import(module = "aerospike", name = "coll_get_i")
    static native int collGetI(int src, int at, long key, Address out, int cap);

    @Import(module = "aerospike", name = "list_get")
    static native int listGet(int src, int at, int index, Address out, int cap);

    @Import(module = "aerospike", name = "cursor_close")
    static native void cursorClose(int cursor);

    @Import(module = "aerospike", name = "pack_handle")
    static native int packHandle(int handle);

    @Import(module = "aerospike", name = "args_values")
    static native int argsValues(int handle, Address out, int cap);

    @Import(module = "aerospike", name = "coll_size")
    static native int collSize(int handle);

    // Metadata.

    @Import(module = "aerospike", name = "gen")
    static native long gen();

    @Import(module = "aerospike", name = "ttl")
    static native long ttl();

    @Import(module = "aerospike", name = "key_type")
    static native int keyType();

    @Import(module = "aerospike", name = "key_int")
    static native long keyInt();

    @Import(module = "aerospike", name = "key_bytes")
    static native int keyBytes(Address out, int cap);

    @Import(module = "aerospike", name = "digest")
    static native int digest(Address out, int cap);

    @Import(module = "aerospike", name = "rec_exists")
    static native int recExists();

    @Import(module = "aerospike", name = "current_time")
    static native long currentTime();

    // Writing.

    @Import(module = "aerospike", name = "set_int")
    static native int setInt(Address name, int nameLen, long value);

    @Import(module = "aerospike", name = "set_double")
    static native int setDouble(Address name, int nameLen, double value);

    @Import(module = "aerospike", name = "set_str")
    static native int setStr(Address name, int nameLen, Address value, int valueLen);

    @Import(module = "aerospike", name = "set_blob")
    static native int setBlob(Address name, int nameLen, Address value, int valueLen);

    @Import(module = "aerospike", name = "set_handle")
    static native int setHandle(Address name, int nameLen, int handle);

    @Import(module = "aerospike", name = "set_at")
    static native int setAt(Address name, int nameLen, int src, int at);

    @Import(module = "aerospike", name = "bin_remove")
    static native int binRemove(Address name, int nameLen);

    @Import(module = "aerospike", name = "rec_create")
    static native int recCreate();

    @Import(module = "aerospike", name = "rec_update")
    static native int recUpdate();

    // Building.

    @Import(module = "aerospike", name = "val_nil")
    static native int valNil();

    @Import(module = "aerospike", name = "val_double")
    static native int valDouble(double value);

    @Import(module = "aerospike", name = "val_bool")
    static native int valBool(int value);

    @Import(module = "aerospike", name = "list_new")
    static native int listNew(int capacity);

    @Import(module = "aerospike", name = "map_new")
    static native int mapNew(int capacity);

    @Import(module = "aerospike", name = "list_push")
    static native int listPush(int handle, int value);

    @Import(module = "aerospike", name = "list_push_int")
    static native int listPushInt(int handle, long value);

    @Import(module = "aerospike", name = "list_push_str")
    static native int listPushStr(int handle, Address value, int valueLen);

    @Import(module = "aerospike", name = "list_push_at")
    static native int listPushAt(int handle, int src, int at);

    @Import(module = "aerospike", name = "list_push_values")
    static native int listPushValues(int handle, Address vals, int valsLen);

    @Import(module = "aerospike", name = "map_put_str_int")
    static native int mapPutStrInt(int handle, Address key, int keyLen, long value);

    @Import(module = "aerospike", name = "map_put_str_str")
    static native int mapPutStrStr(int handle, Address key, int keyLen, Address value, int valueLen);

    @Import(module = "aerospike", name = "map_put_str_handle")
    static native int mapPutStrHandle(int handle, Address key, int keyLen, int value);

    @Import(module = "aerospike", name = "map_put_str_at")
    static native int mapPutStrAt(int handle, Address key, int keyLen, int src, int at);

    @Import(module = "aerospike", name = "map_put_entries")
    static native int mapPutEntries(int handle, Address pairs, int pairsLen);

    @Import(module = "aerospike", name = "map_has")
    static native int mapHas(int handle, Address key, int keyLen);

    @Import(module = "aerospike", name = "map_has_i")
    static native int mapHasI(int handle, long key);

    // Results and reporting.

    @Import(module = "aerospike", name = "result_nil")
    static native void resultNil();

    @Import(module = "aerospike", name = "result_int")
    static native void resultInt(long value);

    @Import(module = "aerospike", name = "result_double")
    static native void resultDouble(double value);

    @Import(module = "aerospike", name = "result_bool")
    static native void resultBool(int value);

    @Import(module = "aerospike", name = "result_str")
    static native void resultStr(Address value, int valueLen);

    @Import(module = "aerospike", name = "result_blob")
    static native void resultBlob(Address value, int valueLen);

    @Import(module = "aerospike", name = "result_handle")
    static native int resultHandle(int handle);

    @Import(module = "aerospike", name = "result_at")
    static native int resultAt(int src, int at);

    @Import(module = "aerospike", name = "fail")
    static native void fail(Address message, int messageLen);

    @Import(module = "aerospike", name = "log")
    static native void log(int level, Address message, int messageLen);

    // Events and timers.

    @Import(module = "aerospike", name = "event_bin_value")
    static native int eventBinValue(int which, Address name, int nameLen, Address out, int cap);

    @Import(module = "aerospike", name = "timer_set")
    static native int timerSet(Address name, int nameLen, long delayMs);

    @Import(module = "aerospike", name = "timer_cancel")
    static native int timerCancel(Address name, int nameLen);

    // The rest of the record: metadata and writes the Rust SDK has.

    @Import(module = "aerospike", name = "last_update")
    static native long lastUpdate();

    @Import(module = "aerospike", name = "record_size")
    static native long recordSize();

    @Import(module = "aerospike", name = "numbins")
    static native long numbins();

    @Import(module = "aerospike", name = "set_name")
    static native int setName(Address out, int cap);

    @Import(module = "aerospike", name = "set_ttl")
    static native int setTtl(int seconds);

    @Import(module = "aerospike", name = "rec_remove")
    static native int recRemove();

    @Import(module = "aerospike", name = "set_bool")
    static native int setBool(Address name, int nameLen, int value);

    @Import(module = "aerospike", name = "set_nil")
    static native int setNil(Address name, int nameLen);

    @Import(module = "aerospike", name = "set_geojson")
    static native int setGeoJson(Address name, int nameLen, Address value, int valueLen);

    @Import(module = "aerospike", name = "set_list_ints")
    static native int setListInts(Address name, int nameLen, Address vals, int valsLen);

    @Import(module = "aerospike", name = "set_list_doubles")
    static native int setListDoubles(Address name, int nameLen, Address vals, int valsLen);

    @Import(module = "aerospike", name = "result_geojson")
    static native void resultGeoJson(Address value, int valueLen);

    // Values by handle, for keys and for the kinds the typed natives do not take.

    @Import(module = "aerospike", name = "val_int")
    static native int valInt(long value);

    @Import(module = "aerospike", name = "val_str")
    static native int valStr(Address value, int valueLen);

    @Import(module = "aerospike", name = "val_blob")
    static native int valBlob(Address value, int valueLen);

    @Import(module = "aerospike", name = "val_geojson")
    static native int valGeoJson(Address value, int valueLen);

    @Import(module = "aerospike", name = "map_put")
    static native int mapPut(int handle, int key, int value);

    @Import(module = "aerospike", name = "map_put_i_at")
    static native int mapPutIAt(int handle, long key, int src, int at);
}

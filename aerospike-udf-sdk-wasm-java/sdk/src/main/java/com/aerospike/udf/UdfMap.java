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

/**
 * A map built on the host, to be written to a bin, returned as a result, nested in another collection or
 * handed back from an event handler: the Java face of the Rust SDK's {@code Map}. Every {@code put} is one
 * import and throws {@link UdfException} when the host refuses it; they chain. The map lives until the
 * call ends. {@link #handle} is what the low-level {@code Udf.mapPut*} calls take, for a hot path.
 *
 * <pre>
 * UdfMap out = UdfMap.create(2).put("visits", visits).put("tags", tags);
 * </pre>
 */
public final class UdfMap {
    private final int handle;

    private UdfMap(int handle) {
        this.handle = handle;
    }

    /** A new map with room for {@code capacity} entries. */
    public static UdfMap create(int capacity) {
        int h = Natives.mapNew(capacity);

        if (h <= 0) {
            throw new UdfException("could not build a map");
        }

        return new UdfMap(h);
    }

    /** A new map with the default room, for 8 entries. */
    public static UdfMap create() {
        return create(8);
    }

    /** A map over a handle the low-level calls built. */
    public static UdfMap wrap(int handle) {
        return new UdfMap(handle);
    }

    /** The host's handle on the map, as the low-level {@code Udf.mapPut*} calls take it. */
    public int handle() {
        return handle;
    }

    /** How many entries the map holds. */
    public int size() {
        return Natives.collSize(handle);
    }

    /** As a value, for a batch or a result. */
    public Value asValue() {
        return Value.ofHandle(handle);
    }

    /** Whether the map holds {@code key}: it packs the map, so for the occasional check, not a loop. */
    public boolean has(String key) {
        return Udf.mapHasKey(handle, key);
    }

    private UdfMap check(int rc, String key) {
        if (rc != Udf.OK) {
            throw new UdfException("could not put " + key + " (" + rc + ")");
        }

        return this;
    }

    private static Address key(byte[] k) {
        return Address.ofData(k);
    }

    // String keys.

    /** An integer under a string key. */
    public UdfMap put(String key, long value) {
        byte[] k = Udf.bytes(key);
        return check(Natives.mapPutStrInt(handle, key(k), k.length, value), key);
    }

    /** A double under a string key. */
    public UdfMap put(String key, double value) {
        byte[] k = Udf.bytes(key);
        return check(Natives.mapPutStrHandle(handle, key(k), k.length, Natives.valDouble(value)), key);
    }

    /** A boolean under a string key. */
    public UdfMap put(String key, boolean value) {
        byte[] k = Udf.bytes(key);
        return check(Natives.mapPutStrHandle(handle, key(k), k.length, Natives.valBool(value ? 1 : 0)), key);
    }

    /** A string under a string key. */
    public UdfMap put(String key, String value) {
        byte[] k = Udf.bytes(key);
        byte[] v = Udf.bytes(value);
        return check(Natives.mapPutStrStr(handle, key(k), k.length, Address.ofData(v), v.length), key);
    }

    /** A blob. */
    public UdfMap put(String key, byte[] value) {
        byte[] k = Udf.bytes(key);
        return check(Natives.mapPutStrHandle(handle, key(k), k.length,
                Natives.valBlob(Address.ofData(value), value.length)), key);
    }

    /** A value of any kind: a scalar, guest bytes, a stored value by position, a built collection. */
    public UdfMap put(String key, Value value) {
        return check(Udf.mapPutValue(handle, key, value), key);
    }

    /** A built map, nested under a string key. */
    public UdfMap put(String key, UdfMap value) {
        return check(Udf.mapPutHandle(handle, key, value.handle), key);
    }

    /** A built list, nested under a string key. */
    public UdfMap put(String key, UdfList value) {
        return check(Udf.mapPutHandle(handle, key, value.handle()), key);
    }

    /** A nil entry: in a record answer, "no such bin". */
    public UdfMap putNull(String key) {
        return check(Udf.mapPutHandle(handle, key, Natives.valNil()), key);
    }

    /** A GeoJSON document under a string key, kept as that kind rather than a string. */
    public UdfMap putGeoJson(String key, String doc) {
        byte[] k = Udf.bytes(key);
        byte[] d = Udf.bytes(doc);
        return check(Natives.mapPutStrHandle(handle, key(k), k.length,
                Natives.valGeoJson(Address.ofData(d), d.length)), key);
    }

    // Integer keys.

    /** An integer under an integer key. */
    public UdfMap put(long key, long value) {
        return check(Udf.mapPutIInt(handle, key, value), Long.toString(key));
    }

    /** A string under an integer key. */
    public UdfMap put(long key, String value) {
        return check(Udf.mapPutIStr(handle, key, value), Long.toString(key));
    }

    /** A value of any kind under an integer key, the kinds as {@link #put(String, Value)} takes them. */
    public UdfMap put(long key, Value value) {
        return check(Udf.mapPutIValue(handle, key, value), Long.toString(key));
    }

    /** A built map, nested under an integer key. */
    public UdfMap put(long key, UdfMap value) {
        return check(Udf.mapPutIHandle(handle, key, value.handle), Long.toString(key));
    }

    /** A built list, nested under an integer key. */
    public UdfMap put(long key, UdfList value) {
        return check(Udf.mapPutIHandle(handle, key, value.handle()), Long.toString(key));
    }

    /** The first {@code n} pairs of {@code pairs} in one import. */
    public UdfMap putAll(Value[] pairs, int n) {
        int put = Udf.mapPutEntries(handle, pairs, n);

        if (put != n / 2) {
            throw new UdfException("could not put entry " + put);
        }

        return this;
    }

    /** Every entry of a stored map, by position. */
    public UdfMap putAll(Value stored) {
        for (Cursor.Entry e : Udf.map(stored)) {
            if (e.key.isInt()) {
                put(e.key.payload, e.value);
            }
            else {
                put(e.key.asString(), e.value);
            }
        }

        return this;
    }
}

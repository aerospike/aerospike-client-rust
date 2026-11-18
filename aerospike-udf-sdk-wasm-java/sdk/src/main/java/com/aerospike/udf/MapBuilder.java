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

import java.util.List;
import java.util.Map;

import org.teavm.interop.Address;

/**
 * A map being built on the server, to be written to a bin, returned as the result, nested in another
 * collection or handed back from an event handler. {@link Udf#map()} starts one; every {@code put} is one
 * call to the host and returns the builder, so they chain; one that the host refuses throws
 * {@link UdfException}. A key is a string or an integer. The map is ordered by key, whatever order its
 * entries are put in, and lives until the call ends.
 *
 * <pre>
 * MapBuilder out = Udf.map().put("visits", visits).put("tags", tags);
 *
 * if (!out.containsKey("profile")) {
 *     out.put("profile", profile);
 * }
 * </pre>
 *
 * <p>It is a builder, not a collection: {@link #containsKey} and {@link #size} are all it answers about
 * what went in. To read a map, read the stored one ({@link MapValue}); to build from plain Java data,
 * {@link #putAll(Map)} takes a {@code java.util.Map}.
 */
public final class MapBuilder {
    private final int handle;

    MapBuilder(int handle) {
        this.handle = handle;
    }

    /** A new map with room for {@code capacity} entries, which it grows past. */
    static MapBuilder create(int capacity) {
        int h = Natives.mapNew(Math.max(capacity, 1));

        if (h <= 0) {
            throw new UdfException("could not build a map");
        }

        return new MapBuilder(h);
    }

    /** The host's handle on the map. */
    int handle() {
        return handle;
    }

    /** How many entries the map holds. */
    public int size() {
        return Natives.collSize(handle);
    }

    /** Whether the map holds nothing. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /** The map as a value, to put where a {@link Value} is taken. */
    public Value asValue() {
        return new Value().assign(Value.HANDLE, 0, handle);
    }

    /** Whether an entry has been put under {@code key}: one call to the host, which asks the map itself. */
    public boolean containsKey(String key) {
        byte[] k = Names.of(key);

        return Natives.mapHas(handle, Address.ofData(k), k.length) == 1;
    }

    /** Whether an entry has been put under the integer {@code key}. */
    public boolean containsKey(long key) {
        return Natives.mapHasI(handle, key) == 1;
    }

    private MapBuilder check(int rc, String key) {
        if (rc != LowLevel.OK) {
            throw new UdfException("could not put " + key + " (" + rc + ")");
        }

        return this;
    }

    // Every put that has no native of its own goes as one entry of the batch call: the key and the value
    // as two 16-byte values in Slots.PAIR. The key is written last, since it may be an address.

    private MapBuilder entry(String key) {
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        if (Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE) != 1) {
            throw new UdfException("could not put " + key);
        }

        return this;
    }

    private MapBuilder entry(long key) {
        Slots.integer(Slots.PAIR, 0, key);

        if (Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE) != 1) {
            throw new UdfException("could not put " + key);
        }

        return this;
    }

    //==========================================================
    // String keys.
    //

    /** An integer under a string key. */
    public MapBuilder put(String key, long value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrInt(handle, Address.ofData(k), k.length, value), key);
    }

    /** A double under a string key. */
    public MapBuilder put(String key, double value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return entry(key);
    }

    /** A boolean under a string key. */
    public MapBuilder put(String key, boolean value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.BOOL, 0, value ? 1 : 0);
        return entry(key);
    }

    /** A string under a string key. */
    public MapBuilder put(String key, String value) {
        byte[] v = LowLevel.bytes(value);
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrStr(handle, Address.ofData(k), k.length, Address.ofData(v), v.length), key);
    }

    /** A blob under a string key. */
    public MapBuilder put(String key, byte[] value) {
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_BLOB, value, value.length);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        if (Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE) != 1) {
            throw new UdfException("could not put " + key);
        }

        return this;
    }

    /**
     * A value of any kind under a string key: a scalar, a string or blob of the guest's, a stored value by
     * position (so a nested document goes in exactly as it is stored), a built collection.
     */
    public MapBuilder put(String key, Value value) {
        byte[] k = Names.of(key);

        if (value.kind() == Value.ABSENT) {
            throw new UdfException("could not put an absent value under " + key);
        }

        // A built collection goes by the native that shares it, as put(String, MapBuilder) does.
        if (value.kind() == Value.HANDLE) {
            return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, (int) value.payload()), key);
        }

        // Both slots are written after the key's bytes exist, so no address is taken before an allocation.
        Slots.value(Slots.PAIR, Slots.SIZE, value);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        if (Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE) != 1) {
            throw new UdfException("could not put " + key);
        }

        return this;
    }

    /** A built map, nested under a string key. */
    public MapBuilder put(String key, MapBuilder value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, value.handle), key);
    }

    /** A built list, nested under a string key. */
    public MapBuilder put(String key, ListBuilder value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, value.handle()), key);
    }

    /** A {@code java.util.List} as a nested list under a string key. */
    public MapBuilder put(String key, List<?> value) {
        return put(key, ListBuilder.create(value.size()).addAll(value));
    }

    /** A {@code java.util.Map} as a nested map under a string key. */
    public MapBuilder put(String key, Map<?, ?> value) {
        return put(key, create(value.size()).putAll(value));
    }

    /** A nil entry: in a record an event handler answers with, "no such bin". */
    public MapBuilder putNull(String key) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.NIL, 0, 0);
        return entry(key);
    }

    /** A GeoJSON document under a string key, kept as that kind rather than a string. */
    public MapBuilder putGeoJson(String key, String doc) {
        byte[] d = LowLevel.bytes(doc);
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_GEOJSON, d, d.length);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        if (Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE) != 1) {
            throw new UdfException("could not put " + key);
        }

        return this;
    }

    //==========================================================
    // Integer keys.
    //

    /** An integer under an integer key. */
    public MapBuilder put(long key, long value) {
        Slots.integer(Slots.PAIR, Slots.SIZE, value);
        return entry(key);
    }

    /** A double under an integer key. */
    public MapBuilder put(long key, double value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return entry(key);
    }

    /** A boolean under an integer key. */
    public MapBuilder put(long key, boolean value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.BOOL, 0, value ? 1 : 0);
        return entry(key);
    }

    /** A string under an integer key. */
    public MapBuilder put(long key, String value) {
        byte[] v = LowLevel.bytes(value);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_STR, v, v.length);
        return entry(key);
    }

    /** A blob under an integer key. */
    public MapBuilder put(long key, byte[] value) {
        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_BLOB, value, value.length);
        return entry(key);
    }

    /** A value of any kind under an integer key, the kinds as {@link #put(String, Value)} takes them. */
    public MapBuilder put(long key, Value value) {
        if (value.kind() == Value.ABSENT) {
            throw new UdfException("could not put an absent value under " + key);
        }

        if (value.kind() == Value.HANDLE) {
            return nested(key, (int) value.payload());
        }

        Slots.value(Slots.PAIR, Slots.SIZE, value);
        return entry(key);
    }

    /** A built collection under an integer key, by the native that shares it: the key goes as a handle too. */
    private MapBuilder nested(long key, int value) {
        return check(Natives.mapPut(handle, Natives.valInt(key), value), Long.toString(key));
    }

    /** A built map, nested under an integer key. */
    public MapBuilder put(long key, MapBuilder value) {
        return nested(key, value.handle);
    }

    /** A built list, nested under an integer key. */
    public MapBuilder put(long key, ListBuilder value) {
        return nested(key, value.handle());
    }

    /** A {@code java.util.List} as a nested list under an integer key. */
    public MapBuilder put(long key, List<?> value) {
        return put(key, ListBuilder.create(value.size()).addAll(value));
    }

    /** A {@code java.util.Map} as a nested map under an integer key. */
    public MapBuilder put(long key, Map<?, ?> value) {
        return put(key, create(value.size()).putAll(value));
    }

    /** A nil entry under an integer key. */
    public MapBuilder putNull(long key) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.NIL, 0, 0);
        return entry(key);
    }

    //==========================================================
    // Many at once.
    //

    /** Every entry of a stored map, each value by position. */
    public MapBuilder putAll(MapValue stored) {
        for (MapValue.Entry e : stored) {
            Value key = e.getKey();

            if (key.isInt()) {
                put(key.asLong(), e.getValue());
            }
            else {
                put(key.asString(), e.getValue());
            }
        }

        return this;
    }

    /**
     * Every entry of a {@code java.util.Map}: a key is a {@code String} or an integer, a value a
     * {@code Long}, {@code Integer}, {@code Double}, {@code Boolean}, {@code String}, {@code byte[]},
     * {@link Value}, a builder, a nested {@code java.util} list or map, or {@code null} for nil. Anything
     * else throws {@link UdfException}.
     */
    public MapBuilder putAll(Map<?, ?> entries) {
        for (Map.Entry<?, ?> e : entries.entrySet()) {
            Convert.put(this, e.getKey(), e.getValue());
        }

        return this;
    }
}

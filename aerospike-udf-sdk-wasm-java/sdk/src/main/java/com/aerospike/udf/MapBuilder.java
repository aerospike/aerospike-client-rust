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
 * entries are put in, and is given back by {@link #close}, or when the call ends.
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
 *
 * <p>Putting under a key the map already has replaces that entry. A key is not a bin name and may be of
 * any length; none is cut. The exception is the map of bins an event handler answers with
 * ({@link Resolution#record}), whose keys are bin names.
 *
 * <p>A builder holds a handle on the host until {@link #close} gives it back (try-with-resources does), or the
 * call ends, and a {@code put} takes none, whatever the key and the value are: a loop may put as many entries
 * as the server lets a call build. A call is bounded by its memory on the server, not by a count of handles,
 * as {@link ListBuilder} says.
 * A {@code put} throws, with the status {@link LowLevel#REFUSED}, when the call has built as much on the
 * server as it is allowed, when the value would nest deeper than 64 levels, and when a collection would end
 * up inside itself.
 */
public final class MapBuilder implements AutoCloseable {
    private int handle;

    MapBuilder(int handle) {
        this.handle = handle;
    }

    /** A new map with room for {@code capacity} entries, which it grows past. */
    static MapBuilder create(int capacity) {
        int h = Natives.mapNew(Math.max(capacity, 1));

        if (h <= 0) {
            // The host gives no reason; the one it has is a refusal: the call's memory is spent.
            throw new UdfException(LowLevel.REFUSED, "could not build a map: the call has spent the memory it"
                    + " may hold on the server");
        }

        return new MapBuilder(h);
    }

    /** The host's handle on the map; 0 once closed. */
    int handle() {
        return handle;
    }

    /**
     * Gives the map's handle back to the host. What the map was set into, added to, put into or
     * returned as keeps it, since the host holds its own reference to what it is handed; so close a builder
     * once it has been handed over, or use it in a try-with-resources block. One not closed is given back when
     * the call ends. Closing twice does nothing. After a close an {@code put} throws {@link UdfException}, and
     * {@link #asValue} names nothing.
     *
     * <p>Not before the call is done with it: a builder given to {@link Resolution#record} is read when the
     * handler has returned, so it must not be closed in the handler.
     */
    @Override
    public void close() {
        if (handle > 0) {
            Natives.cursorClose(handle);
            handle = 0;
        }
    }

    /** How many entries the map holds: keys, not puts. One call to the host. */
    public int size() {
        return Natives.collSize(handle);
    }

    /** Whether the map holds nothing. One call to the host. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * The map as a {@link Value} of kind {@link Value#HANDLE}, to put where a value is taken:
     * {@link Record#set(String, Value)}, another builder, {@link Udf#result}. It stands for this same map,
     * not a copy, and cannot be read back: {@link Value#asMap} throws on it.
     */
    public Value asValue() {
        return new Value().assign(Value.HANDLE, 0, handle);
    }

    /**
     * Whether an entry has been put under {@code key}, a nil entry included: one call to the host, which asks
     * the map itself.
     */
    public boolean containsKey(String key) {
        byte[] k = Names.of(key);

        return Natives.mapHas(handle, Address.ofData(k), k.length) == 1;
    }

    /**
     * Whether an entry has been put under the integer {@code key}, a nil entry included. The integer 1 and
     * the string "1" are different keys.
     */
    public boolean containsKey(long key) {
        return Natives.mapHasI(handle, key) == 1;
    }

    private MapBuilder check(int rc, String key) {
        if (rc != LowLevel.OK) {
            throw new UdfException(rc < 0 ? rc : LowLevel.ERR, "could not put " + key + ": " + ListBuilder.why(rc));
        }

        return this;
    }

    // Every put that has no native of its own goes as one entry of the batch call: the key and the value
    // as two 16-byte values in Slots.PAIR, by value, which takes none of the call's handles. The key is
    // written last, since it may be an address. The key's text is made only for a message.

    private MapBuilder entry(String key) {
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        int n = Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        return n == 1 ? this : check(Reads.one(n), key);
    }

    private MapBuilder entry(long key) {
        Slots.integer(Slots.PAIR, 0, key);

        int n = Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        return n == 1 ? this : check(Reads.one(n), Long.toString(key));
    }

    //==========================================================
    // String keys.
    //

    /** Puts an integer under a string key, replacing any entry the key had, and returns this builder. */
    public MapBuilder put(String key, long value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrInt(handle, Address.ofData(k), k.length, value), key);
    }

    /**
     * Puts a double under a string key, replacing any entry the key had. A whole-number double stays a
     * double.
     */
    public MapBuilder put(String key, double value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return entry(key);
    }

    /** Puts a boolean under a string key, replacing any entry the key had; stored as a boolean, not 0 or 1. */
    public MapBuilder put(String key, boolean value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.BOOL, 0, value ? 1 : 0);
        return entry(key);
    }

    /**
     * Puts a string under a string key, replacing any entry the key had; encoded as UTF-8 and copied by the
     * host. {@code value} must not be null: nil is {@link #putNil(String)}.
     */
    public MapBuilder put(String key, String value) {
        byte[] v = LowLevel.bytes(value);
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrStr(handle, Address.ofData(k), k.length, Address.ofData(v), v.length), key);
    }

    /** Puts a blob holding a copy of all of {@code value} under a string key, replacing any entry the key had. */
    public MapBuilder put(String key, byte[] value) {
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_BLOB, value, value.length);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        int n = Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        return n == 1 ? this : check(Reads.one(n), key);
    }

    /**
     * Puts a value of any kind under a string key: a scalar, a string or blob of the guest's, a stored value
     * by position (so a nested document goes in exactly as it is stored, copied by the server), a built
     * collection. A nil value makes a nil entry. Throws {@link UdfException} for an absent value.
     */
    public MapBuilder put(String key, Value value) {
        byte[] k = Names.of(key);

        if (value.kind() == Value.ABSENT) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "could not put an absent value under " + key);
        }

        // A built collection goes by the native that shares it, as put(String, MapBuilder) does.
        if (value.kind() == Value.HANDLE) {
            return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, (int) value.payload()), key);
        }

        // Both slots are written after the key's bytes exist, so no address is taken before an allocation.
        Slots.value(Slots.PAIR, Slots.SIZE, value);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        int n = Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        return n == 1 ? this : check(Reads.one(n), key);
    }

    /**
     * Puts a built map, nested, under a string key. The nested builder is not used up, and this map holds the
     * same map rather than a copy of it. Throws {@link UdfException} past 64 levels of nesting, and for a map
     * put into itself.
     */
    public MapBuilder put(String key, MapBuilder value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, value.handle), key);
    }

    /**
     * Puts a built list, nested, under a string key. The nested builder is not used up, and the map holds the
     * same list rather than a copy of it. Throws {@link UdfException} past 64 levels of nesting.
     */
    public MapBuilder put(String key, ListBuilder value) {
        byte[] k = Names.of(key);
        return check(Natives.mapPutStrHandle(handle, Address.ofData(k), k.length, value.handle()), key);
    }

    /**
     * Puts a {@code java.util.List} as a nested list under a string key, its elements as
     * {@link ListBuilder#addAll} takes them; an element of another type throws {@link UdfException}.
     */
    public MapBuilder put(String key, List<?> value) {
        try (ListBuilder nested = ListBuilder.create(value.size())) {
            return put(key, nested.addAll(value));
        }
    }

    /**
     * Puts a {@code java.util.Map} as a nested map under a string key, its entries as {@link #putAll(Map)}
     * takes them; a key or value of another type throws {@link UdfException}.
     */
    public MapBuilder put(String key, Map<?, ?> value) {
        try (MapBuilder nested = create(value.size())) {
            return put(key, nested.putAll(value));
        }
    }

    /**
     * Puts a nil entry under a string key: the key is in the map and its value is nil. In the record an event
     * handler answers with, that means "no such bin": the bin is deleted.
     */
    public MapBuilder putNil(String key) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.NIL, 0, 0);
        return entry(key);
    }

    /**
     * Puts a GeoJSON document under a string key from its text, kept as that kind rather than a string. The
     * text is not checked here.
     */
    public MapBuilder putGeoJson(String key, String doc) {
        byte[] d = LowLevel.bytes(doc);
        byte[] k = Names.of(key);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_GEOJSON, d, d.length);
        Slots.guest(Slots.PAIR, 0, Value.GUEST_STR, k, k.length);

        int n = Natives.mapPutEntries(handle, Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        return n == 1 ? this : check(Reads.one(n), key);
    }

    //==========================================================
    // Integer keys.
    //

    /** Puts an integer under an integer key, replacing any entry the key had, and returns this builder. */
    public MapBuilder put(long key, long value) {
        Slots.integer(Slots.PAIR, Slots.SIZE, value);
        return entry(key);
    }

    /**
     * Puts a double under an integer key, replacing any entry the key had. A whole-number double stays a
     * double.
     */
    public MapBuilder put(long key, double value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return entry(key);
    }

    /** Puts a boolean under an integer key, replacing any entry the key had; stored as a boolean, not 0 or 1. */
    public MapBuilder put(long key, boolean value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.BOOL, 0, value ? 1 : 0);
        return entry(key);
    }

    /**
     * Puts a string under an integer key, replacing any entry the key had; encoded as UTF-8 and copied by the
     * host. {@code value} must not be null: nil is {@link #putNil(long)}.
     */
    public MapBuilder put(long key, String value) {
        byte[] v = LowLevel.bytes(value);

        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_STR, v, v.length);
        return entry(key);
    }

    /**
     * Puts a blob holding a copy of all of {@code value} under an integer key, replacing any entry the key
     * had.
     */
    public MapBuilder put(long key, byte[] value) {
        Slots.guest(Slots.PAIR, Slots.SIZE, Value.GUEST_BLOB, value, value.length);
        return entry(key);
    }

    /**
     * Puts a value of any kind under an integer key, the kinds as {@link #put(String, Value)} takes them.
     * Throws {@link UdfException} for an absent value.
     */
    public MapBuilder put(long key, Value value) {
        if (value.kind() == Value.ABSENT) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "could not put an absent value under " + key);
        }

        if (value.kind() == Value.HANDLE) {
            return nested(key, (int) value.payload());
        }

        Slots.value(Slots.PAIR, Slots.SIZE, value);
        return entry(key);
    }

    /**
     * A built collection under an integer key: one entry of the batch call, the key by value and the
     * collection by its handle, so the key takes no handle of its own.
     */
    private MapBuilder nested(long key, int value) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.HANDLE, 0, value);
        return entry(key);
    }

    /**
     * Puts a built map, nested, under an integer key. The nested builder is not used up, and this map holds
     * the same map rather than a copy of it. Throws {@link UdfException} past 64 levels of nesting, and for a
     * map put into itself.
     */
    public MapBuilder put(long key, MapBuilder value) {
        return nested(key, value.handle);
    }

    /**
     * Puts a built list, nested, under an integer key. The nested builder is not used up, and the map holds
     * the same list rather than a copy of it. Throws {@link UdfException} past 64 levels of nesting.
     */
    public MapBuilder put(long key, ListBuilder value) {
        return nested(key, value.handle());
    }

    /**
     * Puts a {@code java.util.List} as a nested list under an integer key, its elements as
     * {@link ListBuilder#addAll} takes them; an element of another type throws {@link UdfException}.
     */
    public MapBuilder put(long key, List<?> value) {
        try (ListBuilder nested = ListBuilder.create(value.size())) {
            return put(key, nested.addAll(value));
        }
    }

    /**
     * Puts a {@code java.util.Map} as a nested map under an integer key, its entries as {@link #putAll(Map)}
     * takes them; a key or value of another type throws {@link UdfException}.
     */
    public MapBuilder put(long key, Map<?, ?> value) {
        try (MapBuilder nested = create(value.size())) {
            return put(key, nested.putAll(value));
        }
    }

    /** Puts a nil entry under an integer key: the key is in the map and its value is nil. */
    public MapBuilder putNil(long key) {
        Slots.scalar(Slots.PAIR, Slots.SIZE, Value.NIL, 0, 0);
        return entry(key);
    }

    //==========================================================
    // Many at once.
    //

    /**
     * Puts every entry of a stored map, each value by position, replacing entries whose keys this map already
     * has. Throws {@link UdfException} for a key that is neither a string nor an integer.
     */
    public MapBuilder putAll(MapValue stored) {
        // forEach gives the walk's cursor back when a put throws part way.
        stored.forEach(e -> {
            Value key = e.getKey();

            if (key.isInt()) {
                put(key.asInt(), e.getValue());
            }
            else {
                put(key.asString(), e.getValue());
            }
        });

        return this;
    }

    /**
     * Puts every entry of a {@code java.util.Map}: a key is a {@code String} or an integer ({@code Long},
     * {@code Integer}, {@code Short}, {@code Byte}), a value a {@code Long}, {@code Integer}, {@code Double},
     * {@code Float}, {@code Boolean}, {@code String}, {@code byte[]}, {@link Value}, a stored list or map, a
     * builder, a nested {@code java.util} list or map, or {@code null} for nil. Anything else throws
     * {@link UdfException} naming its class, with the entries before it already put.
     *
     * <pre>
     * Map&lt;String, Object&gt; profile = new HashMap&lt;&gt;();
     * profile.put("name", "alice");
     * profile.put("age", 42);
     *
     * MapBuilder out = Udf.map().putAll(profile).put("tags", Udf.list().add("a").add("b"));
     *
     * rec.set("profile", out);
     * </pre>
     */
    public MapBuilder putAll(Map<?, ?> entries) {
        for (Map.Entry<?, ?> e : entries.entrySet()) {
            Convert.put(this, e.getKey(), e.getValue());
        }

        return this;
    }
}

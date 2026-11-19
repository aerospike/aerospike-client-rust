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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.teavm.interop.Address;

/**
 * A map the server holds - a map bin, an element of another collection, an argument - to read: an entry
 * by its key, which is a string or an integer, and every entry in the order the server keeps them. It is
 * read where it is, so nothing is copied until an entry is asked for.
 *
 * <pre>
 * MapValue device = rec.getMap("device");
 *
 * long version = device.getInt("version", 0);     // 0 when there is no such key
 * String id = device.getString("deviceId");        // null when there is no such key
 *
 * for (MapValue.Entry e : rec.getMap("counts")) {
 *     total += e.getValue().asInt();
 * }
 * </pre>
 *
 * <p>{@link #get(String)} answers {@code null} for a key the map does not have, as a {@code java.util.Map}
 * does, and so does every typed reader, for a missing key and for a nil entry alike; one with a default
 * answers the default there. Either throws {@link UdfException} naming the key, with the status
 * {@link LowLevel#TYPE_MISMATCH}, when the entry is of another kind: nothing is widened or converted. A key
 * may be of any length. A map is read-only: to change one, build another ({@link MapBuilder}) and
 * write that. {@link #toMap} copies the whole map out as a {@code java.util.Map}.
 */
public final class MapValue implements Iterable<MapValue.Entry> {
    /** One entry of a stored map: a key and a value, both read-only and both safe to keep. */
    public static final class Entry implements Map.Entry<Value, Value> {
        private final Value key;
        private final Value value;

        Entry(Value key, Value value) {
            this.key = key;
            this.value = value;
        }

        /** The entry's key: a string or an integer, as a rule. */
        @Override
        public Value getKey() {
            return key;
        }

        /**
         * The entry's value, of any kind; a nil entry is a {@link Value} whose {@link Value#isNil} is true,
         * not null.
         */
        @Override
        public Value getValue() {
            return value;
        }

        /** A stored map is read-only: this throws {@link UnsupportedOperationException}. */
        @Override
        public Value setValue(Value v) {
            throw new UnsupportedOperationException("a stored map is read-only");
        }
    }

    private static final Value SCRATCH = new Value();

    private final Value map;

    /** A view of {@code map}, which is a stored map, or of nothing at all, which reads as empty. */
    MapValue(Value map) {
        this.map = map;
    }

    /** How many entries the map has; known without a call to the host. */
    public int size() {
        return map.kind() == Value.MAP ? map.length() : 0;
    }

    /** Whether the map has no entries. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * The map as the one value it is, to hand on: into a bin, a builder or the result, by position, so the
     * server copies it without its entries passing through the guest. A view of nothing (a list of names the
     * server did not send, say) answers an empty map, built for the purpose; never an absent value, which no
     * bin, builder or result takes. That empty map is one per call, shared by every view of nothing and made
     * when first asked for, so asking holds at most one handle in a call; the host gives it back when the call
     * ends.
     */
    public Value asValue() {
        if (map.kind() == Value.MAP) {
            return map;
        }

        return Maps.emptyMap();
    }

    //==========================================================
    // Looking a key up. The status is what Reads.state reads.
    //

    private int find(String key, Value out) {
        if (map.kind() != Value.MAP) {
            return LowLevel.NOT_FOUND;
        }

        byte[] k = Names.of(key);
        int rc = Natives.collGet(map.src(), map.at(), Address.ofData(k), k.length, Address.ofData(Slots.ONE),
                Slots.SIZE);

        if (rc == LowLevel.OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    private int find(long key, Value out) {
        if (map.kind() != Value.MAP) {
            return LowLevel.NOT_FOUND;
        }

        int rc = Natives.collGetI(map.src(), map.at(), key, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == LowLevel.OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /** The scratch value when the entry is of {@code kind}; null for a missing key or a nil entry. */
    private Value typed(int rc, String key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state == Reads.NONE) {
            return null;
        }

        throw Reads.problem(state, "key ", key, what);
    }

    /** As {@link #typed(int, String, int, String)}; the key's text is made only for a message. */
    private Value typed(int rc, long key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state == Reads.NONE) {
            return null;
        }

        throw Reads.problem(state, "key ", Long.toString(key), what);
    }

    //==========================================================
    // String keys.
    //

    /**
     * The value under {@code key}, as a fresh {@link Value}, or null when the map has no such key. An entry that is
     * there and holds nil is not null: it is a value whose {@link Value#isNil} is true. One call to the host,
     * which walks the entries to the key, so a lookup is linear in the map's size.
     */
    public Value get(String key) {
        Value out = new Value();

        return find(key, out) == LowLevel.OK ? out : null;
    }

    /** Whether the map has the key; true also for an entry that holds nil. One call to the host. */
    public boolean containsKey(String key) {
        return find(key, SCRATCH) == LowLevel.OK;
    }

    /**
     * The integer under {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the key when the entry is of another kind.
     */
    public Long getInt(String key) {
        Value v = typed(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? null : v.payload();
    }

    /**
     * The integer under {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    public long getInt(String key, long dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? dflt : v.payload();
    }

    /**
     * The double under {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is of another kind; an integer is
     * not widened.
     */
    public Double getDouble(String key) {
        Value v = typed(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? null : v.doubleBits();
    }

    /**
     * The double under {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind, an integer included, still throws {@link UdfException}.
     */
    public double getDouble(String key, double dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? dflt : v.doubleBits();
    }

    /**
     * The boolean under {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is of another kind.
     */
    public Boolean getBool(String key) {
        Value v = typed(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? null : v.payload() != 0;
    }

    /**
     * The boolean under {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind still throws {@link UdfException}.
     */
    public boolean getBool(String key, boolean dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? dflt : v.payload() != 0;
    }

    /**
     * The text of the string under {@code key}, decoded into a new {@code String}, or null when the map has no such
     * key or the entry is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is
     * not a string.
     */
    public String getString(String key) {
        return getString(key, null);
    }

    /**
     * The text of the string under {@code key}, or {@code dflt} (which may be null) when the map has no such key or
     * the entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public String getString(String key, String dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.STR, "a string");

        return v == null ? dflt : v.asString();
    }

    /**
     * The bytes of the blob under {@code key}, copied into a new array, or null when the map has no such key or the
     * entry is nil. It is strict: throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry
     * is anything but a blob, a string included; {@link #getBytes(String)} takes those.
     */
    public byte[] getBlob(String key) {
        return getBlob(key, null);
    }

    /**
     * The bytes of the blob under {@code key}, copied into a new array, or {@code dflt} (which may be null) when the
     * map has no such key or the entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public byte[] getBlob(String key, byte[] dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.BLOB, "a blob");

        return v == null ? dflt : v.asBytes();
    }

    /**
     * The raw bytes of the string, blob, GeoJSON or HLL under {@code key}, copied into a new array, or null when the
     * map has no such key or the entry is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH})
     * when the entry is a number, a boolean, a list or a map.
     */
    public byte[] getBytes(String key) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT || !SCRATCH.hasBytes()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "key ", key,
                    "a string, blob, GeoJSON or HLL");
        }

        return SCRATCH.asBytes();
    }

    /**
     * The list under {@code key}, as a view: nothing is copied. Null when the map has no such key or the entry is
     * nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is not a list.
     */
    public ListValue getList(String key) {
        return getList(key, null);
    }

    /**
     * The list under {@code key} as a view, or {@code dflt} (which may be null) when the map has no such key or the
     * entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public ListValue getList(String key, ListValue dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.LIST, "a list");

        return v == null ? dflt : new ListValue(v.copy());
    }

    /**
     * The map under {@code key}, as another view: nothing is copied. Null when the map has no such key or the entry
     * is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is not a map.
     */
    public MapValue getMap(String key) {
        return getMap(key, null);
    }

    /**
     * The map under {@code key} as a view, or {@code dflt} (which may be null) when the map has no such key or the
     * entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public MapValue getMap(String key, MapValue dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.MAP, "a map");

        return v == null ? dflt : new MapValue(v.copy());
    }

    //==========================================================
    // Integer keys.
    //

    /**
     * The value under the integer {@code key}, as a fresh {@link Value}, or null when the map has no such key. An entry that is
     * there and holds nil is not null: it is a value whose {@link Value#isNil} is true. One call to the host,
     * which walks the entries to the key, so a lookup is linear in the map's size.
     */
    public Value get(long key) {
        Value out = new Value();

        return find(key, out) == LowLevel.OK ? out : null;
    }

    /** Whether the map has the key; true also for an entry that holds nil. One call to the host. */
    public boolean containsKey(long key) {
        return find(key, SCRATCH) == LowLevel.OK;
    }

    /**
     * The integer under the integer {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the key when the entry is of another kind.
     */
    public Long getInt(long key) {
        Value v = typed(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? null : v.payload();
    }

    /**
     * The integer under the integer {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    public long getInt(long key, long dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? dflt : v.payload();
    }

    /**
     * The double under the integer {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is of another kind; an integer is
     * not widened.
     */
    public Double getDouble(long key) {
        Value v = typed(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? null : v.doubleBits();
    }

    /**
     * The double under the integer {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind, an integer included, still throws {@link UdfException}.
     */
    public double getDouble(long key, double dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? dflt : v.doubleBits();
    }

    /**
     * The boolean under the integer {@code key}, or null when the map has no such key or the entry is nil. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is of another kind.
     */
    public Boolean getBool(long key) {
        Value v = typed(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? null : v.payload() != 0;
    }

    /**
     * The boolean under the integer {@code key}, or {@code dflt} when the map has no such key or the entry is nil. An entry of
     * another kind still throws {@link UdfException}.
     */
    public boolean getBool(long key, boolean dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? dflt : v.payload() != 0;
    }

    /**
     * The text of the string under the integer {@code key}, decoded into a new {@code String}, or null when the map has no such
     * key or the entry is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is
     * not a string.
     */
    public String getString(long key) {
        return getString(key, null);
    }

    /**
     * The text of the string under the integer {@code key}, or {@code dflt} (which may be null) when the map has no such key or
     * the entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public String getString(long key, String dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.STR, "a string");

        return v == null ? dflt : v.asString();
    }

    /**
     * The bytes of the blob under the integer {@code key}, copied into a new array, or null when the map has no such key or the
     * entry is nil. It is strict: throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry
     * is anything but a blob, a string included; {@link #getBytes(long)} takes those.
     */
    public byte[] getBlob(long key) {
        return getBlob(key, null);
    }

    /**
     * The bytes of the blob under the integer {@code key}, copied into a new array, or {@code dflt} (which may be null) when the
     * map has no such key or the entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public byte[] getBlob(long key, byte[] dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.BLOB, "a blob");

        return v == null ? dflt : v.asBytes();
    }

    /**
     * The raw bytes of the string, blob, GeoJSON or HLL under the integer {@code key}, copied into a new array, or null when the
     * map has no such key or the entry is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH})
     * when the entry is a number, a boolean, a list or a map.
     */
    public byte[] getBytes(long key) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT || !SCRATCH.hasBytes()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "key ", Long.toString(key),
                    "a string, blob, GeoJSON or HLL");
        }

        return SCRATCH.asBytes();
    }

    /**
     * The list under the integer {@code key}, as a view: nothing is copied. Null when the map has no such key or the entry is
     * nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is not a list.
     */
    public ListValue getList(long key) {
        return getList(key, null);
    }

    /**
     * The list under the integer {@code key} as a view, or {@code dflt} (which may be null) when the map has no such key or the
     * entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public ListValue getList(long key, ListValue dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.LIST, "a list");

        return v == null ? dflt : new ListValue(v.copy());
    }

    /**
     * The map under the integer {@code key}, as another view: nothing is copied. Null when the map has no such key or the entry
     * is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the entry is not a map.
     */
    public MapValue getMap(long key) {
        return getMap(key, null);
    }

    /**
     * The map under the integer {@code key} as a view, or {@code dflt} (which may be null) when the map has no such key or the
     * entry is nil. An entry of another kind still throws {@link UdfException}.
     */
    public MapValue getMap(long key, MapValue dflt) {
        Value v = typed(find(key, SCRATCH), key, Value.MAP, "a map");

        return v == null ? dflt : new MapValue(v.copy());
    }

    //==========================================================
    // The whole map.
    //

    /**
     * The entries in the order the server keeps them, a fresh key and value each, so an entry can be kept
     * after the loop moves on. The loop may be left early. The entries come from the host 64 at a time, so a
     * map costs a call per 64 entries, not one per entry. {@code next()} past the end throws
     * {@code NoSuchElementException}.
     *
     * <p><b>The iterator holds no cursor.</b> It reads every key's and value's position before it hands out
     * the first entry, and gives the host's cursor back then, so a loop left early leaves nothing open. The
     * positions cost 32 bytes of the guest's memory per entry, for as long as the iterator is reachable; the
     * keys and values themselves are not copied. {@link #forEach} reads a window at a time instead, closing the
     * cursor however it ends.
     */
    @Override
    public Iterator<Entry> iterator() {
        final Walk walk = Walk.whole(map.kind() == Value.MAP ? map : Maps.NOTHING);

        return new Iterator<Entry>() {
            @Override
            public boolean hasNext() {
                return walk.more();
            }

            @Override
            public Entry next() {
                if (!walk.more()) {
                    throw new NoSuchElementException();
                }

                Value key = walk.next(new Value());

                if (!walk.more()) {
                    throw new UdfException(LowLevel.ERR, "the map ended on a key");
                }

                return new Entry(key, walk.next(new Value()));
            }
        };
    }

    /**
     * Every entry in the server's order, as the iterator gives them, with the host's cursor given back however
     * the walk ends, an exception from {@code action} included.
     */
    @Override
    public void forEach(java.util.function.Consumer<? super Entry> action) {
        Walk walk = new Walk(map.kind() == Value.MAP ? map : Maps.NOTHING);

        try {
            while (walk.more()) {
                Value key = walk.next(new Value());

                if (!walk.more()) {
                    throw new UdfException(LowLevel.ERR, "the map ended on a key");
                }

                action.accept(new Entry(key, walk.next(new Value())));
            }
        }
        finally {
            walk.close();
        }
    }

    /**
     * The whole map copied out as a {@code java.util.Map} in the server's order, each key and value as
     * {@link Value#toObject} gives it: a string key is a {@code String}, an integer key a {@code Long}, and a
     * nested list or map is copied too. It costs an object per key and value; the views read the same data
     * without.
     */
    public Map<Object, Object> toMap() {
        return Convert.toMap(map);
    }
}

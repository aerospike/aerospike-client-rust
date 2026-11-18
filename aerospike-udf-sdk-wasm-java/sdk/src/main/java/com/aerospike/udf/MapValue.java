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
 * long version = device.getLong("version", 0);     // 0 when there is no such key
 * String id = device.getString("deviceId");        // throws when there is no such key
 *
 * for (MapValue.Entry e : rec.getMap("counts")) {
 *     total += e.getValue().asLong();
 * }
 * </pre>
 *
 * <p>{@link #get(String)} answers {@code null} for a key the map does not have, as a {@code java.util.Map}
 * does. A typed reader without a default throws {@link UdfException} naming the key when the map has no
 * such key; one with a default answers the default for a missing key or a nil entry; either throws when
 * the entry is of another kind. A map is read-only: to change one, build another ({@link MapBuilder}) and
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

        /** The entry's value. */
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

    /** How many entries the map has. */
    public int size() {
        return map.kind() == Value.MAP ? map.length() : 0;
    }

    /** Whether the map has no entries. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /** The map as the one value it is, to hand on: into a bin, a builder or the result, by position. */
    public Value asValue() {
        return map;
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

    /** The scratch value when the entry is there and of {@code kind}; throws when it is missing or another. */
    private Value required(int rc, String key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", key, what);
        }

        return SCRATCH;
    }

    /** As {@link #required}, but null for a missing key or a nil entry. */
    private Value optional(int rc, String key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", key, what);
        }

        return SCRATCH;
    }

    private static Value bytesOf(Value v, String key) {
        if (v != null && (!v.isStored() || v.isList() || v.isMap())) {
            throw Reads.problem(Reads.WRONG, "key ", key, "a string or blob");
        }

        return v;
    }

    //==========================================================
    // String keys.
    //

    /** The value under {@code key}, or null when the map has no such key. */
    public Value get(String key) {
        Value out = new Value();

        return find(key, out) == LowLevel.OK ? out : null;
    }

    /** Whether the map has {@code key}. */
    public boolean containsKey(String key) {
        return find(key, SCRATCH) == LowLevel.OK;
    }

    /** The integer under {@code key}; throws {@link UdfException} when there is none or it is not one. */
    public long getLong(String key) {
        return required(find(key, SCRATCH), key, Value.INT, "an int").payload();
    }

    /** The integer under {@code key}, or {@code dflt} when there is none; throws when it is of another kind. */
    public long getLong(String key, long dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? dflt : v.payload();
    }

    /** The double under {@code key}; an integer is not widened. */
    public double getDouble(String key) {
        return required(find(key, SCRATCH), key, Value.DOUBLE, "a double").doubleBits();
    }

    /** The double under {@code key}, or {@code dflt} when there is none. */
    public double getDouble(String key, double dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? dflt : v.doubleBits();
    }

    /** The boolean under {@code key}. */
    public boolean getBoolean(String key) {
        return required(find(key, SCRATCH), key, Value.BOOL, "a bool").payload() != 0;
    }

    /** The boolean under {@code key}, or {@code dflt} when there is none. */
    public boolean getBoolean(String key, boolean dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? dflt : v.payload() != 0;
    }

    /** The text of the string under {@code key}, copied out. */
    public String getString(String key) {
        return required(find(key, SCRATCH), key, Value.STR, "a string").asString();
    }

    /** The text of the string under {@code key}, or {@code dflt} when there is none. */
    public String getString(String key, String dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.STR, "a string");

        return v == null ? dflt : v.asString();
    }

    /** The bytes of the string, blob, GeoJSON or HLL under {@code key}, copied out. */
    public byte[] getBytes(String key) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", key, "a string or blob");
        }

        return bytesOf(SCRATCH, key).asBytes();
    }

    /** The bytes under {@code key}, or {@code dflt} when there is none. */
    public byte[] getBytes(String key, byte[] dflt) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state == Reads.NONE) {
            return dflt;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", key, "a string or blob");
        }

        return bytesOf(SCRATCH, key).asBytes();
    }

    /** The list under {@code key}. */
    public ListValue getList(String key) {
        return new ListValue(required(find(key, SCRATCH), key, Value.LIST, "a list").copy());
    }

    /** The list under {@code key}, or {@code dflt} (which may be null) when there is none. */
    public ListValue getList(String key, ListValue dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.LIST, "a list");

        return v == null ? dflt : new ListValue(v.copy());
    }

    /** The map under {@code key}. */
    public MapValue getMap(String key) {
        return new MapValue(required(find(key, SCRATCH), key, Value.MAP, "a map").copy());
    }

    /** The map under {@code key}, or {@code dflt} (which may be null) when there is none. */
    public MapValue getMap(String key, MapValue dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.MAP, "a map");

        return v == null ? dflt : new MapValue(v.copy());
    }

    //==========================================================
    // Integer keys. The key's text is made only for a message.
    //

    private Value required(int rc, long key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", Long.toString(key), what);
        }

        return SCRATCH;
    }

    private Value optional(int rc, long key, int kind, String what) {
        if (rc == LowLevel.OK && SCRATCH.kind() == kind) {
            return SCRATCH;
        }

        int state = Reads.state(rc, SCRATCH, kind);

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", Long.toString(key), what);
        }

        return SCRATCH;
    }

    /** The value under the integer {@code key}, or null when the map has no such key. */
    public Value get(long key) {
        Value out = new Value();

        return find(key, out) == LowLevel.OK ? out : null;
    }

    /** Whether the map has the integer {@code key}. */
    public boolean containsKey(long key) {
        return find(key, SCRATCH) == LowLevel.OK;
    }

    /** The integer under the integer {@code key}; throws when there is none or it is not one. */
    public long getLong(long key) {
        return required(find(key, SCRATCH), key, Value.INT, "an int").payload();
    }

    /** The integer under the integer {@code key}, or {@code dflt} when there is none. */
    public long getLong(long key, long dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.INT, "an int");

        return v == null ? dflt : v.payload();
    }

    /** The double under the integer {@code key}; an integer is not widened. */
    public double getDouble(long key) {
        return required(find(key, SCRATCH), key, Value.DOUBLE, "a double").doubleBits();
    }

    /** The double under the integer {@code key}, or {@code dflt} when there is none. */
    public double getDouble(long key, double dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.DOUBLE, "a double");

        return v == null ? dflt : v.doubleBits();
    }

    /** The boolean under the integer {@code key}. */
    public boolean getBoolean(long key) {
        return required(find(key, SCRATCH), key, Value.BOOL, "a bool").payload() != 0;
    }

    /** The boolean under the integer {@code key}, or {@code dflt} when there is none. */
    public boolean getBoolean(long key, boolean dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.BOOL, "a bool");

        return v == null ? dflt : v.payload() != 0;
    }

    /** The text of the string under the integer {@code key}, copied out. */
    public String getString(long key) {
        return required(find(key, SCRATCH), key, Value.STR, "a string").asString();
    }

    /** The text of the string under the integer {@code key}, or {@code dflt} when there is none. */
    public String getString(long key, String dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.STR, "a string");

        return v == null ? dflt : v.asString();
    }

    /** The bytes of the string, blob, GeoJSON or HLL under the integer {@code key}, copied out. */
    public byte[] getBytes(long key) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", Long.toString(key), "a string or blob");
        }

        return bytesOf(SCRATCH, Long.toString(key)).asBytes();
    }

    /** The bytes under the integer {@code key}, or {@code dflt} when there is none. */
    public byte[] getBytes(long key, byte[] dflt) {
        int state = Reads.state(find(key, SCRATCH), SCRATCH, SCRATCH.kind());

        if (state == Reads.NONE) {
            return dflt;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "key ", Long.toString(key), "a string or blob");
        }

        return bytesOf(SCRATCH, Long.toString(key)).asBytes();
    }

    /** The list under the integer {@code key}. */
    public ListValue getList(long key) {
        return new ListValue(required(find(key, SCRATCH), key, Value.LIST, "a list").copy());
    }

    /** The list under the integer {@code key}, or {@code dflt} (which may be null) when there is none. */
    public ListValue getList(long key, ListValue dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.LIST, "a list");

        return v == null ? dflt : new ListValue(v.copy());
    }

    /** The map under the integer {@code key}. */
    public MapValue getMap(long key) {
        return new MapValue(required(find(key, SCRATCH), key, Value.MAP, "a map").copy());
    }

    /** The map under the integer {@code key}, or {@code dflt} (which may be null) when there is none. */
    public MapValue getMap(long key, MapValue dflt) {
        Value v = optional(find(key, SCRATCH), key, Value.MAP, "a map");

        return v == null ? dflt : new MapValue(v.copy());
    }

    //==========================================================
    // The whole map.
    //

    /**
     * The entries in the order the server keeps them, a fresh key and value each, so an entry can be kept
     * after the loop moves on. The loop may be left early.
     */
    @Override
    public Iterator<Entry> iterator() {
        final Walk walk = new Walk(map.kind() == Value.MAP ? map : Maps.NOTHING);

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
                    throw new UdfException("the map ended on a key");
                }

                return new Entry(key, walk.next(new Value()));
            }
        };
    }

    /**
     * The whole map copied out as a {@code java.util.Map} in the server's order, each key and value as
     * {@link Value#toObject} gives it; a nested list or map is copied too.
     */
    public Map<Object, Object> toMap() {
        return Convert.toMap(map);
    }
}

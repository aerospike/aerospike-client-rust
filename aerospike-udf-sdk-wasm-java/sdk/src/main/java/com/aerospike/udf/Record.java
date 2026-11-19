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
 * The record a call is about. A {@code @Function} method and a stream's {@code record} method take it as
 * their first parameter, and everything a UDF does to the record is a method of it:
 * reading a bin, writing one, the record's metadata, saving and deleting.
 *
 * <pre>
 * {@literal @}Function("visit")
 * static long visit(Record rec, String page) {
 *     long visits = rec.getInt("visits", 0) + 1;
 *
 *     rec.set("visits", visits);
 *     rec.set("last", page);
 *     rec.save();
 *
 *     return visits;
 * }
 * </pre>
 *
 * <p><b>Reading.</b> {@link #get} gives a bin of any kind, or null; {@link #bin} gives it or throws. A typed
 * reader ({@link #getInt(String)}, {@link #getString(String)}, {@link #getList(String)} and the rest) has
 * three outcomes: null for a bin that is not there, the value for a bin of that kind, and an
 * {@link UdfException} naming the bin, with the status {@link LowLevel#TYPE_MISMATCH}, for a bin of another
 * kind. Given a default, a reader answers it for an absent bin and still throws for a wrong kind. Nothing
 * is widened: an integer bin is not a double, and a string bin is not a blob. See {@link RecordReader}.
 *
 * <p><b>Writing.</b> {@code set} stages a bin, {@link #remove} stages its removal, and {@link #save}
 * writes what is staged, creating the record when it does not exist. {@link #delete} deletes the record.
 * Nothing is written until {@code save}, with one exception the server makes: {@link #setInts} and
 * {@link #setDoubles} take effect at once. A call that fails writes nothing at all: what a {@code save} that
 * succeeded had written, and what {@code setInts} or {@code setDoubles} had, is undone when the call then
 * fails.
 *
 * <p><b>A key with no record.</b> {@link #exists} is false; {@link #generation}, {@link #lastUpdateTime},
 * {@link #size} and {@link #binCount} are 0; there is no TTL; and reading creates nothing.
 * {@link #setInts}, {@link #setDoubles} and {@link #delete} are refused there, so they throw, and create
 * nothing. A {@code save} with a bin staged creates the record; a {@code save} with no bin staged (only a
 * {@link #remove} or a {@link #setTtl}) creates nothing, and the call fails with "key not found".
 *
 * <p><b>One type, writable or not.</b> A stream's records are read-only, and so is the record of a call
 * the server runs as a read. A write there throws {@link UdfException} saying the record is read-only: it
 * is refused by the server when it is made, not by the type.
 *
 * <p>Bins are addressed by name. The record keeps the encoded form of the names it has seen, so a name
 * passed as a string literal is encoded once, not once per call, and a reader allocates nothing.
 *
 * <p>A bin name is 1 to 15 bytes of UTF-8 with no zero byte in it. A read under any other name finds no bin;
 * a write under one throws.
 *
 * <p>Reading a string, blob, GeoJSON, HLL, list or map bin takes one of the call's 256 handles each time it
 * is read, and the handle is held until the call returns (in a stream, until the next record). Read such a
 * bin once and keep what came back, not once per turn of a loop: when the handles run out the read throws
 * {@link UdfException} saying the bin could not be read. An integer, double or boolean bin costs none.
 *
 * <p>A {@link #save} or {@link #delete} that fails ends every list, map and blob value read from the record
 * before it: a read through one then throws. Read the bin again.
 */
public final class Record implements RecordReader {
    /** There is one record per call, and one call at a time: the generated exports hand this one out. */
    private static final Record CURRENT = new Record();

    /** What the typed readers read into, so that they allocate nothing. */
    private final Value v = new Value();

    private Record() {
    }

    static Record current() {
        return CURRENT;
    }

    //==========================================================
    // Reading.
    //

    private int read(String name) {
        byte[] n = Names.of(name);
        int rc = Natives.binValue(Address.ofData(n), n.length, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == LowLevel.OK) {
            v.fill(Slots.ONE, 0);
        }

        return rc;
    }

    // Every typed reader tests for the hit itself, in line, before anything else: a call costs more on this
    // backend than the comparison it would save repeating, and the hit is the read a hot loop makes. What
    // is left for a read that did not hit is here.

    /** Returns for an absent bin, which is nothing or a default; throws for any other miss. */
    private void absent(int rc, String name, int kind, String what) {
        int state = Reads.state(rc, v, kind);

        if (state != Reads.NONE) {
            throw Reads.problem(state, "bin ", name, what);
        }
    }

    /** The scratch value when the bin is there and of {@code kind}; null when absent; throws for another. */
    private Value typed(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        absent(rc, name, kind, what);

        return null;
    }

    /** The scratch value when the bin has bytes of any kind; null when absent; throws for another. */
    private Value bytes(String name) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT || !v.hasBytes()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "bin ", name,
                    "a string, blob, GeoJSON or HLL");
        }

        return v;
    }

    /**
     * The bin as a {@link Value} of whatever kind it is, or null when the record has no such bin, which
     * includes a record that does not exist yet. The value is a fresh one that is not changed afterwards: a
     * scalar is in it whole, and the text of a string or the elements of a list are read from the server when
     * its {@code as*} reader asks for them. Throws {@link UdfException} only when the host could not read the
     * bin.
     */
    @Override
    public Value get(String name) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "bin ", name, "readable");
        }

        return v.copy();
    }

    /**
     * As {@link #get}, for a bin that has to be there: throws {@link UdfException} reading
     * {@code bin <name> is absent}, with the status {@link LowLevel#NOT_FOUND}, when the record has no such
     * bin, and never answers null. With the value's own reader it is the read that insists:
     * {@code rec.bin("balance").asInt()}.
     */
    @Override
    public Value bin(String name) {
        Value x = get(name);

        if (x == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "readable");
        }

        return x;
    }

    /**
     * Whether the record has the bin, of any kind. It is a read of the bin like any other, so when the value
     * is wanted next, {@link #get} or a typed reader finds out and reads it in one call.
     */
    @Override
    public boolean has(String name) {
        return Reads.state(read(name), v, v.kind()) == Reads.HIT;
    }

    /**
     * An integer bin, or null when the record has no such bin. Throws {@link UdfException} naming the bin
     * ({@code bin n is not an int}, status {@link LowLevel#TYPE_MISMATCH}) when it holds another kind; a
     * double is not truncated to one. The answer is boxed so that it can be null: assigning it to a
     * {@code long} without looking fails the call with a {@code NullPointerException} for an absent bin, so
     * give a default ({@link #getInt(String, long)}) or insist ({@code rec.bin(name).asInt()}) where no test
     * for null is wanted.
     *
     * <pre>
     * Long seen = rec.getInt("seen");
     *
     * if (seen == null) {
     *     // the bin is not there
     * }
     * </pre>
     */
    @Override
    public Long getInt(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        absent(rc, name, Value.INT, "an int");

        return null;
    }

    /**
     * An integer bin, or {@code dflt} when the record has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}): the default stands in for a missing bin, not for
     * a wrong one. Nothing is boxed, so this is the reader for a hot path.
     *
     * <pre>
     * long visits = rec.getInt("visits", 0);      // 0 for a new record
     * String plan = rec.getString("plan", "free");
     * MapValue limits = rec.getMap("limits");     // null when there is no such bin
     *
     * if (limits != null) {
     *     visits = Math.min(visits, limits.getInt(plan, Long.MAX_VALUE));
     * }
     * </pre>
     */
    @Override
    public long getInt(String name, long dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        absent(rc, name, Value.INT, "an int");

        return dflt;
    }

    /**
     * A double bin, or null when the record has no such bin. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it holds another kind, and an integer bin is
     * another kind: it is not widened.
     */
    @Override
    public Double getDouble(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        absent(rc, name, Value.DOUBLE, "a double");

        return null;
    }

    /**
     * A double bin, or {@code dflt} when the record has no such bin. A bin of another kind, an integer
     * included, still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public double getDouble(String name, double dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        absent(rc, name, Value.DOUBLE, "a double");

        return dflt;
    }

    /**
     * A boolean bin, or null when the record has no such bin. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it holds another kind; an integer 0 or 1 is not a
     * boolean.
     */
    @Override
    public Boolean getBool(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        absent(rc, name, Value.BOOL, "a bool");

        return null;
    }

    /**
     * A boolean bin, or {@code dflt} when the record has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public boolean getBool(String name, boolean dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        absent(rc, name, Value.BOOL, "a bool");

        return dflt;
    }

    /**
     * The text of a string bin, decoded from UTF-8 into a new {@code String} on every call, or null when the
     * record has no such bin. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin
     * when it is not a string. A GeoJSON bin is not a string here: {@link #getBytes(String)} gives its text
     * as bytes.
     */
    @Override
    public String getString(String name) {
        return getString(name, null);
    }

    /**
     * The text of a string bin, or {@code dflt} (which may be null) when the record has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public String getString(String name, String dflt) {
        Value x = typed(name, Value.STR, "a string");

        return x == null ? dflt : x.asString();
    }

    /**
     * The bytes of a blob bin, copied into a new array on every call, or null when the record has no such
     * bin. It is strict: throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it
     * holds anything but a blob, a string, a GeoJSON document and an HLL included. {@link #getBytes(String)}
     * is the reader that takes those.
     */
    @Override
    public byte[] getBlob(String name) {
        return getBlob(name, null);
    }

    /**
     * The bytes of a blob bin, copied into a new array, or {@code dflt} (which may be null, and is returned as
     * it is, not copied) when the record has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public byte[] getBlob(String name, byte[] dflt) {
        Value x = typed(name, Value.BLOB, "a blob");

        return x == null ? dflt : x.asBytes();
    }

    /**
     * The raw bytes of a string, blob, GeoJSON or HLL bin, copied into a new array on every call, or null
     * when the record has no such bin: the UTF-8 bytes of a string or a document, the bytes of a blob, an HLL
     * as the server stores it. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin
     * when it is a number, a boolean, a list or a map.
     */
    @Override
    public byte[] getBytes(String name) {
        Value x = bytes(name);

        return x == null ? null : x.asBytes();
    }

    /**
     * A list bin, as a view that reads it where the server holds it: nothing is copied until an element is
     * asked for. Null when the record has no such bin, which is not an empty list: an empty list bin is a
     * view whose {@link ListValue#isEmpty} is true. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it is not a list. The view can be kept for the
     * rest of the call; a save that fails ends it.
     */
    @Override
    public ListValue getList(String name) {
        return getList(name, null);
    }

    /**
     * A list bin as a view, or {@code dflt} (which may be null) when the record has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public ListValue getList(String name, ListValue dflt) {
        Value x = typed(name, Value.LIST, "a list");

        return x == null ? dflt : new ListValue(x.copy());
    }

    /**
     * A map bin, as a view that looks keys up and iterates where the server holds it: nothing is copied until
     * an entry is asked for. Null when the record has no such bin, which is not an empty map. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it is not a map. The view can
     * be kept for the rest of the call; a save that fails ends it.
     */
    @Override
    public MapValue getMap(String name) {
        return getMap(name, null);
    }

    /**
     * A map bin as a view, or {@code dflt} (which may be null) when the record has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public MapValue getMap(String name, MapValue dflt) {
        Value x = typed(name, Value.MAP, "a map");

        return x == null ? dflt : new MapValue(x.copy());
    }

    /**
     * A blob bin of little-endian 32-bit floats straight into {@code out}, element for element, with no copy
     * in between: how a vector is read. Returns how many floats there were, which is the blob's length in
     * bytes divided by 4; the elements of {@code out} past that count are left as they were. A count has no
     * null, so unlike the typed readers this one throws {@link UdfException} for an absent bin
     * ({@link LowLevel#NOT_FOUND}) as well as for one that is not a blob ({@link LowLevel#TYPE_MISMATCH}; a
     * string is not); when the bin holds more floats than {@code out} has room for nothing is copied and the
     * status is at or below {@link LowLevel#NEEDED_BASE}.
     */
    public int getFloats(String name, float[] out) {
        Value x = typed(name, Value.BLOB, "a blob");

        if (x == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "a blob");
        }

        return x.readFloats(out);
    }

    /**
     * How many bins the stored record has; 0 when the record does not exist. A staged bin is not counted
     * until {@link #save} writes it.
     */
    public long binCount() {
        return Natives.numbins();
    }

    //==========================================================
    // Writing: staged until save().
    //

    private void wrote(int rc, String name) {
        if (rc != LowLevel.OK) {
            throw Reads.refused(rc, "could not write bin " + name);
        }
    }

    /**
     * Stages an integer bin. The value replaces whatever the bin held, of any kind, when {@link #save} writes
     * it; until then the stored record is unchanged, though a read of the bin later in this call answers the
     * staged value. Throws {@link UdfException}, as every {@code set} does, when the record cannot be written
     * here (a stream's record, a call the server runs as a read; the status is {@link LowLevel#REFUSED}) or
     * when {@code name} is not 1 to 15 bytes ({@link LowLevel#ERR}).
     */
    public void set(String name, long value) {
        byte[] n = Names.of(name);
        wrote(Natives.setInt(Address.ofData(n), n.length, value), name);
    }

    /**
     * Stages a double bin, to be written by {@link #save}. Throws {@link UdfException} when the record cannot
     * be written or the name is not a bin name.
     */
    public void set(String name, double value) {
        byte[] n = Names.of(name);
        wrote(Natives.setDouble(Address.ofData(n), n.length, value), name);
    }

    /**
     * Stages a boolean bin, to be written by {@link #save}. It is stored as a boolean, not as the integer 0
     * or 1. Throws {@link UdfException} when the record cannot be written or the name is not a bin name.
     */
    public void set(String name, boolean value) {
        byte[] n = Names.of(name);
        wrote(Natives.setBool(Address.ofData(n), n.length, value ? 1 : 0), name);
    }

    /**
     * Stages a string bin, to be written by {@link #save}; the text is encoded as UTF-8 and copied by the
     * server, so the string is not needed afterwards. Throws {@link UdfException} when the record cannot be
     * written or the name is not a bin name.
     */
    public void set(String name, String value) {
        byte[] b = LowLevel.bytes(value);
        byte[] n = Names.of(name);
        wrote(Natives.setStr(Address.ofData(n), n.length, Address.ofData(b), b.length), name);
    }

    /**
     * Stages a blob bin holding a copy of all of {@code value}, to be written by {@link #save}. Throws
     * {@link UdfException} when the record cannot be written, and when the bin now holds an HLL: plain bytes
     * over one would silently stop it being an HLL, so an HLL is written back as the {@link Value} it was
     * read as.
     */
    public void set(String name, byte[] value) {
        byte[] n = Names.of(name);
        wrote(Natives.setBlob(Address.ofData(n), n.length, Address.ofData(value), value.length), name);
    }

    /**
     * Stages a bin from a value of any kind: a scalar, a string or blob of the guest's, a built collection,
     * or a stored value, which is written by position and keeps its kind (a GeoJSON stays one, an HLL stays
     * one). A nil value removes the bin, since a bin cannot hold nil. Throws {@link UdfException} for an
     * absent value (one never filled; the status is {@link LowLevel#TYPE_MISMATCH}), for a stored value read
     * before a save that failed, and when the record cannot be written.
     */
    public void set(String name, Value value) {
        byte[] n = Names.of(name);

        switch (value.kind()) {
        case Value.INT:
            wrote(Natives.setInt(Address.ofData(n), n.length, value.payload()), name);
            break;
        case Value.DOUBLE:
            wrote(Natives.setDouble(Address.ofData(n), n.length, value.doubleBits()), name);
            break;
        case Value.BOOL:
            wrote(Natives.setBool(Address.ofData(n), n.length, value.payload() != 0 ? 1 : 0), name);
            break;
        case Value.NIL:
            wrote(Natives.setNil(Address.ofData(n), n.length), name);
            break;
        case Value.GUEST_STR:
            wrote(Natives.setStr(Address.ofData(n), n.length, Address.ofData(value.guestBytes()), value.guestLen()),
                    name);
            break;
        case Value.GUEST_BLOB:
            wrote(Natives.setBlob(Address.ofData(n), n.length, Address.ofData(value.guestBytes()), value.guestLen()),
                    name);
            break;
        case Value.GUEST_GEOJSON:
            wrote(Natives.setGeoJson(Address.ofData(n), n.length, Address.ofData(value.guestBytes()),
                    value.guestLen()), name);
            break;
        case Value.HANDLE:
            wrote(Natives.setHandle(Address.ofData(n), n.length, (int) value.payload()), name);
            break;
        case Value.ABSENT:
            throw new UdfException(LowLevel.TYPE_MISMATCH, "could not write bin " + name + ": the value is absent");
        default:
            wrote(Natives.setAt(Address.ofData(n), n.length, value.src(), value.at()), name);
            break;
        }
    }

    /**
     * Stages a list bin from a built list; an empty builder makes an empty list. The builder is not used up:
     * the same list can also go into another bin, into a collection or be the result. Throws
     * {@link UdfException} when the record cannot be written.
     */
    public void set(String name, ListBuilder value) {
        byte[] n = Names.of(name);
        wrote(Natives.setHandle(Address.ofData(n), n.length, value.handle()), name);
    }

    /**
     * Stages a map bin from a built map. The builder is not used up: the same map can also go into another
     * bin, into a collection or be the result. Throws {@link UdfException} when the record cannot be written.
     */
    public void set(String name, MapBuilder value) {
        byte[] n = Names.of(name);
        wrote(Natives.setHandle(Address.ofData(n), n.length, value.handle()), name);
    }

    /**
     * Stages a bin from a stored list, by position: the server copies the list from where it is (a bin of
     * this record, an argument, an element of another collection) without its elements passing through the
     * guest.
     */
    public void set(String name, ListValue value) {
        set(name, value.asValue());
    }

    /**
     * Stages a bin from a stored map, by position: the server copies the map from where it is without its
     * entries passing through the guest.
     */
    public void set(String name, MapValue value) {
        set(name, value.asValue());
    }

    /**
     * Stages a list bin from a {@code java.util.List}, its elements as {@link ListBuilder#addAll} takes them:
     * the boxed numbers, {@code Boolean}, {@code String}, {@code byte[]}, {@link Value}, nested lists and
     * maps, and null for nil. Throws {@link UdfException} for an element of any other type.
     */
    public void set(String name, List<?> value) {
        set(name, ListBuilder.create(value.size()).addAll(value));
    }

    /**
     * Stages a map bin from a {@code java.util.Map}, its entries as {@link MapBuilder#putAll(Map)} takes
     * them: a key is a {@code String} or an integer, a value anything a list element may be. Throws
     * {@link UdfException} for any other key or value type.
     */
    public void set(String name, Map<?, ?> value) {
        set(name, MapBuilder.create(value.size()).putAll(value));
    }

    /**
     * Stages a GeoJSON bin from the document's text, kept as the geospatially indexable kind, not a string.
     * The text is not checked here: {@link #save} fails when it is not a document the server accepts.
     */
    public void setGeoJson(String name, String doc) {
        byte[] d = LowLevel.bytes(doc);
        byte[] n = Names.of(name);
        wrote(Natives.setGeoJson(Address.ofData(n), n.length, Address.ofData(d), d.length), name);
    }

    /**
     * Writes a list bin of integers straight from the array, in one call, with no value built per element.
     * Unlike the other writes it is not staged: it is applied when it is made and needs no {@link #save}, so
     * returning without saving does not undo it. A call that fails does: it writes nothing, this included.
     * Throws {@link UdfException} when the record cannot be written or the name is not a bin name, and on a
     * key with no record, where the server refuses it and creates nothing.
     */
    public void setInts(String name, long[] values) {
        byte[] n = Names.of(name);
        wrote(Natives.setListInts(Address.ofData(n), n.length, Address.ofData(values), values.length * 8), name);
    }

    /**
     * As {@link #setInts}, for a list of doubles: applied when it is made, with no {@link #save}, undone when
     * the call fails, and refused on a key with no record.
     */
    public void setDoubles(String name, double[] values) {
        byte[] n = Names.of(name);
        wrote(Natives.setListDoubles(Address.ofData(n), n.length, Address.ofData(values), values.length * 8), name);
    }

    /**
     * Stages the removal of a bin, carried out by {@link #save}; removing one the record does not have is not
     * an error. Throws {@link UdfException} when the record cannot be written or the name is not a bin name.
     */
    public void remove(String name) {
        byte[] n = Names.of(name);
        int rc = Natives.binRemove(Address.ofData(n), n.length);

        if (rc != LowLevel.OK) {
            throw Reads.refused(rc, "could not remove bin " + name);
        }
    }

    //==========================================================
    // The record itself.
    //

    /**
     * Whether the record exists: false for a key that has no record yet, which is where {@link #save} creates
     * one. It answers what it did when the call began for the whole call: a {@link #delete} is carried out
     * when the call returns.
     */
    public boolean exists() {
        return Natives.recExists() != 0;
    }

    /** The record's generation: how many times it has been written. 0 when the record does not exist. */
    @Override
    public long generation() {
        return Natives.gen();
    }

    /**
     * The record's remaining time to live, in seconds. 0 for a record that never expires, and for one that
     * does not exist.
     */
    @Override
    public long ttl() {
        return Natives.ttl();
    }

    /**
     * Sets the record's time to live, in seconds: 0 for the namespace default, -1 for never expire. It is the
     * TTL of the write, so it takes effect when {@link #save} writes the record and does nothing without one.
     * Throws {@link UdfException} when the record cannot take one, as a stream's cannot.
     */
    public void setTtl(int seconds) {
        int rc = Natives.setTtl(seconds);

        if (rc != LowLevel.OK) {
            throw Reads.refused(rc, "could not set the record's TTL");
        }
    }

    /**
     * When the record was last written, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z
     * (Unix milliseconds minus 1 262 304 000 000), which is how the server stores it; 0 when the record does
     * not exist. Not on the same scale as {@link Udf#now}, which counts from the Unix epoch.
     */
    @Override
    public long lastUpdateTime() {
        return Natives.lastUpdate();
    }

    /** The record's size as it is stored, in bytes; 0 when the record does not exist. */
    public long size() {
        return Natives.recordSize();
    }

    private static final byte[] TEXT = new byte[64];

    /** The name of the set the record is in, whatever its length, or null when it is in no set. */
    public String setName() {
        byte[] buf = TEXT;
        int n = Natives.setName(Address.ofData(buf), buf.length);

        // A name longer than the buffer: the status carries the size it needs.
        if (n <= LowLevel.NEEDED_BASE) {
            buf = new byte[-n - 16];
            n = Natives.setName(Address.ofData(buf), buf.length);
        }

        return n <= 0 ? null : LowLevel.string(buf, n);
    }

    /**
     * The record's user key - an integer, a string or a blob - or null when none was stored with the record,
     * which is the server's default: a client has to ask for the key to be sent with the write. The value is
     * a copy of the guest's own, so it stays good whatever is written afterwards. Throws {@link UdfException}
     * when a key that is there could not be read.
     */
    public Value key() {
        int kind = Natives.keyType();

        if (kind == Value.INT) {
            return Value.ofInt(Natives.keyInt());
        }

        if (kind != Value.STR && kind != Value.BLOB) {
            return null;
        }

        byte[] buf = new byte[TEXT.length];
        int n = Natives.keyBytes(Address.ofData(buf), buf.length);

        // A key longer than the first guess: the status carries the size it needs.
        if (n <= LowLevel.NEEDED_BASE) {
            buf = new byte[-n - 16];
            n = Natives.keyBytes(Address.ofData(buf), buf.length);
        }

        if (n < 0) {
            throw new UdfException(LowLevel.ERR, "the record's key could not be read");
        }

        return new Value().guest(kind == Value.STR ? Value.GUEST_STR : Value.GUEST_BLOB, buf, n);
    }

    /**
     * The record's digest: the 20 bytes the server knows the record by, in a new array. It is there whether
     * or not the user key was stored.
     */
    public byte[] digest() {
        byte[] d = new byte[20];

        if (Natives.digest(Address.ofData(d), d.length) != d.length) {
            throw new UdfException(LowLevel.ERR, "the record's digest could not be read");
        }

        return d;
    }

    /**
     * Writes the staged bins, creating the record when it does not exist. With nothing staged it is still a
     * write: the server treats it as a touch, so the generation and the last-update time move; on a key with
     * no record a save with no bin staged (nothing, or only a {@link #remove} or a {@link #setTtl}) makes
     * nothing, since a record must have a bin, and the call fails: the client is told the key was not found.
     * What is staged after a save needs another save. A save is not the last word: when the call fails after
     * one that succeeded, what it wrote is undone.
     *
     * <p>Throws {@link UdfException}, with the status {@link LowLevel#REFUSED}, when the record cannot be
     * written: a stream's record, a call the server runs as a read, a write the server turns down, such as a
     * GeoJSON bin whose text is not GeoJSON. After a
     * save that failed, a list, map or blob value read from the record before it can no longer be read.
     *
     * <pre>
     * if (!rec.exists()) {
     *     rec.set("created", Udf.now());
     * }
     *
     * rec.set("seen", Udf.now());
     * rec.save();                 // creates the record, or updates it
     * </pre>
     */
    public void save() {
        int rc = Natives.recExists() != 0 ? Natives.recUpdate() : Natives.recCreate();

        if (rc != LowLevel.OK) {
            throw Reads.refused(rc, "could not save the record");
        }
    }

    /**
     * Deletes the record, with whatever was staged; no {@link #save} is needed. The server carries it out
     * when the call returns, so {@link #exists} keeps its answer until then. Throws {@link UdfException} when
     * the record cannot be deleted ({@link LowLevel#REFUSED}), as a stream's cannot, and on a key with no
     * record, where the server refuses it.
     *
     * <pre>
     * {@literal @}Function("expire_if_empty")
     * static boolean expireIfEmpty(Record rec) {
     *     ListValue items = rec.getList("items");
     *
     *     if (items == null || items.isEmpty()) {
     *         rec.delete();
     *         return true;
     *     }
     *
     *     return false;
     * }
     * </pre>
     */
    public void delete() {
        int rc = Natives.recRemove();

        if (rc != LowLevel.OK) {
            throw Reads.refused(rc, "could not delete the record");
        }
    }
}

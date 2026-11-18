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
 *     long visits = rec.getLong("visits", 0) + 1;
 *
 *     rec.set("visits", visits);
 *     rec.set("last", page);
 *     rec.save();
 *
 *     return visits;
 * }
 * </pre>
 *
 * <p><b>Reading.</b> {@link #get} gives a bin of any kind, or null. The typed readers
 * ({@link #getLong(String)}, {@link #getString(String)}, {@link #getList(String)} and the rest) throw
 * {@link UdfException} naming the bin when it is absent or of another kind; given a default, they answer
 * it for an absent bin and still throw for a wrong kind. See {@link RecordReader}.
 *
 * <p><b>Writing.</b> {@code set} stages a bin, {@link #remove} stages its removal, and {@link #save}
 * writes what is staged, creating the record when it does not exist. {@link #delete} deletes the record.
 * Nothing is written until {@code save}, with one exception the server makes: {@link #setInts} and
 * {@link #setDoubles} take effect at once.
 *
 * <p><b>One type, writable or not.</b> A stream's records are read-only, and so is the record of a call
 * the server runs as a read. A write there throws {@link UdfException} saying the record is read-only: it
 * is refused by the server when it is made, not by the type.
 *
 * <p>Bins are addressed by name. The record keeps the encoded form of the names it has seen, so a name
 * passed as a string literal is encoded once, not once per call, and a reader allocates nothing.
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

    /** The exception for a bin that had to be there and of {@code kind}, and was not. */
    private UdfException missing(int rc, String name, int kind, String what) {
        return Reads.problem(Reads.state(rc, v, kind), "bin ", name, what);
    }

    /** Returns for an absent bin, which a default stands in for; throws for any other miss. */
    private void absent(int rc, String name, int kind, String what) {
        int state = Reads.state(rc, v, kind);

        if (state != Reads.NONE) {
            throw Reads.problem(state, "bin ", name, what);
        }
    }

    /** The scratch value when the bin is there and of {@code kind}; throws when it is absent or another. */
    private Value required(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        throw missing(rc, name, kind, what);
    }

    /** As {@link #required}, but null for an absent bin. */
    private Value optional(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        absent(rc, name, kind, what);

        return null;
    }

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

    @Override
    public boolean hasBin(String name) {
        return Reads.state(read(name), v, v.kind()) == Reads.HIT;
    }

    @Override
    public long getLong(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        throw missing(rc, name, Value.INT, "an int");
    }

    @Override
    public long getLong(String name, long dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        absent(rc, name, Value.INT, "an int");

        return dflt;
    }

    @Override
    public double getDouble(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        throw missing(rc, name, Value.DOUBLE, "a double");
    }

    @Override
    public double getDouble(String name, double dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        absent(rc, name, Value.DOUBLE, "a double");

        return dflt;
    }

    @Override
    public boolean getBoolean(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        throw missing(rc, name, Value.BOOL, "a bool");
    }

    @Override
    public boolean getBoolean(String name, boolean dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        absent(rc, name, Value.BOOL, "a bool");

        return dflt;
    }

    @Override
    public String getString(String name) {
        return required(name, Value.STR, "a string").asString();
    }

    @Override
    public String getString(String name, String dflt) {
        Value x = optional(name, Value.STR, "a string");

        return x == null ? dflt : x.asString();
    }

    @Override
    public byte[] getBytes(String name) {
        byte[] b = getBytes(name, null);

        if (b == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "a string or blob");
        }

        return b;
    }

    @Override
    public byte[] getBytes(String name, byte[] dflt) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return dflt;
        }

        if (state != Reads.HIT || !v.isStored() || v.isList() || v.isMap()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "bin ", name, "a string or blob");
        }

        return v.asBytes();
    }

    @Override
    public ListValue getList(String name) {
        return new ListValue(required(name, Value.LIST, "a list").copy());
    }

    @Override
    public ListValue getList(String name, ListValue dflt) {
        Value x = optional(name, Value.LIST, "a list");

        return x == null ? dflt : new ListValue(x.copy());
    }

    @Override
    public MapValue getMap(String name) {
        return new MapValue(required(name, Value.MAP, "a map").copy());
    }

    @Override
    public MapValue getMap(String name, MapValue dflt) {
        Value x = optional(name, Value.MAP, "a map");

        return x == null ? dflt : new MapValue(x.copy());
    }

    /**
     * A blob bin of little-endian 32-bit floats straight into {@code out}, element for element, with no
     * copy in between: how a vector is read. Returns how many floats there were; throws
     * {@link UdfException} when the bin is absent, is not a blob, or does not fit.
     */
    public int getFloats(String name, float[] out) {
        return required(name, Value.BLOB, "a blob").readFloats(out);
    }

    /** How many bins the record has. */
    public long binCount() {
        return Natives.numbins();
    }

    //==========================================================
    // Writing: staged until save().
    //

    private void wrote(int rc, String name) {
        if (rc != LowLevel.OK) {
            throw new UdfException("could not write bin " + name + ": " + Reads.why(rc));
        }
    }

    /** Stages an integer bin. Throws {@link UdfException} when the record cannot be written, as every write does. */
    public void set(String name, long value) {
        byte[] n = Names.of(name);
        wrote(Natives.setInt(Address.ofData(n), n.length, value), name);
    }

    /** Stages a double bin. */
    public void set(String name, double value) {
        byte[] n = Names.of(name);
        wrote(Natives.setDouble(Address.ofData(n), n.length, value), name);
    }

    /** Stages a boolean bin. */
    public void set(String name, boolean value) {
        byte[] n = Names.of(name);
        wrote(Natives.setBool(Address.ofData(n), n.length, value ? 1 : 0), name);
    }

    /** Stages a string bin, written as UTF-8. */
    public void set(String name, String value) {
        byte[] b = LowLevel.bytes(value);
        byte[] n = Names.of(name);
        wrote(Natives.setStr(Address.ofData(n), n.length, Address.ofData(b), b.length), name);
    }

    /** Stages a blob bin. */
    public void set(String name, byte[] value) {
        byte[] n = Names.of(name);
        wrote(Natives.setBlob(Address.ofData(n), n.length, Address.ofData(value), value.length), name);
    }

    /**
     * Stages a bin from a value of any kind: a scalar, a string or blob of the guest's, a built collection,
     * or a stored value, which is written by position and keeps its kind (a GeoJSON stays one). A nil value
     * removes the bin, since a bin cannot hold nil.
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
            throw new UdfException("could not write bin " + name + ": the value is absent");
        default:
            wrote(Natives.setAt(Address.ofData(n), n.length, value.src(), value.at()), name);
            break;
        }
    }

    /** Stages a bin from a built list. */
    public void set(String name, ListBuilder value) {
        byte[] n = Names.of(name);
        wrote(Natives.setHandle(Address.ofData(n), n.length, value.handle()), name);
    }

    /** Stages a bin from a built map. */
    public void set(String name, MapBuilder value) {
        byte[] n = Names.of(name);
        wrote(Natives.setHandle(Address.ofData(n), n.length, value.handle()), name);
    }

    /** Stages a bin from a stored list, by position: a copy of a list this or another record holds. */
    public void set(String name, ListValue value) {
        set(name, value.asValue());
    }

    /** Stages a bin from a stored map, by position. */
    public void set(String name, MapValue value) {
        set(name, value.asValue());
    }

    /** Stages a list bin from a {@code java.util.List}, its elements as {@link ListBuilder#addAll} takes them. */
    public void set(String name, List<?> value) {
        set(name, ListBuilder.create(value.size()).addAll(value));
    }

    /** Stages a map bin from a {@code java.util.Map}, its entries as {@link MapBuilder#putAll(Map)} takes them. */
    public void set(String name, Map<?, ?> value) {
        set(name, MapBuilder.create(value.size()).putAll(value));
    }

    /** Stages a GeoJSON bin from the document's text, kept as the geospatially indexable kind, not a string. */
    public void setGeoJson(String name, String doc) {
        byte[] d = LowLevel.bytes(doc);
        byte[] n = Names.of(name);
        wrote(Natives.setGeoJson(Address.ofData(n), n.length, Address.ofData(d), d.length), name);
    }

    /**
     * Writes a list bin of integers straight from the array, in one call, with no value built per element.
     * Unlike the other writes it takes effect at once, without {@link #save}.
     */
    public void setInts(String name, long[] values) {
        byte[] n = Names.of(name);
        wrote(Natives.setListInts(Address.ofData(n), n.length, Address.ofData(values), values.length * 8), name);
    }

    /** As {@link #setInts}, for a list of doubles. */
    public void setDoubles(String name, double[] values) {
        byte[] n = Names.of(name);
        wrote(Natives.setListDoubles(Address.ofData(n), n.length, Address.ofData(values), values.length * 8), name);
    }

    /** Stages the removal of a bin; removing one the record does not have is not an error. */
    public void remove(String name) {
        byte[] n = Names.of(name);
        int rc = Natives.binRemove(Address.ofData(n), n.length);

        if (rc != LowLevel.OK) {
            throw new UdfException("could not remove bin " + name + ": " + Reads.why(rc));
        }
    }

    //==========================================================
    // The record itself.
    //

    /**
     * Whether the record exists. It answers what it did when the call began for the whole call: a
     * {@link #delete} is carried out when the call returns.
     */
    public boolean exists() {
        return Natives.recExists() != 0;
    }

    @Override
    public long generation() {
        return Natives.gen();
    }

    @Override
    public long ttl() {
        return Natives.ttl();
    }

    /**
     * Sets the record's time to live, in seconds, for the next {@link #save}: 0 for the namespace default,
     * -1 for never expire. Throws {@link UdfException} when the host refuses it.
     */
    public void setTtl(int seconds) {
        int rc = Natives.setTtl(seconds);

        if (rc != LowLevel.OK) {
            throw new UdfException("could not set the record's TTL: " + Reads.why(rc));
        }
    }

    /**
     * When the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC), which is how the
     * server stores it. Not on the same scale as {@link Udf#now}, which counts from the Unix epoch.
     */
    @Override
    public long lastUpdateTime() {
        return Natives.lastUpdate();
    }

    /** The record's size on device, in bytes. */
    public long size() {
        return Natives.recordSize();
    }

    private static final byte[] TEXT = new byte[64];

    /** The record's set name, or null when it is in no set. */
    public String setName() {
        int n = Natives.setName(Address.ofData(TEXT), TEXT.length);

        return n <= 0 ? null : LowLevel.string(TEXT, n);
    }

    /**
     * The record's user key - an integer, a string or a blob - or null when none was stored with the
     * record, which is the server's default.
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
            throw new UdfException("the record's key could not be read");
        }

        return new Value().guest(kind == Value.STR ? Value.GUEST_STR : Value.GUEST_BLOB, buf, n);
    }

    /** The record's digest: the 20 bytes the server knows the record by. */
    public byte[] digest() {
        byte[] d = new byte[20];

        if (Natives.digest(Address.ofData(d), d.length) != d.length) {
            throw new UdfException("the record's digest could not be read");
        }

        return d;
    }

    /**
     * Writes the staged bins, creating the record when it does not exist. With nothing staged it is still
     * a write: the server treats it as a touch, so the generation and the last-update time move. Throws
     * {@link UdfException} when the record cannot be written.
     */
    public void save() {
        int rc = Natives.recExists() != 0 ? Natives.recUpdate() : Natives.recCreate();

        if (rc != LowLevel.OK) {
            throw new UdfException("could not save the record: " + Reads.why(rc));
        }
    }

    /**
     * Deletes the record, with whatever was staged; no {@link #save} is needed. The server carries it out
     * when the call returns. Throws {@link UdfException} when the record cannot be deleted.
     */
    public void delete() {
        int rc = Natives.recRemove();

        if (rc != LowLevel.OK) {
            throw new UdfException("could not delete the record: " + Reads.why(rc));
        }
    }
}

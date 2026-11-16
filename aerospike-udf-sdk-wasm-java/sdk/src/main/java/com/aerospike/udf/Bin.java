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
 * A bin of the record in hand, by name: reads as the type expected, writes of every kind, and the walk of
 * a collection bin. The name is encoded once, when the {@code Bin} is made, so a module keeps its bins as
 * constants ({@code static final Bin COUNTS = Bin.of("counts")}) and pays nothing per call; this is what
 * the {@code byte[]} forms of the low-level calls were for. A getter throws {@link UdfException} naming
 * the bin when it is absent or of another kind, unless given a default, which covers absent and nil; a
 * setter throws when the host refuses the write. Writes are staged until {@link Udf#commit}.
 *
 * <pre>
 * static final Bin TICKS = Bin.of("ticks");
 *
 * long n = TICKS.getLong(0) + 1;
 * TICKS.set(n);
 * Udf.commit();
 * </pre>
 */
public final class Bin {
    private final byte[] name;
    private final String text;
    private final Value v = new Value();

    private Bin(String text) {
        this.text = text;
        this.name = Udf.bytes(text);
    }

    /** A bin by name. The name is encoded here, once; nothing is read. */
    public static Bin of(String name) {
        return new Bin(name);
    }

    /** The bin's name. */
    public String name() {
        return text;
    }

    //==========================================================
    // Reading.
    //

    /** Whether the record has this bin. */
    public boolean exists() {
        return Udf.bin(name, v) == Udf.OK;
    }

    /** The bin as a fresh value, or null when the record has no such bin. */
    public Value get() {
        int rc = Udf.bin(name, v);

        if (rc == Udf.NOT_FOUND) {
            return null;
        }

        if (rc != Udf.OK) {
            throw new UdfException("bin " + text + " could not be read");
        }

        return new Value().set(v.kind, v.len, v.payload);
    }

    /** The bin into a reused value; false when absent. */
    public boolean read(Value out) {
        int rc = Udf.bin(name, out);

        if (rc != Udf.OK && rc != Udf.NOT_FOUND) {
            throw new UdfException("bin " + text + " could not be read");
        }

        return rc == Udf.OK;
    }

    private Value typed(int kind, String what) {
        int rc = Udf.bin(name, v);

        if (rc == Udf.NOT_FOUND) {
            throw new UdfException("bin " + text + " is absent");
        }

        if (rc != Udf.OK) {
            throw new UdfException("bin " + text + " could not be read");
        }

        if (v.kind != kind) {
            throw new UdfException("bin " + text + " is not " + what);
        }

        return v;
    }

    /** The reused value when the bin is present and not nil, null otherwise; throws on another kind. */
    private Value optional(int kind, String what) {
        int rc = Udf.bin(name, v);

        if (rc == Udf.NOT_FOUND || (rc == Udf.OK && v.kind == Value.NIL)) {
            return null;
        }

        if (rc != Udf.OK) {
            throw new UdfException("bin " + text + " could not be read");
        }

        if (v.kind != kind) {
            throw new UdfException("bin " + text + " is not " + what);
        }

        return v;
    }

    /** The bin as an integer; throws {@link UdfException} when it is absent or of another kind. */
    public long getLong() {
        return typed(Value.INT, "an int").payload;
    }

    /** The bin as an integer, or {@code dflt} when it is absent or nil; throws when it is of another kind. */
    public long getLong(long dflt) {
        Value x = optional(Value.INT, "an int");
        return x == null ? dflt : x.payload;
    }

    /**
     * The bin as a double; throws {@link UdfException} when it is absent or of another kind. An integer bin is
     * not widened.
     */
    public double getDouble() {
        return typed(Value.DOUBLE, "a double").asDouble();
    }

    /** The bin as a double, or {@code dflt} when it is absent or nil; throws when it is of another kind. */
    public double getDouble(double dflt) {
        Value x = optional(Value.DOUBLE, "a double");
        return x == null ? dflt : x.asDouble();
    }

    /** The bin as a boolean; throws {@link UdfException} when it is absent or of another kind. */
    public boolean getBoolean() {
        return typed(Value.BOOL, "a bool").payload != 0;
    }

    /** The bin as a boolean, or {@code dflt} when it is absent or nil; throws when it is of another kind. */
    public boolean getBoolean(boolean dflt) {
        Value x = optional(Value.BOOL, "a bool");
        return x == null ? dflt : x.payload != 0;
    }

    /**
     * The text of a string bin, copied out; throws {@link UdfException} when it is absent or of another kind.
     */
    public String getString() {
        return typed(Value.STR, "a string").asString();
    }

    /**
     * The text of a string bin, or {@code dflt} when it is absent or nil; throws when it is of another kind.
     */
    public String getString(String dflt) {
        Value x = optional(Value.STR, "a string");
        return x == null ? dflt : x.asString();
    }

    /** The bytes of a string, blob, GeoJSON or HLL bin. */
    public byte[] getBytes() {
        int rc = Udf.bin(name, v);

        if (rc == Udf.NOT_FOUND) {
            throw new UdfException("bin " + text + " is absent");
        }

        if (rc != Udf.OK || v.kind == Value.LIST || v.kind == Value.MAP || !v.isStored()) {
            throw new UdfException("bin " + text + " is not a string or blob");
        }

        return v.asBytes();
    }

    /** A blob of 32-bit floats straight into {@code out}, element for element; how many were copied. */
    public int getFloats(float[] out) {
        Value x = typed(Value.BLOB, "a blob");
        int n = Udf.floats(x, out);

        if (n < 0) {
            throw new UdfException("bin " + text + " does not fit the array");
        }

        return n / 4;
    }

    /** A walk over the list in this bin. */
    public Cursor<Value> asList() {
        return Cursor.list(typed(Value.LIST, "a list"));
    }

    /** A walk over the entries of the map in this bin. */
    public Cursor<Cursor.Entry> asMap() {
        return Cursor.map(typed(Value.MAP, "a map"));
    }

    //==========================================================
    // Writing: staged until Udf.commit().
    //

    private void check(int rc) {
        if (rc != Udf.OK) {
            throw new UdfException("could not write bin " + text + " (" + rc + ")");
        }
    }

    /** An integer. */
    public void set(long value) {
        check(Natives.setInt(Address.ofData(name), name.length, value));
    }

    /** A double. */
    public void set(double value) {
        check(Natives.setDouble(Address.ofData(name), name.length, value));
    }

    /** A boolean. */
    public void set(boolean value) {
        check(Natives.setBool(Address.ofData(name), name.length, value ? 1 : 0));
    }

    /** A string, written as UTF-8. */
    public void set(String value) {
        byte[] b = Udf.bytes(value);
        check(Natives.setStr(Address.ofData(name), name.length, Address.ofData(b), b.length));
    }

    /** A blob. */
    public void set(byte[] value) {
        check(Natives.setBlob(Address.ofData(name), name.length, Address.ofData(value), value.length));
    }

    /** A value read from the record, by position, keeping its kind; or a scalar, guest bytes or a handle. */
    public void set(Value value) {
        switch (value.kind) {
        case Value.INT:
            set(value.payload);
            break;
        case Value.DOUBLE:
            set(value.asDouble());
            break;
        case Value.BOOL:
            set(value.payload != 0);
            break;
        case Value.NIL:
            setNull();
            break;
        case Value.GUEST_STR:
            check(Natives.setStr(Address.ofData(name), name.length, Address.ofData(value.bytes), value.bytesLen));
            break;
        case Value.GUEST_BLOB:
            check(Natives.setBlob(Address.ofData(name), name.length, Address.ofData(value.bytes), value.bytesLen));
            break;
        case Value.GUEST_GEOJSON:
            check(Natives.setGeoJson(Address.ofData(name), name.length, Address.ofData(value.bytes), value.bytesLen));
            break;
        case Value.HANDLE:
            check(Natives.setHandle(Address.ofData(name), name.length, (int) value.payload));
            break;
        case Value.ABSENT:
            throw new UdfException("could not write an absent value to bin " + text);
        default:
            check(Natives.setAt(Address.ofData(name), name.length, value.src(), value.at()));
            break;
        }
    }

    /** A built map. */
    public void set(UdfMap value) {
        check(Natives.setHandle(Address.ofData(name), name.length, value.handle()));
    }

    /** A built list. */
    public void set(UdfList value) {
        check(Natives.setHandle(Address.ofData(name), name.length, value.handle()));
    }

    /** A GeoJSON document, kept as the geospatially indexable kind rather than a string. */
    public void setGeoJson(String doc) {
        byte[] d = Udf.bytes(doc);
        check(Natives.setGeoJson(Address.ofData(name), name.length, Address.ofData(d), d.length));
    }

    /** A nil bin. */
    public void setNull() {
        check(Natives.setNil(Address.ofData(name), name.length));
    }

    /** A list of integers from the array, in one import, no value built per element. */
    public void setInts(long[] values) {
        check(Natives.setListInts(Address.ofData(name), name.length, Address.ofData(values), values.length * 8));
    }

    /** A list of doubles from the array, in one import, no value built per element. */
    public void setDoubles(double[] values) {
        check(Natives.setListDoubles(Address.ofData(name), name.length, Address.ofData(values), values.length * 8));
    }

    /**
     * Removes the bin from the record, staged like a write; throws {@link UdfException} when the host refuses.
     */
    public void remove() {
        check(Natives.binRemove(Address.ofData(name), name.length));
    }
}

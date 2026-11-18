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

import java.nio.charset.StandardCharsets;

import org.teavm.interop.Address;

/**
 * One value of the record, of an argument list or of a collection: an integer, a double, a boolean, nil,
 * or a string, blob, GeoJSON, HLL, list or map that the server holds and this reads by position. A scalar
 * is here whole; a string's text or a list's elements stay on the server until they are asked for
 * ({@link #asString}, {@link #asList}), so handing a stored value on - into a bin, a builder, the result -
 * copies nothing through the guest.
 *
 * <p>The {@code is*} tests say what the value is, {@link #kind} gives the same as one of the constants of
 * this class, and each {@code as*} reader throws {@link UdfException} when the value is of another kind.
 * A value read from the record, from an argument or from an iteration is never changed afterwards, so it
 * can be kept for the rest of the call.
 *
 * <p>{@link #ofInt}, {@link #ofString} and the other factories make a value of the guest's own, for a
 * builder, a bin or a result.
 */
public final class Value {
    // Kinds, as the host hands them out.
    /** No value at all: a {@code Value} never filled, or a map entry that is not there. Distinct from nil. */
    public static final int ABSENT = -1;
    /** Nil. */
    public static final int NIL = 0;
    /** A boolean. */
    public static final int BOOL = 1;
    /** A 64-bit integer. */
    public static final int INT = 2;
    /** A double. */
    public static final int DOUBLE = 3;
    /** A string the host holds, read by position; the length is in bytes. */
    public static final int STR = 4;
    /** A blob the host holds, read by position; the length is in bytes. */
    public static final int BLOB = 5;
    /** A list the host holds, walked by position; the length is in elements. */
    public static final int LIST = 6;
    /** A map the host holds, walked or looked up by position; the length is in entries. */
    public static final int MAP = 7;
    /** A value of a kind the host has no other name for. */
    public static final int OTHER = 8;
    /** A GeoJSON document the host holds; its bytes are read like a string's. */
    public static final int GEOJSON = 9;
    /** A HyperLogLog value the host holds; its bytes are read like a blob's. */
    public static final int HLL = 10;

    // Kinds a guest uses only for a value it hands the host: bytes of its own by address, a built
    // collection by handle.
    /** A string of the guest's own, handed to the host by address. */
    public static final int GUEST_STR = 16;
    /** A blob of the guest's own, handed to the host by address. */
    public static final int GUEST_BLOB = 17;
    /** A GeoJSON document of the guest's own, handed to the host by address. */
    public static final int GUEST_GEOJSON = 18;
    /** A value built on the host, handed back by its handle. */
    public static final int HANDLE = 19;

    private int kind = ABSENT;
    /** Bytes for a string or blob, elements for a list, entries for a map. */
    private int len;
    /**
     * The scalar itself (a double as its bits), a handle, or {@code source << 32 | offset} for a value the
     * host holds.
     */
    private long payload;
    /** For the guest kinds: the bytes handed over, whose address is taken at the moment they are. */
    private byte[] bytes;
    private int bytesLen;

    /**
     * An absent value. Only the low-level tier needs one, as the slot its calls fill ({@link LowLevel});
     * everything else is handed its values.
     */
    public Value() {
    }

    //==========================================================
    // What the value is.
    //

    /** The kind, one of the constants of this class. */
    public int kind() {
        return kind;
    }

    /**
     * The length: bytes for a string, blob, GeoJSON or HLL, elements for a list, entries for a map, and 0
     * for a scalar.
     */
    public int length() {
        return len;
    }

    /** Whether there is no value at all, which is not the same as nil. */
    public boolean isAbsent() {
        return kind == ABSENT;
    }

    /** Whether the value is nil. An absent value is not; see {@link #isNull}. */
    public boolean isNil() {
        return kind == NIL;
    }

    /** Nil, or absent. */
    public boolean isNull() {
        return kind == NIL || kind == ABSENT;
    }

    /** Whether the value is an integer. */
    public boolean isInt() {
        return kind == INT;
    }

    /** Whether the value is a double. */
    public boolean isDouble() {
        return kind == DOUBLE;
    }

    /** Whether the value is a boolean. */
    public boolean isBool() {
        return kind == BOOL;
    }

    /** Whether the value is a string. */
    public boolean isString() {
        return kind == STR || kind == GUEST_STR;
    }

    /** Whether the value is a blob. */
    public boolean isBlob() {
        return kind == BLOB || kind == GUEST_BLOB;
    }

    /** Whether the value is a GeoJSON document. */
    public boolean isGeoJson() {
        return kind == GEOJSON || kind == GUEST_GEOJSON;
    }

    /** Whether the value is a list the host holds; false for a built one, which is a handle. */
    public boolean isList() {
        return kind == LIST;
    }

    /** Whether the value is a map the host holds; false for a built one, which is a handle. */
    public boolean isMap() {
        return kind == MAP;
    }

    /** Whether the value is a string, blob, GeoJSON, HLL, list or map the host holds (read by position). */
    public boolean isStored() {
        return kind == STR || kind == BLOB || kind == GEOJSON || kind == HLL || kind == LIST || kind == MAP;
    }

    //==========================================================
    // As a Java value, or an UdfException when the kind is another.
    //

    private UdfException not(String what) {
        return new UdfException("the value is not " + what + " (kind " + kind + ")");
    }

    /** The integer; throws {@link UdfException} when the value is of another kind. */
    public long asInt() {
        if (kind != INT) {
            throw not("an int");
        }

        return payload;
    }

    /**
     * The double; throws {@link UdfException} when the value is of another kind. An integer is not widened.
     */
    public double asDouble() {
        if (kind != DOUBLE) {
            throw not("a double");
        }

        return Double.longBitsToDouble(payload);
    }

    /** The boolean; throws {@link UdfException} when the value is of another kind. */
    public boolean asBool() {
        if (kind != BOOL) {
            throw not("a bool");
        }

        return payload != 0;
    }

    /** The text of a string, copied out; throws {@link UdfException} when the value is of another kind. */
    public String asString() {
        if (kind == GUEST_STR) {
            return new String(bytes, 0, bytesLen, StandardCharsets.UTF_8);
        }

        if (kind != STR) {
            throw not("a string");
        }

        return new String(asBlob(), StandardCharsets.UTF_8);
    }

    /**
     * The bytes of a string, blob, GeoJSON or HLL, copied out; throws {@link UdfException} when the value
     * is of another kind.
     */
    public byte[] asBlob() {
        if (kind == GUEST_STR || kind == GUEST_BLOB || kind == GUEST_GEOJSON) {
            byte[] out = new byte[bytesLen];
            System.arraycopy(bytes, 0, out, 0, bytesLen);
            return out;
        }

        if (!isStored() || kind == LIST || kind == MAP) {
            throw not("a string or blob");
        }

        byte[] out = new byte[len];

        if (len > 0 && Natives.cursorBytesAt(src(), at(), Address.ofData(out), out.length) < 0) {
            throw new UdfException("the value could not be read");
        }

        return out;
    }

    /**
     * Copies the bytes of a stored string, blob, GeoJSON or HLL into {@code into}, which the caller keeps
     * and reuses, and returns how many there were. Throws {@link UdfException} when the value is of another
     * kind or does not fit.
     */
    public int readBlob(byte[] into) {
        if (!isStored() || kind == LIST || kind == MAP) {
            throw not("a string or blob");
        }

        int n = Natives.cursorBytesAt(src(), at(), Address.ofData(into), into.length);

        if (n < 0) {
            throw new UdfException("the value's " + len + " bytes do not fit an array of " + into.length);
        }

        return n;
    }

    /**
     * Copies a stored blob of little-endian 32-bit floats straight into {@code into}, element for element -
     * the way a vector is read without converting it - and returns how many floats there were. Throws
     * {@link UdfException} when the value is not a blob or does not fit.
     */
    public int readFloats(float[] into) {
        if (kind != BLOB) {
            throw not("a blob");
        }

        int n = Natives.cursorBytesAt(src(), at(), Address.ofData(into), into.length * 4);

        if (n < 0) {
            throw new UdfException("the value's " + len + " bytes do not fit an array of " + into.length + " floats");
        }

        return n / 4;
    }

    /** The list this value is, to index and iterate; throws {@link UdfException} when it is not a list. */
    public ListValue asList() {
        if (kind != LIST) {
            throw not("a list");
        }

        return new ListValue(this);
    }

    /** The map this value is, to look up in and iterate; throws {@link UdfException} when it is not a map. */
    public MapValue asMap() {
        if (kind != MAP) {
            throw not("a map");
        }

        return new MapValue(this);
    }

    /**
     * The value as a plain Java object, copied out whole: {@code Long}, {@code Double}, {@code Boolean},
     * {@code String} (a GeoJSON document as its text), {@code byte[]}, a {@code java.util.List} or a
     * {@code java.util.Map} of the same, and {@code null} for nil or absent.
     */
    public Object toObject() {
        return Convert.toObject(this);
    }

    /**
     * Compares two stored values byte by byte, as memcmp would on their stored encodings, without copying
     * either: negative, zero or positive. It is the last resort of a convergent merge, which needs some
     * order that both sides compute alike. Throws {@link UdfException} when either is not a stored value.
     */
    public int cmpStored(Value other) {
        if (!isStored() || !other.isStored()) {
            throw new UdfException("only stored values compare by their bytes");
        }

        return Natives.collCmpAt(src(), at(), other.src(), other.at());
    }

    //==========================================================
    // Values of the guest's own.
    //

    /** An integer value. */
    public static Value ofInt(long i) {
        return new Value().assign(INT, 0, i);
    }

    /** A double value. */
    public static Value ofDouble(double d) {
        return new Value().assign(DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    /** A boolean value. */
    public static Value ofBool(boolean b) {
        return new Value().assign(BOOL, 0, b ? 1 : 0);
    }

    /** A nil value. */
    public static Value ofNil() {
        return new Value().assign(NIL, 0, 0);
    }

    /** A string of the guest's, encoded as UTF-8. */
    public static Value ofString(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return new Value().guest(GUEST_STR, b, b.length);
    }

    /** A blob of the guest's over {@code b}, which is kept, not copied. */
    public static Value ofBlob(byte[] b) {
        return new Value().guest(GUEST_BLOB, b, b.length);
    }

    /** A GeoJSON document of the guest's, from its text; kept as that kind when it is stored or returned. */
    public static Value ofGeoJson(String doc) {
        byte[] d = doc.getBytes(StandardCharsets.UTF_8);
        return new Value().guest(GUEST_GEOJSON, d, d.length);
    }

    //==========================================================
    // For the SDK itself. A value's fields are written only here, by the calls that fill one.
    //

    /** The scalar, the handle or the position, as the host wrote it. */
    long payload() {
        return payload;
    }

    /** The source handle a string, blob or collection sits in. */
    int src() {
        return (int) (payload >>> 32);
    }

    /** The offset in that source. */
    int at() {
        return (int) payload;
    }

    /** The double a DOUBLE value holds, unchecked. */
    double doubleBits() {
        return Double.longBitsToDouble(payload);
    }

    /** The bytes of a guest kind. */
    byte[] guestBytes() {
        return bytes;
    }

    /** How many of {@link #guestBytes} count. */
    int guestLen() {
        return bytesLen;
    }

    /** Refills from the 16-byte slot at {@code off} in {@code buf}, as the host wrote it (little-endian). */
    Value fill(byte[] buf, int off) {
        kind = Slots.le32(buf, off);
        len = Slots.le32(buf, off + 4);
        payload = Slots.le64(buf, off + 8);
        bytes = null;
        return this;
    }

    /** Sets the three fields and drops any guest bytes. */
    Value assign(int kind, int len, long payload) {
        this.kind = kind;
        this.len = len;
        this.payload = payload;
        this.bytes = null;
        return this;
    }

    /** Makes this a guest string, blob or GeoJSON over the first {@code len} bytes of {@code b}, kept. */
    Value guest(int kind, byte[] b, int len) {
        this.kind = kind;
        this.len = len;
        payload = 0;
        bytes = b;
        bytesLen = len;
        return this;
    }

    /** Makes this what {@code o} is. */
    Value assign(Value o) {
        kind = o.kind;
        len = o.len;
        payload = o.payload;
        bytes = o.bytes;
        bytesLen = o.bytesLen;
        return this;
    }

    /** A fresh value equal to this one: what a reader hands out of its scratch. */
    Value copy() {
        return new Value().assign(this);
    }
}

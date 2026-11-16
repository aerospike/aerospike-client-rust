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

/**
 * The 16-byte value the host hands out, and the one a guest hands back in a batch: a kind, a length (bytes,
 * elements or entries) and a payload, which is the scalar itself or, for a string, blob or collection the
 * host holds, {@code source << 32 | offset}. A {@code Value} is reused: {@link #read} refills it from a
 * 16-byte slot, so a hot loop allocates none.
 */
public final class Value {
    // Kinds, as the host hands them out.
    /** No value at all: a {@code Value} never filled, or a map entry that is not there. Distinct from nil. */
    public static final int ABSENT = -1;
    /** Nil. */
    public static final int NIL = 0;
    /** A boolean; the payload is non-zero for true. */
    public static final int BOOL = 1;
    /** A 64-bit integer; the payload is the integer. */
    public static final int INT = 2;
    /** A double; the payload is its bits, read with {@link #asDouble}. */
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

    // Kinds a guest uses only in a batch it hands the host: bytes of its own by address, a built collection
    // by handle.
    /** A string of the guest's own, handed to the host by address. */
    public static final int GUEST_STR = 16;
    /** A blob of the guest's own, handed to the host by address. */
    public static final int GUEST_BLOB = 17;
    /** A GeoJSON document of the guest's own, handed to the host by address. */
    public static final int GUEST_GEOJSON = 18;
    /** A value built on the host, handed back by its handle, which is the payload. */
    public static final int HANDLE = 19;

    /** The kind, one of the constants of this class; {@link #ABSENT} until the value is filled. */
    public int kind = ABSENT;
    /** The length: bytes for a string or blob, elements for a list, entries for a map. */
    public int len;
    /**
     * The scalar itself (a double as its bits), a handle, or {@code source << 32 | offset} for a value the host
     * holds.
     */
    public long payload;

    /** For the guest kinds: the bytes the payload addresses, whose address is taken when the batch is sent. */
    byte[] bytes;
    int bytesLen;

    public Value() {
    }

    /** The source handle a string, blob or collection sits in. */
    public int src() {
        return (int) (payload >>> 32);
    }

    /** The offset in that source. */
    public int at() {
        return (int) payload;
    }

    /** The payload as a double. */
    public double asDouble() {
        return Double.longBitsToDouble(payload);
    }

    /** Whether there is no value at all, which is not the same as nil. */
    public boolean isAbsent() {
        return kind == ABSENT;
    }

    /** Whether the value is a string, blob, GeoJSON, HLL, list or map the host holds (read by position). */
    public boolean isStored() {
        return kind == STR || kind == BLOB || kind == GEOJSON || kind == HLL || kind == LIST || kind == MAP;
    }

    /** Refill from the 16-byte slot at {@code off} in {@code buf}, as the host wrote it (little-endian). */
    public Value read(byte[] buf, int off) {
        kind = Udf.le32(buf, off);
        len = Udf.le32(buf, off + 4);
        payload = Udf.le64(buf, off + 8);
        bytes = null;
        return this;
    }

    /**
     * Sets the three fields and drops any guest bytes: the way to copy another value's fields, to keep one
     * across cursor steps. Returns this.
     */
    public Value set(int kind, int len, long payload) {
        this.kind = kind;
        this.len = len;
        this.payload = payload;
        this.bytes = null;
        return this;
    }

    /** Makes this an integer. Returns this, as every setter does. */
    public Value setInt(long i) {
        return set(INT, 0, i);
    }

    /** Makes this a double. */
    public Value setDouble(double d) {
        return set(DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    /** Makes this a boolean. */
    public Value setBool(boolean b) {
        return set(BOOL, 0, b ? 1 : 0);
    }

    /** Makes this nil. */
    public Value setNil() {
        return set(NIL, 0, 0);
    }

    /** A built collection, by handle. */
    public Value setHandle(int handle) {
        return set(HANDLE, 0, handle);
    }

    /** A string of the guest's, as UTF-8 bytes the caller keeps alive until the batch is sent. */
    public Value setStr(String s) {
        return setStrBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /** A string of the guest's from UTF-8 bytes, all of {@code b}. The array is kept, not copied. */
    public Value setStrBytes(byte[] b) {
        return setStrBytes(b, b.length);
    }

    /**
     * A string of the guest's from the first {@code len} bytes of {@code b}, as UTF-8. The array is kept, not
     * copied.
     */
    public Value setStrBytes(byte[] b, int len) {
        kind = GUEST_STR;
        this.len = len;
        payload = 0;
        bytes = b;
        bytesLen = len;
        return this;
    }

    /** A blob of the guest's, all of {@code b}. The array is kept, not copied. */
    public Value setBlob(byte[] b) {
        return setBlob(b, b.length);
    }

    /** A blob of the guest's from the first {@code len} bytes of {@code b}. The array is kept, not copied. */
    public Value setBlob(byte[] b, int len) {
        kind = GUEST_BLOB;
        this.len = len;
        payload = 0;
        bytes = b;
        bytesLen = len;
        return this;
    }

    // Fresh values, for the occasional one.

    /** A fresh integer value. */
    public static Value ofInt(long i) {
        return new Value().setInt(i);
    }

    /** A fresh double value. */
    public static Value ofDouble(double d) {
        return new Value().setDouble(d);
    }

    /** A fresh boolean value. */
    public static Value ofBool(boolean b) {
        return new Value().setBool(b);
    }

    /** A fresh nil value. */
    public static Value ofNil() {
        return new Value().setNil();
    }

    /** A fresh value for a built collection, by handle. */
    public static Value ofHandle(int handle) {
        return new Value().setHandle(handle);
    }

    /** A fresh string of the guest's, encoded as UTF-8. */
    public static Value ofStr(String s) {
        return new Value().setStr(s);
    }

    /** A fresh blob of the guest's over {@code b}, which is kept, not copied. */
    public static Value ofBlob(byte[] b) {
        return new Value().setBlob(b);
    }

    /** A GeoJSON document of the guest's, kept as that kind when it is pushed, put or returned. */
    public Value setGeoJson(byte[] doc, int len) {
        kind = GUEST_GEOJSON;
        this.len = len;
        payload = 0;
        bytes = doc;
        bytesLen = len;
        return this;
    }

    /** A fresh GeoJSON document of the guest's, from its text. */
    public static Value ofGeoJson(String doc) {
        byte[] d = doc.getBytes(StandardCharsets.UTF_8);
        return new Value().setGeoJson(d, d.length);
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
    public boolean isBoolean() {
        return kind == BOOL;
    }

    /** Whether the value is a string the host holds; false for a string of the guest's. */
    public boolean isString() {
        return kind == STR;
    }

    /** Whether the value is a list the host holds; false for a built one, which is a handle. */
    public boolean isList() {
        return kind == LIST;
    }

    /** Whether the value is a map the host holds; false for a built one, which is a handle. */
    public boolean isMap() {
        return kind == MAP;
    }

    //==========================================================
    // As a Java value, or an UdfException when the kind is another.
    //

    private UdfException not(String what) {
        return new UdfException("the value is not " + what + " (kind " + kind + ")");
    }

    /** The integer; throws {@link UdfException} when the value is of another kind. */
    public long asLong() {
        if (kind != INT) {
            throw not("an int");
        }

        return payload;
    }

    /** The boolean; throws {@link UdfException} when the value is of another kind. */
    public boolean asBoolean() {
        if (kind != BOOL) {
            throw not("a bool");
        }

        return payload != 0;
    }

    /** The text of a stored string, copied out. */
    public String asString() {
        if (kind == GUEST_STR) {
            return new String(bytes, 0, bytesLen, StandardCharsets.UTF_8);
        }

        if (kind != STR) {
            throw not("a string");
        }

        return Udf.string(asBytes(), len);
    }

    /** The bytes of a stored string, blob, GeoJSON or HLL, copied out. */
    public byte[] asBytes() {
        if (kind == GUEST_STR || kind == GUEST_BLOB || kind == GUEST_GEOJSON) {
            byte[] out = new byte[bytesLen];
            System.arraycopy(bytes, 0, out, 0, bytesLen);
            return out;
        }

        if (!isStored() || kind == LIST || kind == MAP) {
            throw not("a string or blob");
        }

        byte[] out = new byte[len];

        if (len > 0 && Udf.bytes(this, out) < 0) {
            throw new UdfException("the value could not be read");
        }

        return out;
    }

    /** A walk over a stored list. */
    public Cursor<Value> asList() {
        return Udf.list(this);
    }

    /** A walk over a stored map's entries. */
    public Cursor<Cursor.Entry> asMap() {
        return Udf.map(this);
    }
}

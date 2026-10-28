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
    public static final int ABSENT = -1;
    public static final int NIL = 0;
    public static final int BOOL = 1;
    public static final int INT = 2;
    public static final int DOUBLE = 3;
    public static final int STR = 4;
    public static final int BLOB = 5;
    public static final int LIST = 6;
    public static final int MAP = 7;
    public static final int OTHER = 8;
    public static final int GEOJSON = 9;
    public static final int HLL = 10;

    // Kinds a guest uses only in a batch it hands the host: bytes of its own by address, a built collection
    // by handle.
    public static final int GUEST_STR = 16;
    public static final int GUEST_BLOB = 17;
    public static final int GUEST_GEOJSON = 18;
    public static final int HANDLE = 19;

    public int kind = ABSENT;
    public int len;
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

    public Value set(int kind, int len, long payload) {
        this.kind = kind;
        this.len = len;
        this.payload = payload;
        this.bytes = null;
        return this;
    }

    public Value setInt(long i) {
        return set(INT, 0, i);
    }

    public Value setDouble(double d) {
        return set(DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    public Value setBool(boolean b) {
        return set(BOOL, 0, b ? 1 : 0);
    }

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

    public Value setStrBytes(byte[] b) {
        return setStrBytes(b, b.length);
    }

    public Value setStrBytes(byte[] b, int len) {
        kind = GUEST_STR;
        this.len = len;
        payload = 0;
        bytes = b;
        bytesLen = len;
        return this;
    }

    public Value setBlob(byte[] b) {
        return setBlob(b, b.length);
    }

    public Value setBlob(byte[] b, int len) {
        kind = GUEST_BLOB;
        this.len = len;
        payload = 0;
        bytes = b;
        bytesLen = len;
        return this;
    }

    // Fresh values, for the occasional one.

    public static Value ofInt(long i) {
        return new Value().setInt(i);
    }

    public static Value ofDouble(double d) {
        return new Value().setDouble(d);
    }

    public static Value ofBool(boolean b) {
        return new Value().setBool(b);
    }

    public static Value ofNil() {
        return new Value().setNil();
    }

    public static Value ofHandle(int handle) {
        return new Value().setHandle(handle);
    }

    public static Value ofStr(String s) {
        return new Value().setStr(s);
    }

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

    public static Value ofGeoJson(String doc) {
        byte[] d = doc.getBytes(StandardCharsets.UTF_8);
        return new Value().setGeoJson(d, d.length);
    }

    public boolean isNil() {
        return kind == NIL;
    }

    /** Nil, or absent. */
    public boolean isNull() {
        return kind == NIL || kind == ABSENT;
    }

    public boolean isInt() {
        return kind == INT;
    }

    public boolean isDouble() {
        return kind == DOUBLE;
    }

    public boolean isBoolean() {
        return kind == BOOL;
    }

    public boolean isString() {
        return kind == STR;
    }

    public boolean isList() {
        return kind == LIST;
    }

    public boolean isMap() {
        return kind == MAP;
    }

    //==========================================================
    // As a Java value, or an UdfException when the kind is another.
    //

    private UdfException not(String what) {
        return new UdfException("the value is not " + what + " (kind " + kind + ")");
    }

    public long asLong() {
        if (kind != INT) {
            throw not("an int");
        }

        return payload;
    }

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

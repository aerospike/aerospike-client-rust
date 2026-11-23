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
 * this class, and each {@code as*} reader throws {@link UdfException} with the status
 * {@link LowLevel#TYPE_MISMATCH} when the value is of another kind: nothing is widened or converted, so an
 * integer is not a double and a string is not a blob.
 * A value read from the record, from an argument or from an iteration is never changed afterwards, so it
 * can be kept for the rest of the call.
 *
 * <p>{@link #ofInt}, {@link #ofString} and the other factories make a value of the guest's own, for a
 * builder, a bin or a result.
 *
 * <pre>
 * Value score = rec.get("score");             // null when there is no such bin
 *
 * if (score == null) {
 *     return 0;
 * }
 *
 * return score.isInt() ? score.asInt() : (long) score.asDouble();
 * </pre>
 *
 * <p>A value of the guest's own ({@link #ofString}, {@link #ofBlob}) reads back through the same {@code as*}
 * readers. A built list or map in a value ({@link ListBuilder#asValue}) does not: it is of kind
 * {@link #HANDLE} and is only handed on.
 */
public final class Value {
    // Kinds, as the host hands them out.
    /** No value at all: a {@code Value} never filled, or a map entry that is not there. Distinct from nil. */
    public static final int ABSENT = -1;
    /**
     * The kind of nil: a list element, a map entry or an argument that is nil. A bin is never nil, it is
     * absent.
     */
    public static final int NIL = 0;
    /** The kind of a boolean, which {@link #asBool} reads. */
    public static final int BOOL = 1;
    /** The kind of a 64-bit signed integer, which {@link #asInt} reads. */
    public static final int INT = 2;
    /** The kind of a 64-bit double, which {@link #asDouble} reads. */
    public static final int DOUBLE = 3;
    /** A string the host holds, read by position; the length is in bytes. */
    public static final int STR = 4;
    /** A blob the host holds, read by position; the length is in bytes. */
    public static final int BLOB = 5;
    /** A list the host holds, walked by position; the length is in elements. */
    public static final int LIST = 6;
    /** A map the host holds, walked or looked up by position; the length is in entries. */
    public static final int MAP = 7;
    /**
     * A value of a kind the host has no other name for. It can be tested for and nothing else: no reader
     * takes it.
     */
    public static final int OTHER = 8;
    /** A GeoJSON document the host holds; {@link #asBytes} reads its text. */
    public static final int GEOJSON = 9;
    /** A HyperLogLog value the host holds; {@link #asBytes} reads its bytes. */
    public static final int HLL = 10;

    // Kinds a guest uses only for a value it hands the host: bytes of its own by address, a built
    // collection by handle.
    /** A string of the guest's own, handed to the host by address. */
    public static final int GUEST_STR = 16;
    /** A blob of the guest's own, handed to the host by address. */
    public static final int GUEST_BLOB = 17;
    /** A GeoJSON document of the guest's own, handed to the host by address. */
    public static final int GUEST_GEOJSON = 18;
    /**
     * A list or map built on the host ({@link ListBuilder}, {@link MapBuilder}), handed back by its handle.
     * It can be put in a bin, a collection or the result; it cannot be read.
     */
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

    /**
     * The kind, one of the constants of this class: {@link #INT}, {@link #STR}, {@link #LIST} and the rest
     * for a value read from the server, {@link #GUEST_STR} and its neighbours for one made here. The
     * {@code is*} tests cover both, so prefer them unless a {@code switch} is wanted.
     */
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

    /** Whether the value is nil or absent: the test for "nothing here" when the difference does not matter. */
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

    /** Whether the value is a string: a stored one or one of the guest's own. A GeoJSON document is not. */
    public boolean isString() {
        return kind == STR || kind == GUEST_STR;
    }

    /**
     * Whether the value is a blob: a stored one or one of the guest's own. False for a string, a GeoJSON
     * document and an HLL, whose bytes {@link #asBytes} reads.
     */
    public boolean isBlob() {
        return kind == BLOB || kind == GUEST_BLOB;
    }

    /** Whether the value is a GeoJSON document: a stored one or one of the guest's own. */
    public boolean isGeoJson() {
        return kind == GEOJSON || kind == GUEST_GEOJSON;
    }

    /** Whether the value is a HyperLogLog. It is not a blob, though {@link #asBytes} reads its bytes. */
    public boolean isHll() {
        return kind == HLL;
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
        return new UdfException(LowLevel.TYPE_MISMATCH, "the value is not " + what + " (kind " + kind + ")");
    }

    /**
     * The integer; throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the value is of another
     * kind, nil included. A double is not truncated to one.
     */
    public long asInt() {
        if (kind != INT) {
            throw not("an int");
        }

        return payload;
    }

    /**
     * The double; throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the value is of another
     * kind. An integer is not widened.
     */
    public double asDouble() {
        if (kind != DOUBLE) {
            throw not("a double");
        }

        return Double.longBitsToDouble(payload);
    }

    /**
     * The boolean; throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the value is of another
     * kind. An integer 0 or 1 is not one.
     */
    public boolean asBool() {
        if (kind != BOOL) {
            throw not("a bool");
        }

        return payload != 0;
    }

    /**
     * The text of a string, decoded from UTF-8 into a new {@code String} on every call; for a stored string
     * that is one call to the host and one copy. Bytes that are not UTF-8 are replaced, not refused:
     * {@link #asBytes} has them as they are. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when
     * the value is of another kind, a GeoJSON included: its text is {@link #toObject}, or {@link #asBytes}
     * decoded.
     */
    public String asString() {
        if (kind == GUEST_STR) {
            return new String(bytes, 0, bytesLen, StandardCharsets.UTF_8);
        }

        if (kind != STR) {
            throw not("a string");
        }

        return new String(asBytes(), StandardCharsets.UTF_8);
    }

    /**
     * The bytes of a blob, copied into a new array on every call. It is strict: throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) for anything that is not a blob, a string, a GeoJSON document and an
     * HLL included. {@link #asBytes} is the reader that takes those too.
     */
    public byte[] asBlob() {
        if (kind != BLOB && kind != GUEST_BLOB) {
            throw not("a blob");
        }

        return asBytes();
    }

    /**
     * The raw bytes of a string, blob, GeoJSON document or HLL, copied into a new array on every call: the
     * UTF-8 bytes of a string or a document, the bytes of a blob, an HLL as the server stores it. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the value is a scalar, nil, a list or a map,
     * and ({@link LowLevel#ERR}) when a stored value can no longer be read: it was read before a save that
     * failed.
     */
    public byte[] asBytes() {
        if (kind == GUEST_STR || kind == GUEST_BLOB || kind == GUEST_GEOJSON) {
            byte[] out = new byte[bytesLen];
            System.arraycopy(bytes, 0, out, 0, bytesLen);
            return out;
        }

        if (!hasBytes()) {
            throw not("a string, blob, GeoJSON or HLL");
        }

        byte[] out = new byte[len];

        if (len > 0) {
            int n = Natives.cursorBytesAt(src(), at(), Address.ofData(out), out.length);

            if (n < 0) {
                throw new UdfException(n, "the value could not be read");
            }
        }

        return out;
    }

    /** Whether this is a stored value that has bytes: a string, blob, GeoJSON or HLL the host holds. */
    boolean hasBytes() {
        return kind == STR || kind == BLOB || kind == GEOJSON || kind == HLL;
    }

    /**
     * Copies the raw bytes of a stored string, blob, GeoJSON or HLL into {@code into}, which the caller keeps
     * and reuses, and returns how many there were; the rest of {@code into} is left as it was. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the value is of another kind or is one of
     * the guest's own ({@link #ofBlob}). When {@code into} is shorter than {@link #length} nothing is copied
     * and the exception's status is the host's "too small": at or below {@link LowLevel#NEEDED_BASE}, with
     * {@code -(status) - 16} the size needed.
     */
    public int readBytes(byte[] into) {
        if (!hasBytes()) {
            throw not("a stored string, blob, GeoJSON or HLL");
        }

        int n = Natives.cursorBytesAt(src(), at(), Address.ofData(into), into.length);

        if (n < 0) {
            throw new UdfException(n, n <= LowLevel.NEEDED_BASE
                    ? "the value's " + len + " bytes do not fit an array of " + into.length
                    : "the value could not be read");
        }

        return n;
    }

    /**
     * Copies a stored blob of little-endian 32-bit floats straight into {@code into}, element for element -
     * the way a vector is read without converting it - and returns how many floats there were, which is
     * {@link #length} divided by 4. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the
     * value is not a stored blob (a string is not). When it holds more floats than {@code into} has room for
     * nothing is copied and the exception's status is at or below {@link LowLevel#NEEDED_BASE}.
     */
    public int readFloats(float[] into) {
        if (kind != BLOB) {
            throw not("a blob");
        }

        int n = Natives.cursorBytesAt(src(), at(), Address.ofData(into), into.length * 4);

        if (n < 0) {
            throw new UdfException(n, n <= LowLevel.NEEDED_BASE
                    ? "the value's " + len + " bytes do not fit an array of " + into.length + " floats"
                    : "the value could not be read");
        }

        return n / 4;
    }

    /**
     * The list this value is, as a view to index and iterate where it is stored; throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) when it is not a stored list. A built list ({@link ListBuilder#asValue}) is not one.
     */
    public ListValue asList() {
        if (kind != LIST) {
            throw not("a list");
        }

        return new ListValue(this);
    }

    /**
     * The map this value is, as a view to look up in and iterate where it is stored; throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when it is not a stored map. A built map ({@link MapBuilder#asValue}) is not one.
     */
    public MapValue asMap() {
        if (kind != MAP) {
            throw not("a map");
        }

        return new MapValue(this);
    }

    /**
     * The value as a plain Java object, copied out whole: {@code Long}, {@code Double}, {@code Boolean},
     * {@code String} (a GeoJSON document as its text), {@code byte[]} (a blob or an HLL), a
     * {@code java.util.List} or a {@code java.util.Map} of the same, and {@code null} for nil or absent.
     * Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) for a built collection and for a value of
     * kind {@link #OTHER}, which have no Java form.
     */
    public Object toObject() {
        return Convert.toObject(this);
    }

    /**
     * Compares two stored values byte by byte, as memcmp would on their stored encodings, without copying
     * either: exactly -1, 0 or 1, a value that is a prefix of the other coming first. Zero means the same
     * bytes. It is not the order of the numbers or texts the values hold: it is the last resort of a
     * convergent merge, which needs some order that both sides compute alike. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) when either is not a stored value (a scalar, nil, or one of the
     * guest's own) or the host cannot compare them: a status is never returned as if it were an ordering.
     */
    public int cmpStored(Value other) {
        if (!isStored() || !other.isStored()) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "only stored values compare by their bytes");
        }

        int c = Natives.collCmpAt(src(), at(), other.src(), other.at());

        if (c < -1 || c > 1) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "the values could not be compared (" + c + ")");
        }

        return c;
    }

    //==========================================================
    // Values of the guest's own.
    //

    /** An integer value of the guest's own, for where a {@link Value} is taken: a bin, a builder, the result. */
    public static Value ofInt(long i) {
        return new Value().assign(INT, 0, i);
    }

    /** A double value of the guest's own, for where a {@link Value} is taken: a bin, a builder, the result. */
    public static Value ofDouble(double d) {
        return new Value().assign(DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    /** A boolean value of the guest's own, for where a {@link Value} is taken: a bin, a builder, the result. */
    public static Value ofBool(boolean b) {
        return new Value().assign(BOOL, 0, b ? 1 : 0);
    }

    /**
     * A nil value: a nil element in a list, a nil entry in a map, a nil result. Written to a bin it removes
     * the bin.
     */
    public static Value ofNil() {
        return new Value().assign(NIL, 0, 0);
    }

    /**
     * A string of the guest's, encoded as UTF-8 here, once; the host copies the bytes when the value is
     * handed to it.
     */
    public static Value ofString(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return new Value().guest(GUEST_STR, b, b.length);
    }

    /**
     * A blob of the guest's over all of {@code b}, which is kept, not copied: the host reads the array when
     * the value is handed to it, so a change to {@code b} before then is a change to the value.
     */
    public static Value ofBlob(byte[] b) {
        return new Value().guest(GUEST_BLOB, b, b.length);
    }

    /**
     * A GeoJSON document of the guest's, from its text; kept as that kind when it is stored or returned. The
     * text is not checked here: the write that stores it fails when it is not a document the server accepts.
     */
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

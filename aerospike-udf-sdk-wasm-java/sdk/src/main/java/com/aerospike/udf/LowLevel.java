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
 * The server's calls one at a time, for a hot path that must allocate nothing: names as {@code byte[]}
 * constants, values read into a {@link Value} the caller owns and reuses, collections as {@code int}
 * handles, cursors opened and closed by hand, and a status code where the rest of the SDK throws. Nothing
 * here is needed to write a UDF; {@link Record}, {@link ListValue}, {@link MapValue} and the builders are
 * the API, and they are built on these same calls. {@code examples/vector} is what this tier is for: a
 * stream that reads a blob from every record of a scan and must not create garbage per record.
 *
 * <p>The rules the rest of the SDK keeps for its callers are the caller's own here:
 *
 * <ul>
 * <li>A status is {@link #OK} or negative: {@link #NOT_FOUND}, {@link #TYPE_MISMATCH}, {@link #REFUSED},
 * {@link #ERR}, and at or below {@link #NEEDED_BASE} a buffer that was too small, {@code -(rc) - 16}
 * being the size needed.</li>
 * <li>A cursor from {@link #enter} holds one of the call's 256 handle slots until {@link #close}, and
 * the host gives a slot back only when it is the last one handed out: close cursors in the reverse of
 * the order they were opened, on every path out of the loop.</li>
 * <li>A {@code Value} filled here is overwritten by the next call that fills it; {@link #copy} keeps
 * one.</li>
 * </ul>
 */
public final class LowLevel {
    private LowLevel() {
    }

    // Status codes a native returns.
    /** The call succeeded. */
    public static final int OK = 0;
    /**
     * The call failed on what it was given, such as a bin name that is empty or longer than 15 bytes, or a
     * handle the host does not hold.
     */
    public static final int ERR = -1;
    /** There is no such bin, key or entry. */
    public static final int NOT_FOUND = -2;
    /** The value is not of a kind the call takes. */
    public static final int TYPE_MISMATCH = -3;
    /**
     * The host refused the operation: a write the record does not accept, or a call not allowed where it was
     * made.
     */
    public static final int REFUSED = -4;
    /** The buffer was too small: a status at or below this carries the size needed, {@code -(rc) - 16}. */
    public static final int NEEDED_BASE = -16;

    /** Values cross the boundary at most this many per import. */
    public static final int BATCH = Slots.BATCH;

    //==========================================================
    // Bytes and names.
    //

    /** The UTF-8 bytes of a string; keep the result in a constant for a name used on every call. */
    public static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A string from the first {@code len} bytes of {@code b}, decoded as UTF-8. */
    public static String string(byte[] b, int len) {
        return new String(b, 0, len, StandardCharsets.UTF_8);
    }

    //==========================================================
    // A Value to reuse. These write to a value the caller owns; a value the SDK handed out is never one.
    //

    /** Makes {@code v} an integer. */
    public static Value setInt(Value v, long i) {
        return v.assign(Value.INT, 0, i);
    }

    /** Makes {@code v} a double. */
    public static Value setDouble(Value v, double d) {
        return v.assign(Value.DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    /** Makes {@code v} a boolean. */
    public static Value setBool(Value v, boolean b) {
        return v.assign(Value.BOOL, 0, b ? 1 : 0);
    }

    /** Makes {@code v} nil. */
    public static Value setNil(Value v) {
        return v.assign(Value.NIL, 0, 0);
    }

    /** Makes {@code v} a string of the guest's: the first {@code len} bytes of {@code b}, kept, not copied. */
    public static Value setStr(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_STR, b, len);
    }

    /** Makes {@code v} a blob of the guest's: the first {@code len} bytes of {@code b}, kept, not copied. */
    public static Value setBlob(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_BLOB, b, len);
    }

    /** Makes {@code v} a GeoJSON document of the guest's: the first {@code len} bytes of {@code b}, kept. */
    public static Value setGeoJson(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_GEOJSON, b, len);
    }

    /** Makes {@code v} a built collection, by its handle. */
    public static Value setHandle(Value v, int handle) {
        return v.assign(Value.HANDLE, 0, handle);
    }

    /** Makes {@code into} what {@code from} is: the way to keep a value across cursor steps. */
    public static Value copy(Value from, Value into) {
        return into.assign(from);
    }

    //==========================================================
    // Reading.
    //

    /** Reads a bin of any kind into {@code out}, in one import. NOT_FOUND for no such bin. */
    public static int bin(byte[] name, Value out) {
        int rc = Natives.binValue(Address.ofData(name), name.length, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /** Reads a bin of one of an event's records into {@code out}. NOT_FOUND when that side has no such bin. */
    public static int eventBin(EventRecord rec, byte[] name, Value out) {
        return rec.read(name, out);
    }

    /** Copies the bytes of a string, blob, GeoJSON or HLL value into {@code buf}. Returns the length, or a status. */
    public static int bytes(Value v, byte[] buf) {
        return Natives.cursorBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length);
    }

    /**
     * Copies a blob of little-endian 32-bit floats straight into a float array, element for element: the way a
     * vector is read without converting it. Returns the length in bytes, or a status.
     */
    public static int floats(Value v, float[] buf) {
        return Natives.cursorBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length * 4);
    }

    /**
     * Copies a stored value's bytes as the server stores them, msgpack header included, into {@code buf}.
     * Returns the length, or a status.
     */
    public static int storedBytes(Value v, byte[] buf) {
        return Natives.collBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length);
    }

    /**
     * Compares two stored values byte by byte, as memcmp would on their stored encodings, without copying
     * either: negative, zero or positive; TYPE_MISMATCH when a value is not a stored one.
     */
    public static int cmpStored(Value a, Value b) {
        return Natives.collCmpAt(a.src(), a.at(), b.src(), b.at());
    }

    /**
     * Decodes the call's arguments into {@code out}, in one import, filling (or allocating) a Value per slot.
     * Returns how many there are, bounded by {@code out.length} and {@link #BATCH}.
     */
    public static int args(int handle, int argc, Value[] out) {
        if (handle <= 0 || argc <= 0 || out.length == 0) {
            return 0;
        }

        int cap = Math.min(out.length, BATCH);
        int n = Natives.argsValues(handle, Address.ofData(Slots.MANY), cap * Slots.SIZE);

        if (n < 0) {
            return 0;
        }

        decode(out, n);

        return n;
    }

    private static void decode(Value[] out, int n) {
        for (int i = 0; i < n; i++) {
            if (out[i] == null) {
                out[i] = new Value();
            }

            out[i].fill(Slots.MANY, i * Slots.SIZE);
        }
    }

    /** Opens a cursor over the stored list or map {@code v}. 0 when it could not. */
    public static int enter(Value v) {
        return Natives.cursorEnterAt(v.src(), v.at());
    }

    /**
     * Fills {@code out} with the next values of a cursor, {@link #BATCH} at most per import. A map yields key,
     * value, key, value. Returns how many, or a status; fewer than {@code out.length} means the end.
     */
    public static int next(int cursor, Value[] out) {
        int cap = Math.min(out.length, BATCH);
        int n = Natives.cursorValues(cursor, Address.ofData(Slots.MANY), cap * Slots.SIZE);

        if (n > 0) {
            decode(out, n);
        }

        return n;
    }

    /** Gives a cursor's slot back. */
    public static void close(int cursor) {
        Natives.cursorClose(cursor);
    }

    /** Looks a string key up in the stored map {@code m}, in one import. */
    public static int get(Value m, byte[] key, Value out) {
        int rc = Natives.collGet(m.src(), m.at(), Address.ofData(key), key.length, Address.ofData(Slots.ONE),
                Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /** Looks an integer key up in the stored map {@code m}. */
    public static int get(Value m, long key, Value out) {
        int rc = Natives.collGetI(m.src(), m.at(), key, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /** The element at {@code index} of the stored list {@code list}. NOT_FOUND past the end. */
    public static int listGet(Value list, int index, Value out) {
        int rc = Natives.listGet(list.src(), list.at(), index, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    //==========================================================
    // The record in hand, where Record would allocate: its digest and key into buffers the caller owns.
    //

    /** Copies the record's digest (20 bytes) into {@code buf}. Returns the length, or a status. */
    public static int digest(byte[] buf) {
        return Natives.digest(Address.ofData(buf), buf.length);
    }

    /** The kind of the record's user key ({@link Value#INT}, {@link Value#STR}, {@link Value#BLOB}), or ABSENT. */
    public static int keyType() {
        return Natives.keyType();
    }

    /** The user key when it is an integer. */
    public static long keyInt() {
        return Natives.keyInt();
    }

    /** Copies a string or blob user key into {@code buf}. Returns the length, or a status. */
    public static int keyBytes(byte[] buf) {
        return Natives.keyBytes(Address.ofData(buf), buf.length);
    }

    //==========================================================
    // Writing the record. Every write is staged until save().
    //

    /** Writes an integer bin. Returns OK or a status. */
    public static int setInt(byte[] name, long value) {
        return Natives.setInt(Address.ofData(name), name.length, value);
    }

    /** Writes a double bin. */
    public static int setDouble(byte[] name, double value) {
        return Natives.setDouble(Address.ofData(name), name.length, value);
    }

    /** Writes a boolean bin. */
    public static int setBool(byte[] name, boolean value) {
        return Natives.setBool(Address.ofData(name), name.length, value ? 1 : 0);
    }

    /** Writes a string bin from the first {@code valueLen} bytes of {@code value}, which are UTF-8. */
    public static int setStr(byte[] name, byte[] value, int valueLen) {
        return Natives.setStr(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /** Writes a blob bin from the first {@code valueLen} bytes of {@code value}. */
    public static int setBlob(byte[] name, byte[] value, int valueLen) {
        return Natives.setBlob(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /** Writes a GeoJSON document, kept as the geospatially indexable kind rather than a string. */
    public static int setGeoJson(byte[] name, byte[] doc, int docLen) {
        return Natives.setGeoJson(Address.ofData(name), name.length, Address.ofData(doc), docLen);
    }

    /** Writes a built list or map to a bin, by its handle. */
    public static int setHandle(byte[] name, int handle) {
        return Natives.setHandle(Address.ofData(name), name.length, handle);
    }

    /** Writes a stored value to a bin, by position. */
    public static int setValue(byte[] name, Value v) {
        return Natives.setAt(Address.ofData(name), name.length, v.src(), v.at());
    }

    /** Writes a list of integers from the array, in one import; it takes effect at once, not on save. */
    public static int setListInts(byte[] name, long[] values) {
        return Natives.setListInts(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /** Writes a list of doubles from the array, in one import; it takes effect at once, not on save. */
    public static int setListDoubles(byte[] name, double[] values) {
        return Natives.setListDoubles(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /** Removes a bin from the record, staged like a write. */
    public static int remove(byte[] name) {
        return Natives.binRemove(Address.ofData(name), name.length);
    }

    /** Writes the staged bins, creating the record when it does not exist. Returns OK or a status. */
    public static int save() {
        return Natives.recExists() != 0 ? Natives.recUpdate() : Natives.recCreate();
    }

    //==========================================================
    // Building collections on the host, by handle.
    //

    /** Starts a list on the host; the handle is what the other calls take. 0 when it could not. */
    public static int newList(int capacity) {
        return Natives.listNew(capacity);
    }

    /** Starts a map on the host, ordered by key whatever order its entries are put in. */
    public static int newMap(int capacity) {
        return Natives.mapNew(capacity);
    }

    /** The handle of a builder, for these calls. */
    public static int handle(ListBuilder list) {
        return list.handle();
    }

    /** The handle of a builder, for these calls. */
    public static int handle(MapBuilder map) {
        return map.handle();
    }

    /** A builder over a list handle these calls made. */
    public static ListBuilder list(int handle) {
        return new ListBuilder(handle);
    }

    /** A builder over a map handle these calls made. */
    public static MapBuilder map(int handle) {
        return new MapBuilder(handle);
    }

    /** A nil value as a handle. */
    public static int nilHandle() {
        return Natives.valNil();
    }

    /** A guest string as a handle, for {@link #mapPut}. */
    public static int strHandle(byte[] s, int len) {
        return Natives.valStr(Address.ofData(s), len);
    }

    /** An integer as a handle, for {@link #mapPut}. */
    public static int intHandle(long i) {
        return Natives.valInt(i);
    }

    /**
     * Packs a built collection into a source the host holds, for reading it back by position; returns the
     * source, whose value sits at offset 0, and which lives until the call ends.
     */
    public static int packHandle(int handle) {
        return Natives.packHandle(handle);
    }

    /** The size of a built collection. */
    public static int size(int handle) {
        return Natives.collSize(handle);
    }

    /** Whether a built map has the string key: 1 or 0, negative for a bad handle. */
    public static int mapHas(int m, byte[] key) {
        return Natives.mapHas(m, Address.ofData(key), key.length);
    }

    /** Whether a built map has the integer key: 1 or 0, negative for a bad handle. */
    public static int mapHas(int m, long key) {
        return Natives.mapHasI(m, key);
    }

    /** Appends an integer to a built list. Returns OK or a status. */
    public static int listPushInt(int list, long value) {
        return Natives.listPushInt(list, value);
    }

    /** Appends a string from the first {@code valueLen} bytes of {@code value}, which are UTF-8. */
    public static int listPushStr(int list, byte[] value, int valueLen) {
        return Natives.listPushStr(list, Address.ofData(value), valueLen);
    }

    /** Appends a built value by its handle: a nested list or map, or a value from one of the handle calls. */
    public static int listPushHandle(int list, int value) {
        return Natives.listPush(list, value);
    }

    /** Appends a stored value, by position. */
    public static int listPushValue(int list, Value v) {
        return Natives.listPushAt(list, v.src(), v.at());
    }

    /**
     * Appends the first {@code n} of {@code vals} in one import: scalars, guest bytes by address, stored values
     * by position, built collections by handle. Returns how many went in.
     */
    public static int listPushValues(int list, Value[] vals, int n) {
        if (n <= 0) {
            return 0;
        }

        n = Math.min(n, BATCH);
        encode(vals, n);

        return Natives.listPushValues(list, Address.ofData(Slots.MANY), n * Slots.SIZE);
    }

    /** Puts an integer under a string key of a built map. Returns OK or a status. */
    public static int mapPutInt(int m, byte[] key, long value) {
        return Natives.mapPutStrInt(m, Address.ofData(key), key.length, value);
    }

    /** Puts a string, the first {@code valueLen} bytes of {@code value} as UTF-8, under a string key. */
    public static int mapPutStr(int m, byte[] key, byte[] value, int valueLen) {
        return Natives.mapPutStrStr(m, Address.ofData(key), key.length, Address.ofData(value), valueLen);
    }

    /** Puts a built value, by its handle, under a string key. */
    public static int mapPutHandle(int m, byte[] key, int value) {
        return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, value);
    }

    /** Puts a stored value, by position, under a string key. */
    public static int mapPutValue(int m, byte[] key, Value v) {
        return Natives.mapPutStrAt(m, Address.ofData(key), key.length, v.src(), v.at());
    }

    /** Puts a stored value, by position, under an integer key. */
    public static int mapPutValue(int m, long key, Value v) {
        return Natives.mapPutIAt(m, key, v.src(), v.at());
    }

    /** Puts a built value under a built key, both by handle: for a key of any kind. */
    public static int mapPut(int m, int key, int value) {
        return Natives.mapPut(m, key, value);
    }

    /**
     * Puts the first {@code n} values of {@code pairs} as entries ({@code pairs[0]} the first key,
     * {@code pairs[1]} its value) in one import, each of any kind {@link #listPushValues} takes. Returns how
     * many entries went in.
     */
    public static int mapPutEntries(int m, Value[] pairs, int n) {
        if (n < 2) {
            return 0;
        }

        n = Math.min(n - n % 2, BATCH);
        encode(pairs, n);

        return Natives.mapPutEntries(m, Address.ofData(Slots.MANY), n * Slots.SIZE);
    }

    // Writes n values into the batch buffer. A guest string's or blob's address is taken here, which is the
    // last moment before the host reads it, and nothing is allocated between here and the import.
    private static void encode(Value[] vals, int n) {
        for (int i = 0; i < n; i++) {
            Slots.value(Slots.MANY, i * Slots.SIZE, vals[i]);
        }
    }

    //==========================================================
    // Results, by bytes and by handle. Udf has the typed ones.
    //

    /** Sets a string as the result: the first {@code len} bytes of {@code s}, which are UTF-8. */
    public static void resultStr(byte[] s, int len) {
        Natives.resultStr(Address.ofData(s), len);
    }

    /** Sets a blob as the result: the first {@code len} bytes of {@code b}. */
    public static void resultBlob(byte[] b, int len) {
        Natives.resultBlob(Address.ofData(b), len);
    }

    /** Sets a GeoJSON document as the result, kept as that kind. */
    public static void resultGeoJson(byte[] doc, int len) {
        Natives.resultGeoJson(Address.ofData(doc), len);
    }

    /** Sets a built list or map as the result, by its handle. Returns OK, or ERR for a handle the host lacks. */
    public static int resultHandle(int handle) {
        return Natives.resultHandle(handle);
    }

    /** Sets a stored value as the result, by position. */
    public static int resultValue(Value v) {
        return Natives.resultAt(v.src(), v.at());
    }
}

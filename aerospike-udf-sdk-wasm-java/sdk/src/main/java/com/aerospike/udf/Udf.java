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
 * Aerospike WASM UDFs in Java, for the server's mod-wamr engine, compiled with TeaVM's linear-memory
 * WebAssembly backend. The contract is the one the Rust SDK (aerospike-udf-sdk-wasm) documents: every read
 * is a 16-byte {@link Value}, a string or collection is read by position from a source the host holds,
 * nothing is msgpack. A UDF is an exported static method {@code int name(int args, int argc)} returning 0 on
 * success, which sets its result with {@code result*} and fails with {@link #fail}. A stream (aggregation)
 * UDF exports {@code <name>_record} and {@code <name>_end}.
 *
 * <p>Names and text cross as UTF-8 bytes: every method taking a {@code String} has a {@code byte[]} twin,
 * and a hot path keeps its names as {@code byte[]} constants ({@link #bytes}) so it allocates nothing.
 *
 * <p>One rule of this backend shapes the code: the address of an array is valid only until the next
 * allocation, since the collector may move it, so an address is always taken inside the expression that
 * hands it to the host and never kept.
 */
public final class Udf {
    private Udf() {
    }

    // Status codes a native returns. Anything at or below NEEDED_BASE means the buffer was too small, and
    // -(rc) - 16 is the size needed.
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

    /** A {@link #log} level: a warning in the server log. */
    public static final int LOG_WARN = 0;
    /** A {@link #log} level: an info line in the server log. */
    public static final int LOG_INFO = 1;
    /** A {@link #log} level: a debug line in the server log. */
    public static final int LOG_DEBUG = 2;

    /** Values cross the boundary at most this many per import. */
    public static final int BATCH = 128;

    private static final int SLOT_SIZE = 16;

    // Scratch the SDK writes through. The 16-byte slot for one value, the batch for up to BATCH of them.
    private static final byte[] SLOT = new byte[SLOT_SIZE];
    private static final byte[] BATCHBUF = new byte[BATCH * SLOT_SIZE];
    private static final byte[] TEXT = new byte[256];

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

    static int le32(byte[] b, int off) {
        return (b[off] & 0xff) | (b[off + 1] & 0xff) << 8 | (b[off + 2] & 0xff) << 16 | (b[off + 3] & 0xff) << 24;
    }

    static long le64(byte[] b, int off) {
        return (le32(b, off) & 0xffffffffL) | (long) le32(b, off + 4) << 32;
    }

    static void putLe32(byte[] b, int off, int v) {
        b[off] = (byte) v;
        b[off + 1] = (byte) (v >>> 8);
        b[off + 2] = (byte) (v >>> 16);
        b[off + 3] = (byte) (v >>> 24);
    }

    static void putLe64(byte[] b, int off, long v) {
        putLe32(b, off, (int) v);
        putLe32(b, off + 4, (int) (v >>> 32));
    }

    //==========================================================
    // Reading.
    //

    /** Reads a bin of any kind in one import. The status is NOT_FOUND for no such bin. */
    public static int bin(byte[] name, Value out) {
        int rc = Natives.binValue(Address.ofData(name), name.length, Address.ofData(SLOT), SLOT_SIZE);

        if (rc == OK) {
            out.read(SLOT, 0);
        }

        return rc;
    }

    /** As {@link #bin(byte[], Value)}, encoding the name on each call. */
    public static int bin(String name, Value out) {
        return bin(bytes(name), out);
    }

    //==========================================================
    // Typed reads: a bin as the type the UDF expects, or an UdfException naming what it found, the way the
    // Rust SDK's `bin(name)?.int()?` reads one.
    //

    private static final Value TYPED = new Value();

    private static Value typed(String name, int kind, String what) {
        int rc = bin(name, TYPED);

        if (rc == NOT_FOUND) {
            throw new UdfException("bin " + name + " is absent");
        }

        if (rc != OK) {
            throw new UdfException("bin " + name + " could not be read");
        }

        if (TYPED.kind != kind) {
            throw new UdfException("bin " + name + " is not " + what);
        }

        return TYPED;
    }

    /** An integer bin; throws {@link UdfException} when it is absent or of another kind. */
    public static long binInt(String name) {
        return typed(name, Value.INT, "an int").payload;
    }

    /**
     * A double bin; throws {@link UdfException} when it is absent or of another kind. An integer bin is not
     * widened.
     */
    public static double binDouble(String name) {
        return typed(name, Value.DOUBLE, "a double").asDouble();
    }

    /** A boolean bin; throws {@link UdfException} when it is absent or of another kind. */
    public static boolean binBool(String name) {
        return typed(name, Value.BOOL, "a bool").payload != 0;
    }

    /**
     * The text of a string bin, copied out; throws {@link UdfException} when it is absent or of another kind.
     */
    public static String binStr(String name) {
        Value v = typed(name, Value.STR, "a string");
        byte[] b = new byte[v.len];

        if (v.len > 0 && bytes(v, b) < 0) {
            throw new UdfException("bin " + name + " could not be read");
        }

        return string(b, b.length);
    }

    /** The bytes of a string, blob, GeoJSON or HLL bin. */
    public static byte[] binBytes(String name) {
        int rc = bin(name, TYPED);

        if (rc == NOT_FOUND) {
            throw new UdfException("bin " + name + " is absent");
        }

        if (rc != OK || !TYPED.isStored() || TYPED.kind == Value.LIST || TYPED.kind == Value.MAP) {
            throw new UdfException("bin " + name + " is not a string or blob");
        }

        byte[] b = new byte[TYPED.len];

        if (TYPED.len > 0 && bytes(TYPED, b) < 0) {
            throw new UdfException("bin " + name + " could not be read");
        }

        return b;
    }

    /** A list bin, as a fresh value to walk or hand on by position. */
    public static Value binList(String name) {
        Value v = typed(name, Value.LIST, "a list");

        return new Value().set(v.kind, v.len, v.payload);
    }

    /** A map bin, as a fresh value to walk, look up in or hand on by position. */
    public static Value binMap(String name) {
        Value v = typed(name, Value.MAP, "a map");

        return new Value().set(v.kind, v.len, v.payload);
    }

    /** A walk over a stored list; see {@link Cursor}. Throws when the value is not a list. */
    public static Cursor<Value> list(Value v) {
        if (v.kind != Value.LIST) {
            throw new UdfException("not a list");
        }

        return Cursor.list(v);
    }

    /** A walk over the list in a bin. */
    public static Cursor<Value> list(String name) {
        return Cursor.list(binList(name));
    }

    /** A walk over a stored map's entries: {@code key} and {@code value} per step. */
    public static Cursor<Cursor.Entry> map(Value v) {
        if (v.kind != Value.MAP) {
            throw new UdfException("not a map");
        }

        return Cursor.map(v);
    }

    /** A walk over the entries of the map in a bin. */
    public static Cursor<Cursor.Entry> map(String name) {
        return Cursor.map(binMap(name));
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
     * Copies a stored value's bytes as the server stores them, msgpack header included, into {@code buf}: the
     * encoding a convergent merge compares when nothing else tells two candidates apart. Returns the length,
     * or a status.
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
        int n = Natives.argsValues(handle, Address.ofData(BATCHBUF), cap * SLOT_SIZE);

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

            out[i].read(BATCHBUF, i * SLOT_SIZE);
        }
    }

    /** Opens a cursor over the list or map at a value's position. 0 when it could not. */
    public static int enter(Value v) {
        return Natives.cursorEnterAt(v.src(), v.at());
    }

    /**
     * Fills {@code out} with the next values of a cursor, {@link #BATCH} at most per import. A map yields key,
     * value, key, value. Returns how many, or a status.
     */
    public static int next(int cursor, Value[] out) {
        int cap = Math.min(out.length, BATCH);
        int n = Natives.cursorValues(cursor, Address.ofData(BATCHBUF), cap * SLOT_SIZE);

        if (n > 0) {
            decode(out, n);
        }

        return n;
    }

    /** Gives a cursor's slot back. */
    public static void close(int cursor) {
        Natives.cursorClose(cursor);
    }

    /** Looks a string key up in the map at a value's position, in one import. */
    public static int get(Value m, byte[] key, Value out) {
        int rc = Natives.collGet(m.src(), m.at(), Address.ofData(key), key.length, Address.ofData(SLOT), SLOT_SIZE);

        if (rc == OK) {
            out.read(SLOT, 0);
        }

        return rc;
    }

    /** As {@link #get(Value, byte[], Value)}, encoding the key on each call. */
    public static int get(Value m, String key, Value out) {
        return get(m, bytes(key), out);
    }

    /** Looks an integer key up in the map at a value's position. */
    public static int get(Value m, long key, Value out) {
        int rc = Natives.collGetI(m.src(), m.at(), key, Address.ofData(SLOT), SLOT_SIZE);

        if (rc == OK) {
            out.read(SLOT, 0);
        }

        return rc;
    }

    /** Copies the record's digest (20 bytes) into {@code buf}. Returns the length, or a status. */
    public static int digest(byte[] buf) {
        return Natives.digest(Address.ofData(buf), buf.length);
    }

    /** The kind of the record's user key, or ABSENT when none was stored. */
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

    /** The record's modification count. */
    public static long generation() {
        return Natives.gen();
    }

    /** The record's time to live, in seconds. */
    public static long ttl() {
        return Natives.ttl();
    }

    /** Whether the record exists. */
    public static boolean exists() {
        return Natives.recExists() != 0;
    }

    /** The server's clock in ms since the Unix epoch, or -1 inside an event handler, where it is refused. */
    public static long now() {
        return Natives.currentTime();
    }

    /**
     * When the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC), which is how
     * the server stores it. Not on the same scale as {@link #now}, which counts from the Unix epoch.
     */
    public static long lastUpdateTime() {
        return Natives.lastUpdate();
    }

    /** The record's size on device, in bytes. */
    public static long recordSize() {
        return Natives.recordSize();
    }

    /** How many bins the record has. */
    public static long binCount() {
        return Natives.numbins();
    }

    private static final byte[] SET_NAME = new byte[64];

    /** The record's set name, or null when it is in no set. */
    public static String setName() {
        int n = Natives.setName(Address.ofData(SET_NAME), SET_NAME.length);

        return n <= 0 ? null : string(SET_NAME, n);
    }

    /**
     * Sets the record's time to live, in seconds, for the next commit: 0 for the namespace default, -1 for
     * never expire, subject to the namespace's rule on TTL reductions.
     */
    public static int setTtl(int seconds) {
        return Natives.setTtl(seconds);
    }

    /** Deletes the record. */
    public static int removeRecord() {
        return Natives.recRemove();
    }

    //==========================================================
    // Writing the record.
    //

    /**
     * Writes an integer bin. Returns OK or a status. Like every write here, it is staged until the record is
     * committed.
     */
    public static int setInt(byte[] name, long value) {
        return Natives.setInt(Address.ofData(name), name.length, value);
    }

    /** As {@link #setInt(byte[], long)}, encoding the name on each call. */
    public static int setInt(String name, long value) {
        return setInt(bytes(name), value);
    }

    /** Writes a double bin. Returns OK or a status. */
    public static int setDouble(byte[] name, double value) {
        return Natives.setDouble(Address.ofData(name), name.length, value);
    }

    /** As {@link #setDouble(byte[], double)}, encoding the name on each call. */
    public static int setDouble(String name, double value) {
        return setDouble(bytes(name), value);
    }

    /**
     * Writes a string bin from the first {@code valueLen} bytes of {@code value}, which are UTF-8. Returns OK
     * or a status.
     */
    public static int setStr(byte[] name, byte[] value, int valueLen) {
        return Natives.setStr(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /** Writes a string bin, encoding the name and the text on each call. */
    public static int setStr(String name, String value) {
        byte[] v = bytes(value);
        return setStr(bytes(name), v, v.length);
    }

    /** Writes a blob bin from the first {@code valueLen} bytes of {@code value}. Returns OK or a status. */
    public static int setBlob(byte[] name, byte[] value, int valueLen) {
        return Natives.setBlob(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /** Writes all of {@code value} as a blob bin, encoding the name on each call. */
    public static int setBlob(String name, byte[] value) {
        return setBlob(bytes(name), value, value.length);
    }

    /** Writes a built list or map to a bin. */
    public static int setHandle(byte[] name, int handle) {
        return Natives.setHandle(Address.ofData(name), name.length, handle);
    }

    /** As {@link #setHandle(byte[], int)}, encoding the name on each call. */
    public static int setHandle(String name, int handle) {
        return setHandle(bytes(name), handle);
    }

    /** Writes a value read from the record to a bin, by position. */
    public static int setValue(byte[] name, Value v) {
        return Natives.setAt(Address.ofData(name), name.length, v.src(), v.at());
    }

    /** As {@link #setValue(byte[], Value)}, encoding the name on each call. */
    public static int setValue(String name, Value v) {
        return setValue(bytes(name), v);
    }

    /** Writes a boolean bin. Returns OK or a status. */
    public static int setBool(byte[] name, boolean value) {
        return Natives.setBool(Address.ofData(name), name.length, value ? 1 : 0);
    }

    /** As {@link #setBool(byte[], boolean)}, encoding the name on each call. */
    public static int setBool(String name, boolean value) {
        return setBool(bytes(name), value);
    }

    /** Writes a nil bin. */
    public static int setNil(byte[] name) {
        return Natives.setNil(Address.ofData(name), name.length);
    }

    /** As {@link #setNil(byte[])}, encoding the name on each call. */
    public static int setNil(String name) {
        return setNil(bytes(name));
    }

    /** Writes a GeoJSON document, kept as the geospatially indexable kind rather than a string. */
    public static int setGeoJson(byte[] name, byte[] doc, int docLen) {
        return Natives.setGeoJson(Address.ofData(name), name.length, Address.ofData(doc), docLen);
    }

    /** As {@link #setGeoJson(byte[], byte[], int)}, encoding the name and the document on each call. */
    public static int setGeoJson(String name, String doc) {
        byte[] d = bytes(doc);
        return setGeoJson(bytes(name), d, d.length);
    }

    /** Writes a list of integers from the array, in one import, with no value built per element. */
    public static int setListInts(byte[] name, long[] values) {
        return Natives.setListInts(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /** As {@link #setListInts(byte[], long[])}, encoding the name on each call. */
    public static int setListInts(String name, long[] values) {
        return setListInts(bytes(name), values);
    }

    /** Writes a list of doubles from the array, in one import, with no value built per element. */
    public static int setListDoubles(byte[] name, double[] values) {
        return Natives.setListDoubles(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /** As {@link #setListDoubles(byte[], double[])}, encoding the name on each call. */
    public static int setListDoubles(String name, double[] values) {
        return setListDoubles(bytes(name), values);
    }

    /** Removes a bin from the record, staged like a write. Returns OK or a status. */
    public static int remove(byte[] name) {
        return Natives.binRemove(Address.ofData(name), name.length);
    }

    /** As {@link #remove(byte[])}, encoding the name on each call. */
    public static int remove(String name) {
        return remove(bytes(name));
    }

    /** Commits the staged writes to an existing record. */
    public static int update() {
        return Natives.recUpdate();
    }

    /** Commits the staged writes as a new record. */
    public static int create() {
        return Natives.recCreate();
    }

    /** Commits the staged writes, creating the record if it does not exist; throws when the host refuses. */
    public static void commit() {
        int rc = exists() ? Natives.recUpdate() : Natives.recCreate();

        if (rc != OK) {
            throw new UdfException("could not commit the record (" + rc + ")");
        }
    }

    /** Deletes the record; throws when the host refuses. */
    public static void delete() {
        int rc = Natives.recRemove();

        if (rc != OK) {
            throw new UdfException("could not delete the record (" + rc + ")");
        }
    }

    //==========================================================
    // Building collections on the host.
    //

    /** Starts a list on the host; the handle is what the other builders take. */
    public static int newList(int capacity) {
        return Natives.listNew(capacity);
    }

    /**
     * Starts a map on the host, as {@link #newList} starts a list. The map is ordered by key, whatever order
     * its entries are put in.
     */
    public static int newMap(int capacity) {
        return Natives.mapNew(capacity);
    }

    /** A nil value as a handle, for a map entry that means "no such bin" in a record resolution. */
    public static int nilHandle() {
        return Natives.valNil();
    }

    /**
     * Packs a built collection into a source the host holds, for reading it back by position ({@link #get},
     * {@link #enter}); the source lives until the call ends.
     */
    public static int packHandle(int handle) {
        return Natives.packHandle(handle);
    }

    /** Whether a built map holds {@code key}. It packs the map, so it is for the occasional check, not a loop. */
    public static boolean mapHasKey(int m, byte[] key) {
        int src = Natives.packHandle(m);

        if (src <= 0) {
            return false;
        }

        return Natives.collGet(src, 0, Address.ofData(key), key.length, Address.ofData(SLOT), SLOT_SIZE) == OK;
    }

    /** As {@link #mapHasKey(int, byte[])}, encoding the key on each call. */
    public static boolean mapHasKey(int m, String key) {
        return mapHasKey(m, bytes(key));
    }

    /** Appends an integer to a built list. Returns OK or a status. */
    public static int listPushInt(int list, long value) {
        return Natives.listPushInt(list, value);
    }

    /**
     * Appends a string from the first {@code valueLen} bytes of {@code value}, which are UTF-8. Returns OK or a
     * status.
     */
    public static int listPushStr(int list, byte[] value, int valueLen) {
        return Natives.listPushStr(list, Address.ofData(value), valueLen);
    }

    /** Appends a string, encoding it on each call. */
    public static int listPushStr(int list, String value) {
        byte[] v = bytes(value);
        return listPushStr(list, v, v.length);
    }

    /**
     * Appends a built value by its handle: a nested list or map, or a value from {@link #nilHandle},
     * {@link #strHandle} or {@link #intHandle}. Returns OK or a status.
     */
    public static int listPushHandle(int list, int value) {
        return Natives.listPush(list, value);
    }

    /** Appends a value read from the record, by position. */
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

        return Natives.listPushValues(list, Address.ofData(BATCHBUF), n * SLOT_SIZE);
    }

    /** Puts an integer under a string key of a built map. Returns OK or a status. */
    public static int mapPutInt(int m, byte[] key, long value) {
        return Natives.mapPutStrInt(m, Address.ofData(key), key.length, value);
    }

    /** As {@link #mapPutInt(int, byte[], long)}, encoding the key on each call. */
    public static int mapPutInt(int m, String key, long value) {
        return mapPutInt(m, bytes(key), value);
    }

    /**
     * Puts a string, the first {@code valueLen} bytes of {@code value} as UTF-8, under a string key. Returns OK
     * or a status.
     */
    public static int mapPutStr(int m, byte[] key, byte[] value, int valueLen) {
        return Natives.mapPutStrStr(m, Address.ofData(key), key.length, Address.ofData(value), valueLen);
    }

    /** Puts a string under a string key, encoding both on each call. */
    public static int mapPutStr(int m, String key, String value) {
        byte[] v = bytes(value);
        return mapPutStr(m, bytes(key), v, v.length);
    }

    /**
     * Puts a built value, by its handle, under a string key: a nested list or map, or nil from
     * {@link #nilHandle}. Returns OK or a status.
     */
    public static int mapPutHandle(int m, byte[] key, int value) {
        return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, value);
    }

    /** As {@link #mapPutHandle(int, byte[], int)}, encoding the key on each call. */
    public static int mapPutHandle(int m, String key, int value) {
        return mapPutHandle(m, bytes(key), value);
    }

    /**
     * Puts any value under a string key: a scalar by value, guest bytes by address, a stored value by position,
     * a built collection by handle.
     */
    public static int mapPutValue(int m, byte[] key, Value v) {
        switch (v.kind) {
        case Value.INT:
            return Natives.mapPutStrInt(m, Address.ofData(key), key.length, v.payload);
        case Value.DOUBLE:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, Natives.valDouble(v.asDouble()));
        case Value.BOOL:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, Natives.valBool((int) v.payload));
        case Value.NIL:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, Natives.valNil());
        case Value.GUEST_STR:
            return Natives.mapPutStrStr(m, Address.ofData(key), key.length, Address.ofData(v.bytes), v.bytesLen);
        case Value.GUEST_BLOB:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length,
                    Natives.valBlob(Address.ofData(v.bytes), v.bytesLen));
        case Value.GUEST_GEOJSON:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length,
                    Natives.valGeoJson(Address.ofData(v.bytes), v.bytesLen));
        case Value.HANDLE:
            return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, (int) v.payload);
        case Value.ABSENT:
            return NOT_FOUND;
        default:
            return Natives.mapPutStrAt(m, Address.ofData(key), key.length, v.src(), v.at());
        }
    }

    /** As {@link #mapPutValue(int, byte[], Value)}, encoding the key on each call. */
    public static int mapPutValue(int m, String key, Value v) {
        return mapPutValue(m, bytes(key), v);
    }

    /** Puts a double under a string key. Returns OK or a status. */
    public static int mapPutDouble(int m, String key, double value) {
        byte[] k = bytes(key);
        return Natives.mapPutStrHandle(m, Address.ofData(k), k.length, Natives.valDouble(value));
    }

    /** Puts a boolean under a string key. Returns OK or a status. */
    public static int mapPutBool(int m, String key, boolean value) {
        byte[] k = bytes(key);
        return Natives.mapPutStrHandle(m, Address.ofData(k), k.length, Natives.valBool(value ? 1 : 0));
    }

    /** Appends a double to a built list. Returns OK or a status. */
    public static int listPushDouble(int list, double value) {
        return Natives.listPush(list, Natives.valDouble(value));
    }

    /** Appends a boolean to a built list. Returns OK or a status. */
    public static int listPushBool(int list, boolean value) {
        return Natives.listPush(list, Natives.valBool(value ? 1 : 0));
    }

    //==========================================================
    // Map entries under an integer key: the Rust SDK's `Map::put_i`.
    //

    /** Puts an integer under an integer key. Returns OK or a status. */
    public static int mapPutIInt(int m, long key, long value) {
        return Natives.mapPut(m, Natives.valInt(key), Natives.valInt(value));
    }

    /** Puts a string under an integer key. Returns OK or a status. */
    public static int mapPutIStr(int m, long key, String value) {
        byte[] v = bytes(value);
        return Natives.mapPut(m, Natives.valInt(key), Natives.valStr(Address.ofData(v), v.length));
    }

    /** Puts a built value, by its handle, under an integer key. Returns OK or a status. */
    public static int mapPutIHandle(int m, long key, int value) {
        return Natives.mapPut(m, Natives.valInt(key), value);
    }

    /** Puts any value under an integer key, the kinds as {@link #mapPutValue} takes them. */
    public static int mapPutIValue(int m, long key, Value v) {
        switch (v.kind) {
        case Value.INT:
            return mapPutIInt(m, key, v.payload);
        case Value.DOUBLE:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valDouble(v.asDouble()));
        case Value.BOOL:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valBool((int) v.payload));
        case Value.NIL:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valNil());
        case Value.GUEST_STR:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valStr(Address.ofData(v.bytes), v.bytesLen));
        case Value.GUEST_BLOB:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valBlob(Address.ofData(v.bytes), v.bytesLen));
        case Value.GUEST_GEOJSON:
            return Natives.mapPut(m, Natives.valInt(key), Natives.valGeoJson(Address.ofData(v.bytes), v.bytesLen));
        case Value.HANDLE:
            return mapPutIHandle(m, key, (int) v.payload);
        case Value.ABSENT:
            return NOT_FOUND;
        default:
            return Natives.mapPutIAt(m, key, v.src(), v.at());
        }
    }

    /** Puts a built value under a built key, both by handle: for a key of any other kind. */
    public static int mapPut(int m, int key, int value) {
        return Natives.mapPut(m, key, value);
    }

    /** A guest string as a handle, for {@link #mapPut}. */
    public static int strHandle(String s) {
        byte[] b = bytes(s);
        return Natives.valStr(Address.ofData(b), b.length);
    }

    /** An integer as a handle, for {@link #mapPut}. */
    public static int intHandle(long i) {
        return Natives.valInt(i);
    }

    /**
     * Puts the first {@code n} pairs of {@code pairs} ({@code pairs[0]}, {@code pairs[1]} is the first) in one
     * import. Returns how many pairs went in.
     */
    public static int mapPutEntries(int m, Value[] pairs, int n) {
        if (n < 2) {
            return 0;
        }

        n = Math.min(n - n % 2, BATCH);
        encode(pairs, n);

        return Natives.mapPutEntries(m, Address.ofData(BATCHBUF), n * SLOT_SIZE);
    }

    // Writes n values into the batch buffer. A guest string's or blob's address is taken here, which is the
    // last moment before the host reads it, and nothing is allocated between here and the import.
    private static void encode(Value[] vals, int n) {
        for (int i = 0; i < n; i++) {
            Value v = vals[i];
            int off = i * SLOT_SIZE;

            putLe32(BATCHBUF, off, v.kind);
            putLe32(BATCHBUF, off + 4, v.len);

            if ((v.kind == Value.GUEST_STR || v.kind == Value.GUEST_BLOB || v.kind == Value.GUEST_GEOJSON)
                    && v.bytes != null) {
                putLe64(BATCHBUF, off + 8, v.bytesLen == 0 ? 0 : Address.ofData(v.bytes).toInt() & 0xffffffffL);
            }
            else {
                putLe64(BATCHBUF, off + 8, v.payload);
            }
        }
    }

    /** The size of a built collection. */
    public static int size(int handle) {
        return Natives.collSize(handle);
    }

    //==========================================================
    // Results and reporting.
    //

    /** Sets nil as the result of the call. Of several results set in one call, the last is the one returned. */
    public static void resultNil() {
        Natives.resultNil();
    }

    /** Sets an integer as the result. */
    public static void resultInt(long v) {
        Natives.resultInt(v);
    }

    /** Sets a double as the result. */
    public static void resultDouble(double v) {
        Natives.resultDouble(v);
    }

    /** Sets a boolean as the result. */
    public static void resultBool(boolean v) {
        Natives.resultBool(v ? 1 : 0);
    }

    /** Sets a string as the result: the first {@code len} bytes of {@code s}, which are UTF-8. */
    public static void resultStr(byte[] s, int len) {
        Natives.resultStr(Address.ofData(s), len);
    }

    /** Sets a string as the result, encoding it on each call. */
    public static void resultStr(String s) {
        byte[] b = bytes(s);
        resultStr(b, b.length);
    }

    /** Sets a blob as the result: the first {@code len} bytes of {@code b}. */
    public static void resultBlob(byte[] b, int len) {
        Natives.resultBlob(Address.ofData(b), len);
    }

    /** Sets all of {@code b} as a blob result. */
    public static void resultBlob(byte[] b) {
        resultBlob(b, b.length);
    }

    /** A GeoJSON document as the result, kept as that kind. */
    public static void resultGeoJson(byte[] doc, int len) {
        Natives.resultGeoJson(Address.ofData(doc), len);
    }

    /** As {@link #resultGeoJson(byte[], int)}, from the text of the document. */
    public static void resultGeoJson(String doc) {
        byte[] d = bytes(doc);
        resultGeoJson(d, d.length);
    }

    /**
     * Sets a built list or map as the result, by its handle. Returns OK, or ERR when the host has no value
     * under the handle.
     */
    public static int resultHandle(int handle) {
        return Natives.resultHandle(handle);
    }

    /** Hands a value read from the record back as the result, built by the server from its bytes. */
    public static int resultValue(Value v) {
        return Natives.resultAt(v.src(), v.at());
    }

    /** Fails the call with a message the client sees, and returns the non-zero status a UDF then returns. */
    public static int fail(String message) {
        byte[] b = bytes(message);
        Natives.fail(Address.ofData(b), b.length);
        return 1;
    }

    /** Writes a line to the server's log at {@link #LOG_WARN}, {@link #LOG_INFO} or {@link #LOG_DEBUG}. */
    public static void log(int level, String message) {
        byte[] b = bytes(message);
        Natives.log(level, Address.ofData(b), b.length);
    }

    //==========================================================
    // Signatures and timers.
    //

    /**
     * The text behind a {@code __udf_sig_<name>} export: a NUL-terminated byte array, kept in a static field.
     * The export returns {@link #signature} of it.
     */
    public static byte[] signatureBytes(String text) {
        byte[] t = bytes(text);
        byte[] out = new byte[t.length + 1];
        System.arraycopy(t, 0, out, 0, t.length);
        return out;
    }

    /** The address the host reads a signature from: valid until the next allocation, which is after the host read it. */
    public static int signature(byte[] text) {
        return Address.ofData(text).toInt();
    }

    /** Runs this set's timer handler on the record in hand {@code delayMs} from now, under {@code name}. */
    public static int timerSet(String name, long delayMs) {
        byte[] b = bytes(name);
        return Natives.timerSet(Address.ofData(b), b.length, delayMs);
    }

    /** Cancels the timer set on the record in hand under {@code name}. Returns OK, or REFUSED. */
    public static int timerCancel(String name) {
        byte[] b = bytes(name);
        return Natives.timerCancel(Address.ofData(b), b.length);
    }

    //==========================================================
    // Walking stored collections.
    //

    /** What {@link #eachStr} calls with every string of a stored list, as UTF-8 bytes in a buffer the SDK owns. */
    public interface StrVisitor {
        void visit(byte[] text, int len, Value v);
    }

    /** Calls {@code fn} with every string of a stored list. */
    public static void eachStr(Value list, StrVisitor fn) {
        if (list.kind != Value.LIST) {
            return;
        }

        int cur = enter(list);

        if (cur <= 0) {
            return;
        }

        Value[] w = new Value[32];

        for (;;) {
            int n = next(cur, w);

            for (int i = 0; i < n; i++) {
                if (w[i].kind == Value.STR) {
                    int k = bytes(w[i], TEXT);

                    if (k >= 0) {
                        fn.visit(TEXT, k, w[i]);
                    }
                }
            }

            if (n < 32) {
                break;
            }
        }

        close(cur);
    }

    /** Whether a stored list of strings holds {@code text}. */
    public static boolean listHasStr(Value list, byte[] text) {
        if (list.kind != Value.LIST) {
            return false;
        }

        int cur = enter(list);

        if (cur <= 0) {
            return false;
        }

        Value[] w = new Value[32];
        boolean found = false;

        outer:
        for (;;) {
            int n = next(cur, w);

            for (int i = 0; i < n; i++) {
                if (w[i].kind == Value.STR && w[i].len == text.length) {
                    int k = bytes(w[i], TEXT);

                    if (k == text.length && equalBytes(TEXT, text, k)) {
                        found = true;
                        break outer;
                    }
                }
            }

            if (n < 32) {
                break;
            }
        }

        close(cur);

        return found;
    }

    /** As {@link #listHasStr(Value, byte[])}, encoding the text on each call. */
    public static boolean listHasStr(Value list, String text) {
        return listHasStr(list, bytes(text));
    }

    /** Whether a stored string value reads as {@code text}. */
    public static boolean strIs(Value v, byte[] text) {
        if (v.kind != Value.STR || v.len != text.length) {
            return false;
        }

        int n = bytes(v, TEXT);

        return n == text.length && equalBytes(TEXT, text, n);
    }

    /** As {@link #strIs(Value, byte[])}, encoding the text on each call. */
    public static boolean strIs(Value v, String text) {
        return strIs(v, bytes(text));
    }

    /** A stored string as a Java string, or "" when it is not one. */
    public static String str(Value v) {
        if (v.kind != Value.STR) {
            return "";
        }

        int n = bytes(v, TEXT);

        return n < 0 ? "" : string(TEXT, n);
    }

    static boolean equalBytes(byte[] a, byte[] b, int n) {
        for (int i = 0; i < n; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }

        return true;
    }

    /** Whether the first {@code alen} bytes of {@code a} are the first {@code blen} bytes of {@code b}. */
    public static boolean equalBytes(byte[] a, byte[] b, int alen, int blen) {
        return alen == blen && equalBytes(a, b, alen);
    }

    //==========================================================
    // Maps by key, for descriptions.
    //

    /** A map entry by string key: a value of ABSENT kind when there is none or {@code m} is not a map. */
    public static Value mapGet(Value m, String key) {
        Value v = new Value();

        if (m.kind == Value.MAP) {
            get(m, key, v);
        }

        return v;
    }

    /**
     * An integer map entry by string key, or {@code dflt} when there is none, it is not an integer, or
     * {@code m} is not a map.
     */
    public static long mapInt(Value m, String key, long dflt) {
        Value v = mapGet(m, key);

        return v.kind == Value.INT ? v.payload : dflt;
    }

    /**
     * A string map entry by string key, or "" when there is none, it is not a string, or {@code m} is not a
     * map. A string over 256 bytes also reads as "".
     */
    public static String mapStr(Value m, String key) {
        return str(mapGet(m, key));
    }

    /**
     * Whether a map entry by string key is the boolean true; false when there is none or it is of another kind.
     */
    public static boolean mapBool(Value m, String key) {
        Value v = mapGet(m, key);

        return v.kind == Value.BOOL && v.payload != 0;
    }
}

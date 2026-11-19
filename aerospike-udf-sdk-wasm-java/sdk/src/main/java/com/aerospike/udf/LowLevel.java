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
 * <li>A cursor from {@link #enter}, a collection from {@link #newList} or {@link #newMap}, and a value from
 * the {@code *Handle} calls each hold a handle until {@link #close} gives it back, in any order, or the
 * call ends: close each on every path out of the loop that made it. A call is bounded by its memory on
 * the server, not by a count of handles.</li>
 * <li>A {@code Value} filled here is overwritten by the next call that fills it; {@link #copy} keeps
 * one.</li>
 * </ul>
 *
 * <p>The calls that read a stored {@link Value} ({@link #bytes(Value, byte[])}, {@link #floats},
 * {@link #storedBytes}, {@link #cmpStored}, {@link #enter}, {@link #get(Value, byte[], Value)},
 * {@link #listGet}) check its kind first and answer {@link #TYPE_MISMATCH} (0 from {@link #enter}) for a
 * value that is not of a kind they read; none takes a scalar's bits for a position. The calls that hand a
 * stored value on ({@link #setValue} and the {@code *Value} puts) do not look: pass them a value for which
 * {@link Value#isStored} is true.
 *
 * <pre>
 * private static final byte[] TAGS = LowLevel.bytes("tags");
 * private static final Value BIN = new Value();
 * private static final Value[] WINDOW = new Value[32];
 *
 * static long countTags() {
 *     if (LowLevel.bin(TAGS, BIN) != LowLevel.OK || !BIN.isList()) {
 *         return 0;
 *     }
 *
 *     int cursor = LowLevel.enter(BIN);
 *     long count = 0;
 *
 *     if (cursor &lt;= 0) {
 *         return 0;
 *     }
 *
 *     for (int n = LowLevel.next(cursor, WINDOW); n &gt; 0; n = LowLevel.next(cursor, WINDOW)) {
 *         count += n;
 *     }
 *
 *     LowLevel.close(cursor);
 *     return count;
 * }
 * </pre>
 */
public final class LowLevel {
    private LowLevel() {
    }

    // Status codes a native returns.
    /** The call succeeded. Every other status is negative. */
    public static final int OK = 0;
    /**
     * The call failed on what it was given, such as a bin name that is empty or longer than 15 bytes, or a
     * handle the host does not hold.
     */
    public static final int ERR = -1;
    /** There is no such bin, key or element. The {@link Value} the call was to fill is left as it was. */
    public static final int NOT_FOUND = -2;
    /**
     * The value is not of a kind the call takes: the bytes of a list asked for, plain bytes written over an
     * HLL bin.
     */
    public static final int TYPE_MISMATCH = -3;
    /**
     * The host refused the operation: a write the record does not accept (a stream's record, a call run as a
     * read, a save the server turns down), a value that would nest too deep or make the call build more than
     * it may, or a call not allowed where it was made.
     */
    public static final int REFUSED = -4;
    /** The buffer was too small: a status at or below this carries the size needed, {@code -(rc) - 16}. */
    public static final int NEEDED_BASE = -16;

    /**
     * Values cross the boundary at most this many per import: 128. It bounds what one {@link #args},
     * {@link #next} or {@link #listPushValues} moves, and {@link #mapPutEntries} moves half as many entries.
     * A longer array is not an error; the call takes this many and is called again for the rest.
     */
    public static final int BATCH = Slots.BATCH;

    //==========================================================
    // Bytes and names.
    //

    /**
     * The UTF-8 bytes of a string, in a new array; keep the result in a {@code static final} for a name used
     * on every call, which is what makes a call here allocate nothing.
     */
    public static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A string from the first {@code len} bytes of {@code b}, decoded as UTF-8: the way back from a buffer
     * {@link #bytes(Value, byte[])} or {@link #keyBytes} filled, {@code len} being the length that call
     * returned.
     */
    public static String string(byte[] b, int len) {
        return new String(b, 0, len, StandardCharsets.UTF_8);
    }

    //==========================================================
    // A Value to reuse. These write to a value the caller owns; a value the SDK handed out is never one.
    //

    /**
     * Makes {@code v} an integer of the guest's own and returns it, to send in a batch or as a result. Like
     * every {@code set*(Value, ...)} here it overwrites what {@code v} was, so use a value this code owns,
     * never one the SDK handed out.
     */
    public static Value setInt(Value v, long i) {
        return v.assign(Value.INT, 0, i);
    }

    /** Makes {@code v} a double of the guest's own and returns it. */
    public static Value setDouble(Value v, double d) {
        return v.assign(Value.DOUBLE, 0, Double.doubleToRawLongBits(d));
    }

    /** Makes {@code v} a boolean of the guest's own and returns it. */
    public static Value setBool(Value v, boolean b) {
        return v.assign(Value.BOOL, 0, b ? 1 : 0);
    }

    /** Makes {@code v} nil and returns it: a nil element or entry in a batch. */
    public static Value setNil(Value v) {
        return v.assign(Value.NIL, 0, 0);
    }

    /**
     * Makes {@code v} a string of the guest's: the first {@code len} bytes of {@code b}, which are UTF-8 and
     * are kept, not copied. The host reads {@code b} when the value is handed to it, so {@code b} must not
     * change before then.
     */
    public static Value setStr(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_STR, b, len);
    }

    /**
     * Makes {@code v} a blob of the guest's: the first {@code len} bytes of {@code b}, kept, not copied. The
     * host reads {@code b} when the value is handed to it, so {@code b} must not change before then.
     */
    public static Value setBlob(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_BLOB, b, len);
    }

    /**
     * Makes {@code v} a GeoJSON document of the guest's: the first {@code len} bytes of {@code b}, its UTF-8
     * text, kept, not copied.
     */
    public static Value setGeoJson(Value v, byte[] b, int len) {
        return v.guest(Value.GUEST_GEOJSON, b, len);
    }

    /**
     * Makes {@code v} a built list or map, by the handle {@link #newList} or {@link #newMap} returned: how a
     * nested collection goes into a batch.
     */
    public static Value setHandle(Value v, int handle) {
        return v.assign(Value.HANDLE, 0, handle);
    }

    /**
     * Makes {@code into} what {@code from} is and returns {@code into}: the way to keep a value across cursor
     * steps, which overwrite the values of the window. Nothing on the host is copied; a stored value's copy
     * reads the same position.
     */
    public static Value copy(Value from, Value into) {
        return into.assign(from);
    }

    //==========================================================
    // Reading.
    //

    /**
     * Reads a bin of any kind into {@code out}, in one import. Returns {@link #OK} with {@code out} filled;
     * {@link #NOT_FOUND} for no such bin, or a name that is not 1 to 15 bytes, with {@code out} left as it
     * was; {@link #ERR} when the host could not hand the value out. Each read of a string, blob, list or map
     * bin takes a handle that is kept until the call ends, since the value reads from it, so read such a bin
     * once per call (per record, in a stream).
     */
    public static int bin(byte[] name, Value out) {
        int rc = Natives.binValue(Address.ofData(name), name.length, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /**
     * Reads a bin of one of an event's records into {@code out}. Returns {@link #OK} with {@code out} filled,
     * or {@link #NOT_FOUND} when that side has no such bin or holds nil in it, with {@code out} left as it
     * was. Inside a handler this is the read that works: {@link #bin} has no record there and finds nothing.
     */
    public static int eventBin(EventRecord rec, byte[] name, Value out) {
        return rec.read(name, out);
    }

    /**
     * Copies the raw bytes of a stored string, blob, GeoJSON or HLL value into {@code buf}. Returns the
     * length copied. When {@code buf} is too small nothing is copied and the status is at or below
     * {@link #NEEDED_BASE}, the length needed being {@code -(rc) - 16}, which is also {@link Value#length}.
     * {@link #TYPE_MISMATCH} for a value of any other kind (a scalar, nil, a list, a map, one of the guest's
     * own), {@link #ERR} for a value that can no longer be read.
     */
    public static int bytes(Value v, byte[] buf) {
        if (!v.hasBytes()) {
            return TYPE_MISMATCH;
        }

        return Natives.cursorBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length);
    }

    /**
     * Copies a stored blob of little-endian 32-bit floats straight into a float array, element for element:
     * the way a vector is read without converting it. Returns the length in bytes, so the count of floats is
     * a quarter of it, or a status as {@link #bytes(Value, byte[])} gives: a blob longer than
     * {@code buf.length * 4} bytes copies nothing and answers the size needed, and a value that is not a
     * stored blob is {@link #TYPE_MISMATCH}.
     */
    public static int floats(Value v, float[] buf) {
        if (v.kind() != Value.BLOB) {
            return TYPE_MISMATCH;
        }

        return Natives.cursorBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length * 4);
    }

    /**
     * Copies a stored value's bytes as the server stores them, msgpack header included, into {@code buf}: any
     * kind, a list or map whole. Returns the length, a status at or below {@link #NEEDED_BASE} when
     * {@code buf} is too small (nothing is copied; {@code -(rc) - 16} is the size), {@link #TYPE_MISMATCH}
     * for a value that is not a stored one, or {@link #ERR} for one that can no longer be read.
     */
    public static int storedBytes(Value v, byte[] buf) {
        if (!v.isStored()) {
            return TYPE_MISMATCH;
        }

        return Natives.collBytesAt(v.src(), v.at(), Address.ofData(buf), buf.length);
    }

    /**
     * Compares two stored values byte by byte, as memcmp would on their stored encodings, without copying
     * either, and makes {@code out} the ordering as an integer: exactly -1, 0 or 1, a value that is a prefix
     * of the other coming first. Returns {@link #OK}, or {@link #TYPE_MISMATCH} with {@code out} left as it
     * was when either is not a stored value or the host cannot compare them. The ordering and the status
     * come apart on purpose: -3 is never an ordering.
     */
    public static int cmpStored(Value a, Value b, Value out) {
        if (!a.isStored() || !b.isStored()) {
            return TYPE_MISMATCH;
        }

        int c = Natives.collCmpAt(a.src(), a.at(), b.src(), b.at());

        if (c < -1 || c > 1) {
            return TYPE_MISMATCH;
        }

        out.assign(Value.INT, 0, c);

        return OK;
    }

    /**
     * Decodes the call's arguments into {@code out}, in one import, filling (or allocating, where an element
     * is null) a Value per slot. {@code handle} and {@code argc} are the two parameters of the export, passed
     * on as they came. Returns how many were filled, bounded by {@code out.length} and {@link #BATCH}; 0 when
     * there are none or they could not be read. A nil argument is a value whose {@link Value#isNil} is true;
     * a string, blob, list or map argument is a stored value read by position.
     *
     * <p>A call may pass more arguments than {@code out} holds, and 128 is the most any call can be read for.
     * The rest are not decoded and nothing here fails: {@code argc} larger than the count returned is how to
     * tell, and a function that does not want to ignore them fails the call on it.
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

    /**
     * Opens a cursor over the stored list or map {@code v}, placed before its first element. Returns the
     * cursor, which is positive, or 0 when it could not: {@code v} is not a stored list or map, or the call's
     * memory on the server has no room for it. Every cursor opened is closed with {@link #close}, on every
     * path out of the walk, in any order.
     */
    public static int enter(Value v) {
        if (v.kind() != Value.LIST && v.kind() != Value.MAP) {
            return 0;
        }

        return Natives.cursorEnterAt(v.src(), v.at());
    }

    /**
     * Fills {@code out} with the next values of a cursor, {@link #BATCH} at most per import, allocating a
     * Value where an element of {@code out} is null. A map yields key, value, key, value, so give it an array
     * of even length. Returns how many were filled, and 0 once the end has been reached; fewer than the
     * smaller of {@code out.length} and {@link #BATCH} means these were the last. A negative status means the
     * handle is not an open cursor. The values are overwritten by the next call: {@link #copy} keeps one. A
     * nested list or map comes as one value, to {@link #enter} in turn.
     */
    public static int next(int cursor, Value[] out) {
        int cap = Math.min(out.length, BATCH);
        int n = Natives.cursorValues(cursor, Address.ofData(Slots.MANY), cap * Slots.SIZE);

        if (n > 0) {
            decode(out, n);
        }

        return n;
    }

    /**
     * Gives a handle back to the host: a cursor, a built list or map once it has been set, put, pushed or
     * returned (each of which takes its own reference), a {@code *Handle} value once it is in. The host reuses
     * the slot, in whatever order handles are closed, and refuses a handle used after its close. Closing one
     * the host still needs (a bin or argument values read from), one already closed, or 0, does nothing. What
     * is left open is given back when the call ends.
     */
    public static void close(int handle) {
        Natives.cursorClose(handle);
    }

    /**
     * Looks a string key up in the stored map {@code m}, in one import; the host walks the entries to it.
     * Returns {@link #OK} with {@code out} filled (a nil entry is a nil value), {@link #NOT_FOUND} for no
     * such key, {@link #TYPE_MISMATCH} when {@code m} is not a stored map. {@code out} is left as it was
     * unless the status is OK. The key may be of any length.
     */
    public static int get(Value m, byte[] key, Value out) {
        if (m.kind() != Value.MAP) {
            return TYPE_MISMATCH;
        }

        int rc = Natives.collGet(m.src(), m.at(), Address.ofData(key), key.length, Address.ofData(Slots.ONE),
                Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /**
     * Looks an integer key up in the stored map {@code m}; the statuses are those of
     * {@link #get(Value, byte[], Value)}.
     */
    public static int get(Value m, long key, Value out) {
        if (m.kind() != Value.MAP) {
            return TYPE_MISMATCH;
        }

        int rc = Natives.collGetI(m.src(), m.at(), key, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    /**
     * The element at {@code index}, counted from 0, of the stored list {@code list}, into {@code out}.
     * Returns {@link #OK}; {@link #NOT_FOUND} past the end, with {@code out} left as it was;
     * {@link #TYPE_MISMATCH} when the value is not a stored list. The host steps over the elements before it,
     * so walking a list this way is quadratic: use a cursor.
     */
    public static int listGet(Value list, int index, Value out) {
        if (list.kind() != Value.LIST) {
            return TYPE_MISMATCH;
        }

        int rc = Natives.listGet(list.src(), list.at(), index, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc == OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    //==========================================================
    // The record in hand, where Record would allocate: its digest and key into buffers the caller owns.
    //

    /**
     * Copies the record's digest (20 bytes) into {@code buf}. Returns 20, or a status at or below
     * {@link #NEEDED_BASE} when {@code buf} is shorter than that.
     */
    public static int digest(byte[] buf) {
        return Natives.digest(Address.ofData(buf), buf.length);
    }

    /**
     * The kind of the record's user key ({@link Value#INT}, {@link Value#STR}, {@link Value#BLOB}), or
     * {@link Value#ABSENT} when no key was stored with the record, which is the server's default.
     */
    public static int keyType() {
        return Natives.keyType();
    }

    /**
     * The user key when {@link #keyType} is {@link Value#INT}; 0 when the key is of another kind or there is
     * none.
     */
    public static long keyInt() {
        return Natives.keyInt();
    }

    /**
     * Copies a string or blob user key into {@code buf}. Returns the length; a status at or below
     * {@link #NEEDED_BASE} when {@code buf} is too small, {@code -(rc) - 16} being the size;
     * {@link #NOT_FOUND} when no key was stored with the record.
     */
    public static int keyBytes(byte[] buf) {
        return Natives.keyBytes(Address.ofData(buf), buf.length);
    }

    //==========================================================
    // Writing the record. Every write is staged until save().
    //

    /**
     * Stages an integer bin, written by {@link #save}. Returns {@link #OK}; {@link #ERR} for a name that is
     * not 1 to 15 bytes; {@link #REFUSED} when the record cannot be written. Every {@code set*} on a name
     * answers these.
     */
    public static int setInt(byte[] name, long value) {
        return Natives.setInt(Address.ofData(name), name.length, value);
    }

    /**
     * Stages a double bin, written by {@link #save}. Returns {@link #OK} or a status, as
     * {@link #setInt(byte[], long)} does.
     */
    public static int setDouble(byte[] name, double value) {
        return Natives.setDouble(Address.ofData(name), name.length, value);
    }

    /**
     * Stages a boolean bin, written by {@link #save}. Returns {@link #OK} or a status, as
     * {@link #setInt(byte[], long)} does.
     */
    public static int setBool(byte[] name, boolean value) {
        return Natives.setBool(Address.ofData(name), name.length, value ? 1 : 0);
    }

    /**
     * Stages a string bin from the first {@code valueLen} bytes of {@code value}, which are UTF-8; the host
     * copies them, so the array is free to reuse. Returns {@link #OK} or a status.
     */
    public static int setStr(byte[] name, byte[] value, int valueLen) {
        return Natives.setStr(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /**
     * Stages a blob bin from the first {@code valueLen} bytes of {@code value}, which the host copies.
     * Returns {@link #OK}, or {@link #TYPE_MISMATCH} when the bin holds an HLL, which plain bytes would
     * silently replace.
     */
    public static int setBlob(byte[] name, byte[] value, int valueLen) {
        return Natives.setBlob(Address.ofData(name), name.length, Address.ofData(value), valueLen);
    }

    /**
     * Stages a GeoJSON bin from the first {@code docLen} bytes of {@code doc}, its UTF-8 text, kept as the
     * geospatially indexable kind rather than a string. The text is checked by {@link #save}, not here.
     */
    public static int setGeoJson(byte[] name, byte[] doc, int docLen) {
        return Natives.setGeoJson(Address.ofData(name), name.length, Address.ofData(doc), docLen);
    }

    /**
     * Stages a bin from a built list or map, by its handle. The handle stays good and the collection can be
     * used again. Returns {@link #OK}, or {@link #ERR} for a handle the host does not hold.
     */
    public static int setHandle(byte[] name, int handle) {
        return Natives.setHandle(Address.ofData(name), name.length, handle);
    }

    /**
     * Stages a bin from a stored value, by position: the server copies it from where it is, and it keeps its
     * kind. Returns {@link #OK}; {@link #ERR} when {@code v} can no longer be read; {@link #REFUSED} when the
     * record cannot be written, and for a plain blob written over an HLL bin.
     */
    public static int setValue(byte[] name, Value v) {
        return Natives.setAt(Address.ofData(name), name.length, v.src(), v.at());
    }

    /**
     * Writes a list bin of integers from the whole array, in one import. It is applied when it is made, not
     * staged: it needs no {@link #save} and is not undone by leaving one out, though a call that fails undoes
     * it with everything else the call wrote. Returns {@link #OK}, {@link #ERR} for a bad name,
     * {@link #REFUSED} when the record cannot be written, which includes a key with no record: nothing is
     * created there.
     */
    public static int setListInts(byte[] name, long[] values) {
        return Natives.setListInts(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /**
     * Writes a list bin of doubles from the whole array, in one import; applied when it is made, as
     * {@link #setListInts} is.
     */
    public static int setListDoubles(byte[] name, double[] values) {
        return Natives.setListDoubles(Address.ofData(name), name.length, Address.ofData(values), values.length * 8);
    }

    /**
     * Stages the removal of a bin, carried out by {@link #save}; a bin the record does not have is not an
     * error. Returns {@link #OK} or a status, as a {@code set*} does.
     */
    public static int remove(byte[] name) {
        return Natives.binRemove(Address.ofData(name), name.length);
    }

    /**
     * Writes the staged bins, creating the record when it does not exist and a bin is staged; with none
     * staged (only a removal or a TTL) on a key with no record it creates nothing and the call fails with
     * "key not found". What a save wrote is undone when the call then fails. Returns {@link #OK}, or
     * {@link #REFUSED} when the record cannot be written or the server turns the write down. After a refused
     * save, the list, map and blob values read from the record before it can no longer be read: read the bins
     * again.
     */
    public static int save() {
        return Natives.recExists() != 0 ? Natives.recUpdate() : Natives.recCreate();
    }

    //==========================================================
    // Building collections on the host, by handle.
    //

    /**
     * Starts a list on the host and returns its handle, which is positive and is what the {@code listPush*}
     * calls, {@link #setHandle(byte[], int)} and {@link #resultHandle} take. 0 when it could not: the call
     * has spent the memory it may hold on the server. {@link #close} gives it back. {@code capacity} is a hint, 0 meaning none; the
     * list grows past it.
     */
    public static int newList(int capacity) {
        return Natives.listNew(capacity);
    }

    /**
     * Starts a map on the host and returns its handle, or 0 when it could not, as {@link #newList} does. The
     * map is ordered by key whatever order its entries are put in, and a put under a key it has replaces the
     * entry.
     */
    public static int newMap(int capacity) {
        return Natives.mapNew(capacity);
    }

    /** The handle of a list builder, for these calls: the bridge from {@link Udf#list()} down to this tier. */
    public static int handle(ListBuilder list) {
        return list.handle();
    }

    /** The handle of a map builder, for these calls: the bridge from {@link Udf#map()} down to this tier. */
    public static int handle(MapBuilder map) {
        return map.handle();
    }

    /**
     * A builder over a list handle {@link #newList} returned: the bridge up to the methods that throw. It
     * does not check the handle; a bad one shows in the first {@code add}.
     */
    public static ListBuilder list(int handle) {
        return new ListBuilder(handle);
    }

    /**
     * A builder over a map handle {@link #newMap} returned, which is also how a map built here becomes an
     * event handler's answer: {@code Resolution.record(LowLevel.map(handle))}. It does not check the handle.
     */
    public static MapBuilder map(int handle) {
        return new MapBuilder(handle);
    }

    /**
     * A nil value as a handle, for {@link #mapPut} and {@link #listPushHandle}. Each call takes a handle until
     * {@link #close}: a loop closes each once it is in, or uses {@link #listPushValues} and
     * {@link #mapPutEntries}, which take the same value in a {@link Value}, with no handle. 0 when the call's
     * memory is spent.
     */
    public static int nilHandle() {
        return Natives.valNil();
    }

    /**
     * A string as a handle, copied from the first {@code len} bytes of {@code s}: a key or value for
     * {@link #mapPut}. Each call takes a handle until {@link #close}; 0 when the call's memory is spent.
     */
    public static int strHandle(byte[] s, int len) {
        return Natives.valStr(Address.ofData(s), len);
    }

    /**
     * An integer as a handle: a key or value for {@link #mapPut}. Each call takes a handle until
     * {@link #close}, so a loop that puts under integer keys uses {@link #mapPutEntries} or
     * {@link #mapPutValue(int, long, Value)}, which take none; 0 when the call's memory is spent.
     */
    public static int intHandle(long i) {
        return Natives.valInt(i);
    }

    /**
     * The size of a built collection: the elements of a list, the entries of a map. A negative status for a
     * handle that is neither.
     */
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

    /**
     * Appends an integer to a built list. Returns {@link #OK}; {@link #ERR} when {@code list} is not a list
     * this call built; {@link #REFUSED} when the call has built as much as it may. The other
     * {@code listPush*} calls answer the same.
     */
    public static int listPushInt(int list, long value) {
        return Natives.listPushInt(list, value);
    }

    /**
     * Appends a string from the first {@code valueLen} bytes of {@code value}, which are UTF-8 and are copied
     * by the host. Returns {@link #OK} or a status, as {@link #listPushInt} does.
     */
    public static int listPushStr(int list, byte[] value, int valueLen) {
        return Natives.listPushStr(list, Address.ofData(value), valueLen);
    }

    /**
     * Appends a built value by its handle: a nested list or map, or a value from {@link #nilHandle},
     * {@link #strHandle} or {@link #intHandle}. The handle stays good. Returns {@link #OK}; {@link #ERR} for
     * a handle the host does not hold; {@link #REFUSED} past 64 levels of nesting or for a list pushed into
     * itself.
     */
    public static int listPushHandle(int list, int value) {
        return Natives.listPush(list, value);
    }

    /**
     * Appends a stored value, by position: the server copies it from where it is. Returns {@link #OK};
     * {@link #ERR} when {@code list} is not a list this call built or {@code v} can no longer be read;
     * {@link #REFUSED} when the list turns the value down.
     */
    public static int listPushValue(int list, Value v) {
        return Natives.listPushAt(list, v.src(), v.at());
    }

    /**
     * Appends the first {@code n} of {@code vals} in one import, {@link #BATCH} at most: scalars, guest bytes
     * by address, stored values by position, built collections by handle. Returns how many went in. Fewer
     * than asked means the value at that index was turned down, with those before it already in the list;
     * {@link #ERR} means {@code list} is not a list this call built.
     */
    public static int listPushValues(int list, Value[] vals, int n) {
        if (n <= 0) {
            return 0;
        }

        n = Math.min(n, BATCH);
        encode(vals, n);

        return Natives.listPushValues(list, Address.ofData(Slots.MANY), n * Slots.SIZE);
    }

    /**
     * Puts an integer under a string key of a built map, replacing any entry the key had. Returns
     * {@link #OK}; {@link #ERR} when {@code m} is not a map this call built; {@link #REFUSED} when the call
     * has built as much as it may. The other {@code mapPut*} calls answer the same.
     */
    public static int mapPutInt(int m, byte[] key, long value) {
        return Natives.mapPutStrInt(m, Address.ofData(key), key.length, value);
    }

    /**
     * Puts a string, the first {@code valueLen} bytes of {@code value} as UTF-8, under a string key; the host
     * copies both. Returns {@link #OK} or a status, as {@link #mapPutInt} does.
     */
    public static int mapPutStr(int m, byte[] key, byte[] value, int valueLen) {
        return Natives.mapPutStrStr(m, Address.ofData(key), key.length, Address.ofData(value), valueLen);
    }

    /**
     * Puts a built value, by its handle, under a string key: a nested list or map. The handle stays good.
     * Returns {@link #OK}; {@link #ERR} for a handle the host does not hold; {@link #REFUSED} past 64 levels
     * of nesting or for a map put into itself.
     */
    public static int mapPutHandle(int m, byte[] key, int value) {
        return Natives.mapPutStrHandle(m, Address.ofData(key), key.length, value);
    }

    /**
     * Puts a stored value, by position, under a string key: the server copies it from where it is. Returns
     * {@link #OK}, or {@link #ERR} when {@code m} is not a map this call built, {@code v} can no longer be
     * read, or the map turns the value down.
     */
    public static int mapPutValue(int m, byte[] key, Value v) {
        return Natives.mapPutStrAt(m, Address.ofData(key), key.length, v.src(), v.at());
    }

    /**
     * Puts a stored value, by position, under an integer key; the statuses are those of
     * {@link #mapPutValue(int, byte[], Value)}.
     */
    public static int mapPutValue(int m, long key, Value v) {
        return Natives.mapPutIAt(m, key, v.src(), v.at());
    }

    /**
     * Puts a built value under a built key, both by handle: for a key of any kind, such as one from
     * {@link #intHandle}. Both handles stay good until closed, and each took a handle, which is why the typed
     * puts are the ones for a loop. Returns {@link #OK}, {@link #ERR} for a handle the host does not hold,
     * {@link #REFUSED} when the map turns the entry down.
     */
    public static int mapPut(int m, int key, int value) {
        return Natives.mapPut(m, key, value);
    }

    /**
     * Puts the first {@code n} values of {@code pairs} as entries ({@code pairs[0]} the first key,
     * {@code pairs[1]} its value) in one import, each of any kind {@link #listPushValues} takes. {@code n}
     * counts values, not entries: an odd one is rounded down, and at most {@link #BATCH} values, 64 entries,
     * go per call. Returns how many entries went in; fewer than {@code n / 2} means the entry at that index
     * was turned down, with those before it already in the map; {@link #ERR} means {@code m} is not a map
     * this call built.
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

    /**
     * Sets a string as the result of the call: the first {@code len} bytes of {@code s}, which are UTF-8 and
     * are copied by the host. It replaces any result set before.
     */
    public static void resultStr(byte[] s, int len) {
        Natives.resultStr(Address.ofData(s), len);
    }

    /** Sets a blob as the result of the call: the first {@code len} bytes of {@code b}, copied by the host. */
    public static void resultBlob(byte[] b, int len) {
        Natives.resultBlob(Address.ofData(b), len);
    }

    /**
     * Sets a GeoJSON document as the result, from the first {@code len} bytes of {@code doc}, its UTF-8 text;
     * kept as that kind, so the client receives a GeoJSON value and not a string.
     */
    public static void resultGeoJson(byte[] doc, int len) {
        Natives.resultGeoJson(Address.ofData(doc), len);
    }

    /**
     * Sets a built list or map as the result, by its handle. Returns {@link #OK}, or {@link #ERR} for a
     * handle the host does not hold, in which case the result is as it was.
     */
    public static int resultHandle(int handle) {
        return Natives.resultHandle(handle);
    }

    /**
     * Sets a stored value as the result, by position: the server copies it from where it is. Returns
     * {@link #OK}, or {@link #ERR} when {@code v} can no longer be read, in which case the result is as it
     * was.
     */
    public static int resultValue(Value v) {
        return Natives.resultAt(v.src(), v.at());
    }
}

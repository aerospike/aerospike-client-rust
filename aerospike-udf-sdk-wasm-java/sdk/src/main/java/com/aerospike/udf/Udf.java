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
 * Aerospike WASM UDFs in Java, for the server's mod-wamr engine, compiled with TeaVM's linear-memory
 * WebAssembly backend. This class is what a UDF reaches for that is not about the record: the clock, the
 * server's log, the builders of lists and maps, and - for a function written in the raw form -
 * the result and the failure. Everything about the record is a method of the {@link Record} a function
 * takes as its first parameter.
 *
 * <pre>
 * {@literal @}Function("increment")
 * static long increment(Record rec, long by) {
 *     long n = rec.getInt("n", 0) + by;
 *
 *     rec.set("n", n);
 *     rec.save();
 *
 *     return n;
 * }
 * </pre>
 *
 * <p>The contract underneath is the one the Rust SDK (aerospike-udf-sdk-wasm) documents: every read is a
 * 16-byte {@link Value}, a string or collection is read by position from a source the host holds, nothing
 * is msgpack. {@link LowLevel} has that contract one call at a time, for code that must allocate nothing.
 */
public final class Udf {
    private Udf() {
    }

    /** A {@link #log} level: a warning in the server log, the most severe a UDF can write. */
    public static final int LOG_WARN = 0;
    /** A {@link #log} level: an info line in the server log. */
    public static final int LOG_INFO = 1;
    /**
     * A {@link #log} level: a debug line in the server log, written only when the server's logging is set to
     * show them.
     */
    public static final int LOG_DEBUG = 2;

    /**
     * The server's clock, in milliseconds since the Unix epoch (1970-01-01 UTC). -1 inside an event handler,
     * which is refused the clock because a merge that read it could not converge. Not on the scale of
     * {@link Record#lastUpdateTime}, which counts from 2010.
     */
    public static long now() {
        return Natives.currentTime();
    }

    /**
     * Writes one line to the server's log at {@link #LOG_WARN}, {@link #LOG_INFO} or {@link #LOG_DEBUG}; any
     * other {@code level} is taken as debug. The message is data to the server: control characters are
     * escaped, so it cannot forge a line, and what does not fit in about 500 bytes is cut. A call may write
     * 16 lines and a module 100 a second; the lines over that are dropped and the server reports how many. In
     * an {@code xdr-merge} handler every line is dropped. It returns nothing and does not fail.
     *
     * <pre>
     * Udf.log(Udf.LOG_INFO, "rebalance: moved " + moved + " of " + total);
     * </pre>
     */
    public static void log(int level, String message) {
        byte[] b = LowLevel.bytes(message);
        Natives.log(level, Address.ofData(b), b.length);
    }

    //==========================================================
    // Builders.
    //

    /**
     * Starts an empty list on the server, to add to and then write to a bin, return, or nest. Throws
     * {@link UdfException} when the call has no handle left for it, or has built as much as it is allowed.
     *
     * <pre>
     * ListBuilder recent = Udf.list().add(page).addAll(rec.getList("recent"));
     *
     * rec.set("recent", recent);
     * rec.save();
     * </pre>
     */
    public static ListBuilder list() {
        return ListBuilder.create(8);
    }

    /**
     * As {@link #list()}, with room for {@code capacity} elements to begin with. It is a hint that saves
     * regrowing: the list grows past it, and the server takes no more than 1024 of it.
     */
    public static ListBuilder list(int capacity) {
        return ListBuilder.create(capacity);
    }

    /**
     * Starts an empty map on the server, to put into and then write to a bin, return, or nest. Throws
     * {@link UdfException} when the call has no handle left for it, or has built as much as it is allowed.
     *
     * <pre>
     * MapBuilder stats = Udf.map().put("visits", visits).put("last", page);
     *
     * rec.set("stats", stats);
     * rec.save();
     * </pre>
     */
    public static MapBuilder map() {
        return MapBuilder.create(8);
    }

    /**
     * As {@link #map()}, with room for {@code capacity} entries to begin with. It is a hint: the map grows
     * past it.
     */
    public static MapBuilder map(int capacity) {
        return MapBuilder.create(capacity);
    }

    //==========================================================
    // The result and the failure, for a function in the raw form. A typed function returns its result and
    // throws its failure, and the generated export calls these.
    //

    /**
     * Sets nil as the result of the call. The {@code result*} methods are for a function in the raw form
     * {@code int name(int args, int argc)}, which sets its result and returns 0; a typed function returns its
     * value and the generated export calls these. Of several results set in one call, the last is the one
     * returned. A call that sets none returns nil.
     *
     * <pre>
     * {@literal @}Function(value = "peek", params = "(bin: str)")
     * static int peek(int args, int argc) {
     *     Value[] a = new Value[1];
     *
     *     if (LowLevel.args(args, argc, a) != 1 || !a[0].isString()) {
     *         return Udf.fail("peek: expected one string argument");
     *     }
     *
     *     Udf.result(Params.record().get(a[0].asString()));       // nil when there is no such bin
     *     return 0;
     * }
     * </pre>
     */
    public static void resultNil() {
        Natives.resultNil();
    }

    /** Sets an integer as the result of the call, replacing any result set before. */
    public static void resultInt(long v) {
        Natives.resultInt(v);
    }

    /** Sets a double as the result of the call, replacing any result set before. */
    public static void resultDouble(double v) {
        Natives.resultDouble(v);
    }

    /** Sets a boolean as the result of the call, replacing any result set before. */
    public static void resultBool(boolean v) {
        Natives.resultBool(v ? 1 : 0);
    }

    /**
     * Sets a string as the result of the call, encoded as UTF-8 and copied by the host. {@code s} must not be
     * null: {@link #resultNil} is the nil result.
     */
    public static void resultString(String s) {
        byte[] b = LowLevel.bytes(s);
        Natives.resultStr(Address.ofData(b), b.length);
    }

    /** Sets all of {@code b} as a blob result; the host copies it, so the array is free to reuse afterwards. */
    public static void resultBlob(byte[] b) {
        Natives.resultBlob(Address.ofData(b), b.length);
    }

    /**
     * Sets a GeoJSON document as the result from its text, kept as that kind: the client receives a GeoJSON
     * value, not a string.
     */
    public static void resultGeoJson(String doc) {
        byte[] d = LowLevel.bytes(doc);
        Natives.resultGeoJson(Address.ofData(d), d.length);
    }

    /**
     * Sets a {@link Value} as the result: a scalar by value, a string or blob of the guest's, a built
     * collection, a stored value by position (the server copies it from where it is); null or absent is nil.
     * A stored value that can no longer be read leaves the result as it was, without an error.
     */
    public static void result(Value v) {
        if (v == null) {
            Natives.resultNil();
            return;
        }

        switch (v.kind()) {
        case Value.INT:
            Natives.resultInt(v.payload());
            break;
        case Value.DOUBLE:
            Natives.resultDouble(v.doubleBits());
            break;
        case Value.BOOL:
            Natives.resultBool(v.payload() != 0 ? 1 : 0);
            break;
        case Value.NIL:
        case Value.ABSENT:
            Natives.resultNil();
            break;
        case Value.GUEST_STR:
            Natives.resultStr(Address.ofData(v.guestBytes()), v.guestLen());
            break;
        case Value.GUEST_BLOB:
            Natives.resultBlob(Address.ofData(v.guestBytes()), v.guestLen());
            break;
        case Value.GUEST_GEOJSON:
            Natives.resultGeoJson(Address.ofData(v.guestBytes()), v.guestLen());
            break;
        case Value.HANDLE:
            Natives.resultHandle((int) v.payload());
            break;
        default:
            Natives.resultAt(v.src(), v.at());
            break;
        }
    }

    /**
     * Fails the call with a message the client sees as the error, and returns the non-zero status a raw UDF
     * then returns: {@code return Udf.fail("no such account")}. The message is cut at 511 bytes. Calling it
     * does not stop the method: the failure is what the server reports once the export returns, whatever
     * result was set. A typed function throws {@link UdfException} instead, and the generated export calls
     * this.
     */
    public static int fail(String message) {
        byte[] b = LowLevel.bytes(message);
        Natives.fail(Address.ofData(b), b.length);
        return 1;
    }
}

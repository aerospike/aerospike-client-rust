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

    /** A {@link #log} level: a warning in the server log. */
    public static final int LOG_WARN = 0;
    /** A {@link #log} level: an info line in the server log. */
    public static final int LOG_INFO = 1;
    /** A {@link #log} level: a debug line in the server log. */
    public static final int LOG_DEBUG = 2;

    /** The server's clock in ms since the Unix epoch, or -1 inside an event handler, where it is refused. */
    public static long now() {
        return Natives.currentTime();
    }

    /** Writes a line to the server's log at {@link #LOG_WARN}, {@link #LOG_INFO} or {@link #LOG_DEBUG}. */
    public static void log(int level, String message) {
        byte[] b = LowLevel.bytes(message);
        Natives.log(level, Address.ofData(b), b.length);
    }

    //==========================================================
    // Builders.
    //

    /** Starts a list on the server, to add to and then write to a bin, return, or nest. */
    public static ListBuilder list() {
        return ListBuilder.create(8);
    }

    /** As {@link #list()}, with room for {@code capacity} elements to begin with. */
    public static ListBuilder list(int capacity) {
        return ListBuilder.create(capacity);
    }

    /** Starts a map on the server, to put into and then write to a bin, return, or nest. */
    public static MapBuilder map() {
        return MapBuilder.create(8);
    }

    /** As {@link #map()}, with room for {@code capacity} entries to begin with. */
    public static MapBuilder map(int capacity) {
        return MapBuilder.create(capacity);
    }

    //==========================================================
    // The result and the failure, for a function in the raw form. A typed function returns its result and
    // throws its failure, and the generated export calls these.
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

    /** Sets a string as the result. */
    public static void resultString(String s) {
        byte[] b = LowLevel.bytes(s);
        Natives.resultStr(Address.ofData(b), b.length);
    }

    /** Sets all of {@code b} as a blob result. */
    public static void resultBlob(byte[] b) {
        Natives.resultBlob(Address.ofData(b), b.length);
    }

    /** Sets a GeoJSON document as the result, kept as that kind. */
    public static void resultGeoJson(String doc) {
        byte[] d = LowLevel.bytes(doc);
        Natives.resultGeoJson(Address.ofData(d), d.length);
    }

    /**
     * Sets a {@link Value} as the result: a scalar by value, a string or blob of the guest's, a built
     * collection, a stored value by position; null or absent is nil.
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

    /** Fails the call with a message the client sees, and returns the non-zero status a raw UDF then returns. */
    public static int fail(String message) {
        byte[] b = LowLevel.bytes(message);
        Natives.fail(Address.ofData(b), b.length);
        return 1;
    }
}

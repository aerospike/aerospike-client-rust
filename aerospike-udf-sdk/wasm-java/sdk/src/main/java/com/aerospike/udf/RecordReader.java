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

/**
 * Reading a record: what the record of a call ({@link Record}) and a record an event hands over
 * ({@link EventRecord}) have in common, so that a helper which only reads bins is written once and works
 * on both.
 *
 * <pre>
 * static long balanceOf(RecordReader rec) {
 *     return rec.getInt("balance", 0);
 * }
 * </pre>
 *
 * <p>A typed reader has three outcomes, and a caller can tell them apart without reading a message: null
 * when the bin is absent (or holds nil), the value when the bin is of that kind, and an
 * {@link UdfException} whose {@link UdfException#status status} is {@link LowLevel#TYPE_MISMATCH} when the
 * bin holds another kind. A wrong type is a bug to hear about, not a value to paper over, so nothing is
 * widened or converted: an integer is not a double, a string is not a blob. A reader given a default
 * answers the default in place of null, and for nothing else. {@link #bin} is the read that insists: it
 * throws ({@link LowLevel#NOT_FOUND}) for a bin that is not there.
 *
 * <p>The scalar readers without a default answer a boxed value, so that it can be null. Unboxing one
 * without a test fails the call with a {@code NullPointerException} when the bin is absent; where that is
 * not what is meant, give a default or use {@code bin(name).asInt()}.
 */
public interface RecordReader {
    /** The bin as a value of whatever kind it is, or null when the record has no such bin. */
    Value get(String name);

    /** The bin as a value of whatever kind it is; throws {@link UdfException} when the record has no such bin. */
    Value bin(String name);

    /** Whether the record has the bin. */
    boolean has(String name);

    /** An integer bin, or null when it is absent; throws when it is of another kind. */
    Long getInt(String name);

    /** An integer bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    long getInt(String name, long dflt);

    /** A double bin, or null when it is absent; throws when it is of another kind. An integer bin is not widened. */
    Double getDouble(String name);

    /** A double bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    double getDouble(String name, double dflt);

    /** A boolean bin, or null when it is absent; throws when it is of another kind. */
    Boolean getBool(String name);

    /** A boolean bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    boolean getBool(String name, boolean dflt);

    /** The text of a string bin, copied out, or null when it is absent; throws when it is of another kind. */
    String getString(String name);

    /** The text of a string bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    String getString(String name, String dflt);

    /** The bytes of a blob bin, copied out, or null when it is absent; throws when it is anything but a blob. */
    byte[] getBlob(String name);

    /** The bytes of a blob bin, or {@code dflt} when it is absent; throws when it is anything but a blob. */
    byte[] getBlob(String name, byte[] dflt);

    /**
     * The raw bytes of a string, blob, GeoJSON or HLL bin, copied out, or null when it is absent; throws when
     * it is a scalar or a collection.
     */
    byte[] getBytes(String name);

    /**
     * A list bin, to index and iterate where it is stored, or null when it is absent; throws when it is of
     * another kind.
     */
    ListValue getList(String name);

    /** A list bin, or {@code dflt} (which may be null) when it is absent; throws when it is of another kind. */
    ListValue getList(String name, ListValue dflt);

    /**
     * A map bin, to look up in and iterate where it is stored, or null when it is absent; throws when it is
     * of another kind.
     */
    MapValue getMap(String name);

    /** A map bin, or {@code dflt} (which may be null) when it is absent; throws when it is of another kind. */
    MapValue getMap(String name, MapValue dflt);

    /** The record's generation: how many times it has been written. */
    long generation();

    /**
     * When the record was last written, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z
     * (Unix milliseconds minus 1 262 304 000 000).
     */
    long lastUpdateTime();
}

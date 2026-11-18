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
 * <p>The readers come in two forms. Without a default, the bin must be there: the reader throws
 * {@link UdfException} naming the bin when it is absent. With a default, an absent bin (or one holding
 * nil) answers the default. Either way a bin of another kind throws: a wrong type is a bug to hear about,
 * not a value to paper over.
 */
public interface RecordReader {
    /** The bin as a value of whatever kind it is, or null when the record has no such bin. */
    Value get(String name);

    /** The bin as a value of whatever kind it is; throws {@link UdfException} when the record has no such bin. */
    Value bin(String name);

    /** Whether the record has the bin. */
    boolean has(String name);

    /** An integer bin; throws {@link UdfException} when it is absent or of another kind. */
    long getInt(String name);

    /** An integer bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    long getInt(String name, long dflt);

    /** A double bin; throws when it is absent or of another kind. An integer bin is not widened. */
    double getDouble(String name);

    /** A double bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    double getDouble(String name, double dflt);

    /** A boolean bin; throws when it is absent or of another kind. */
    boolean getBool(String name);

    /** A boolean bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    boolean getBool(String name, boolean dflt);

    /** The text of a string bin, copied out; throws when it is absent or of another kind. */
    String getString(String name);

    /** The text of a string bin, or {@code dflt} when it is absent; throws when it is of another kind. */
    String getString(String name, String dflt);

    /** The bytes of a string, blob, GeoJSON or HLL bin, copied out; throws when it is absent or a scalar or collection. */
    byte[] getBlob(String name);

    /** The bytes of a string, blob, GeoJSON or HLL bin, or {@code dflt} when it is absent. */
    byte[] getBlob(String name, byte[] dflt);

    /** A list bin, to index and iterate where it is stored; throws when it is absent or of another kind. */
    ListValue getList(String name);

    /** A list bin, or {@code dflt} (which may be null) when it is absent; throws when it is of another kind. */
    ListValue getList(String name, ListValue dflt);

    /** A map bin, to look up in and iterate where it is stored; throws when it is absent or of another kind. */
    MapValue getMap(String name);

    /** A map bin, or {@code dflt} (which may be null) when it is absent; throws when it is of another kind. */
    MapValue getMap(String name, MapValue dflt);

    /** The record's generation: how many times it has been written. */
    long generation();

    /** The record's time to live, in seconds. */
    long ttl();

    /** When the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC). */
    long lastUpdateTime();
}

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
 * The two records of a merge side by side, read bin by bin into a {@link Both}: the Java face of the Rust
 * SDK's {@code Pair}. {@link #of} makes one from the two records a handler is given. A reader answers null
 * for a side that has no such bin or holds nil, and throws {@link UdfException} for a bin of another kind.
 */
public final class Pair {
    /** The current record. */
    public final EventRecord current;
    /** The incoming write. */
    public final EventRecord incoming;

    private Pair(EventRecord current, EventRecord incoming) {
        this.current = current;
        this.incoming = incoming;
    }

    /** The two records a handler is given, side by side: the current record, then the incoming write. */
    public static Pair of(EventRecord current, EventRecord incoming) {
        return new Pair(current, incoming);
    }

    /**
     * Reads an event call's arguments into its two records, {@link #current} the current one and
     * {@link #incoming} the incoming write; null when the arguments are not an xdr-merge. This is what the
     * generated export of a typed handler calls, and what a handler in the raw form
     * {@code int name(int args, int argc)} starts with.
     */
    public static Pair decode(int args, int argc) {
        EventRecord current = new EventRecord();
        EventRecord incoming = new EventRecord();

        return EventRecord.decode(args, argc, current, incoming) ? new Pair(current, incoming) : null;
    }

    /** An integer bin on each side. */
    public Both<Long> getInt(String name) {
        return both(name, current.boxedInt(name), incoming.boxedInt(name));
    }

    /** A double bin on each side. */
    public Both<Double> getDouble(String name) {
        return both(name, current.boxedDouble(name), incoming.boxedDouble(name));
    }

    /** A boolean bin on each side. */
    public Both<Boolean> getBool(String name) {
        return both(name, current.boxedBool(name), incoming.boxedBool(name));
    }

    /** A string bin on each side, its text copied out. */
    public Both<String> getString(String name) {
        return both(name, current.getString(name, null), incoming.getString(name, null));
    }

    /** The bytes of a string, blob, GeoJSON or HLL bin on each side, copied out. */
    public Both<byte[]> getBlob(String name) {
        return both(name, current.getBlob(name, null), incoming.getBlob(name, null));
    }

    /** A list bin on each side, read where it is stored. */
    public Both<ListValue> getList(String name) {
        return both(name, current.getList(name, null), incoming.getList(name, null));
    }

    /** A map bin on each side, read where it is stored. */
    public Both<MapValue> getMap(String name) {
        return both(name, current.getMap(name, null), incoming.getMap(name, null));
    }

    /** The bin as a value on each side; absent and nil sides are null. */
    public Both<Value> bin(String name) {
        return both(name, current.get(name), incoming.get(name));
    }

    /** Whether the incoming side wrote {@code name} later, per bin: the order the bin-lut policy uses. */
    public boolean incomingIsNewer(String name) {
        long cl = current.binLastUpdateTime(name);
        long il = incoming.binLastUpdateTime(name);

        return il > cl || (il == cl && incoming.binSourceId(name) > current.binSourceId(name));
    }

    /**
     * Puts into {@code overrides} every bin either side has that it lacks, from the side that wrote the bin
     * later - which is what the policy would have kept - so a handler merges the bins it knows about and
     * leaves the rest as they would have been. Symmetric: both DCs order the two sides the same way.
     * Returns {@code overrides}.
     */
    public MapBuilder withNewer(MapBuilder overrides) {
        for (int side = 0; side < 2; side++) {
            EventRecord rec = side == 0 ? current : incoming;

            for (Value name : rec.binNames()) {
                String bin = name.asString();

                if (overrides.containsKey(bin)) {
                    continue;
                }

                Value newer = bin(bin).newer();

                if (newer != null) {
                    overrides.put(bin, newer);
                }
            }
        }

        return overrides;
    }

    private <T> Both<T> both(String name, T c, T i) {
        return new Both<>(c, i, current.binLastUpdateTime(name), incoming.binLastUpdateTime(name),
                current.binSourceId(name), incoming.binSourceId(name));
    }
}

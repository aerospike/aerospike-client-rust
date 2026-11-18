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
 * The two sides of a conflict, read bin by bin into a {@link Both}: the Java face of the Rust SDK's
 * {@code Pair}. {@link Conflict#pair} gives one. A reader answers null for a side that has no such bin or
 * holds nil, and throws {@link UdfException} for a bin of another kind.
 */
public final class Pair {
    /** The current record. */
    public final EventRecord local;
    /** The incoming write. */
    public final EventRecord remote;

    Pair(EventRecord local, EventRecord remote) {
        this.local = local;
        this.remote = remote;
    }

    /** An integer bin on each side. */
    public Both<Long> int64(String name) {
        return both(name, local.boxedLong(name), remote.boxedLong(name));
    }

    /** A double bin on each side. */
    public Both<Double> float64(String name) {
        return both(name, local.boxedDouble(name), remote.boxedDouble(name));
    }

    /** A boolean bin on each side. */
    public Both<Boolean> bool(String name) {
        return both(name, local.boxedBoolean(name), remote.boxedBoolean(name));
    }

    /** A string bin on each side, its text copied out. */
    public Both<String> str(String name) {
        return both(name, local.getString(name, null), remote.getString(name, null));
    }

    /** A list bin on each side, read where it is stored. */
    public Both<ListValue> list(String name) {
        return both(name, local.getList(name, null), remote.getList(name, null));
    }

    /** A map bin on each side, read where it is stored. */
    public Both<MapValue> map(String name) {
        return both(name, local.getMap(name, null), remote.getMap(name, null));
    }

    /** The bin as a value on each side; absent and nil sides are null. */
    public Both<Value> bin(String name) {
        return both(name, local.get(name), remote.get(name));
    }

    /** Whether the remote side wrote {@code name} later, per bin: the order the bin-lut policy uses. */
    public boolean remoteNewer(String name) {
        long ll = local.binLut(name);
        long rl = remote.binLut(name);

        return rl > ll || (rl == ll && remote.binSrc(name) > local.binSrc(name));
    }

    /**
     * Puts into {@code overrides} every bin either side has that it lacks, from the side that wrote the bin
     * later - which is what the policy would have kept - so a handler merges the bins it knows about and
     * leaves the rest as they would have been. Symmetric: both DCs order the two sides the same way.
     * Returns {@code overrides}.
     */
    public MapBuilder withNewer(MapBuilder overrides) {
        for (int side = 0; side < 2; side++) {
            EventRecord rec = side == 0 ? local : remote;

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

    private <T> Both<T> both(String name, T l, T r) {
        return new Both<>(l, r, local.binLut(name), remote.binLut(name), local.binSrc(name), remote.binSrc(name));
    }
}

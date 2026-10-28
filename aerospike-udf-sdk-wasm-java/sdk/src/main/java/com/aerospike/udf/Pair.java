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
    public final EventRecord local;
    public final EventRecord remote;

    Pair(EventRecord local, EventRecord remote) {
        this.local = local;
        this.remote = remote;
    }

    public Both<Long> int64(String name) {
        return both(name, local.binInt(name), remote.binInt(name));
    }

    public Both<Double> float64(String name) {
        return both(name, local.binDouble(name), remote.binDouble(name));
    }

    public Both<Boolean> bool(String name) {
        return both(name, local.binBool(name), remote.binBool(name));
    }

    public Both<String> str(String name) {
        return both(name, local.binStr(name), remote.binStr(name));
    }

    /** A list bin on each side, as stored values read by position. */
    public Both<Value> list(String name) {
        return both(name, local.binList(name), remote.binList(name));
    }

    public Both<Value> map(String name) {
        return both(name, local.binMap(name), remote.binMap(name));
    }

    /** The bin as a value on each side; absent and nil sides are null. */
    public Both<Value> bin(String name) {
        return both(name, local.binValue(name), remote.binValue(name));
    }

    /** Whether the remote side wrote {@code name} later, per bin: the order the bin-lut policy uses. */
    public boolean remoteNewer(String name) {
        long ll = local.binLut(name);
        long rl = remote.binLut(name);

        return rl > ll || (rl == ll && remote.binSrc(name) > local.binSrc(name));
    }

    /**
     * Every bin either side has that {@code overrides} (a built map) lacks, from the side that wrote it
     * later - which is what the policy would have kept - so a handler merges the bins it knows about and
     * leaves the rest as they would have been. Symmetric: both DCs order the two sides the same way.
     * Returns {@code overrides}.
     */
    public int withNewer(int overrides) {
        for (int side = 0; side < 2; side++) {
            EventRecord rec = side == 0 ? local : remote;

            Udf.eachStr(rec.bins, (text, len, v) -> {
                byte[] name = new byte[len];
                System.arraycopy(text, 0, name, 0, len);

                if (Udf.mapHasKey(overrides, name)) {
                    return;
                }

                Value newer = bin(Udf.string(name, len)).newer();

                if (newer != null) {
                    Udf.mapPutValue(overrides, name, newer);
                }
            });
        }

        return overrides;
    }

    /** {@link #withNewer(int)} over a built map. */
    public UdfMap withNewer(UdfMap overrides) {
        withNewer(overrides.handle());

        return overrides;
    }

    private <T> Both<T> both(String name, T l, T r) {
        return new Both<>(l, r, local.binLut(name), remote.binLut(name), local.binSrc(name), remote.binSrc(name));
    }
}

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

import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

/**
 * A bin on both sides of a merge, either of which may be missing (null), with the merges a resolver
 * reaches for: the Java face of the Rust SDK's {@code Both<T>}. {@link Pair} builds one per bin.
 */
public final class Both<T> {
    /** The current record's value, or null when it has no such bin or the bin is nil. */
    public final T current;
    /** The incoming write's value, likewise. */
    public final T incoming;
    private final long currentLastUpdateTime;
    private final long incomingLastUpdateTime;
    private final long currentSourceId;
    private final long incomingSourceId;

    Both(T current, T incoming, long currentLastUpdateTime, long incomingLastUpdateTime, long currentSourceId,
            long incomingSourceId) {
        this.current = current;
        this.incoming = incoming;
        this.currentLastUpdateTime = currentLastUpdateTime;
        this.incomingLastUpdateTime = incomingLastUpdateTime;
        this.currentSourceId = currentSourceId;
        this.incomingSourceId = incomingSourceId;
    }

    /**
     * Whether the incoming side wrote this bin later: by the bin's LUT where the server keeps one, the
     * record's otherwise, and on a tie by source id - the order the server's own bin convergence uses, so
     * "newer" means the same on both DCs even when the clocks agreed to the millisecond.
     */
    public boolean incomingIsNewer() {
        return incomingLastUpdateTime > currentLastUpdateTime
                || (incomingLastUpdateTime == currentLastUpdateTime && incomingSourceId > currentSourceId);
    }

    /** The side written later; whichever exists when only one does. */
    public T newer() {
        return incomingIsNewer() ? or(incoming, current) : or(current, incoming);
    }

    /** The side written earlier; whichever exists when only one does. */
    public T older() {
        return incomingIsNewer() ? or(current, incoming) : or(incoming, current);
    }

    /**
     * Both sides as a list of two, the older and then the newer, either of which may be null: the order
     * both DCs agree on, for a merge whose result depends on the order its inputs are taken in.
     */
    public List<T> ordered() {
        return incomingIsNewer() ? Arrays.asList(current, incoming) : Arrays.asList(incoming, current);
    }

    /** The current value when there is one, else the incoming. */
    public T currentOrIncoming() {
        return or(current, incoming);
    }

    /** The incoming value when there is one, else the current. */
    public T incomingOrCurrent() {
        return or(incoming, current);
    }

    /** Combines the two when both exist; the one that exists otherwise. */
    public T merge(BinaryOperator<T> f) {
        if (current != null && incoming != null) {
            return f.apply(current, incoming);
        }

        return or(current, incoming);
    }

    /** The larger of the two; the one that exists when only one does. For a {@code Comparable} T. */
    @SuppressWarnings("unchecked")
    public T max() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) > 0 ? i : c);
    }

    /** The smaller of the two; the one that exists when only one does. For a {@code Comparable} T. */
    @SuppressWarnings("unchecked")
    public T min() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) < 0 ? i : c);
    }

    /** The sum of a {@code Both<Long>}, a missing side counting as zero. */
    public long sumInt() {
        return (current == null ? 0 : (Long) current) + (incoming == null ? 0 : (Long) incoming);
    }

    /** The sum of a {@code Both<Double>}, a missing side counting as zero. */
    public double sumDouble() {
        return (current == null ? 0.0 : (Double) current) + (incoming == null ? 0.0 : (Double) incoming);
    }

    private static <T> T or(T a, T b) {
        return a != null ? a : b;
    }
}

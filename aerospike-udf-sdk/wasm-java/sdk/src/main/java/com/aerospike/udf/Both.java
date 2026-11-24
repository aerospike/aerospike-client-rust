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
 *
 * <pre>
 * Both&lt;Long&gt; visits = pair.getInt("visits");
 *
 * Long merged = visits.merge(Math::max);       // null only when neither side has the bin
 * Long total = visits.sumInt();                // a missing side is 0; null when both are
 *
 * if (visits.current == null) {
 *     // the bin is new to this cluster
 * }
 * </pre>
 *
 * <p>Every merge here answers null when both sides are missing. {@code current} and {@code incoming} are the
 * values themselves, to read when no merge here fits.
 */
public final class Both<T> {
    /** The current record's value, or null when it has no such bin or the bin is nil. */
    public final T current;
    /** The incoming write's value, or null when it has no such bin or the bin is nil. */
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
     * Whether the incoming side wrote this bin later: by the bin's LUT, and on a tie by source id, so
     * "newer" means the same on both DCs even when the clocks agreed to the millisecond.
     */
    public boolean incomingIsNewer() {
        return incomingLastUpdateTime > currentLastUpdateTime
                || (incomingLastUpdateTime == currentLastUpdateTime && incomingSourceId > currentSourceId);
    }

    /**
     * The side written later, by {@link #incomingIsNewer}; whichever exists when only one does, and null when
     * neither does. A missing side never wins over one that exists, however late it was deleted: a merge that
     * must honour deletes asks {@link EventRecord#deleted} of the later side, as {@link Pair#withNewer} does.
     */
    public T newer() {
        return incomingIsNewer() ? or(incoming, current) : or(current, incoming);
    }

    /**
     * The side written earlier, by {@link #incomingIsNewer}; whichever exists when only one does, and null
     * when neither does.
     */
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

    /**
     * The current value when there is one, else the incoming, else null. It prefers this cluster's side, so
     * the two clusters answer differently: not for a merge that has to converge.
     */
    public T currentOrIncoming() {
        return or(current, incoming);
    }

    /**
     * The incoming value when there is one, else the current, else null. It prefers the other cluster's side,
     * so the two clusters answer differently: not for a merge that has to converge.
     */
    public T incomingOrCurrent() {
        return or(incoming, current);
    }

    /**
     * Combines the two with {@code f} when both exist, the current value as its first argument and the
     * incoming as its second; the one that exists otherwise, and null when neither does. {@code f} is not
     * called with a null. For the two clusters to agree it has to give the same result with its arguments
     * swapped.
     */
    public T merge(BinaryOperator<T> f) {
        if (current != null && incoming != null) {
            return f.apply(current, incoming);
        }

        return or(current, incoming);
    }

    /**
     * The larger of the two by {@code compareTo}; the one that exists when only one does, null when neither
     * does. Only for a {@code Comparable} T: {@code Long}, {@code Double}, {@code String}, {@code Boolean}.
     * Called on a {@code Both} of bytes, values, lists or maps it is a bad cast, not a comparison.
     */
    @SuppressWarnings("unchecked")
    public T max() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) > 0 ? i : c);
    }

    /**
     * The smaller of the two by {@code compareTo}; the one that exists when only one does, null when neither
     * does. For a {@code Comparable} T, as {@link #max} is.
     */
    @SuppressWarnings("unchecked")
    public T min() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) < 0 ? i : c);
    }

    /**
     * The sum of a {@code Both<Long>}; the one that exists when only one does, a missing side counting as
     * zero, and null when neither does, as {@link #max} and {@link #min}. It wraps on overflow as {@code long}
     * addition does. Only for a {@code Both<Long>}: on any other it is a bad cast.
     */
    public Long sumInt() {
        if (current == null && incoming == null) {
            return null;
        }

        return (current == null ? 0 : (Long) current) + (incoming == null ? 0 : (Long) incoming);
    }

    /**
     * The sum of a {@code Both<Double>}; the one that exists when only one does, and null when neither does,
     * as {@link #max} and {@link #min}. Only for a {@code Both<Double>}: on any other it is a bad cast.
     */
    public Double sumDouble() {
        if (current == null && incoming == null) {
            return null;
        }

        return (current == null ? 0.0 : (Double) current) + (incoming == null ? 0.0 : (Double) incoming);
    }

    private static <T> T or(T a, T b) {
        return a != null ? a : b;
    }
}

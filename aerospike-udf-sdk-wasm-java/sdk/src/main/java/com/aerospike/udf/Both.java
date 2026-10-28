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

import java.util.function.BinaryOperator;

/**
 * A bin on both sides of a conflict, either of which may be missing (null), with the merges a resolver
 * reaches for: the Java face of the Rust SDK's {@code Both<T>}. {@link Pair} builds one per bin.
 */
public final class Both<T> {
    /** The current record's value, or null when it has no such bin or the bin is nil. */
    public final T local;
    /** The incoming write's value, likewise. */
    public final T remote;
    private final long localLut;
    private final long remoteLut;
    private final long localSrc;
    private final long remoteSrc;

    Both(T local, T remote, long localLut, long remoteLut, long localSrc, long remoteSrc) {
        this.local = local;
        this.remote = remote;
        this.localLut = localLut;
        this.remoteLut = remoteLut;
        this.localSrc = localSrc;
        this.remoteSrc = remoteSrc;
    }

    /**
     * Whether the remote side wrote this bin later: by the bin's LUT where the server keeps one, the
     * record's otherwise, and on a tie by source id - the order the server's own bin convergence uses, so
     * "newer" means the same on both DCs even when the clocks agreed to the millisecond.
     */
    public boolean remoteNewer() {
        return remoteLut > localLut || (remoteLut == localLut && remoteSrc > localSrc);
    }

    /** The side written later; whichever exists when only one does. */
    public T newer() {
        return remoteNewer() ? or(remote, local) : or(local, remote);
    }

    /** The side written earlier; whichever exists when only one does. */
    public T older() {
        return remoteNewer() ? or(local, remote) : or(remote, local);
    }

    /** The local value when there is one, else the remote. */
    public T localOrRemote() {
        return or(local, remote);
    }

    /** The remote value when there is one, else the local. */
    public T remoteOrLocal() {
        return or(remote, local);
    }

    /** Combines the two when both exist; the one that exists otherwise. */
    public T merge(BinaryOperator<T> f) {
        if (local != null && remote != null) {
            return f.apply(local, remote);
        }

        return or(local, remote);
    }

    /** The larger of the two; the one that exists when only one does. For a {@code Comparable} T. */
    @SuppressWarnings("unchecked")
    public T max() {
        return merge((l, r) -> ((Comparable<T>) r).compareTo(l) > 0 ? r : l);
    }

    /** The smaller of the two; the one that exists when only one does. For a {@code Comparable} T. */
    @SuppressWarnings("unchecked")
    public T min() {
        return merge((l, r) -> ((Comparable<T>) r).compareTo(l) < 0 ? r : l);
    }

    /** The sum of a {@code Both<Long>}, a missing side counting as zero. */
    public long sumInt() {
        return (local == null ? 0 : (Long) local) + (remote == null ? 0 : (Long) remote);
    }

    /** The sum of a {@code Both<Double>}, a missing side counting as zero. */
    public double sumDouble() {
        return (local == null ? 0.0 : (Double) local) + (remote == null ? 0.0 : (Double) remote);
    }

    private static <T> T or(T a, T b) {
        return a != null ? a : b;
    }
}

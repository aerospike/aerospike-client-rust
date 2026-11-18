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
 * What an {@code xdr-merge} handler returns: the Java face of the Rust SDK's {@code Resolution}. Keep the
 * current record, apply the incoming write whole, let the namespace's policy decide, remove the record, or
 * the final record.
 */
public final class Resolution {
    private final String verdict;
    private final int bins;
    private final long ttl;

    private Resolution(String verdict, int bins, long ttl) {
        this.verdict = verdict;
        this.bins = bins;
        this.ttl = ttl;
    }

    /** Keep the local record; the incoming write is dropped. */
    public static Resolution keepLocal() {
        return new Resolution("keep-local", 0, -1);
    }

    /** Apply the incoming write whole, as if it had won every bin. */
    public static Resolution takeRemote() {
        return new Resolution("take-remote", 0, -1);
    }

    /** Let the namespace's policy decide, as it would have without the event. */
    public static Resolution policy() {
        return new Resolution("policy", 0, -1);
    }

    /**
     * Remove the record on this cluster. The other cluster decides for itself, so a merge that deletes has
     * to delete on both sides for the two to agree. A record resolution with no bins left in it is not
     * this: it is the handler's failure.
     */
    public static Resolution delete() {
        return new Resolution("delete", 0, -1);
    }

    /**
     * The final record: these bins, whole - a bin not in the map, or set to nil, is deleted - keeping the
     * TTL the incoming write asked for.
     */
    public static Resolution record(MapBuilder bins) {
        return new Resolution("record", bins.handle(), -1);
    }

    /** As {@link #record(MapBuilder)}, with a TTL in seconds: 0 for the namespace default. */
    public static Resolution record(MapBuilder bins, long ttl) {
        return new Resolution("record", bins.handle(), ttl);
    }

    /** Sets the resolution as the result of the call. Called by the generated export; returns the status. */
    public int deliver() {
        if (bins == 0) {
            Udf.resultStr(verdict);
            return 0;
        }

        return Conflict.record(bins, ttl);
    }

    /** The verdict word as the server reads it, such as {@code keep-local} or {@code record}. */
    public String verdict() {
        return verdict;
    }
}

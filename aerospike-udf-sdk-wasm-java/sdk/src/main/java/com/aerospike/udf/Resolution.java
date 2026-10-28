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
 * current record, apply the incoming write whole, let the namespace's policy decide, or the final record.
 */
public final class Resolution extends Answer {
    private Resolution(String verdict, int bins, long ttl) {
        super(verdict, 0, null, bins, ttl);
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
     * The final record: these bins, whole - a bin not in the map, or set to nil, is deleted - keeping the
     * TTL the incoming write asked for.
     */
    public static Resolution record(UdfMap bins) {
        return new Resolution("record", bins.handle(), -1);
    }

    /** As {@link #record(UdfMap)}, with a TTL in seconds: 0 for the namespace default. */
    public static Resolution record(UdfMap bins, long ttl) {
        return new Resolution("record", bins.handle(), ttl);
    }

    @Override
    public int deliver() {
        if (bins == 0) {
            Udf.resultStr(verdict);
            return 0;
        }

        return Conflict.record(bins, ttl);
    }
}

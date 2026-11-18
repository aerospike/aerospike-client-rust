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

/** What a {@code before-write} handler returns: let the write through, refuse it, or replace the record. */
public final class Decision extends Answer {
    private Decision(String verdict, long code, String message, int bins, long ttl) {
        super(verdict, code, message, bins, ttl);
    }

    /** Lets the write through as it is. */
    public static Decision accept() {
        return new Decision("accept", 0, null, 0, -1);
    }

    /** Refuses the write with a result code the client sees and a message. */
    public static Decision reject(long code, String message) {
        return new Decision("reject", code, message, 0, -1);
    }

    /** Writes these bins instead; a bin absent from the map, or nil, is deleted. */
    public static Decision record(MapBuilder bins) {
        return new Decision("record", 0, null, bins.handle(), -1);
    }

    /** As {@link #record(MapBuilder)}, with a TTL in seconds (0 for the namespace default). */
    public static Decision record(MapBuilder bins, long ttl) {
        return new Decision("record", 0, null, bins.handle(), ttl);
    }
}

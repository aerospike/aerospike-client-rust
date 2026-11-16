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

/** What a {@code before-expire} handler returns: let the record expire, or keep it for another TTL. */
public final class Expiry extends Answer {
    private Expiry(String verdict, long ttl) {
        super(verdict, 0, null, 0, ttl);
    }

    /** Lets the record expire. */
    public static Expiry expire() {
        return new Expiry("expire", -1);
    }

    /** Keeps the record, with this many more seconds to live. */
    public static Expiry keep(long ttl) {
        return new Expiry("keep", ttl);
    }
}

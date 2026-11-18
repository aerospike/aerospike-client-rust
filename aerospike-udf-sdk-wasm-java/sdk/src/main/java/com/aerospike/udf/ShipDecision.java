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

/** What a {@code before-ship} handler returns: ship the record, skip it, or ship these bins instead. */
public final class ShipDecision extends Answer {
    private ShipDecision(String verdict, int bins) {
        super(verdict, 0, null, bins, -1);
    }

    /** Ships the record as it is. */
    public static ShipDecision ship() {
        return new ShipDecision("ship", 0);
    }

    /** Does not ship the record. */
    public static ShipDecision skip() {
        return new ShipDecision("skip", 0);
    }

    /** Ships these bins instead of the record. */
    public static ShipDecision record(MapBuilder bins) {
        return new ShipDecision("record", bins.handle());
    }
}

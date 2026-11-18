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

package com.aerospike.udf.examples;

import com.aerospike.udf.Decision;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Events;
import com.aerospike.udf.Gate;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.On;
import com.aerospike.udf.Record;
import com.aerospike.udf.Udf;

/**
 * Guards in Java: the same handlers as aerospike-udf-sdk-wasm/examples/guard - a before-write that refuses
 * a negative balance and stamps every write that gets through, a before-delete lock, a truncate fence, and
 * a timer that counts ticks and re-arms itself up to three.
 */
public final class Guard {
    /** Refuses a negative balance; otherwise stamps the write's source and counts it, keeping every other bin. */
    @On.BeforeWrite("guard_account")
    static Decision guardAccount(EventRecord before, EventRecord after, Events.WriteInfo write) {
        if (after.getLong("balance", 0) < 0) {
            return Decision.reject(22, "balance may not go negative");
        }

        MapBuilder out = Udf.map(4)
                .put("stamped", write.source)
                .put("writes", before.getLong("writes", 0) + 1);

        return Decision.record(after.withBins(out));
    }

    @On.BeforeDelete("guard_delete")
    static Gate guardDelete(EventRecord rec, Events.DeleteInfo delete) {
        if (rec.getBoolean("locked", false)) {
            return Gate.refuse(22, "record is locked");
        }

        return Gate.allow();
    }

    @On.BeforeTruncate("guard_truncate")
    static Gate guardTruncate(Events.TruncateInfo t) {
        if (t.set.startsWith("keep")) {
            return Gate.refuse(22, "this set is protected from truncation");
        }

        return Gate.allow();
    }

    /** The timer handler runs as a record UDF: it takes the record, and may write it. */
    @On.Timer("tick")
    static void tick(Record rec, Events.TimerInfo t) {
        long n = rec.getLong("ticks", 0) + 1;

        rec.set("ticks", n);
        rec.save();

        if (n < 3) {
            Udf.timerSet("tick", 200);
        }
    }
}

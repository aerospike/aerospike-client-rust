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

import com.aerospike.udf.Bin;
import com.aerospike.udf.Decision;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Events;
import com.aerospike.udf.Gate;
import com.aerospike.udf.On;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfMap;

/**
 * Guards in Java: the same handlers as aerospike-udf-sdk-wasm/examples/guard - a before-write that refuses
 * a negative balance and stamps every write that gets through, a before-delete lock, a truncate fence, and
 * a timer that counts ticks and re-arms itself up to three.
 */
public final class Guard {
    private static final Bin TICKS = Bin.of("ticks");

    /** Refuses a negative balance; otherwise stamps the write's source and counts it, keeping every other bin. */
    @On.BeforeWrite("guard_account")
    static Decision guardAccount(EventRecord before, EventRecord after, Events.WriteInfo write) {
        Long balance = after.binInt("balance");

        if (balance != null && balance < 0) {
            return Decision.reject(22, "balance may not go negative");
        }

        Long writes = before.binInt("writes");
        UdfMap out = UdfMap.create(4)
                .put("stamped", write.source)
                .put("writes", (writes == null ? 0 : writes) + 1);

        return Decision.record(after.withBins(out));
    }

    @On.BeforeDelete("guard_delete")
    static Gate guardDelete(EventRecord rec, Events.DeleteInfo delete) {
        Boolean locked = rec.binBool("locked");

        if (locked != null && locked) {
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

    /** The timer handler runs as a record UDF: the record functions work. */
    @On.Timer("tick")
    static void tick(Events.TimerInfo t) {
        long n = TICKS.getLong(0) + 1;

        TICKS.set(n);
        Udf.commit();

        if (n < 3) {
            Udf.timerSet("tick", 200);
        }
    }
}

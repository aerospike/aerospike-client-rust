/*
 * Copyright 2015-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.udf.tutorial;

// ANCHOR: setup
import com.aerospike.udf.Function;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.MapValue;
import com.aerospike.udf.Record;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

public final class Tutorial {
    // ANCHOR_END: setup

    // ANCHOR: visit
    @Function("visit")
    static long visit(Record rec, long by) {
        long n = rec.getInt("visits", 0) + by;    // 0 when the bin is absent

        rec.set("visits", n);                     // staged, not yet written
        rec.save();                               // creates the record when there is none

        return n;
    }
    // ANCHOR_END: visit

    // ANCHOR: withdraw
    @Function("withdraw")
    static long withdraw(Record rec, long amount) {
        long balance = rec.bin("balance").asInt();   // throws when absent or not an integer

        if (amount > balance) {
            throw new UdfException("insufficient funds");
        }

        rec.set("balance", balance - amount);
        rec.save();

        return balance - amount;
    }
    // ANCHOR_END: withdraw

    // ANCHOR: rename
    @Function("rename")
    static boolean rename(Record rec, String name) {
        if (rec.bin("owner").asString().equals(name)) {   // copied out of the record
            return false;
        }

        rec.set("owner", name);
        rec.save();

        return true;
    }
    // ANCHOR_END: rename

    // ANCHOR: total
    @Function("total")
    static long total(Record rec) {
        long sum = 0;

        for (Value payment : rec.bin("history").asList()) {
            sum += payment.asInt();
        }

        return sum;
    }
    // ANCHOR_END: total

    // ANCHOR: limit_for
    @Function("limit_for")
    static long limitFor(Record rec, String kind) {
        return rec.bin("limits").asMap().getInt(kind, 0);   // 0 when the map has no such key
    }
    // ANCHOR_END: limit_for

    // ANCHOR: limits_total
    @Function("limits_total")
    static long limitsTotal(Record rec) {
        long sum = 0;

        for (MapValue.Entry e : rec.bin("limits").asMap()) {
            sum += e.getValue().asInt();
        }

        return sum;
    }
    // ANCHOR_END: limits_total

    // ANCHOR: log_payment
    @Function("log_payment")
    static long logPayment(Record rec, long amount) {
        ListValue stored = rec.bin("history").asList();
        int count = stored.size() + 1;

        rec.set("history", Udf.list(count).addAll(stored).add(amount));   // the stored elements, then the new one
        rec.save();

        return count;
    }
    // ANCHOR_END: log_payment

    // ANCHOR: summary
    @Function("summary")
    static MapBuilder summary(Record rec) {
        if (!rec.has("owner") || !rec.has("balance")) {
            throw new UdfException("no owner or balance");
        }

        return Udf.map(5)
                .put("owner", rec.get("owner"))       // a stored value goes in as it is
                .put("balance", rec.get("balance"))
                .put("generation", rec.generation())
                .put("bins", rec.binCount())
                .put("ttl", rec.ttl());
    }
    // ANCHOR_END: summary

    // ANCHOR: renew
    @Function("renew")
    static long renew(Record rec, long seconds) {
        long n = rec.getInt("renewals", 0) + 1;

        rec.set("renewals", n);
        rec.setTtl((int) seconds);   // takes effect with the save; throws when it is refused
        rec.save();

        return n;
    }
    // ANCHOR_END: renew

    // ANCHOR: clear_note
    @Function("clear_note")
    static void clearNote(Record rec) {
        rec.remove("note");   // staged like a write
        rec.save();
    }
    // ANCHOR_END: clear_note

    // ANCHOR: close
    @Function("close")
    static void close(Record rec) {
        rec.delete();     // no save: the delete is the write
    }
    // ANCHOR_END: close

    private Tutorial() {
    }
}

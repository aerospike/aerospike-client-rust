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
import com.aerospike.udf.Decision;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Events;
import com.aerospike.udf.Expiry;
import com.aerospike.udf.Function;
import com.aerospike.udf.Gate;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.MapValue;
import com.aerospike.udf.On;
import com.aerospike.udf.ReadResult;
import com.aerospike.udf.Record;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

public final class Tutorial {
    // ANCHOR_END: setup

    // ANCHOR: visit
    @Function("visit")
    static long visit(Record rec, long by) {
        long n = rec.getLong("visits", 0) + by;   // 0 when the bin is absent

        rec.set("visits", n);                     // staged, not yet written
        rec.save();                               // creates the record when there is none

        return n;
    }
    // ANCHOR_END: visit

    // ANCHOR: withdraw
    @Function("withdraw")
    static long withdraw(Record rec, long amount) {
        long balance = rec.getLong("balance");    // throws when absent or not an integer

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
        if (rec.getString("owner").equals(name)) {   // copied out of the record
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

        for (Value payment : rec.getList("history")) {
            sum += payment.asLong();
        }

        return sum;
    }
    // ANCHOR_END: total

    // ANCHOR: limit_for
    @Function("limit_for")
    static long limitFor(Record rec, String kind) {
        return rec.getMap("limits").getLong(kind, 0);   // 0 when the map has no such key
    }
    // ANCHOR_END: limit_for

    // ANCHOR: limits_total
    @Function("limits_total")
    static long limitsTotal(Record rec) {
        long sum = 0;

        for (MapValue.Entry e : rec.getMap("limits")) {
            sum += e.getValue().asLong();
        }

        return sum;
    }
    // ANCHOR_END: limits_total

    // ANCHOR: log_payment
    @Function("log_payment")
    static long logPayment(Record rec, long amount) {
        ListValue stored = rec.getList("history");
        int count = stored.size() + 1;

        rec.set("history", Udf.list(count).addAll(stored).add(amount));   // the stored elements, then the new one
        rec.save();

        return count;
    }
    // ANCHOR_END: log_payment

    // ANCHOR: summary
    @Function("summary")
    static MapBuilder summary(Record rec) {
        if (!rec.hasBin("owner") || !rec.hasBin("balance")) {
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
        long n = rec.getLong("renewals", 0) + 1;

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

    // ANCHOR: no_overdraft
    @On.BeforeWrite("no_overdraft")
    static Decision noOverdraft(EventRecord before, EventRecord after, Events.WriteInfo write) {
        if (after.getLong("balance", 0) < 0) {   // 0 when absent or nil
            return Decision.reject(22, "balance may not go negative");
        }

        return Decision.accept();
    }
    // ANCHOR_END: no_overdraft

    // ANCHOR: stamp
    @On.BeforeWrite("stamp")
    static Decision stamp(EventRecord before, EventRecord after, Events.WriteInfo write) {
        MapBuilder out = Udf.map(2)
                .put("writes", before.getLong("writes", 0) + 1)
                .put("source", write.source);

        // Every bin the write produced, with the two above on top.
        return Decision.record(after.withBins(out));
    }
    // ANCHOR_END: stamp

    // ANCHOR: keep_locked
    @On.BeforeDelete("keep_locked")
    static Gate keepLocked(EventRecord record, Events.DeleteInfo delete) {
        if (record.getBoolean("locked", false)) {
            return Gate.refuse(22, "record is locked");
        }

        return Gate.allow();
    }
    // ANCHOR_END: keep_locked

    // ANCHOR: hide_secret
    @On.AfterRead("hide_secret")
    static ReadResult hideSecret(EventRecord record, Events.ReadInfo read) {
        if (!record.hasBin("secret")) {
            return ReadResult.unchanged();
        }

        MapBuilder out = Udf.map(1).putNull("secret");   // nil in an answer: no such bin

        return ReadResult.record(record.withBins(out));
    }
    // ANCHOR_END: hide_secret

    // ANCHOR: keep_marked
    @On.BeforeExpire("keep_marked")
    static Expiry keepMarked(EventRecord record, Events.ExpireInfo expire) {
        if (record.getBoolean("keep", false)) {
            return Expiry.keep(3600);
        }

        return Expiry.expire();
    }
    // ANCHOR_END: keep_marked

    // ANCHOR: tick
    @On.Timer("tick")
    static void tick(Record rec, Events.TimerInfo timer) {
        long n = rec.getLong("ticks", 0) + 1;   // a timer runs as a record UDF: it takes the record

        rec.set("ticks", n);
        rec.save();

        if (n < 3) {
            Udf.timerSet("tick", 200);   // run again in 200 ms
        }
    }
    // ANCHOR_END: tick

    private Tutorial() {
    }
}

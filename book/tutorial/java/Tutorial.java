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
import com.aerospike.udf.Bin;
import com.aerospike.udf.Cursor;
import com.aerospike.udf.Decision;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Events;
import com.aerospike.udf.Expiry;
import com.aerospike.udf.Function;
import com.aerospike.udf.Gate;
import com.aerospike.udf.On;
import com.aerospike.udf.ReadResult;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.UdfList;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

public final class Tutorial {
    private static final Bin VISITS = Bin.of("visits");
    private static final Bin BALANCE = Bin.of("balance");
    private static final Bin OWNER = Bin.of("owner");
    private static final Bin HISTORY = Bin.of("history");
    private static final Bin LIMITS = Bin.of("limits");
    private static final Bin RENEWALS = Bin.of("renewals");
    private static final Bin NOTE = Bin.of("note");
    private static final Bin TICKS = Bin.of("ticks");
    // ANCHOR_END: setup

    // ANCHOR: visit
    @Function("visit")
    static long visit(long by) {
        long n = VISITS.getLong(0) + by;   // 0 when the bin is absent

        VISITS.set(n);                     // staged, not yet written
        Udf.commit();                      // creates the record when there is none

        return n;
    }
    // ANCHOR_END: visit

    // ANCHOR: withdraw
    @Function("withdraw")
    static long withdraw(long amount) {
        long balance = BALANCE.getLong();  // throws when absent or not an integer

        if (amount > balance) {
            throw new UdfException("insufficient funds");
        }

        BALANCE.set(balance - amount);
        Udf.commit();

        return balance - amount;
    }
    // ANCHOR_END: withdraw

    // ANCHOR: rename
    @Function("rename")
    static boolean rename(String name) {
        if (OWNER.getString().equals(name)) {   // copied out of the record
            return false;
        }

        OWNER.set(name);
        Udf.commit();

        return true;
    }
    // ANCHOR_END: rename

    // ANCHOR: total
    @Function("total")
    static long total() {
        long sum = 0;

        for (Value payment : HISTORY.asList()) {
            sum += payment.asLong();
        }

        return sum;
    }
    // ANCHOR_END: total

    // ANCHOR: limit_for
    @Function("limit_for")
    static long limitFor(String kind) {
        Value limits = LIMITS.get();

        if (limits == null) {
            throw new UdfException("bin limits is absent");
        }

        Value limit = new Value();

        if (Udf.get(limits, kind, limit) != Udf.OK) {   // no such key
            return 0;
        }

        return limit.asLong();
    }
    // ANCHOR_END: limit_for

    // ANCHOR: limits_total
    @Function("limits_total")
    static long limitsTotal() {
        long sum = 0;

        for (Cursor.Entry e : LIMITS.asMap()) {
            sum += e.value.asLong();
        }

        return sum;
    }
    // ANCHOR_END: limits_total

    // ANCHOR: log_payment
    @Function("log_payment")
    static long logPayment(long amount) {
        Value stored = HISTORY.get();

        if (stored == null) {
            throw new UdfException("bin history is absent");
        }

        int count = stored.len + 1;

        HISTORY.set(UdfList.create(count).addAll(stored).add(amount));   // the stored elements, then the new one
        Udf.commit();

        return count;
    }
    // ANCHOR_END: log_payment

    // ANCHOR: summary
    @Function("summary")
    static UdfMap summary() {
        Value owner = OWNER.get();
        Value balance = BALANCE.get();

        if (owner == null || balance == null) {
            throw new UdfException("no owner or balance");
        }

        return UdfMap.create(5)
                .put("owner", owner)       // a stored value goes in as it is
                .put("balance", balance)
                .put("generation", Udf.generation())
                .put("bins", Udf.binCount())
                .put("ttl", Udf.ttl());
    }
    // ANCHOR_END: summary

    // ANCHOR: renew
    @Function("renew")
    static long renew(long seconds) {
        long n = RENEWALS.getLong(0) + 1;

        RENEWALS.set(n);

        if (Udf.setTtl((int) seconds) != Udf.OK) {   // takes effect with the commit
            throw new UdfException("the TTL was refused");
        }

        Udf.commit();

        return n;
    }
    // ANCHOR_END: renew

    // ANCHOR: clear_note
    @Function("clear_note")
    static void clearNote() {
        NOTE.setNull();   // a nil write drops the bin
        Udf.commit();
    }
    // ANCHOR_END: clear_note

    // ANCHOR: close
    @Function("close")
    static void close() {
        Udf.delete();     // no commit: the delete is the write
    }
    // ANCHOR_END: close

    // ANCHOR: no_overdraft
    @On.BeforeWrite("no_overdraft")
    static Decision noOverdraft(EventRecord before, EventRecord after, Events.WriteInfo write) {
        Long balance = after.binInt("balance");   // null when absent or nil

        if (balance != null && balance < 0) {
            return Decision.reject(22, "balance may not go negative");
        }

        return Decision.accept();
    }
    // ANCHOR_END: no_overdraft

    // ANCHOR: stamp
    @On.BeforeWrite("stamp")
    static Decision stamp(EventRecord before, EventRecord after, Events.WriteInfo write) {
        Long writes = before.binInt("writes");

        UdfMap out = UdfMap.create(2)
                .put("writes", (writes == null ? 0 : writes) + 1)
                .put("source", write.source);

        // Every bin the write produced, with the two above on top.
        return Decision.record(after.withBins(out));
    }
    // ANCHOR_END: stamp

    // ANCHOR: keep_locked
    @On.BeforeDelete("keep_locked")
    static Gate keepLocked(EventRecord record, Events.DeleteInfo delete) {
        Boolean locked = record.binBool("locked");

        if (locked != null && locked) {
            return Gate.refuse(22, "record is locked");
        }

        return Gate.allow();
    }
    // ANCHOR_END: keep_locked

    // ANCHOR: hide_secret
    @On.AfterRead("hide_secret")
    static ReadResult hideSecret(EventRecord record, Events.ReadInfo read) {
        if (record.binValue("secret") == null) {
            return ReadResult.unchanged();
        }

        UdfMap out = UdfMap.create(1).putNull("secret");   // nil in an answer: no such bin

        return ReadResult.record(record.withBins(out));
    }
    // ANCHOR_END: hide_secret

    // ANCHOR: keep_marked
    @On.BeforeExpire("keep_marked")
    static Expiry keepMarked(EventRecord record, Events.ExpireInfo expire) {
        Boolean keep = record.binBool("keep");

        if (keep != null && keep) {
            return Expiry.keep(3600);
        }

        return Expiry.expire();
    }
    // ANCHOR_END: keep_marked

    // ANCHOR: tick
    @On.Timer("tick")
    static void tick(Events.TimerInfo timer) {
        long n = TICKS.getLong(0) + 1;   // the record API works here

        TICKS.set(n);
        Udf.commit();

        if (n < 3) {
            Udf.timerSet("tick", 200);   // run again in 200 ms
        }
    }
    // ANCHOR_END: tick

    private Tutorial() {
    }
}

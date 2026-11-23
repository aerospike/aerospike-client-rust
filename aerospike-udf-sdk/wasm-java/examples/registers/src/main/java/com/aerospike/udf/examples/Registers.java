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

import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.On;
import com.aerospike.udf.Record;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

/**
 * A record of registers, in Java: every bin is a last-writer-wins register, and the stamps are the
 * server's. The same module as aerospike-udf-sdk/wasm-rust/examples/registers.
 *
 * <p>{@code examples/lwwset} keeps a timestamp per element inside the record, and its own UDFs write
 * them. A namespace with bin convergence ({@code conflict-resolve-writes true}) already keeps one per
 * bin: when the bin was last written, and by which cluster. An xdr-merge handler reads the two with
 * {@link EventRecord#binLastUpdateTime} and {@link EventRecord#binSourceId}, and that pair is a
 * register's stamp: the later write wins, and of two writes in the same millisecond, the one from the
 * cluster with the larger id.
 *
 * <p>A register therefore has no write UDF. A client writes {@code theme}, {@code lang} or {@code email}
 * with a plain put and deletes one by writing nil to it; the server stamps the bin, and
 * {@code merge_settings} takes the later side bin by bin. A deleted bin keeps its stamp
 * ({@link EventRecord#deletedBins} lists it), so a delete that is the later write stays a delete, and one
 * that is the earlier write does not undo what came after it.
 *
 * <p>The server does that much by itself under bin convergence. The handler exists for the one bin that
 * is not a register: {@code seen}, the highest message number this user has read on any device. Its
 * merge is the larger number, whichever side wrote last, which no policy that picks a side can express.
 * It has the module's only write UDF, {@code saw(n)}, which never lowers it: a merge can keep a maximum
 * only if the writes kept it too.
 *
 * <pre>
 * event-set:event=xdr-merge;namespace=testbc;set=settings;module=registers;function=merge_settings;on-failure=abandon
 * </pre>
 */
public final class Registers {
    private static final String SEEN = "seen";

    /** Whether this side deleted the bin and still remembers that it did. */
    private static boolean deleted(EventRecord side, String name) {
        for (Value bin : side.deletedBins()) {
            if (bin.asString().equals(name)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Whether this side has a stamp for the bin: it holds the bin, or lists it as deleted. A side with
     * neither has nothing to say about the bin, which is less than any stamp.
     */
    private static boolean stamped(EventRecord side, String name) {
        return side.has(name) || deleted(side, name);
    }

    /**
     * Whether the incoming write wins the register: only when its stamp is strictly greater. A stamp is
     * when the bin was last written on that side, then by which cluster.
     */
    private static boolean incomingWins(EventRecord current, EventRecord incoming, String name) {
        if (!stamped(incoming, name)) {
            return false;
        }

        if (!stamped(current, name)) {
            return true;
        }

        long ours = current.binLastUpdateTime(name);
        long theirs = incoming.binLastUpdateTime(name);

        if (theirs != ours) {
            return theirs > ours;
        }

        return incoming.binSourceId(name) > current.binSourceId(name);
    }

    /** Decides every bin in one of a side's lists of names that an earlier list has not decided. */
    private static void decide(Iterable<Value> names, EventRecord current, EventRecord incoming, MapBuilder out) {
        for (Value bin : names) {
            String name = bin.asString();

            // `seen` is not a register, and a name both sides know comes round more than once.
            if (name.equals(SEEN) || out.containsKey(name)) {
                continue;
            }

            Value val = (incomingWins(current, incoming, name) ? incoming : current).get(name);

            // A nil in the answer is "no such bin": the winner's delete, kept.
            if (val == null) {
                out.putNil(name);
            }
            else {
                out.put(name, val);
            }
        }
    }

    /** The merge: {@code seen} is the larger number; every other bin is the side that wrote it last. */
    @On.XdrMerge("merge_settings")
    static Resolution mergeSettings(EventRecord current, EventRecord incoming) {
        // Without bin convergence there is no stamp per bin, and a merge that pretended otherwise would
        // decide every bin by the record's time. Failing hands the write to the binding's `on-failure`.
        if (!current.usesBinConvergence()) {
            throw new UdfException("registers need a namespace with conflict-resolve-writes");
        }

        MapBuilder out = Udf.map(16);

        if (current.has(SEEN) || incoming.has(SEEN)) {
            out.put(SEEN, Math.max(current.getInt(SEEN, Long.MIN_VALUE), incoming.getInt(SEEN, Long.MIN_VALUE)));
        }

        decide(current.binNames(), current, incoming, out);
        decide(current.deletedBins(), current, incoming, out);
        decide(incoming.binNames(), current, incoming, out);
        decide(incoming.deletedBins(), current, incoming, out);

        return Resolution.record(out);
    }

    /** Records that message {@code n} was read. Returns the highest number read so far. */
    @Function("saw")
    static long saw(Record rec, long n) {
        long seen = rec.getInt(SEEN, 0);

        if (n > seen) {
            rec.set(SEEN, n);
            rec.save();
        }

        return Math.max(seen, n);
    }
}

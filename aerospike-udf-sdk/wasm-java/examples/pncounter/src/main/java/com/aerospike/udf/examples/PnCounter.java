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
import com.aerospike.udf.MapValue;
import com.aerospike.udf.On;
import com.aerospike.udf.Record;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;

/**
 * A counter that can go down, in Java: the PN-Counter, two grow-only counters glued together, merged
 * across data centres by the xdr-merge event. The same module as
 * aerospike-udf-sdk/wasm-rust/examples/pncounter.
 *
 * <p>The grow-only counter cannot subtract: a decrement would shrink a site's entry, and the per-site
 * maximum would throw the shrink away. So this module never subtracts. The record keeps two maps of
 * {@code site -> amount}, {@code inc} and {@code dec}, and both only ever grow. A site is the cluster's XDR
 * src-id as a decimal string: on the cluster with src-id 1, {@code increment(3)} adds to {@code inc["1"]},
 * {@code decrement(1)} adds to {@code dec["1"]}, and the counter's
 * value is the sum of {@code inc} minus the sum of {@code dec}. The merge is the grow-only counter's, run
 * over each map on its own: for every site, the larger amount.
 *
 * <p>What it cannot do is hold a floor: two sites that each see a stock of one and each take one merge
 * to minus one. A bound is not a property a merge can keep.
 */
public final class PnCounter {
    private static final int MAX_SITES = 64;
    private static final String INC = "inc";
    private static final String DEC = "dec";

    /** The per-site amounts of one map, gathered from one or both sides. */
    private static final class Table {
        final String[] sites = new String[MAX_SITES];
        final long[] counts = new long[MAX_SITES];
        int size;

        int find(String site) {
            for (int i = 0; i < size; i++) {
                if (sites[i].equals(site)) {
                    return i;
                }
            }

            return -1;
        }

        /** Keeps the larger amount for a site. */
        void offer(String site, long count) {
            int i = find(site);

            if (i >= 0) {
                counts[i] = Math.max(counts[i], count);
                return;
            }

            if (size == MAX_SITES) {
                throw new UdfException("too many sites");
            }

            sites[size] = site;
            counts[size] = count;
            size++;
        }

        /** Every entry of a stored map. */
        void offerAll(MapValue map) {
            if (map == null) {
                return;
            }

            for (MapValue.Entry e : map) {
                offer(e.getKey().asString(), e.getValue().asInt());
            }
        }

        /** Adds n to a site's amount, creating the site if it is new. */
        void add(String site, long n) {
            int i = find(site);

            if (i >= 0) {
                counts[i] += n;
            }
            else {
                offer(site, n);
            }
        }

        long total() {
            long t = 0;

            for (int i = 0; i < size; i++) {
                t += counts[i];
            }

            return t;
        }

        /** The amounts as a built map, for a bin or a resolution. */
        MapBuilder toMap() {
            MapBuilder m = Udf.map(size);

            for (int i = 0; i < size; i++) {
                m.put(sites[i], counts[i]);
            }

            return m;
        }
    }

    private static final Table TABLE = new Table();

    /** The merge: for every site of each map, the larger amount. Every other bin is the incoming one's. */
    @On.XdrMerge("merge_pncounter")
    static Resolution mergePnCounter(EventRecord current, EventRecord incoming) {
        MapBuilder out = Udf.map(2);

        for (String bin : new String[] {"inc", "dec"}) {
            TABLE.size = 0;
            TABLE.offerAll(current.getMap(bin, null));
            TABLE.offerAll(incoming.getMap(bin, null));
            out.put(bin, TABLE.toMap());
        }

        return Resolution.record(incoming.withBins(out));
    }

    /** One of the record's maps into the table. */
    private static void load(Record rec, String bin) {
        TABLE.size = 0;
        TABLE.offerAll(rec.getMap(bin, null));
    }

    /** The counter's value: everything added minus everything taken. */
    private static long valueOf(Record rec) {
        load(rec, INC);
        long added = TABLE.total();
        load(rec, DEC);

        return added - TABLE.total();
    }

    /**
     * This cluster's site: its XDR src-id, as the decimal string the amounts are keyed by. The server knows which
     * cluster runs the write, so no client can count under another cluster's entry.
     */
    private static String thisSite() {
        long id = Udf.clusterSrcId();

        if (id <= 0) {
            throw new UdfException("no XDR src-id on this cluster: set src-id in its xdr stanza");
        }

        return Long.toString(id);
    }

    /** Adds n to this cluster's entry of one map and saves; answers the counter's value. */
    private static long grow(Record rec, String bin, long n) {
        if (n < 0) {
            throw new UdfException("the amount must not be negative: an entry only grows");
        }

        String site = thisSite();

        load(rec, bin);
        TABLE.add(site, n);
        rec.set(bin, TABLE.toMap());
        rec.save();

        return valueOf(rec);
    }

    /** Adds n to this cluster's increments, under its src-id. Returns the counter's value. */
    @Function("increment")
    static long increment(Record rec, long n) {
        return grow(rec, INC, n);
    }

    /** Adds n to this cluster's decrements, under its src-id. Returns the counter's value. */
    @Function("decrement")
    static long decrement(Record rec, long n) {
        return grow(rec, DEC, n);
    }

    /** The counter's value: the sum of every site's increments minus the sum of every site's decrements. */
    @Function("value")
    static long value(Record rec) {
        return valueOf(rec);
    }
}

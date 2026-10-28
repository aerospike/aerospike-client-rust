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
import com.aerospike.udf.Conflict;
import com.aerospike.udf.Cursor;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.On;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

/**
 * A counter that can go down, in Java: the PN-Counter, two grow-only counters glued together, merged
 * across data centres by the xdr-merge event. The same module as
 * aerospike-udf-sdk-wasm/examples/pncounter.
 *
 * <p>The grow-only counter cannot subtract: a decrement would shrink a site's entry, and the per-site
 * maximum would throw the shrink away. So this module never subtracts. The record keeps two maps of
 * {@code site -> amount}, {@code inc} and {@code dec}, and both only ever grow: {@code increment("dc1", 3)}
 * adds to {@code inc["dc1"]}, {@code decrement("dc1", 1)} adds to {@code dec["dc1"]}, and the counter's
 * value is the sum of {@code inc} minus the sum of {@code dec}. The merge is the grow-only counter's, run
 * over each map on its own: for every site, the larger amount.
 *
 * <p>What it cannot do is hold a floor: two sites that each see a stock of one and each take one merge
 * to minus one. A bound is not a property a merge can keep.
 */
public final class PnCounter {
    private static final int MAX_SITES = 64;
    private static final int SITE_MAX = 32;
    private static final Bin INC = Bin.of("inc");
    private static final Bin DEC = Bin.of("dec");

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
        void offerAll(Value map) {
            if (map == null) {
                return;
            }

            for (Cursor.Entry e : map.asMap()) {
                offer(e.key.asString(), e.value.asLong());
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
        UdfMap toMap() {
            UdfMap m = UdfMap.create(size);

            for (int i = 0; i < size; i++) {
                m.put(sites[i], counts[i]);
            }

            return m;
        }
    }

    private static final Table TABLE = new Table();

    /** The merge: for every site of each map, the larger amount. Every other bin is the incoming one's. */
    @On.XdrMerge("merge_pncounter")
    static Resolution mergePnCounter(EventRecord current, EventRecord incoming, Conflict conflict) {
        UdfMap out = UdfMap.create(2);

        for (String bin : new String[] {"inc", "dec"}) {
            TABLE.size = 0;
            TABLE.offerAll(current.binMap(bin));
            TABLE.offerAll(incoming.binMap(bin));
            out.put(bin, TABLE.toMap());
        }

        return Resolution.record(incoming.withBins(out));
    }

    /** One of the record's maps into the table. */
    private static void load(Bin bin) {
        TABLE.size = 0;
        TABLE.offerAll(bin.get());
    }

    /** The counter's value: everything added minus everything taken. */
    private static long valueOf() {
        load(INC);
        long added = TABLE.total();
        load(DEC);

        return added - TABLE.total();
    }

    /** Adds n to this site's entry of one map and commits; answers the counter's value. */
    private static long grow(Bin bin, String site, long n) {
        // The Rust module bounds the site name with a Str<32>; the same limit, as a check.
        if (site.length() > SITE_MAX) {
            throw new UdfException("the site name is too long");
        }

        if (n < 0) {
            throw new UdfException("the amount must not be negative: an entry only grows");
        }

        load(bin);
        TABLE.add(site, n);
        bin.set(TABLE.toMap());
        Udf.commit();

        return valueOf();
    }

    /** Adds n to this site's increments. Returns the counter's value. */
    @Function("increment")
    static long increment(String site, long n) {
        return grow(INC, site, n);
    }

    /** Adds n to this site's decrements. Returns the counter's value. */
    @Function("decrement")
    static long decrement(String site, long n) {
        return grow(DEC, site, n);
    }

    /** The counter's value: the sum of every site's increments minus the sum of every site's decrements. */
    @Function("value")
    static long value() {
        return valueOf();
    }
}

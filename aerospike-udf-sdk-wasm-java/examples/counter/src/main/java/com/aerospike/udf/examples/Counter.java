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
 * The smallest CRDT in Java: a grow-only counter, merged across data centres by the xdr-merge event. The
 * same module as aerospike-udf-sdk-wasm/examples/counter.
 *
 * <p>One bin, {@code counts}, holds a map of {@code site -> count}. Each data centre adds only to its own
 * entry ({@code increment("dc1", 3)}), and the counter's value is the sum of every entry ({@code value()}).
 * When a write from one site arrives at the other, {@code merge_counter} keeps, for every site, the larger
 * of the two counts. A site's count only ever grows, so the larger one is always the later one: the merge
 * needs no clock, no LUT and no writer id, and it is commutative, idempotent and associative by
 * construction, which the server's dry run checks on real bytes.
 *
 * <p>The annotations are the exports: {@code @On.XdrMerge} and {@code @Function} make the annotation
 * processor generate {@code CounterExports}, with the exported functions, their signatures and the
 * module's entry point, which the pom names as TeaVM's {@code mainClass}.
 */
public final class Counter {
    private static final int MAX_SITES = 64;
    private static final int SITE_MAX = 32;
    private static final Bin COUNTS = Bin.of("counts");

    /** The per-site counts gathered from one or both sides. */
    private static final String[] sites = new String[MAX_SITES];
    private static final long[] counts = new long[MAX_SITES];
    private static int size;

    private static int find(String site) {
        for (int i = 0; i < size; i++) {
            if (sites[i].equals(site)) {
                return i;
            }
        }

        return -1;
    }

    /** Keeps the larger count for a site. */
    private static void offer(String site, long count) {
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

    /** Every entry of a stored counts map. */
    private static void offerAll(Value map) {
        if (map == null) {
            return;
        }

        for (Cursor.Entry e : map.asMap()) {
            offer(e.key.asString(), e.value.asLong());
        }
    }

    private static long total() {
        long t = 0;

        for (int i = 0; i < size; i++) {
            t += counts[i];
        }

        return t;
    }

    /** The counts as a built map, for a bin or a resolution. */
    private static UdfMap toMap() {
        UdfMap m = UdfMap.create(size);

        for (int i = 0; i < size; i++) {
            m.put(sites[i], counts[i]);
        }

        return m;
    }

    /** The merge: for every site, the larger of the two counts. Every other bin is the incoming one's. */
    @On.XdrMerge("merge_counter")
    static Resolution mergeCounter(EventRecord current, EventRecord incoming, Conflict conflict) {
        size = 0;
        offerAll(current.binMap("counts"));
        offerAll(incoming.binMap("counts"));

        return Resolution.record(incoming.withBins(UdfMap.create(2).put("counts", toMap())));
    }

    /** The record's counts into the table. */
    private static void stored() {
        size = 0;
        offerAll(COUNTS.get());
    }

    /** Adds n to this site's count. Returns the counter's value. */
    @Function("increment")
    static long increment(String site, long n) {
        // The Rust module bounds the site name with a Str<32>; the same limit, as a check.
        if (site.length() > SITE_MAX) {
            throw new UdfException("increment: the site name is too long");
        }

        stored();

        int i = find(site);

        if (i >= 0) {
            counts[i] += n;
        }
        else {
            offer(site, n);
        }

        COUNTS.set(toMap());
        Udf.commit();

        return total();
    }

    /** The counter's value: the sum of every site's count. */
    @Function("value")
    static long value() {
        stored();

        return total();
    }
}

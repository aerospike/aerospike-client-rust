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
import com.aerospike.udf.UdfList;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

/**
 * A set with re-adds, in Java: the LWW-Element-Set, merged across data centres by the xdr-merge event.
 * The same module as aerospike-udf-sdk-wasm/examples/lwwset.
 *
 * <p>Two map bins, {@code addedAt} and {@code removedAt}, hold a timestamp per tag, the server's clock in
 * milliseconds at the write. A tag is a member when its add is later than its removal; at equal stamps
 * the removal wins. Adding a tag back after a removal is just a later stamp. The merge is the
 * PN-Counter's: for every tag of each map, the larger stamp. The merge never reads the clock (the server
 * refuses it inside a handler); only the write UDFs stamp, bumping past the tag's last stamp when the
 * clock has not moved.
 */
public final class LwwSet {
    private static final int MAX_TAGS = 64;
    private static final int TAG_MAX = 32;
    private static final Bin ADDED_AT = Bin.of("addedAt");
    private static final Bin REMOVED_AT = Bin.of("removedAt");

    /** The stamps of one map, gathered from one or both sides. */
    private static final class Stamps {
        final String[] tags = new String[MAX_TAGS];
        final long[] at = new long[MAX_TAGS];
        int size;

        int find(String tag) {
            for (int i = 0; i < size; i++) {
                if (tags[i].equals(tag)) {
                    return i;
                }
            }

            return -1;
        }

        /** The tag's stamp, or 0 for none. */
        long get(String tag) {
            int i = find(tag);

            return i >= 0 ? at[i] : 0;
        }

        /** Keeps the later stamp for a tag. */
        void offer(String tag, long stamp) {
            int i = find(tag);

            if (i >= 0) {
                at[i] = Math.max(at[i], stamp);
                return;
            }

            if (size == MAX_TAGS) {
                throw new UdfException("too many tags");
            }

            tags[size] = tag;
            at[size] = stamp;
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

        UdfMap toMap() {
            UdfMap m = UdfMap.create(Math.max(size, 1));

            for (int i = 0; i < size; i++) {
                m.put(tags[i], at[i]);
            }

            return m;
        }
    }

    private static final Stamps ADDED = new Stamps();
    private static final Stamps REMOVED = new Stamps();

    /** The merge: for every tag of each map, the later stamp. Every other bin is the incoming one's. */
    @On.XdrMerge("merge_lwwset")
    static Resolution mergeLwwSet(EventRecord current, EventRecord incoming, Conflict conflict) {
        UdfMap out = UdfMap.create(2);

        for (String bin : new String[] {"addedAt", "removedAt"}) {
            ADDED.size = 0;
            ADDED.offerAll(current.binMap(bin));
            ADDED.offerAll(incoming.binMap(bin));
            out.put(bin, ADDED.toMap());
        }

        return Resolution.record(incoming.withBins(out));
    }

    private static void load() {
        ADDED.size = 0;
        ADDED.offerAll(ADDED_AT.get());
        REMOVED.size = 0;
        REMOVED.offerAll(REMOVED_AT.get());
    }

    /** Whether a tag is a member: added, and later than removed. A tie goes to the removal. */
    private static boolean isMember(String tag) {
        long a = ADDED.get(tag);

        return a > 0 && a > REMOVED.get(tag);
    }

    /** The members, in order, as a built list. */
    private static UdfList membersOf() {
        load();

        // The stored map comes out key-ordered, but a tag added on this site sits at the end: pick the
        // smallest remaining each time, so the answer is sorted.
        boolean[] done = new boolean[ADDED.size];
        UdfList l = UdfList.create(Math.max(ADDED.size, 1));

        for (int k = 0; k < ADDED.size; k++) {
            int next = -1;

            for (int i = 0; i < ADDED.size; i++) {
                if (!done[i] && (next < 0 || ADDED.tags[i].compareTo(ADDED.tags[next]) < 0)) {
                    next = i;
                }
            }

            done[next] = true;

            if (isMember(ADDED.tags[next])) {
                l.add(ADDED.tags[next]);
            }
        }

        return l;
    }

    /** Stamps the tag in one of the maps and commits: the clock, or one past the tag's last stamp. */
    private static void stamp(Bin bin, Stamps into, String tag) {
        // The Rust module bounds a tag with a Str<32>; the same limit, as a check.
        if (tag.length() > TAG_MAX) {
            throw new UdfException("the tag is too long");
        }

        load();

        long last = Math.max(ADDED.get(tag), REMOVED.get(tag));
        long at = Math.max(Udf.now(), last + 1);

        into.offer(tag, at);
        bin.set(into.toMap());
        Udf.commit();
    }

    /** Adds a tag, or adds it back. Returns the members. */
    @Function("add")
    static UdfList add(String tag) {
        stamp(ADDED_AT, ADDED, tag);

        return membersOf();
    }

    /** Removes a tag that is a member. Returns the members. */
    @Function("remove")
    static UdfList remove(String tag) {
        load();

        if (!isMember(tag)) {
            throw new UdfException("not a member");
        }

        stamp(REMOVED_AT, REMOVED, tag);

        return membersOf();
    }

    /** The members, in order. */
    @Function("members")
    static UdfList members() {
        return membersOf();
    }
}

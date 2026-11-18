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

import com.aerospike.udf.Conflict;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.ListBuilder;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.On;
import com.aerospike.udf.Record;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

/**
 * A set with removals, in Java: the 2P-Set, merged across data centres by the xdr-merge event. The same
 * module as aerospike-udf-sdk-wasm/examples/twopset.
 *
 * <p>Two list bins, {@code added} and {@code removed}, both only ever grow; a tag is a member when it is in
 * {@code added} and not in {@code removed}; the merge takes the union of each. A tag in {@code removed} is
 * a tombstone: it beats a concurrent add on the other side, and it stays forever, which is why a removed
 * tag can never come back. Both lists are kept sorted and without duplicates, so the same set is always
 * the same bytes.
 */
public final class TwoPSet {
    private static final int MAX_TAGS = 64;
    private static final int TAG_MAX = 32;
    private static final String ADDED = "added";
    private static final String REMOVED = "removed";

    /** A sorted set of tags with no duplicates, gathered from one or both sides. */
    private static final class Tags {
        final String[] tags = new String[MAX_TAGS];
        int size;

        /** Where the tag is (non-negative) or where it would go (-(index) - 1). */
        int position(String tag) {
            for (int i = 0; i < size; i++) {
                int c = tags[i].compareTo(tag);

                if (c == 0) {
                    return i;
                }

                if (c > 0) {
                    return -i - 1;
                }
            }

            return -size - 1;
        }

        boolean contains(String tag) {
            return position(tag) >= 0;
        }

        /** Adds a tag in order; nothing when it is there already. */
        void insert(String tag) {
            int at = position(tag);

            if (at >= 0) {
                return;
            }

            if (size == MAX_TAGS) {
                throw new UdfException("too many tags");
            }

            at = -at - 1;
            System.arraycopy(tags, at, tags, at + 1, size - at);
            tags[at] = tag;
            size++;
        }

        /** Every item of a stored list. */
        void offerAll(ListValue list) {
            if (list == null) {
                return;
            }

            for (Value item : list) {
                insert(item.asString());
            }
        }

        /** The tags not in {@code except}, as a built list, in order. */
        ListBuilder toList(Tags except) {
            ListBuilder l = Udf.list(Math.max(size, 1));

            for (int i = 0; i < size; i++) {
                if (except == null || !except.contains(tags[i])) {
                    l.add(tags[i]);
                }
            }

            return l;
        }
    }

    private static final Tags ADDED_TAGS = new Tags();
    private static final Tags REMOVED_TAGS = new Tags();

    /** The merge: the union of each list. Every other bin is the incoming one's. */
    @On.XdrMerge("merge_twopset")
    static Resolution mergeTwoPSet(EventRecord current, EventRecord incoming, Conflict conflict) {
        MapBuilder out = Udf.map(2);

        for (String bin : new String[] {"added", "removed"}) {
            ADDED_TAGS.size = 0;
            ADDED_TAGS.offerAll(current.getList(bin, null));
            ADDED_TAGS.offerAll(incoming.getList(bin, null));
            out.put(bin, ADDED_TAGS.toList(null));
        }

        return Resolution.record(incoming.withBins(out));
    }

    /** One of the record's lists into a table. */
    private static void load(Record rec, String bin, Tags into) {
        into.size = 0;
        into.offerAll(rec.getList(bin, null));
    }

    /** The members: added and not removed. */
    private static ListBuilder membersOf(Record rec) {
        load(rec, ADDED, ADDED_TAGS);
        load(rec, REMOVED, REMOVED_TAGS);

        return ADDED_TAGS.toList(REMOVED_TAGS);
    }

    /** Adds a tag to one of the lists and saves. */
    private static void grow(Record rec, String bin, Tags into, String tag) {
        // The Rust module bounds a tag with a Str<32>; the same limit, as a check.
        if (tag.length() > TAG_MAX) {
            throw new UdfException("the tag is too long");
        }

        load(rec, bin, into);

        if (into.contains(tag)) {
            return;
        }

        into.insert(tag);
        rec.set(bin, into.toList(null));
        rec.save();
    }

    /** Adds a tag. Returns the members. A tag removed before stays removed. */
    @Function("add")
    static ListBuilder add(Record rec, String tag) {
        grow(rec, ADDED, ADDED_TAGS, tag);

        return membersOf(rec);
    }

    /** Removes a tag that is a member, forever. Returns the members. */
    @Function("remove")
    static ListBuilder remove(Record rec, String tag) {
        load(rec, ADDED, ADDED_TAGS);
        load(rec, REMOVED, REMOVED_TAGS);

        if (!ADDED_TAGS.contains(tag) || REMOVED_TAGS.contains(tag)) {
            throw new UdfException("not a member");
        }

        grow(rec, REMOVED, REMOVED_TAGS, tag);

        return membersOf(rec);
    }

    /** The members, in order. */
    @Function("members")
    static ListBuilder members(Record rec) {
        return membersOf(rec);
    }
}

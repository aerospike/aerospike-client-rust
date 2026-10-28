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
    private static final Bin ADDED = Bin.of("added");
    private static final Bin REMOVED = Bin.of("removed");

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
        void offerAll(Value list) {
            if (list == null) {
                return;
            }

            for (Value item : list.asList()) {
                insert(item.asString());
            }
        }

        /** The tags not in {@code except}, as a built list, in order. */
        UdfList toList(Tags except) {
            UdfList l = UdfList.create(Math.max(size, 1));

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
        UdfMap out = UdfMap.create(2);

        for (String bin : new String[] {"added", "removed"}) {
            ADDED_TAGS.size = 0;
            ADDED_TAGS.offerAll(current.binList(bin));
            ADDED_TAGS.offerAll(incoming.binList(bin));
            out.put(bin, ADDED_TAGS.toList(null));
        }

        return Resolution.record(incoming.withBins(out));
    }

    /** One of the record's lists into a table. */
    private static void load(Bin bin, Tags into) {
        into.size = 0;
        into.offerAll(bin.get());
    }

    /** The members: added and not removed. */
    private static UdfList membersOf() {
        load(ADDED, ADDED_TAGS);
        load(REMOVED, REMOVED_TAGS);

        return ADDED_TAGS.toList(REMOVED_TAGS);
    }

    /** Adds a tag to one of the lists and commits. */
    private static void grow(Bin bin, Tags into, String tag) {
        // The Rust module bounds a tag with a Str<32>; the same limit, as a check.
        if (tag.length() > TAG_MAX) {
            throw new UdfException("the tag is too long");
        }

        load(bin, into);

        if (into.contains(tag)) {
            return;
        }

        into.insert(tag);
        bin.set(into.toList(null));
        Udf.commit();
    }

    /** Adds a tag. Returns the members. A tag removed before stays removed. */
    @Function("add")
    static UdfList add(String tag) {
        grow(ADDED, ADDED_TAGS, tag);

        return membersOf();
    }

    /** Removes a tag that is a member, forever. Returns the members. */
    @Function("remove")
    static UdfList remove(String tag) {
        load(ADDED, ADDED_TAGS);
        load(REMOVED, REMOVED_TAGS);

        if (!ADDED_TAGS.contains(tag) || REMOVED_TAGS.contains(tag)) {
            throw new UdfException("not a member");
        }

        grow(REMOVED, REMOVED_TAGS, tag);

        return membersOf();
    }

    /** The members, in order. */
    @Function("members")
    static UdfList members() {
        return membersOf();
    }
}

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

package com.aerospike.udf;

import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

import org.teavm.interop.Address;

/**
 * A bin on both sides of a merge, either of which may be missing (null), with the merges a resolver
 * reaches for: the Java face of the Rust SDK's {@code Both<T>}. {@link Pair} builds one per bin.
 *
 * <pre>
 * Both&lt;Long&gt; visits = pair.getInt("visits");
 *
 * Long merged = visits.merge(Math::max);       // null only when neither side has the bin
 * Long total = visits.sumInt();                // a missing side is 0; null when both are
 *
 * if (visits.current == null) {
 *     // the bin is new to this cluster
 * }
 * </pre>
 *
 * <p>Every merge here answers null when both sides are missing. {@code current} and {@code incoming} are the
 * values themselves, to read when no merge here fits.
 */
public final class Both<T> {
    /** The current record's value, or null when it has no such bin or the bin is nil. */
    public final T current;
    /** The incoming write's value, or null when it has no such bin or the bin is nil. */
    public final T incoming;
    private final long currentLastUpdateTime;
    private final long incomingLastUpdateTime;
    private final long currentSourceId;
    private final long incomingSourceId;

    Both(T current, T incoming, long currentLastUpdateTime, long incomingLastUpdateTime, long currentSourceId,
            long incomingSourceId) {
        this.current = current;
        this.incoming = incoming;
        this.currentLastUpdateTime = currentLastUpdateTime;
        this.incomingLastUpdateTime = incomingLastUpdateTime;
        this.currentSourceId = currentSourceId;
        this.incomingSourceId = incomingSourceId;
    }

    /**
     * Whether the incoming side wrote this bin later: by the bin's LUT, and on a tie by source id, so
     * "newer" means the same on both DCs even when the clocks agreed to the millisecond.
     */
    public boolean incomingIsNewer() {
        return incomingLastUpdateTime > currentLastUpdateTime
                || (incomingLastUpdateTime == currentLastUpdateTime && incomingSourceId > currentSourceId);
    }

    /**
     * The side written later, by {@link #incomingIsNewer}; whichever exists when only one does, and null when
     * neither does. A missing side never wins over one that exists, however late it was deleted: a merge that
     * must honour deletes asks {@link EventRecord#deleted} of the later side, as {@link Pair#withNewer} does.
     */
    public T newer() {
        return incomingIsNewer() ? or(incoming, current) : or(current, incoming);
    }

    /**
     * The side written earlier, by {@link #incomingIsNewer}; whichever exists when only one does, and null
     * when neither does.
     */
    public T older() {
        return incomingIsNewer() ? or(current, incoming) : or(incoming, current);
    }

    /**
     * Both sides as a list of two, the older and then the newer, either of which may be null: the order
     * both DCs agree on, for a merge whose result depends on the order its inputs are taken in.
     */
    public List<T> ordered() {
        return incomingIsNewer() ? Arrays.asList(current, incoming) : Arrays.asList(incoming, current);
    }

    /**
     * The current value when there is one, else the incoming, else null. It prefers this cluster's side, so
     * the two clusters answer differently: not for a merge that has to converge.
     */
    public T currentOrIncoming() {
        return or(current, incoming);
    }

    /**
     * The incoming value when there is one, else the current, else null. It prefers the other cluster's side,
     * so the two clusters answer differently: not for a merge that has to converge.
     */
    public T incomingOrCurrent() {
        return or(incoming, current);
    }

    /**
     * Combines the two with {@code f} when both exist, the current value as its first argument and the
     * incoming as its second; the one that exists otherwise, and null when neither does. {@code f} is not
     * called with a null. For the two clusters to agree it has to give the same result with its arguments
     * swapped.
     */
    public T merge(BinaryOperator<T> f) {
        if (current != null && incoming != null) {
            return f.apply(current, incoming);
        }

        return or(current, incoming);
    }

    /**
     * The larger of the two by {@code compareTo}; the one that exists when only one does, null when neither
     * does. Only for a {@code Comparable} T: {@code Long}, {@code Double}, {@code String}, {@code Boolean}.
     * Called on a {@code Both} of bytes, values, lists or maps it is a bad cast, not a comparison.
     */
    @SuppressWarnings("unchecked")
    public T max() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) > 0 ? i : c);
    }

    /**
     * The smaller of the two by {@code compareTo}; the one that exists when only one does, null when neither
     * does. For a {@code Comparable} T, as {@link #max} is.
     */
    @SuppressWarnings("unchecked")
    public T min() {
        return merge((c, i) -> ((Comparable<T>) i).compareTo(c) < 0 ? i : c);
    }

    /**
     * The sum of a {@code Both<Long>}; the one that exists when only one does, a missing side counting as
     * zero, and null when neither does, as {@link #max} and {@link #min}. It wraps on overflow as {@code long}
     * addition does. Only for a {@code Both<Long>}: on any other it is a bad cast.
     */
    public Long sumInt() {
        if (current == null && incoming == null) {
            return null;
        }

        return (current == null ? 0 : (Long) current) + (incoming == null ? 0 : (Long) incoming);
    }

    /**
     * The sum of a {@code Both<Double>}; the one that exists when only one does, and null when neither does,
     * as {@link #max} and {@link #min}. Only for a {@code Both<Double>}: on any other it is a bad cast.
     */
    public Double sumDouble() {
        if (current == null && incoming == null) {
            return null;
        }

        return (current == null ? 0.0 : (Double) current) + (incoming == null ? 0.0 : (Double) incoming);
    }

    //==========================================================
    // Merge building blocks: the merges of collections a convergent handler is built from. Each is here
    // twice: on a Both of the bin, as Pair reads it, and as a static method on the two sides as values,
    // either of which may be missing, so a write UDF can use one too.
    //

    /**
     * {@link #maxPerKey(Value, Value)} of a map bin on both sides, as {@link Pair#getMap} reads it: every key
     * of either side, each with the larger of its two integers; null when neither side has the bin.
     *
     * <pre>
     * MapBuilder counts = pair.getMap("counts").maxPerKey();
     *
     * if (counts != null) {
     *     out.put("counts", counts);
     * }
     * </pre>
     *
     * <p>Symmetric, idempotent and associative, so the two clusters converge on the same bytes. For a
     * {@code Both<MapValue>}.
     */
    public MapBuilder maxPerKey() {
        return maxPerKeyOf(side(current, Value.MAP), side(incoming, Value.MAP));
    }

    /**
     * {@link #sortedUnion(Value, Value)} of a list bin on both sides, as {@link Pair#getList} reads it: every
     * element of either side once, sorted; null when neither side has the bin.
     *
     * <pre>
     * ListBuilder tags = pair.getList("tags").sortedUnion();
     *
     * if (tags != null) {
     *     out.put("tags", tags);
     * }
     * </pre>
     *
     * <p>Symmetric, idempotent and associative. For a {@code Both<ListValue>}.
     */
    public ListBuilder sortedUnion() {
        return sortedUnionOf(side(current, Value.LIST), side(incoming, Value.LIST));
    }

    /**
     * {@link #counterMerge(Value, Value)} of a counter bin on both sides, as {@link Pair#getMap} reads it: a map
     * of origin to count, merged by the larger count per origin; null when neither side has the bin.
     * {@link #counterTotal} reads the merged counter's value.
     *
     * <pre>
     * MapBuilder hits = pair.getMap("hits").counterMerge();
     *
     * if (hits != null) {
     *     out.put("hits", hits);
     * }
     * </pre>
     *
     * <p>Symmetric, idempotent and associative. For a {@code Both<MapValue>}.
     */
    public MapBuilder counterMerge() {
        return maxPerKeyOf(side(current, Value.MAP), side(incoming, Value.MAP));
    }

    /**
     * {@link #lwwByVersion(Value, Value)} of a {@code [version, value]} bin on both sides, as
     * {@link Pair#getList} reads it: the side with the higher version, ties broken by the stored bytes; null
     * when neither side has the bin.
     *
     * <pre>
     * Value price = pair.getList("price").lwwByVersion();
     *
     * if (price != null) {
     *     out.put("price", price);
     * }
     * </pre>
     *
     * <p>Symmetric, idempotent and associative. For a {@code Both<ListValue>}.
     */
    public Value lwwByVersion() {
        return lwwByVersionOf(side(current, Value.LIST), side(incoming, Value.LIST));
    }

    /**
     * The per-key maximum of two maps: a new map with every key of either side, each under the larger of its
     * two values, or under its one value when only one side has the key. Built on the host, so it is ordered
     * by key and both clusters build the same bytes. Null when both sides are missing.
     *
     * <p>A side is missing when it is null, absent or nil; otherwise it must be a map whose keys are strings
     * or integers and whose values are integers. Anything else throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}), a side that is not a map included: a merge does not guess what a double
     * or a nested map should become. It throws with {@link LowLevel#REFUSED} when the host does not build the
     * map.
     *
     * <pre>
     * MapBuilder merged = Both.maxPerKey(rec.get("seen"), theirs);
     * </pre>
     *
     * <p>Symmetric ({@code maxPerKey(a, b)} and {@code maxPerKey(b, a)} are the same map), idempotent (merging
     * a map with itself, or with a merge it is part of, changes nothing) and associative (three sides merge to
     * the same map in any order): the laws a convergent merge needs. It is the merge of any map of values that
     * only grow; {@link #counterMerge(Value, Value)} is the same merge under the name of the structure it
     * serves.
     */
    public static MapBuilder maxPerKey(Value current, Value incoming) {
        return maxPerKeyOf(side(current, Value.MAP), side(incoming, Value.MAP));
    }

    /**
     * The union of two lists as a set: a new list with every element of either side once, sorted, so both
     * clusters build the same bytes whatever order the elements arrived in. Null when both sides are missing.
     *
     * <p>The elements are integers, strings or blobs: the kinds the server keys a map by, since the host
     * sorts them as a map built on it sorts its keys. Integers come first, by value, then strings and then
     * blobs, each by its bytes. An integer and a string are never the same element, however they read.
     *
     * <p>A side is missing when it is null, absent or nil; otherwise it must be a list of those kinds.
     * Anything else throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}): a side that is not a list,
     * or an element of another kind (a double, a nested list or map, nil). It throws with
     * {@link LowLevel#REFUSED} when the host does not build the list.
     *
     * <p>Symmetric, idempotent and associative: the merge of a set that only grows. A removal needs a second
     * set, as the 2P-Set of the CRDT tutorial keeps one.
     */
    public static ListBuilder sortedUnion(Value current, Value incoming) {
        return sortedUnionOf(side(current, Value.LIST), side(incoming, Value.LIST));
    }

    /**
     * The merge of a per-origin counter: a map of origin to count, each origin's count raised only by the
     * cluster it names, merged by the larger count per origin. It is {@link #maxPerKey(Value, Value)} under
     * the name of the structure: the same rules, the same exceptions, and null when both sides are missing.
     * {@link #counterTotal} reads the counter's value.
     *
     * <pre>
     * MapBuilder counts = Both.counterMerge(current.get("hits"), incoming.get("hits"));
     * </pre>
     *
     * <p>Symmetric, idempotent and associative. A count that goes down is not a count this merges: a
     * PN-Counter keeps a second counter for the decrements.
     */
    public static MapBuilder counterMerge(Value current, Value incoming) {
        return maxPerKeyOf(side(current, Value.MAP), side(incoming, Value.MAP));
    }

    /**
     * The value of a per-origin counter: the sum of its counts, wrapping on overflow as {@code long} addition
     * does. 0 for a missing counter (null, absent or nil). Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) when it is not a map, or when a key is not a string or an integer or a
     * count is not an integer, as for {@link #counterMerge(Value, Value)}. A built map
     * ({@link MapBuilder#asValue}) is read too, so a merged counter's total needs no second read.
     *
     * <pre>
     * long total = Both.counterTotal(rec.get("hits"));
     * </pre>
     */
    public static long counterTotal(Value counter) {
        Value counts = side(counter, Value.MAP);

        if (counts == null) {
            return 0;
        }

        long total = 0;

        for (MapValue.Entry entry : counts.asMap()) {
            mergeKey(entry.getKey());
            total += count(entry.getValue());
        }

        return total;
    }

    /**
     * Last writer wins by an application version: of two {@code [version, value]} lists, the one with the
     * higher version, as it is, to write back. Two equal versions are decided by the lists' stored bytes, the
     * greater winning, so both clusters pick the same side; two lists with the same bytes are the same either
     * way. Null when both sides are missing.
     *
     * <p>A side is missing when it is null, absent or nil; otherwise it must be a list of exactly two elements
     * whose first is an integer (anything else throws {@link UdfException}, {@link LowLevel#TYPE_MISMATCH}).
     * The value may be of any kind.
     *
     * <pre>
     * // The write UDF moves the version and the value in one write: [version + 1, price].
     * Value price = Both.lwwByVersion(current.get("price"), incoming.get("price"));
     *
     * if (price != null) {
     *     out.put("price", price);
     * }
     * </pre>
     *
     * <p>Symmetric, idempotent and associative: it is the larger of the two by version, then by bytes, which
     * is a total order. The version is the application's, raised by every write that changes the value, so no
     * clock decides.
     */
    public static Value lwwByVersion(Value current, Value incoming) {
        return lwwByVersionOf(side(current, Value.LIST), side(incoming, Value.LIST));
    }

    /**
     * One side of a merge of collections as a value of {@code kind}: null when it is missing, the value when
     * it is a stored one of that kind or a built one, and a type mismatch otherwise. A side of a Both is a
     * view, a value or null.
     */
    private static Value side(Object side, int kind) {
        Value v;

        if (side == null) {
            return null;
        }
        else if (side instanceof MapValue) {
            v = ((MapValue) side).asValue();
        }
        else if (side instanceof ListValue) {
            v = ((ListValue) side).asValue();
        }
        else if (side instanceof Value) {
            v = (Value) side;
        }
        else {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "a merge of collections takes lists and maps");
        }

        if (v.isAbsent() || v.isNil()) {
            return null;
        }

        // A built one is read through a packed copy of it, as the host walks any built collection.
        if (v.kind() == Value.HANDLE) {
            int src = Natives.packHandle((int) v.payload());

            if (src <= 0) {
                throw new UdfException(LowLevel.REFUSED, "the built collection could not be read");
            }

            return new Value().assign(kind, Natives.collSize((int) v.payload()), (long) src << 32);
        }

        if (v.kind() != kind) {
            throw new UdfException(LowLevel.TYPE_MISMATCH,
                    kind == Value.MAP ? "a map merge takes maps" : "a list merge takes lists");
        }

        return v;
    }

    /** A key a map merge takes: a string or an integer. */
    private static void mergeKey(Value key) {
        if (!key.isInt() && !key.isString()) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "a map merge takes string and integer keys");
        }
    }

    /** A count a map merge takes: an integer. */
    private static long count(Value value) {
        if (!value.isInt()) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "a map merge takes integer values");
        }

        return value.asInt();
    }

    /** The value under {@code key}, a key read from another map, in the stored map {@code m}; null for none. */
    private static Value valueUnder(Value m, Value key) {
        int rc;

        if (key.isInt()) {
            rc = Natives.collGetI(m.src(), m.at(), key.asInt(), Address.ofData(Slots.ONE), Slots.SIZE);
        }
        else {
            byte[] k = key.asBytes();

            rc = Natives.collGet(m.src(), m.at(), Address.ofData(k), k.length, Address.ofData(Slots.ONE), Slots.SIZE);
        }

        if (rc == LowLevel.NOT_FOUND) {
            return null;
        }

        if (rc != LowLevel.OK) {
            throw new UdfException(rc, "a key of the map could not be looked up");
        }

        return new Value().fill(Slots.ONE, 0);
    }

    /** One entry into a built map, the key as it was read: a stored string goes by position. */
    private static void putEntry(MapBuilder out, Value key, long value) {
        Slots.value(Slots.PAIR, 0, key);
        Slots.integer(Slots.PAIR, Slots.SIZE, value);

        int n = Natives.mapPutEntries(out.handle(), Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

        if (n != 1) {
            throw new UdfException(n < 0 ? n : LowLevel.REFUSED, "the merged map did not take an entry");
        }
    }

    private static MapBuilder maxPerKeyOf(Value a, Value b) {
        if (a == null && b == null) {
            return null;
        }

        MapBuilder out = MapBuilder.create((a == null ? 0 : a.length()) + (b == null ? 0 : b.length()));

        // Every key of the first side, under the larger of its values on the two sides.
        if (a != null) {
            for (MapValue.Entry entry : a.asMap()) {
                Value key = entry.getKey();
                long most = count(entry.getValue());

                mergeKey(key);

                if (b != null) {
                    Value other = valueUnder(b, key);

                    if (other != null) {
                        most = Math.max(most, count(other));
                    }
                }

                putEntry(out, key, most);
            }
        }

        // Then the keys only the second side has.
        if (b != null) {
            for (MapValue.Entry entry : b.asMap()) {
                Value key = entry.getKey();
                long n = count(entry.getValue());

                mergeKey(key);

                if (a != null && valueUnder(a, key) != null) {
                    continue;
                }

                putEntry(out, key, n);
            }
        }

        return out;
    }

    private static ListBuilder sortedUnionOf(Value a, Value b) {
        if (a == null && b == null) {
            return null;
        }

        // A map built on the host keeps its keys once each and in order: the elements go in as its keys, and
        // come out as the list.
        MapBuilder set = MapBuilder.create((a == null ? 0 : a.length()) + (b == null ? 0 : b.length()));
        Value[] sides = { a, b };

        for (Value side : sides) {
            if (side == null) {
                continue;
            }

            for (Value element : side.asList()) {
                if (!element.isInt() && !element.isString() && !element.isBlob()) {
                    throw new UdfException(LowLevel.TYPE_MISMATCH,
                            "a sorted union takes integers, strings and blobs");
                }

                Slots.value(Slots.PAIR, 0, element);
                Slots.scalar(Slots.PAIR, Slots.SIZE, Value.BOOL, 0, 1);

                int n = Natives.mapPutEntries(set.handle(), Address.ofData(Slots.PAIR), 2 * Slots.SIZE);

                if (n != 1) {
                    throw new UdfException(n < 0 ? n : LowLevel.REFUSED, "the union did not take an element");
                }
            }
        }

        Value keys = side(set.asValue(), Value.MAP);
        ListBuilder out = ListBuilder.create(keys.length());

        for (MapValue.Entry entry : keys.asMap()) {
            out.add(entry.getKey());
        }

        set.close();

        return out;
    }

    /** The version of a {@code [version, value]} list; a type mismatch for any other shape. */
    private static long version(Value side) {
        Value v = side.length() == 2 ? side.asList().get(0) : null;

        if (v == null || !v.isInt()) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "a version merge takes [version, value] lists");
        }

        return v.asInt();
    }

    private static Value lwwByVersionOf(Value a, Value b) {
        if (a == null && b == null) {
            return null;
        }

        if (a == null || b == null) {
            Value one = a != null ? a : b;

            version(one);

            return one;
        }

        long va = version(a);
        long vb = version(b);

        if (va != vb) {
            return va > vb ? a : b;
        }

        return a.cmpStored(b) >= 0 ? a : b;
    }

    private static <T> T or(T a, T b) {
        return a != null ? a : b;
    }
}

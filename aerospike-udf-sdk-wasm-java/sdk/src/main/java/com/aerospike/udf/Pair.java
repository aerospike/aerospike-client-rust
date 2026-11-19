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

/**
 * The two records of a merge side by side, read bin by bin into a {@link Both}: the Java face of the Rust
 * SDK's {@code Pair}. {@link #of} makes one from the two records a handler is given. A reader answers null
 * for a side that has no such bin or holds nil, and throws {@link UdfException}, with the status
 * {@link LowLevel#TYPE_MISMATCH}, for a bin of another kind.
 *
 * <pre>
 * Pair pair = Pair.of(current, incoming);
 *
 * Long visits = pair.getInt("visits").max();          // null when neither side has the bin
 * String name = pair.getString("name").newer();       // last writer wins
 * Long spent = pair.getInt("spent").sumInt();         // a missing side counts as 0
 * </pre>
 *
 * <p>Every reader reads the bin on both sides, so each costs two reads; keep the {@link Both} when more than
 * one merge is asked of it.
 */
public final class Pair {
    /**
     * The current record: this cluster's, as it is before the incoming write is applied. Read it directly for
     * what is not a bin, such as {@link EventRecord#isTombstone}.
     */
    public final EventRecord current;
    /**
     * The incoming write: the record as the other cluster shipped it. Read it directly for what is not a bin,
     * such as {@link EventRecord#isDelete} and {@link EventRecord#isPartial}.
     */
    public final EventRecord incoming;

    private Pair(EventRecord current, EventRecord incoming) {
        this.current = current;
        this.incoming = incoming;
    }

    /**
     * The two records a handler is given, side by side: the current record, then the incoming write, in the
     * order the handler's parameters have them. It reads nothing yet and copies nothing.
     */
    public static Pair of(EventRecord current, EventRecord incoming) {
        return new Pair(current, incoming);
    }

    /**
     * Reads an event call's arguments into its two records, {@link #current} the current one and
     * {@link #incoming} the incoming write; null when the arguments are not an xdr-merge. This is what the
     * generated export of a typed handler calls, and what a handler in the raw form
     * {@code int name(int args, int argc)} starts with.
     */
    public static Pair decode(int args, int argc) {
        EventRecord current = new EventRecord();
        EventRecord incoming = new EventRecord();

        return EventRecord.decode(args, argc, current, incoming) ? new Pair(current, incoming) : null;
    }

    /**
     * An integer bin on each side, boxed: a side is null when it has no such bin. Throws {@link UdfException}
     * when either side holds another kind in the bin.
     */
    public Both<Long> getInt(String name) {
        return both(name, current.getInt(name), incoming.getInt(name));
    }

    /**
     * A double bin on each side, boxed: a side is null when it has no such bin. Throws {@link UdfException}
     * when either side holds another kind in the bin; an integer is not widened.
     */
    public Both<Double> getDouble(String name) {
        return both(name, current.getDouble(name), incoming.getDouble(name));
    }

    /**
     * A boolean bin on each side, boxed: a side is null when it has no such bin. Throws {@link UdfException}
     * when either side holds another kind in the bin.
     */
    public Both<Boolean> getBool(String name) {
        return both(name, current.getBool(name), incoming.getBool(name));
    }

    /**
     * A string bin on each side, its text copied out: a side is null when it has no such bin. Throws
     * {@link UdfException} when either side holds another kind in the bin.
     */
    public Both<String> getString(String name) {
        return both(name, current.getString(name), incoming.getString(name));
    }

    /**
     * The bytes of a blob bin on each side, copied out: a side is null when it has no such bin. It is strict:
     * throws {@link UdfException} when either side holds anything but a blob in it, a string included;
     * {@link #getBytes} takes those. {@link Both#max} and {@link Both#min} do not apply: a {@code byte[]} is
     * not {@code Comparable}.
     */
    public Both<byte[]> getBlob(String name) {
        return both(name, current.getBlob(name), incoming.getBlob(name));
    }

    /**
     * The raw bytes of a string, blob, GeoJSON or HLL bin on each side, copied out: a side is null when it
     * has no such bin. Throws {@link UdfException} when either side holds a number, a boolean, a list or a
     * map in it.
     */
    public Both<byte[]> getBytes(String name) {
        return both(name, current.getBytes(name), incoming.getBytes(name));
    }

    /**
     * A list bin on each side, as a view read where it is stored: a side is null when it has no such bin.
     * Throws {@link UdfException} when either side holds another kind in the bin.
     */
    public Both<ListValue> getList(String name) {
        return both(name, current.getList(name), incoming.getList(name));
    }

    /**
     * A map bin on each side, as a view read where it is stored: a side is null when it has no such bin.
     * Throws {@link UdfException} when either side holds another kind in the bin.
     */
    public Both<MapValue> getMap(String name) {
        return both(name, current.getMap(name), incoming.getMap(name));
    }

    /**
     * The bin as a value of whatever kind it is on each side; a side that has no such bin, or holds nil in
     * it, is null. The way to carry a bin into the answer without caring what it holds:
     * {@code out.put("profile", pair.bin("profile").newer())}, after checking the result for null.
     */
    public Both<Value> bin(String name) {
        return both(name, current.get(name), incoming.get(name));
    }

    /**
     * Whether the incoming side wrote {@code name} later: by the bin's time where the server keeps one, and
     * on a tie (which is every bin when it keeps none) by the larger source id. It is the order the server's
     * own bin convergence uses, and both clusters compute it alike. It reads no bin, so it also answers for a
     * bin neither side has.
     */
    public boolean incomingIsNewer(String name) {
        long cl = current.binLastUpdateTime(name);
        long il = incoming.binLastUpdateTime(name);

        return il > cl || (il == cl && incoming.binSourceId(name) > current.binSourceId(name));
    }

    /**
     * Puts into {@code overrides} every bin either side has that it lacks, from the side that wrote the bin
     * later - which is what the policy would have kept - so a handler merges the bins it knows about and
     * leaves the rest as they would have been. Symmetric: both DCs order the two sides the same way.
     *
     * <p>A delete is a write like any other. When the side that wrote a bin later deleted it (the bin is in
     * that side's {@link EventRecord#deletedBins}), the bin is left out of the answer, which is how a
     * {@code record} answer deletes it: the older side's value is not brought back. Returns
     * {@code overrides}.
     */
    public MapBuilder withNewer(MapBuilder overrides) {
        for (int side = 0; side < 2; side++) {
            EventRecord rec = side == 0 ? current : incoming;

            // forEach gives the walk's cursor back when a read or a put throws part way.
            rec.binNames().forEach(name -> {
                String bin = name.asString();

                if (overrides.containsKey(bin)) {
                    return;
                }

                // The later side's delete stands, though the earlier side still has a value to offer.
                if ((incomingIsNewer(bin) ? incoming : current).deleted(bin)) {
                    return;
                }

                Value newer = bin(bin).newer();

                if (newer != null) {
                    overrides.put(bin, newer);
                }
            });
        }

        return overrides;
    }

    private <T> Both<T> both(String name, T c, T i) {
        return new Both<>(c, i, current.binLastUpdateTime(name), incoming.binLastUpdateTime(name),
                current.binSourceId(name), incoming.binSourceId(name));
    }
}

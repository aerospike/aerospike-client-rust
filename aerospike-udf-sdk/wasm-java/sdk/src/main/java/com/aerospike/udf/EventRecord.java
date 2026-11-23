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

import org.teavm.interop.Address;

/**
 * One record an event hands over: each side of a merge, the current record and the incoming write. It is
 * read-only, and it reads like the record of a call - the same readers, through
 * {@link RecordReader} - with what an event adds: the names of its bins, the bins this side deleted,
 * where the server keeps them, when and by whom each bin was last written, and the few facts about the
 * merge that are facts about one side ({@link #isDelete}, {@link #isPartial} and {@link #lostBins} of the
 * incoming write, {@link #isTombstone} of the current record). One asked of the side it is not about
 * answers false, or an empty list.
 *
 * <p>A handler must be pure and convergent: both data centres merge the same two records with the sides
 * swapped and must land on the same record. {@link Pair} orders a bin's two sides by its own LUT and then
 * source id, which is symmetric; "mine first" is not.
 *
 * <p>A handler changes a record by answering with the bins it should have
 * ({@link Resolution#record}); {@link #withBins} fills such an answer with every bin of this record the
 * handler did not decide.
 *
 * <pre>
 * {@literal @}On.XdrMerge("keep_higher_score")
 * static Resolution keepHigherScore(EventRecord current, EventRecord incoming) {
 *     if (incoming.isDelete()) {
 *         return Resolution.policy();
 *     }
 *
 *     long mine = current.getInt("score", 0);
 *     long theirs = incoming.getInt("score", 0);
 *
 *     return theirs &gt; mine ? Resolution.takeRemote() : Resolution.keepLocal();
 * }
 * </pre>
 *
 * <p>Both records are good for the whole call and for nothing after it. A handler has no record of its own
 * and no clock: {@link Udf#now} answers -1 in one, a {@link Udf#log} line is dropped, and the
 * {@link Record} of a record UDF is not there to read or write.
 */
public final class EventRecord implements RecordReader {
    /** The index the record is read by: 0 for the first record the event hands over, 1 for the second. */
    private int which;
    private long generation;
    private long lastUpdateTime;
    private long ttl;
    private long sourceId;
    private Value bins = Maps.NOTHING;
    private Value deleted = Maps.NOTHING;
    private Value binLastUpdateTimes = Maps.NOTHING;
    private Value binSourceIds = Maps.NOTHING;
    private Value lost = Maps.NOTHING;
    private boolean delete;
    private boolean partial;
    private boolean tombstone;
    private boolean binConvergence;

    /** What the typed readers read into, so that they allocate nothing. */
    private final Value v = new Value();

    EventRecord() {
    }

    //==========================================================
    // What the event says about the record.
    //

    /**
     * The record's generation. It is 0 on both sides of an {@code xdr-merge}: each cluster counts its own
     * writes, so a merge that read it could not converge.
     */
    @Override
    public long generation() {
        return generation;
    }

    /**
     * When the record was last written. It is 0 on both sides of an {@code xdr-merge}, for the reason
     * {@link #generation} is; {@link #binLastUpdateTime} is the time a merge orders by.
     */
    @Override
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    /** This side's time to live, in seconds; 0 for a record that does not expire. */
    @Override
    public long ttl() {
        return ttl;
    }

    /**
     * The XDR source id of the data centre that wrote this version: the number each cluster is configured
     * with as its {@code src-id}. It is the tie-break when two writes carry the same time.
     */
    public long sourceId() {
        return sourceId;
    }

    /**
     * The names of the bins this side has, as a list of strings to iterate; empty for an incoming write that
     * deletes the record and for a tombstone.
     */
    public ListValue binNames() {
        return new ListValue(bins);
    }

    /**
     * The names of the bins this side deleted (bin deletes, or tombstones), as a list of strings; empty when
     * it deleted none, which is also what it is when the server sent no such list. A bin named here must not be brought back from the other side by a merge that copies
     * whatever either side has.
     */
    public ListValue deletedBins() {
        return new ListValue(deleted);
    }

    /** Whether this side deleted any bin: {@link #deletedBins} is not empty. */
    public boolean deletedAny() {
        return deleted.kind() == Value.LIST && deleted.length() > 0;
    }

    /**
     * Whether this side deleted the bin {@code name}: it is in {@link #deletedBins}. A merge that takes the
     * later write of a bin asks this of the later side, so that a delete is not undone by the older value.
     */
    public boolean deleted(String name) {
        return deletedAny() && deletedBins().contains(name);
    }

    /**
     * When this side last wrote the bin, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z
     * (Unix milliseconds minus 1 262 304 000 000), for comparing with the other side's: the bin's own time
     * where the server keeps one ({@link #usesBinConvergence} says whether it is meaningful), and
     * {@link #lastUpdateTime} otherwise, also for a bin this side does not have. The record's time is 0 in an
     * {@code xdr-merge}, so where no per-bin time is kept every bin answers 0 on both sides and only
     * {@link #binSourceId} tells them apart.
     */
    public long binLastUpdateTime(String name) {
        return Maps.getInt(binLastUpdateTimes, name, lastUpdateTime);
    }

    /**
     * The source id of the data centre that last wrote the bin on this side: the bin's own where the server
     * keeps one, the record's {@link #sourceId} otherwise, also for a bin this side does not have.
     */
    public long binSourceId(String name) {
        return Maps.getInt(binSourceIds, name, sourceId);
    }

    /**
     * Whether this is an incoming write that deletes the record, so that it has no bins. False of the
     * current record.
     */
    public boolean isDelete() {
        return delete;
    }

    /**
     * Whether this is an incoming write that carries only the bins that changed, so that a bin it lacks may
     * well exist on the other cluster. False of the current record.
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * The names of the incoming bins that would lose under bin convergence, as a list of strings. Empty
     * for the current record, and when the server sent no such list.
     */
    public ListValue lostBins() {
        return new ListValue(lost);
    }

    /** Whether this is a current record that exists but has no bins. False of the incoming write. */
    public boolean isTombstone() {
        return tombstone;
    }

    /**
     * Whether per-bin timestamps are meaningful in this merge, for a time-based merge that wants to check
     * before trusting {@link #binLastUpdateTime}. The same answer on both records.
     */
    public boolean usesBinConvergence() {
        return binConvergence;
    }

    //==========================================================
    // Reading bins: through the event's own import, never the record imports, which answer nothing in a
    // handler.
    //

    /** One of this side's bins into {@code out}; the status. */
    int read(byte[] name, Value out) {
        int rc = Natives.eventBinValue(which, Address.ofData(name), name.length, Address.ofData(Slots.ONE),
                Slots.SIZE);

        if (rc == LowLevel.OK) {
            out.fill(Slots.ONE, 0);
        }

        return rc;
    }

    private int read(String name) {
        return read(Names.of(name), v);
    }

    /** Returns for an absent bin, which is nothing or a default; throws for any other miss. */
    private void absent(int rc, String name, int kind, String what) {
        int state = Reads.state(rc, v, kind);

        if (state != Reads.NONE) {
            throw Reads.problem(state, "bin ", name, what);
        }
    }

    /** The scratch value when the bin is there and of {@code kind}; null when absent; throws for another. */
    private Value typed(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        absent(rc, name, kind, what);

        return null;
    }

    /** The scratch value when the bin has bytes of any kind; null when absent; throws for another. */
    private Value bytes(String name) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT || !v.hasBytes()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "bin ", name,
                    "a string, blob, GeoJSON or HLL");
        }

        return v;
    }

    /**
     * The bin as a {@link Value} of whatever kind it is, or null when this side has no such bin or holds nil in it
     * (a bin it deleted). The value is a fresh one that is not changed afterwards: a
     * scalar is in it whole, and the text of a string or the elements of a list are read from the server when
     * its {@code as*} reader asks for them. Throws {@link UdfException} only when the host could not read the
     * bin.
     */
    @Override
    public Value get(String name) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return null;
        }

        if (state != Reads.HIT) {
            throw Reads.problem(state, "bin ", name, "readable");
        }

        return v.copy();
    }

    /**
     * As {@link #get}, for a bin that has to be there: throws {@link UdfException} reading
     * {@code bin <name> is absent}, with the status {@link LowLevel#NOT_FOUND}, when this side has no such
     * bin, and never answers null. With the value's own reader it is the read that insists:
     * {@code incoming.bin("score").asInt()}.
     */
    @Override
    public Value bin(String name) {
        Value x = get(name);

        if (x == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "readable");
        }

        return x;
    }

    /**
     * Whether this side has the bin, of any kind. False for a bin it deleted, and for a name that is not 1 to
     * 15 bytes.
     */
    @Override
    public boolean has(String name) {
        return Reads.state(read(name), v, v.kind()) == Reads.HIT;
    }

    /**
     * An integer bin, or null when this side has no such bin. Throws {@link UdfException} naming the bin
     * ({@code bin n is not an int}, status {@link LowLevel#TYPE_MISMATCH}) when it holds another kind; a
     * double is not truncated to one. The answer is boxed so that it can be null: assigning it to a
     * {@code long} without looking fails the call with a {@code NullPointerException} for an absent bin, so
     * give a default ({@link #getInt(String, long)}) or insist ({@code incoming.bin(name).asInt()}) where no test
     * for null is wanted.
     *
     * <pre>
     * Long theirs = incoming.getInt("score");
     *
     * if (theirs == null) {
     *     // the incoming write does not carry the bin
     * }
     * </pre>
     */
    @Override
    public Long getInt(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        absent(rc, name, Value.INT, "an int");

        return null;
    }

    /**
     * An integer bin, or {@code dflt} when this side has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}): the default stands in for a missing bin, not for
     * a wrong one. In a merge the default is what a side that never wrote the bin counts as, so choose one
     * that leaves the other side's value standing: 0 for a sum or a maximum of counts.
     */
    @Override
    public long getInt(String name, long dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.INT) {
            return v.payload();
        }

        absent(rc, name, Value.INT, "an int");

        return dflt;
    }

    /**
     * A double bin, or null when this side has no such bin. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it holds another kind, and an integer bin is
     * another kind: it is not widened.
     */
    @Override
    public Double getDouble(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        absent(rc, name, Value.DOUBLE, "a double");

        return null;
    }

    /**
     * A double bin, or {@code dflt} when this side has no such bin. A bin of another kind, an integer
     * included, still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public double getDouble(String name, double dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        absent(rc, name, Value.DOUBLE, "a double");

        return dflt;
    }

    /**
     * A boolean bin, or null when this side has no such bin. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it holds another kind; an integer 0 or 1 is not a
     * boolean.
     */
    @Override
    public Boolean getBool(String name) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        absent(rc, name, Value.BOOL, "a bool");

        return null;
    }

    /**
     * A boolean bin, or {@code dflt} when this side has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public boolean getBool(String name, boolean dflt) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == Value.BOOL) {
            return v.payload() != 0;
        }

        absent(rc, name, Value.BOOL, "a bool");

        return dflt;
    }

    /**
     * The text of a string bin, decoded from UTF-8 into a new {@code String} on every call, or null when this
     * side has no such bin. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin
     * when it is not a string. A GeoJSON bin is not a string here: {@link #getBytes(String)} gives its text
     * as bytes.
     */
    @Override
    public String getString(String name) {
        return getString(name, null);
    }

    /**
     * The text of a string bin, or {@code dflt} (which may be null) when this side has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public String getString(String name, String dflt) {
        Value x = typed(name, Value.STR, "a string");

        return x == null ? dflt : x.asString();
    }

    /**
     * The bytes of a blob bin, copied into a new array on every call, or null when this side has no such
     * bin. It is strict: throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it
     * holds anything but a blob, a string, a GeoJSON document and an HLL included. {@link #getBytes(String)}
     * is the reader that takes those.
     */
    @Override
    public byte[] getBlob(String name) {
        return getBlob(name, null);
    }

    /**
     * The bytes of a blob bin, copied into a new array, or {@code dflt} (which may be null, and is returned as
     * it is, not copied) when this side has no such bin. A bin of another kind still throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public byte[] getBlob(String name, byte[] dflt) {
        Value x = typed(name, Value.BLOB, "a blob");

        return x == null ? dflt : x.asBytes();
    }

    /**
     * The raw bytes of a string, blob, GeoJSON or HLL bin, copied into a new array on every call, or null
     * when this side has no such bin: the UTF-8 bytes of a string or a document, the bytes of a blob, an HLL
     * as the server stores it. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin
     * when it is a number, a boolean, a list or a map.
     */
    @Override
    public byte[] getBytes(String name) {
        Value x = bytes(name);

        return x == null ? null : x.asBytes();
    }

    /**
     * A list bin, as a view that reads it where the server holds it: nothing is copied until an element is
     * asked for. Null when this side has no such bin, which is not an empty list: an empty list bin is a
     * view whose {@link ListValue#isEmpty} is true. Throws {@link UdfException}
     * ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it is not a list. The view goes into an answer by position:
     * {@code out.put("tags", incoming.getList("tags").asValue())}.
     */
    @Override
    public ListValue getList(String name) {
        return getList(name, null);
    }

    /**
     * A list bin as a view, or {@code dflt} (which may be null) when this side has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public ListValue getList(String name, ListValue dflt) {
        Value x = typed(name, Value.LIST, "a list");

        return x == null ? dflt : new ListValue(x.copy());
    }

    /**
     * A map bin, as a view that looks keys up and iterates where the server holds it: nothing is copied until
     * an entry is asked for. Null when this side has no such bin, which is not an empty map. Throws
     * {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) naming the bin when it is not a map.
     */
    @Override
    public MapValue getMap(String name) {
        return getMap(name, null);
    }

    /**
     * A map bin as a view, or {@code dflt} (which may be null) when this side has no such bin. A bin of
     * another kind still throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}).
     */
    @Override
    public MapValue getMap(String name, MapValue dflt) {
        Value x = typed(name, Value.MAP, "a map");

        return x == null ? dflt : new MapValue(x.copy());
    }

    //==========================================================
    // Answering with the record.
    //

    /**
     * Puts every bin of this record that {@code overrides} lacks into it, by position, and returns it: for a
     * {@code record} answer that changes a few bins and keeps the rest. A bin this side deleted is not put,
     * so in the answer it stays deleted. This prefers one side, so an {@code xdr-merge} handler that uses it
     * on the current record is not symmetric; {@link Pair#withNewer} is the symmetric form. It never brings
     * back a bin from the other side, so a bin the other side has and this one deleted stays out too. Throws
     * {@link UdfException} when a bin could not be read or put.
     *
     * <pre>
     * MapBuilder out = Udf.map(4).put("visits", visits);
     *
     * return Resolution.record(incoming.withBins(out));   // visits as merged, the rest as it arrived
     * </pre>
     */
    public MapBuilder withBins(MapBuilder overrides) {
        // forEach gives the walk's cursor back when a read or a put throws part way.
        binNames().forEach(name -> {
            String bin = name.asString();

            if (overrides.containsKey(bin)) {
                return;
            }

            Value val = get(bin);

            if (val != null) {
                overrides.put(bin, val);
            }
        });

        return overrides;
    }

    //==========================================================
    // Decoding the event. The server hands a handler one argument, the description map; what it says is
    // attached here to the record it is true of, and the map itself is never shown.
    //

    /**
     * Reads an event call's arguments into the two records: the current one, read by index 0, into
     * {@code current} and the incoming write, read by index 1, into {@code incoming}. False when the
     * arguments are not an xdr-merge.
     */
    static boolean decode(int args, int argc, EventRecord current, EventRecord incoming) {
        Value[] a = new Value[1];

        if (LowLevel.args(args, argc, a) < 1 || a[0].kind() != Value.MAP) {
            return false;
        }

        Value desc = a[0];
        Value cur = Maps.get(desc, "current");
        Value inc = Maps.get(desc, "incoming");

        if (!current.fill(cur, 0) || !incoming.fill(inc, 1)) {
            return false;
        }

        boolean binConvergence = Maps.is(desc, "policy", "bin-lut");

        current.binConvergence = binConvergence;
        current.tombstone = Maps.getBool(cur, "tombstone");
        incoming.binConvergence = binConvergence;
        incoming.partial = Maps.getBool(inc, "partial");
        incoming.delete = Maps.getBool(inc, "deleted-record");
        incoming.lost = Maps.get(desc, "lost");

        return true;
    }

    /** Fills this from a description's entry ({@code current}, {@code incoming}). */
    private boolean fill(Value m, int which) {
        if (m.kind() != Value.MAP) {
            return false;
        }

        this.which = which;
        generation = Maps.getInt(m, "gen", 0);
        lastUpdateTime = Maps.getInt(m, "lut", 0);
        ttl = Maps.getInt(m, "ttl", 0);
        sourceId = Maps.getInt(m, "src-id", 0);
        bins = Maps.get(m, "bins");
        deleted = Maps.get(m, "deleted");
        binLastUpdateTimes = Maps.get(m, "bin-luts");
        binSourceIds = Maps.get(m, "bin-srcs");

        return bins.kind() == Value.LIST;
    }
}

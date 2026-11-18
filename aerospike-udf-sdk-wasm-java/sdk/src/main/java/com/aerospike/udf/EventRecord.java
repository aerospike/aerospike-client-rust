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

    /** The record's generation; 0 on both sides of an {@code xdr-merge}. */
    @Override
    public long generation() {
        return generation;
    }

    /** When the record was last written; 0 on both sides of an {@code xdr-merge}. */
    @Override
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long ttl() {
        return ttl;
    }

    /** The XDR source id of the data centre that wrote this version. */
    public long sourceId() {
        return sourceId;
    }

    /** The names of the record's bins, as a list of strings. */
    public ListValue binNames() {
        return new ListValue(bins);
    }

    /** The names of the bins this side deleted (bin deletes, or tombstones); empty when it deleted none. */
    public ListValue deletedBins() {
        return new ListValue(deleted);
    }

    /** Whether this side deleted any bin. */
    public boolean deletedAny() {
        return deleted.kind() == Value.LIST && deleted.length() > 0;
    }

    /** When this side last wrote the bin: per bin where the server keeps one, the record's otherwise. */
    public long binLastUpdateTime(String name) {
        return Maps.getInt(binLastUpdateTimes, name, lastUpdateTime);
    }

    /** The source id that wrote the bin: per bin where the server keeps one, the record's otherwise. */
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
     * for the current record.
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

    private Value required(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        throw Reads.problem(Reads.state(rc, v, kind), "bin ", name, what);
    }

    private Value optional(String name, int kind, String what) {
        int rc = read(name);

        if (rc == LowLevel.OK && v.kind() == kind) {
            return v;
        }

        int state = Reads.state(rc, v, kind);

        if (state != Reads.NONE) {
            throw Reads.problem(state, "bin ", name, what);
        }

        return null;
    }

    /** The bin as a value, or null when this side has no such bin or holds nil in it (a bin it deleted). */
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

    @Override
    public Value bin(String name) {
        Value x = get(name);

        if (x == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "readable");
        }

        return x;
    }

    @Override
    public boolean has(String name) {
        return Reads.state(read(name), v, v.kind()) == Reads.HIT;
    }

    @Override
    public long getInt(String name) {
        return required(name, Value.INT, "an int").payload();
    }

    @Override
    public long getInt(String name, long dflt) {
        Value x = optional(name, Value.INT, "an int");

        return x == null ? dflt : x.payload();
    }

    @Override
    public double getDouble(String name) {
        return required(name, Value.DOUBLE, "a double").doubleBits();
    }

    @Override
    public double getDouble(String name, double dflt) {
        Value x = optional(name, Value.DOUBLE, "a double");

        return x == null ? dflt : x.doubleBits();
    }

    @Override
    public boolean getBool(String name) {
        return required(name, Value.BOOL, "a bool").payload() != 0;
    }

    @Override
    public boolean getBool(String name, boolean dflt) {
        Value x = optional(name, Value.BOOL, "a bool");

        return x == null ? dflt : x.payload() != 0;
    }

    @Override
    public String getString(String name) {
        return required(name, Value.STR, "a string").asString();
    }

    @Override
    public String getString(String name, String dflt) {
        Value x = optional(name, Value.STR, "a string");

        return x == null ? dflt : x.asString();
    }

    @Override
    public byte[] getBlob(String name) {
        byte[] b = getBlob(name, null);

        if (b == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "a string or blob");
        }

        return b;
    }

    @Override
    public byte[] getBlob(String name, byte[] dflt) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return dflt;
        }

        if (state != Reads.HIT || !v.isStored() || v.isList() || v.isMap()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "bin ", name, "a string or blob");
        }

        return v.asBlob();
    }

    @Override
    public ListValue getList(String name) {
        return new ListValue(required(name, Value.LIST, "a list").copy());
    }

    @Override
    public ListValue getList(String name, ListValue dflt) {
        Value x = optional(name, Value.LIST, "a list");

        return x == null ? dflt : new ListValue(x.copy());
    }

    @Override
    public MapValue getMap(String name) {
        return new MapValue(required(name, Value.MAP, "a map").copy());
    }

    @Override
    public MapValue getMap(String name, MapValue dflt) {
        Value x = optional(name, Value.MAP, "a map");

        return x == null ? dflt : new MapValue(x.copy());
    }

    // The boxed readers Pair builds a Both from: null for a side without the bin.

    Long boxedInt(String name) {
        Value x = optional(name, Value.INT, "an int");

        return x == null ? null : x.payload();
    }

    Double boxedDouble(String name) {
        Value x = optional(name, Value.DOUBLE, "a double");

        return x == null ? null : x.doubleBits();
    }

    Boolean boxedBool(String name) {
        Value x = optional(name, Value.BOOL, "a bool");

        return x == null ? null : x.payload() != 0;
    }

    //==========================================================
    // Answering with the record.
    //

    /**
     * Puts every bin of this record that {@code overrides} lacks into it, by position, and returns it: for
     * a {@code record} answer that changes a few bins and keeps the rest. This prefers one side, so an
     * {@code xdr-merge} handler that uses it on the current record is not symmetric;
     * {@link Pair#withNewer} is the symmetric form.
     */
    public MapBuilder withBins(MapBuilder overrides) {
        for (Value name : binNames()) {
            String bin = name.asString();

            if (overrides.containsKey(bin)) {
                continue;
            }

            Value val = get(bin);

            if (val != null) {
                overrides.put(bin, val);
            }
        }

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

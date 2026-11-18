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
 * {@link RecordReader} - with what an event adds: the names of its bins, the bins this side deleted, and,
 * where the server keeps them, when and by whom each bin was last written.
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
    private Value binLuts = Maps.NOTHING;
    private Value binSrcs = Maps.NOTHING;

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
    public long binLut(String name) {
        return Maps.getLong(binLuts, name, lastUpdateTime);
    }

    /** The source id that wrote the bin: per bin where the server keeps one, the record's otherwise. */
    public long binSrc(String name) {
        return Maps.getLong(binSrcs, name, sourceId);
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
    public boolean hasBin(String name) {
        return Reads.state(read(name), v, v.kind()) == Reads.HIT;
    }

    @Override
    public long getLong(String name) {
        return required(name, Value.INT, "an int").payload();
    }

    @Override
    public long getLong(String name, long dflt) {
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
    public boolean getBoolean(String name) {
        return required(name, Value.BOOL, "a bool").payload() != 0;
    }

    @Override
    public boolean getBoolean(String name, boolean dflt) {
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
    public byte[] getBytes(String name) {
        byte[] b = getBytes(name, null);

        if (b == null) {
            throw Reads.problem(Reads.NONE, "bin ", name, "a string or blob");
        }

        return b;
    }

    @Override
    public byte[] getBytes(String name, byte[] dflt) {
        int state = Reads.state(read(name), v, v.kind());

        if (state == Reads.NONE) {
            return dflt;
        }

        if (state != Reads.HIT || !v.isStored() || v.isList() || v.isMap()) {
            throw Reads.problem(state == Reads.HIT ? Reads.WRONG : state, "bin ", name, "a string or blob");
        }

        return v.asBytes();
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

    Long boxedLong(String name) {
        Value x = optional(name, Value.INT, "an int");

        return x == null ? null : x.payload();
    }

    Double boxedDouble(String name) {
        Value x = optional(name, Value.DOUBLE, "a double");

        return x == null ? null : x.doubleBits();
    }

    Boolean boxedBoolean(String name) {
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

    /** Fills this from a description's entry ({@code current}, {@code incoming}). */
    boolean decode(Value desc, String key, int which) {
        Value m = Maps.get(desc, key);

        if (m.kind() != Value.MAP) {
            return false;
        }

        this.which = which;
        generation = Maps.getLong(m, "gen", 0);
        lastUpdateTime = Maps.getLong(m, "lut", 0);
        ttl = Maps.getLong(m, "ttl", 0);
        sourceId = Maps.getLong(m, "src-id", 0);
        bins = Maps.get(m, "bins");
        deleted = Maps.get(m, "deleted");
        binLuts = Maps.get(m, "bin-luts");
        binSrcs = Maps.get(m, "bin-srcs");

        return bins.kind() == Value.LIST;
    }
}

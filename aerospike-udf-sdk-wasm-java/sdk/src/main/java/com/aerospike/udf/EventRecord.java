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
 * One record an event hands over: read-only, with its metadata, its bins read through the event's own
 * import (never the record imports, which answer nothing in a handler). {@code which} is the index the
 * record is read by: 0 for the first record the event hands over, 1 for the second.
 */
public final class EventRecord {
    /** The index the record is read by: 0 or 1. */
    public int which;
    /** The record's generation; 0 on both sides of an {@code xdr-merge}. */
    public long gen;
    /** When the record was last written; 0 on both sides of an {@code xdr-merge}. */
    public long lut;
    /** The time to live, in seconds. */
    public long ttl;
    /** The XDR source id of the DC that wrote this version. */
    public long srcId;
    /** The list of bin names. */
    public Value bins = new Value();
    /** The bin names this side deleted (bin deletes, or tombstones); ABSENT when none. */
    public Value deleted = new Value();
    /** name -> LUT per bin, when the server keeps one; ABSENT otherwise. */
    public Value binLuts = new Value();
    /** name -> source id per bin, likewise. */
    public Value binSrcs = new Value();

    private static final byte[] SLOT = new byte[16];

    /** Reads one of this side's bins. NOT_FOUND when the side has no such bin. */
    public int bin(byte[] name, Value out) {
        int rc = Natives.eventBinValue(which, Address.ofData(name), name.length, Address.ofData(SLOT), 16);

        if (rc == Udf.OK) {
            out.read(SLOT, 0);
        }

        return rc;
    }

    /** As {@link #bin(byte[], Value)}, encoding the name on each call. */
    public int bin(String name, Value out) {
        return bin(Udf.bytes(name), out);
    }

    /** When this side last wrote the bin: per bin where the server keeps one, the record's otherwise. */
    public long binLut(String name) {
        return Udf.mapInt(binLuts, name, lut);
    }

    /** The source id that wrote the bin, likewise. */
    public long binSrc(String name) {
        return Udf.mapInt(binSrcs, name, srcId);
    }

    //==========================================================
    // Typed reads, the Rust SDK's `Record::int` and friends: null when the bin is absent or nil, an
    // UdfException when it is of another kind.
    //

    private final Value typed = new Value();

    /** The bin as a fresh value, or null when absent or nil. */
    public Value binValue(String name) {
        int rc = bin(name, typed);

        if (rc == Udf.NOT_FOUND || (rc == Udf.OK && typed.kind == Value.NIL)) {
            return null;
        }

        if (rc != Udf.OK) {
            throw new UdfException("bin " + name + " could not be read");
        }

        return new Value().set(typed.kind, typed.len, typed.payload);
    }

    private Value typed(String name, int kind, String what) {
        Value v = binValue(name);

        if (v != null && v.kind != kind) {
            throw new UdfException("bin " + name + " is not " + what);
        }

        return v;
    }

    /**
     * An integer bin, or null when it is absent or nil; throws {@link UdfException} when it is of another kind.
     */
    public Long binInt(String name) {
        Value v = typed(name, Value.INT, "an int");

        return v == null ? null : v.payload;
    }

    /**
     * A double bin, or null when it is absent or nil; throws {@link UdfException} when it is of another kind.
     */
    public Double binDouble(String name) {
        Value v = typed(name, Value.DOUBLE, "a double");

        return v == null ? null : v.asDouble();
    }

    /**
     * A boolean bin, or null when it is absent or nil; throws {@link UdfException} when it is of another kind.
     */
    public Boolean binBool(String name) {
        Value v = typed(name, Value.BOOL, "a bool");

        return v == null ? null : v.payload != 0;
    }

    /**
     * The text of a string bin, copied out, or null when it is absent or nil; throws {@link UdfException} when
     * it is of another kind.
     */
    public String binStr(String name) {
        Value v = typed(name, Value.STR, "a string");

        if (v == null) {
            return null;
        }

        byte[] b = new byte[v.len];

        if (v.len > 0 && Udf.bytes(v, b) < 0) {
            throw new UdfException("bin " + name + " could not be read");
        }

        return Udf.string(b, b.length);
    }

    /**
     * A list bin as a fresh value to walk or hand on by position, or null when it is absent or nil; throws
     * {@link UdfException} when it is of another kind.
     */
    public Value binList(String name) {
        return typed(name, Value.LIST, "a list");
    }

    /**
     * A map bin as a fresh value to walk or hand on by position, or null when it is absent or nil; throws
     * {@link UdfException} when it is of another kind.
     */
    public Value binMap(String name) {
        return typed(name, Value.MAP, "a map");
    }

    /** A walk over a list bin, or null when the bin is absent or nil. */
    public Cursor<Value> list(String name) {
        Value v = binList(name);

        return v == null ? null : Cursor.list(v);
    }

    /** A walk over a map bin's entries, or null when the bin is absent or nil. */
    public Cursor<Cursor.Entry> map(String name) {
        Value v = binMap(name);

        return v == null ? null : Cursor.map(v);
    }

    /** {@link #withBins(int)} over a built map. */
    public UdfMap withBins(UdfMap overrides) {
        withBins(overrides.handle());

        return overrides;
    }

    /** Whether this side deleted any bin. */
    public boolean deletedAny() {
        return deleted.kind == Value.LIST && deleted.len > 0;
    }

    /**
     * {@code overrides} (a built map) with every bin of this record it lacks put on top, by position: the
     * Rust SDK's {@code Record::with_bins}, for a {@code record} answer that changes a few bins and keeps
     * the rest. This prefers one side, so an {@code xdr-merge} handler that uses it on the current record
     * is not symmetric; {@link Pair#withNewer} is the symmetric form. Returns {@code overrides}.
     */
    public int withBins(int overrides) {
        Udf.eachStr(bins, (text, len, v) -> {
            byte[] name = new byte[len];
            System.arraycopy(text, 0, name, 0, len);

            if (Udf.mapHasKey(overrides, name)) {
                return;
            }

            Value val = new Value();

            if (bin(name, val) == Udf.OK) {
                Udf.mapPutValue(overrides, name, val);
            }
        });

        return overrides;
    }

    /** Fills this from a description's entry (`before`, `after`, `record`, `current`, `incoming`). */
    boolean decode(Value desc, String key, int which) {
        Value m = Udf.mapGet(desc, key);

        if (m.kind != Value.MAP) {
            return false;
        }

        this.which = which;
        gen = Udf.mapInt(m, "gen", 0);
        lut = Udf.mapInt(m, "lut", 0);
        ttl = Udf.mapInt(m, "ttl", 0);
        srcId = Udf.mapInt(m, "src-id", 0);
        bins = Udf.mapGet(m, "bins");
        deleted = Udf.mapGet(m, "deleted");
        binLuts = Udf.mapGet(m, "bin-luts");
        binSrcs = Udf.mapGet(m, "bin-srcs");

        return bins.kind == Value.LIST;
    }
}

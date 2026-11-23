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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.udf.Function;
import com.aerospike.udf.ListBuilder;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.MapValue;
import com.aerospike.udf.Record;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

/**
 * The Java counterpart of the Rust SDK's test fixture (aerospike-udf-sdk/wasm-rust/examples/udf): one function
 * per corner of the record API that the Rust SDK has and the client tests check, so that
 * {@code tests/src/udf_java.rs} can hold the Java SDK to the same surface. Nothing here is an example to
 * copy; it is the SDK's own coverage.
 */
public final class Fixture {
    private static final String GEO = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";

    /** The sum of the integers in list bin {@code l}, walked with a for-each; any other element fails the call. */
    @Function("sum_list")
    static long sumList(Record rec) {
        long total = 0;

        for (Value v : rec.bin("l").asList()) {
            total += v.asInt();
        }

        return total;
    }

    /** Everything the record knows about itself, as a map. */
    @Function("describe")
    static MapBuilder describe(Record rec) {
        MapBuilder out = Udf.map(8)
                .put("generation", rec.generation())
                .put("bins", rec.binCount())
                .put("size", rec.size())
                .put("lut", rec.lastUpdateTime())
                .put("ttl", rec.ttl());
        String set = rec.setName();

        return set != null ? out.put("set", set) : out.putNil("set");
    }

    /** The typed readers over scalar bins, and a map walked entry by entry. */
    @Function("typed")
    static MapBuilder typed(Record rec) {
        MapBuilder out = Udf.map(8)
                .put("i", rec.bin("i").asInt())
                .put("d", rec.bin("d").asDouble())
                .put("s", rec.bin("s").asString())
                .put("b", rec.bin("b").asBool());
        long sum = 0;
        ListBuilder keys = Udf.list(4);

        for (MapValue.Entry e : rec.bin("m").asMap()) {
            if (e.getValue().isInt()) {
                sum += e.getValue().asInt();
            }

            keys.add(e.getKey());
        }

        return out.put("msum", sum).put("keys", keys);
    }

    /** The writes the typed natives do not cover: packed lists, a bool, a nil and a GeoJSON. */
    @Function("write_lists")
    static long writeLists(Record rec) {
        rec.setInts("w", new long[] { 5, -1, 1L << 40 });
        rec.setDoubles("wd", new double[] { 0.5, -4.0 });
        rec.set("flag", true);
        rec.set("gone", Value.ofNil());
        rec.setGeoJson("geo", GEO);
        rec.save();

        return 1;
    }

    /**
     * {@code [doc[index], <geojson>, {1: "one", "first": doc[0]}]}: stored values into the builders by
     * position, a GeoJSON that stays a GeoJSON, and a map with an integer key.
     */
    @Function("relist")
    static ListBuilder relist(Record rec, long index) {
        ListValue doc = rec.bin("doc").asList();

        if (doc.isEmpty() || index < 0 || index >= doc.size()) {
            throw new UdfException("relist: doc has no element " + index);
        }

        MapBuilder inner = Udf.map(2).put(1, "one").put("first", doc.get(0));

        return Udf.list(3).add(doc.get((int) index)).addGeoJson(GEO).add(inner);
    }

    /** Writes a GeoJSON bin back as the kind it was read as, and reports the document's length. */
    @Function("geo_roundtrip")
    static long geoRoundtrip(Record rec, String name) {
        Value doc = rec.get(name);

        if (doc == null || !doc.isGeoJson()) {
            throw new UdfException("that bin does not hold geojson");
        }

        rec.set(name, doc);
        rec.save();

        return doc.length();
    }

    /** A GeoJSON of the guest's as the result. */
    @Function("geo_result")
    static Value geoResult() {
        return Value.ofGeoJson(GEO);
    }

    /** Sets the record's TTL and saves. */
    @Function("touch_ttl")
    static long touchTtl(Record rec, long seconds) {
        rec.setTtl((int) seconds);
        rec.set("touched", 1);
        rec.save();

        return 1;
    }

    /** Deletes the record. */
    @Function("remove_me")
    static long removeMe(Record rec) {
        rec.delete();

        return 1;
    }

    /**
     * Saves with nothing staged, and reports what that did: whether the record exists afterwards. It is
     * there so that a test can pin down what {@code save} means when there is nothing to save, on a record
     * that is there and on one that is not.
     */
    @Function("save_nothing")
    static boolean saveNothing(Record rec) {
        rec.save();

        return rec.exists();
    }

    /** Deletes the record, staged writes and all. Answers what {@code exists} says straight afterwards. */
    @Function("delete_it")
    static boolean deleteIt(Record rec) {
        rec.set("n", 99);
        rec.delete();

        return rec.exists();
    }

    /** Removes a bin by name and saves. */
    @Function("drop_bin")
    static long dropBin(Record rec, String name) {
        rec.remove(name);
        rec.save();

        return rec.binCount();
    }

    /** Sets a bin to nil and saves, which removes it: a bin cannot hold nil. */
    @Function("nil_bin")
    static long nilBin(Record rec, String name) {
        rec.set(name, Value.ofNil());
        rec.save();

        return rec.binCount();
    }

    /** One element of a list bin by its index, and whether a map the call builds knows its keys. */
    @Function("index_probe")
    static MapBuilder indexProbe(Record rec, String name, long index) {
        MapBuilder out = Udf.map(4);
        ListValue list = rec.bin(name).asList();

        out.put("len", list.size());

        // An index past the end, however far, is asked about before it is used: get() throws for one.
        if (index >= 0 && index < list.size()) {
            out.put("at", list.get((int) index));
        }
        else {
            out.put("at", "absent");
        }

        out.put(7, "seven");

        boolean knows = out.containsKey("len") && out.containsKey(7);
        boolean invents = out.containsKey("nope") || out.containsKey(8);

        return out.put("knows", knows).put("invents", invents);
    }

    /**
     * The bridge to {@code java.util}, both ways: list bin {@code l} and map bin {@code m} copied out,
     * changed as plain Java collections, and handed back - one written to a bin, one nested in the other,
     * the whole returned as a {@code java.util.Map}.
     */
    @Function("bridge")
    static Map<String, Object> bridge(Record rec) {
        List<Object> items = rec.bin("l").asList().toList();
        Map<Object, Object> entries = rec.bin("m").asMap().toMap();

        List<Object> doubled = new ArrayList<>();

        for (Object item : items) {
            doubled.add(((Long) item) * 2);
        }

        rec.set("doubled", doubled);
        rec.save();

        Map<String, Object> out = new LinkedHashMap<>();

        out.put("items", items);
        out.put("entries", entries);
        out.put("size", (long) items.size());
        out.put("half", 0.5);
        out.put("yes", true);
        out.put("nothing", null);

        return out;
    }

    /**
     * Leaves a for-each over list bin {@code l} early, {@code rounds} times over, and then walks it whole:
     * more rounds than the call has handle slots, so a walk that held one after an early exit would run
     * out of them. Answers the sum of the full walk.
     */
    @Function("early_exit")
    static long earlyExit(Record rec, long rounds) {
        ListValue list = rec.bin("l").asList();

        for (long round = 0; round < rounds; round++) {
            for (Value v : list) {
                if (v.isInt()) {
                    break;
                }
            }
        }

        long total = 0;

        for (Value v : list) {
            total += v.asInt();
        }

        return total;
    }

    /** Timing only: n reads of an integer bin. */
    @Function("bench_get")
    static long benchGet(Record rec, long n) {
        long sum = 0;

        for (long k = 0; k < n; k++) {
            sum += rec.getInt("i", 0);
        }

        return sum;
    }

    /** Timing only: n reads and staged writes of an integer bin; nothing is saved. */
    @Function("bench_set")
    static long benchSet(Record rec, long n) {
        for (long k = 0; k < n; k++) {
            rec.set("i", rec.getInt("i", 0) + 1);
        }

        return rec.getInt("i", 0);
    }

    /**
     * Builds {@code count} maps, each holding a list, nested in one outer list and closed by
     * try-with-resources once they are in; then walks every list of list bin {@code nested} with a for-each
     * of its own, whose cursor goes back when the loop ends. Answers {@code [elements, sum]}. The call holds a
     * few handles at a time however large {@code count} is: what bounds it is its memory, not a number of
     * handles.
     */
    @Function("churn")
    static ListBuilder churn(Record rec, long count) {
        try (ListBuilder out = Udf.list(0)) {
            for (long i = 0; i < count; i++) {
                try (ListBuilder inner = Udf.list(2).add(i).add(1); MapBuilder entry = Udf.map(1)) {
                    out.add(entry.put("l", inner));
                }
            }

            long sum = 0;

            for (Value row : rec.bin("nested").asList()) {
                for (Value cell : row.asList()) {
                    sum += cell.asInt();
                }
            }

            return Udf.list(2).add(out.size()).add(sum);
        }
    }
}

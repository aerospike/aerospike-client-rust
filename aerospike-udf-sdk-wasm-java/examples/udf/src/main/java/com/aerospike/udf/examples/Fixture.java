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
import com.aerospike.udf.Cursor;
import com.aerospike.udf.Function;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.UdfList;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

/**
 * The Java counterpart of the Rust SDK's test fixture (aerospike-udf-sdk-wasm/examples/udf): one function
 * per corner of the record API that the Rust SDK has and the client tests check, so that
 * {@code tests/src/udf_java.rs} can hold the Java SDK to the same surface. Nothing here is an example to
 * copy; it is the SDK's own coverage.
 */
public final class Fixture {
    private static final String GEO = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";

    private static final Bin L = Bin.of("l");
    private static final Bin M = Bin.of("m");
    private static final Bin DOC = Bin.of("doc");

    /** The sum of the integers in list bin {@code l}, walked with a cursor; any other element fails the call. */
    @Function("sum_list")
    static long sumList() {
        long total = 0;

        for (Value v : L.asList()) {
            total += v.asLong();
        }

        return total;
    }

    /** Everything the record knows about itself, as a map. */
    @Function("describe")
    static UdfMap describe() {
        UdfMap out = UdfMap.create(8)
                .put("generation", Udf.generation())
                .put("bins", Udf.binCount())
                .put("size", Udf.recordSize())
                .put("lut", Udf.lastUpdateTime())
                .put("ttl", Udf.ttl());

        String set = Udf.setName();

        return set != null ? out.put("set", set) : out.putNull("set");
    }

    /** The typed readers over scalar bins, and a map walked entry by entry. */
    @Function("typed")
    static UdfMap typed() {
        UdfMap out = UdfMap.create(8)
                .put("i", Bin.of("i").getLong())
                .put("d", Bin.of("d").getDouble())
                .put("s", Bin.of("s").getString())
                .put("b", Bin.of("b").getBoolean());

        long sum = 0;
        UdfList keys = UdfList.create(4);

        try (Cursor<Cursor.Entry> c = M.asMap()) {
            while (c.next()) {
                if (c.value.isInt()) {
                    sum += c.value.payload;
                }

                keys.add(c.key);
            }
        }

        return out.put("msum", sum).put("keys", keys);
    }

    /** The writes the typed natives do not cover: packed lists, a bool, a nil and a GeoJSON. */
    @Function("write_lists")
    static long writeLists() {
        Bin.of("w").setInts(new long[] { 5, -1, 1L << 40 });
        Bin.of("wd").setDoubles(new double[] { 0.5, -4.0 });
        Bin.of("flag").set(true);
        Bin.of("gone").setNull();
        Bin.of("geo").setGeoJson(GEO);
        Udf.commit();

        return 1;
    }

    /**
     * {@code [doc[index], <geojson>, {1: "one", "first": doc[0]}]}: stored values into the builders by
     * position, a GeoJSON that stays a GeoJSON, and a map with an integer key.
     */
    @Function("relist")
    static UdfList relist(long index) {
        Value first = null;
        Value picked = null;
        long i = 0;

        for (Value v : DOC.asList()) {
            if (i == 0) {
                first = v;
            }

            if (i == index) {
                picked = v;
                break;
            }

            i++;
        }

        if (first == null || picked == null) {
            throw new UdfException("relist: doc has no element " + index);
        }

        UdfMap inner = UdfMap.create(2).put(1, "one").put("first", first);

        return UdfList.create(3).add(picked).addGeoJson(GEO).add(inner);
    }

    /** Writes a GeoJSON bin back as the kind it was read as, and reports the document's length. */
    @Function("geo_roundtrip")
    static long geoRoundtrip(String name) {
        Bin bin = Bin.of(name);
        Value doc = bin.get();

        if (doc == null || doc.kind != Value.GEOJSON) {
            throw new UdfException("that bin does not hold geojson");
        }

        bin.set(doc);
        Udf.commit();

        return doc.len;
    }

    /** A GeoJSON of the guest's as the result. */
    @Function("geo_result")
    static Value geoResult() {
        return Value.ofGeoJson(GEO);
    }

    /** Sets the record's TTL and commits. */
    @Function("touch_ttl")
    static long touchTtl(long seconds) {
        if (Udf.setTtl((int) seconds) != Udf.OK) {
            throw new UdfException("touch_ttl: the TTL was refused");
        }

        Bin.of("touched").set(1);
        Udf.commit();

        return 1;
    }

    /** Deletes the record. */
    @Function("remove_me")
    static long removeMe() {
        Udf.delete();

        return 1;
    }
}

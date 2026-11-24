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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.aerospike.udf.Both;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.ListBuilder;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.LowLevel;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.MapValue;
import com.aerospike.udf.Nullable;
import com.aerospike.udf.On;
import com.aerospike.udf.Pair;
import com.aerospike.udf.Record;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Stream;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

/**
 * The conformance module every guest SDK has, in Java: one export per corner of the public API, each
 * answering in the form the other four languages' {@code apitest} modules answer in, so that a harness can
 * call all five with the same record and arguments and compare. Nothing here is an example to copy; it is
 * the SDK used the way its documentation says, with every answer the SDK gives passed on as it is.
 *
 * <p>An answer is a list whose first element is a tag: {@code "some"} before a value, {@code "none"} where
 * the SDK answers null, {@code "ok"} for a call that returned, and for one that threw the tag of the status
 * its {@link UdfException} carries: {@code "wrong"} for a value of another kind, {@code "absent"} for
 * something that is not there, {@code "refused"}, {@code "small"} or {@code "err"}.
 *
 * <p>The class is the {@code agg} stream, and carries the record UDFs and the merge handler as static
 * methods beside it.
 */
@Stream("agg")
public final class ApiTest {
    private static final String GEO = "{\"type\":\"Point\",\"coordinates\":[1,2]}";

    //==========================================================
    // The stream: how many records have an integer bin n, and the sum of it.
    //

    private long count;
    private long sum;

    void record(Record rec) {
        Value n = rec.get("n");

        if (n != null && n.isInt()) {
            count++;
            sum += n.asInt();
        }
    }

    ListBuilder end() {
        ListBuilder out = Udf.list(2).add(count).add(sum);

        count = 0;
        sum = 0;

        return out;
    }

    //==========================================================
    // Answers.
    //

    private static ListBuilder tag(String tag) {
        return Udf.list(1).add(tag);
    }

    private static ListBuilder some() {
        return Udf.list(2).add("some");
    }

    /** The tag of the status a failure carries. */
    private static String status(UdfException e) {
        switch (e.status()) {
        case LowLevel.NOT_FOUND:
            return "absent";
        case LowLevel.TYPE_MISMATCH:
            return "wrong";
        case LowLevel.REFUSED:
            return "refused";
        default:
            return e.status() <= LowLevel.NEEDED_BASE ? "small" : "err";
        }
    }

    /** The kind of a value by its own tests. */
    private static String kind(Value v) {
        if (v.isNil()) {
            return "nil";
        }

        if (v.isBool()) {
            return "bool";
        }

        if (v.isInt()) {
            return "int";
        }

        if (v.isDouble()) {
            return "double";
        }

        if (v.isString()) {
            return "string";
        }

        if (v.isBlob()) {
            return "blob";
        }

        if (v.isList()) {
            return "list";
        }

        if (v.isMap()) {
            return "map";
        }

        if (v.isGeoJson()) {
            return "geojson";
        }

        return v.isHll() ? "hll" : "other";
    }

    //==========================================================
    // rd: one reader of the record.
    //

    @Function("rd")
    static ListBuilder rd(Record rec, String name, String how) {
        // A typed reader answers null for a bin that is not there and throws for one of another kind; what
        // it throws carries the status. Only the read is in the try: building the answer can fail too (a
        // stored value nested deeper than a list may hold cannot be put into the answer), and that is a
        // failure of the call, not something the reader said.
        Object got;

        try {
            got = read(rec, name, how);
        }
        catch (UdfException e) {
            return tag(status(e));
        }

        if (got == null) {
            throw new UdfException("rd: unknown how");
        }

        if (got == NONE) {
            return tag("none");
        }

        ListBuilder out = some();

        if (got instanceof Value) {
            out.add((Value) got);
        }
        else if (got instanceof Long) {
            out.add(((Long) got).longValue());
        }
        else if (got instanceof Double) {
            out.add(((Double) got).doubleValue());
        }
        else if (got instanceof Boolean) {
            out.add(((Boolean) got).booleanValue());
        }
        else if (got instanceof String) {
            out.add((String) got);
        }
        else {
            out.add((byte[]) got);
        }

        return out;
    }

    /** What a reader answers when the bin is not there or holds nil. */
    private static final Object NONE = new Object();

    /**
     * What reader {@code how} reads - a Value, Long, Double, Boolean, String or byte[], or NONE - or null
     * when there is no such reader. In a method of its own: TeaVM 0.13.1 compiles this switch, written
     * inline in a try whose catch returns, into code the server's WAMR loader refuses ("type mismatch:
     * expect data but stack was empty").
     */
    private static Object read(Record rec, String name, String how) {
        switch (how) {
        case "has":
            return Boolean.valueOf(rec.has(name));
        case "get": {
            Value v = rec.get(name);

            return v == null ? NONE : v;
        }
        case "bin":
            // bin() throws for a record without the bin: that exception is the SDK's "no such bin".
            return rec.bin(name);
        case "int": {
            Long v = rec.getInt(name);

            return v == null ? NONE : v;
        }
        case "double": {
            Double v = rec.getDouble(name);

            return v == null ? NONE : v;
        }
        case "bool": {
            Boolean v = rec.getBool(name);

            return v == null ? NONE : v;
        }
        case "string": {
            String v = rec.getString(name);

            return v == null ? NONE : v;
        }
        case "blob": {
            byte[] v = rec.getBlob(name);

            return v == null ? NONE : v;
        }
        case "list": {
            ListValue v = rec.getList(name);

            return v == null ? NONE : v.asValue();
        }
        case "map": {
            MapValue v = rec.getMap(name);

            return v == null ? NONE : v.asValue();
        }
        case "int_or":
            return Long.valueOf(rec.getInt(name, 7));
        case "double_or":
            return Double.valueOf(rec.getDouble(name, 7.5));
        case "bool_or":
            return Boolean.valueOf(rec.getBool(name, true));
        default:
            return null;
        }
    }

    //==========================================================
    // wr: one staged write, and a save.
    //

    /** The elements of a list when it has at most 256 and every one is an integer; null otherwise. */
    private static long[] ints(Value v) {
        if (!v.isList() || v.length() > 256) {
            return null;
        }

        long[] out = new long[v.length()];
        int n = 0;

        for (Value e : v.asList()) {
            if (!e.isInt()) {
                return null;
            }

            out[n++] = e.asInt();
        }

        return out;
    }

    /** The elements of a list when it has at most 256 and every one is a double; null otherwise. */
    private static double[] doubles(Value v) {
        if (!v.isList() || v.length() > 256) {
            return null;
        }

        double[] out = new double[v.length()];
        int n = 0;

        for (Value e : v.asList()) {
            if (!e.isDouble()) {
                return null;
            }

            out[n++] = e.asDouble();
        }

        return out;
    }

    /**
     * Stages the write {@code how} names; false, with nothing staged, when {@code v} is missing or of a kind
     * the write cannot take.
     */
    private static boolean stage(Record rec, String name, String how, Value v) {
        switch (how) {
        case "set":
            if (v == null) {
                return false;
            }

            rec.set(name, v);
            return true;
        case "blob":
            if (v == null || !(v.isString() || v.isBlob())) {
                return false;
            }

            rec.set(name, v.asBytes());
            return true;
        case "geojson":
            if (v == null || !v.isString()) {
                return false;
            }

            rec.setGeoJson(name, v.asString());
            return true;
        case "ints": {
            long[] longs = v == null ? null : ints(v);

            if (longs == null) {
                return false;
            }

            rec.setInts(name, longs);
            return true;
        }
        case "doubles": {
            double[] reals = v == null ? null : doubles(v);

            if (reals == null) {
                return false;
            }

            rec.setDoubles(name, reals);
            return true;
        }
        default:
            rec.remove(name);
            return true;
        }
    }

    // v is optional, and a nil v is not a missing one: a @Nullable parameter is null for both, so the
    // optional argument is taken as the rest, and more than one of them is too many.
    @Function("wr")
    static ListBuilder wr(Record rec, String name, String how, Value[] rest) {
        if (rest.length > 1) {
            throw new UdfException("wr takes at most 3 arguments, got " + (2 + rest.length));
        }

        switch (how) {
        case "set":
        case "blob":
        case "geojson":
        case "ints":
        case "doubles":
        case "remove":
            break;
        default:
            throw new UdfException("wr: unknown how");
        }

        String stage;
        String save = "skipped";

        try {
            stage = stage(rec, name, how, rest.length == 0 ? null : rest[0]) ? "ok" : "wrong";
        }
        catch (UdfException e) {
            stage = status(e);
        }

        if (stage.equals("ok")) {
            save = saved(rec);
        }

        return Udf.list(3).add(stage).add(save).add(rec.binCount());
    }

    //==========================================================
    // meta: what the record knows about itself.
    //

    @Function("meta")
    static MapBuilder meta(Record rec) {
        long ttl = rec.ttl();
        String set = rec.setName();
        Value key = rec.key();

        MapBuilder out = Udf.map(8)
                .put("exists", rec.exists())
                .put("generation", rec.generation())
                .put("ttl", ttl == 0 ? -1 : ttl)
                .put("bins", rec.binCount())
                .put("sized", rec.size() > 0)
                .put("set", set == null ? "" : set)
                .put("digest", rec.digest().length);

        return key == null ? out.putNil("key") : out.put("key", key);
    }

    //==========================================================
    // val: a value's kind, and the value handed back.
    //

    @Function("val")
    static ListBuilder val(Value v) {
        ListBuilder out = Udf.list(3).add(kind(v)).add(v);

        if (v.isList()) {
            out.add(v.asList().size());
        }
        else if (v.isMap()) {
            out.add(v.asMap().size());
        }

        return out;
    }

    //==========================================================
    // mk: a value from the builders, an element at a time.
    //

    /** Element {@code i} of the mixed list, appended. */
    private static void mixed(ListBuilder out, long i) {
        switch ((int) (i % 6)) {
        case 0:
            out.add(i);
            break;
        case 1:
            out.add(i + 0.5);
            break;
        case 2:
            out.add(i % 4 == 2);
            break;
        case 3:
            out.add("s" + i);
            break;
        case 4:
            out.add(new byte[] { (byte) i });
            break;
        default:
            out.addNil();
            break;
        }
    }

    /** Element {@code i} of the mixed list, put under {@code key}. */
    private static void mixed(MapBuilder out, String key, long i) {
        switch ((int) (i % 6)) {
        case 0:
            out.put(key, i);
            break;
        case 1:
            out.put(key, i + 0.5);
            break;
        case 2:
            out.put(key, i % 4 == 2);
            break;
        case 3:
            out.put(key, "s" + i);
            break;
        case 4:
            out.put(key, new byte[] { (byte) i });
            break;
        default:
            out.putNil(key);
            break;
        }
    }

    /** The value {@code what} names, or null when it names none. */
    private static Value make(String what, long n) {
        switch (what) {
        case "list_int": {
            ListBuilder out = Udf.list();

            for (long i = 0; i < n; i++) {
                out.add(i);
            }

            return out.asValue();
        }
        case "list_mixed": {
            ListBuilder out = Udf.list();

            for (long i = 0; i < n; i++) {
                mixed(out, i);
            }

            return out.asValue();
        }
        case "map_str": {
            MapBuilder out = Udf.map();

            for (long i = 0; i < n; i++) {
                out.put("k" + i, i);
            }

            return out.asValue();
        }
        case "map_int": {
            MapBuilder out = Udf.map();

            for (long i = 0; i < n; i++) {
                out.put(i, "v" + i);
            }

            return out.asValue();
        }
        case "map_mixed": {
            MapBuilder out = Udf.map();

            for (long i = 0; i < n; i++) {
                mixed(out, "k" + i, i);
            }

            return out.asValue();
        }
        case "nested": {
            ListBuilder out = Udf.list(1).add(n);

            for (long i = 1; i < n; i++) {
                ListBuilder inner = out;

                out = Udf.list(1).add(inner);
                inner.close(); // the outer list keeps it
            }

            return out.asValue();
        }
        case "string": {
            char[] text = new char[(int) n];

            Arrays.fill(text, 'x');

            return Value.ofString(new String(text));
        }
        case "blob": {
            byte[] bytes = new byte[(int) n];

            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) i;
            }

            return Value.ofBlob(bytes);
        }
        default:
            return null;
        }
    }

    @Function("mk")
    static Value mk(String what, long n) {
        if (n < 0 || n > 1_000_000) {
            throw new UdfException("mk: refused");
        }

        Value out;

        try {
            out = make(what, n);
        }
        catch (UdfException e) {
            // A builder that fails throws, with the host's status: one list too deep, or one too many.
            throw new UdfException("mk: " + status(e));
        }

        if (out == null) {
            throw new UdfException("mk: unknown what");
        }

        return out;
    }

    //==========================================================
    // walk: a stored list or map, iterated and looked up in.
    //

    @Function("walk")
    static ListBuilder walk(Record rec, String name) {
        Value v = rec.get(name);

        if (v == null) {
            return tag("none");
        }

        if (v.isList()) {
            ListValue list = v.asList();
            int len = list.size();
            long sum = 0;
            long strings = 0;

            for (Value e : list) {
                if (e.isInt()) {
                    sum += e.asInt();
                }
                else if (e.isString()) {
                    strings++;
                }
            }

            ListBuilder out = Udf.list(7).add("list").add(len).add(sum).add(strings);

            if (len > 0) {
                out.add(list.get(0)).add(list.get(len - 1));
            }
            else {
                out.addNil().addNil();
            }

            // An index past the end has no null answer in this SDK: get() throws for it, as a
            // java.util.List does, and the status it throws with is the "nothing there" of an index.
            try {
                list.get(len);
                out.add("some");
            }
            catch (UdfException e) {
                out.add(e.status() == LowLevel.NOT_FOUND ? "none" : status(e));
            }

            return out;
        }

        if (v.isMap()) {
            MapValue map = v.asMap();
            long sum = 0;

            for (MapValue.Entry e : map) {
                if (e.getValue().isInt()) {
                    sum += e.getValue().asInt();
                }
            }

            ListBuilder out = Udf.list(7).add("map").add(map.size()).add(sum)
                    .add(map.containsKey("k1")).add(map.containsKey(1));
            Value atK1 = map.get("k1");
            Value at1 = map.get(1);

            if (atK1 != null) {
                out.add(atK1);
            }
            else {
                out.addNil();
            }

            if (at1 != null) {
                out.add(at1);
            }
            else {
                out.addNil();
            }

            return out;
        }

        return tag("wrong");
    }

    //==========================================================
    // ar: a required, an optional and the rest.
    //

    @Function("ar")
    static ListBuilder ar(long a, @Nullable String b, Value[] rest) {
        ListBuilder out = Udf.list(3 + rest.length).add(a);

        if (b != null) {
            out.add(b);
        }
        else {
            out.addNil();
        }

        out.add(rest.length);

        for (Value v : rest) {
            out.add(kind(v));
        }

        return out;
    }

    //==========================================================
    // life: staging, saving, deleting, and failing afterwards.
    //

    private static String saved(Record rec) {
        try {
            rec.save();
            return "ok";
        }
        catch (UdfException e) {
            return status(e);
        }
    }

    @Function("life")
    static ListBuilder life(Record rec, String what) {
        // A staging call that fails (the record cannot be written) ends the answer with its status alone.
        switch (what) {
        case "staged":
            try {
                rec.set("t", 1);
            }
            catch (UdfException e) {
                return tag(status(e));
            }

            try {
                return some().add(rec.getInt("t", -1)).add(rec.binCount());
            }
            catch (UdfException e) {
                return tag(status(e)).add(rec.binCount());
            }
        case "save_twice": {
            String first;

            try {
                rec.set("t", 1);
                first = saved(rec);
                rec.set("t", 2);
            }
            catch (UdfException e) {
                return tag(status(e));
            }

            return Udf.list(2).add(first).add(saved(rec));
        }
        case "delete": {
            String status = "ok";

            try {
                rec.delete();
            }
            catch (UdfException e) {
                status = status(e);
            }

            return Udf.list(2).add(status).add(rec.exists());
        }
        case "ttl":
            // A TTL is staged like a bin: it is the save that writes it.
            try {
                rec.setTtl(100);
            }
            catch (UdfException e) {
                return tag(status(e));
            }

            return Udf.list(2).add("ok").add(saved(rec));
        case "fail_after_save":
            try {
                rec.set("t", 9);
            }
            catch (UdfException e) {
                return tag(status(e));
            }

            rec.save();

            throw new UdfException("life: failing");
        default:
            throw new UdfException("life: unknown what");
        }
    }

    //==========================================================
    // res: each kind of result.
    //

    @Function("res")
    static Value res(String kind) {
        switch (kind) {
        case "nil":
            return Value.ofNil();
        case "int":
            return Value.ofInt(Long.MIN_VALUE);
        case "double":
            return Value.ofDouble(1e308);
        case "bool":
            return Value.ofBool(true);
        case "string":
            return Value.ofString("");
        case "blob":
            return Value.ofBlob(new byte[0]);
        case "geojson":
            return Value.ofGeoJson(GEO);
        case "list":
            return Udf.list().asValue();
        case "map":
            return Udf.map().asValue();
        case "fail_long": {
            char[] text = new char[600];

            Arrays.fill(text, 'x');

            throw new UdfException(new String(text));
        }
        default:
            throw new UdfException("res: unknown kind");
        }
    }

    //==========================================================
    // txt: a string in and out.
    //

    @Function("txt")
    static ListBuilder txt(String s) {
        // A String counts UTF-16 units; the length the caller sent is that of its UTF-8 bytes.
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);

        return Udf.list(3).add(utf8.length).add(s).add(utf8.length == 0 ? -1 : utf8[0] & 0xff);
    }

    //==========================================================
    // cmp: two stored values, by their bytes.
    //

    @Function("cmp")
    static ListBuilder cmp(Record rec, String a, String b) {
        Value x = rec.get(a);
        Value y = rec.get(b);

        if (x == null || y == null) {
            return tag("none");
        }

        // Scalars, and any pair the host cannot compare, throw with the wrong-kind status.
        try {
            return some().add(x.cmpStored(y));
        }
        catch (UdfException e) {
            return tag(status(e));
        }
    }

    //==========================================================
    // merge_api: everything an xdr-merge handler is told, answered as a record.
    //

    @On.XdrMerge("merge_api")
    static Resolution mergeApi(EventRecord current, EventRecord incoming) {
        Pair pair = Pair.of(current, incoming);
        MapBuilder out = Udf.map(20);

        Both<Long> n = pair.getInt("n");
        Long max = n.max();
        Long min = n.min();

        if (max != null) {
            out.put("max", max);
        }

        if (min != null) {
            out.put("min", min);
        }

        // A missing side counts as zero; neither side, and sumInt() answers null, as max() and min() do.
        Long sum = n.sumInt();

        if (sum != null) {
            out.put("sum", sum);
        }

        Both<String> s = pair.getString("s");
        String newer = s.newer();
        String older = s.older();
        String currentFirst = s.currentOrIncoming();
        String incomingFirst = s.incomingOrCurrent();

        if (newer != null) {
            out.put("newer_s", newer);
        }

        if (older != null) {
            out.put("older_s", older);
        }

        if (currentFirst != null) {
            out.put("c_or_i", currentFirst);
        }

        if (incomingFirst != null) {
            out.put("i_or_c", incomingFirst);
        }

        out.put("i_newer", pair.incomingIsNewer("s"));

        // The name lists are views, each put as the one value it is; a list the server did not send is an
        // empty one.
        out.put("c_names", current.binNames().asValue());
        out.put("i_names", incoming.binNames().asValue());
        out.put("c_del", current.deletedBins().asValue());
        out.put("i_del", incoming.deletedBins().asValue());

        out.put("c_meta", Udf.list(6).add(current.generation()).add(current.lastUpdateTime()).add(current.ttl())
                .add(current.sourceId()).add(current.isTombstone()).add(current.usesBinConvergence()));
        out.put("i_meta", Udf.list(6).add(incoming.generation()).add(incoming.lastUpdateTime()).add(incoming.ttl())
                .add(incoming.sourceId()).add(incoming.isDelete()).add(incoming.isPartial()));
        out.put("blut", Udf.list(2).add(current.binLastUpdateTime("s")).add(incoming.binLastUpdateTime("s")));
        out.put("bsrc", Udf.list(2).add(current.binSourceId("s")).add(incoming.binSourceId("s")));

        return Resolution.record(out);
    }
}

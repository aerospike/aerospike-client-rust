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

import java.util.List;
import java.util.Map;

import org.teavm.interop.Address;

/**
 * A list being built on the server, to be written to a bin, returned as the result, nested in another
 * collection or handed back from an event handler. {@link Udf#list()} starts one; every {@code add} is
 * one call to the host, appends, and returns the builder, so they chain; one that the host refuses throws
 * {@link UdfException}. The list lives until the call ends.
 *
 * <pre>
 * ListBuilder tags = Udf.list().add("a").add("b").addAll(rec.getList("more"));
 *
 * rec.set("tags", tags);
 * </pre>
 *
 * <p>It is a builder, not a collection: what went in is not read back from it. To read a list, read the
 * stored one ({@link ListValue}); to build from plain Java data, {@link #addAll(Iterable)} takes a
 * {@code java.util.List}.
 *
 * <p>A builder holds one of the call's 256 handles for as long as the call runs, and an {@code add} takes
 * none, whatever is added: a loop may add as many elements as the server lets a call build. An {@code add}
 * throws, with the status {@link LowLevel#REFUSED}, when the call has built as much on the server as it is
 * allowed, when the value would nest deeper than 64 levels, and when a collection would end up inside
 * itself. What was added before the throw stays in the list.
 */
public final class ListBuilder {
    private final int handle;

    ListBuilder(int handle) {
        this.handle = handle;
    }

    /** A new list with room for {@code capacity} elements, which it grows past. */
    static ListBuilder create(int capacity) {
        int h = Natives.listNew(Math.max(capacity, 1));

        if (h <= 0) {
            // The host gives no reason; both it has are a refusal: no handle left, or too much built.
            throw new UdfException(LowLevel.REFUSED, "could not build a list: the call has no handle left for"
                    + " it, or has built as much as it may");
        }

        return new ListBuilder(h);
    }

    /** The host's handle on the list. */
    int handle() {
        return handle;
    }

    /** How many elements have been added. One call to the host. */
    public int size() {
        return Natives.collSize(handle);
    }

    /** Whether nothing has been added. One call to the host. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * The list as a {@link Value} of kind {@link Value#HANDLE}, to put where a value is taken:
     * {@link Record#set(String, Value)}, another builder, {@link Udf#result}. It stands for this same list,
     * not a copy, and cannot be read back: {@link Value#asList} throws on it.
     */
    public Value asValue() {
        return new Value().assign(Value.HANDLE, 0, handle);
    }

    private ListBuilder check(int rc) {
        if (rc != LowLevel.OK) {
            throw new UdfException(rc < 0 ? rc : LowLevel.ERR, "could not add to the list: " + why(rc));
        }

        return this;
    }

    /** Why the host turned an element or an entry down, for a message. */
    static String why(int rc) {
        switch (rc) {
        case LowLevel.REFUSED:
            return "the server refused it (the call has built as much as it may, or the value nests too deep)";
        case LowLevel.TYPE_MISMATCH:
            return "the value is of a kind that cannot go there";
        case LowLevel.ERR:
            return "the host rejected the call";
        default:
            return "status " + rc;
        }
    }

    /**
     * Sends the one value written to {@link Slots#ONE}, by value: the batch import with a single element,
     * which takes none of the call's handles, so a loop of any length can add this way.
     */
    private ListBuilder push() {
        return check(Reads.one(Natives.listPushValues(handle, Address.ofData(Slots.ONE), Slots.SIZE)));
    }

    /** Appends an integer and returns this builder. An {@code int} argument widens to this one. */
    public ListBuilder add(long value) {
        return check(Natives.listPushInt(handle, value));
    }

    /**
     * Appends a double and returns this builder. A whole-number double stays a double: 3.0 is not stored as
     * 3.
     */
    public ListBuilder add(double value) {
        Slots.scalar(Slots.ONE, 0, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return push();
    }

    /** Appends a boolean, stored as one and not as 0 or 1. */
    public ListBuilder add(boolean value) {
        Slots.scalar(Slots.ONE, 0, Value.BOOL, 0, value ? 1 : 0);
        return push();
    }

    /**
     * Appends a string, encoded as UTF-8 and copied by the host. {@code value} must not be null: nil is
     * {@link #addNil}.
     */
    public ListBuilder add(String value) {
        byte[] v = LowLevel.bytes(value);
        return check(Natives.listPushStr(handle, Address.ofData(v), v.length));
    }

    /** Appends a blob holding a copy of all of {@code value}. */
    public ListBuilder add(byte[] value) {
        Slots.guest(Slots.ONE, 0, Value.GUEST_BLOB, value, value.length);
        return push();
    }

    /**
     * Appends a value of any kind: a scalar, a string or blob of the guest's, a stored value by position (so
     * a nested document goes in exactly as it is stored, copied by the server), a built collection. A nil
     * value appends nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) for an absent value.
     */
    public ListBuilder add(Value value) {
        if (value.kind() == Value.ABSENT) {
            throw new UdfException(LowLevel.TYPE_MISMATCH, "could not add an absent value to the list");
        }

        // A built collection goes by the native that shares it, as add(ListBuilder) does.
        if (value.kind() == Value.HANDLE) {
            return check(Natives.listPush(handle, (int) value.payload()));
        }

        Slots.value(Slots.ONE, 0, value);
        return push();
    }

    /**
     * Appends a built map as one nested element. The builder is not used up, and the list holds the same map
     * rather than a copy of it. Throws {@link UdfException} past 64 levels of nesting.
     */
    public ListBuilder add(MapBuilder value) {
        return check(Natives.listPush(handle, value.handle()));
    }

    /**
     * Appends a built list as one nested element, not its elements one by one. The builder is not used up,
     * and this list holds the same list rather than a copy of it. Throws {@link UdfException} past 64 levels
     * of nesting, and for a list added to itself.
     */
    public ListBuilder add(ListBuilder value) {
        return check(Natives.listPush(handle, value.handle));
    }

    /** Appends a {@code java.util.List} as one nested list, its elements as {@link #addAll(Iterable)} takes them. */
    public ListBuilder add(List<?> value) {
        return add(create(value.size()).addAll(value));
    }

    /** Appends a {@code java.util.Map} as one nested map, its entries as {@link MapBuilder#putAll(Map)} takes them. */
    public ListBuilder add(Map<?, ?> value) {
        return add(MapBuilder.create(value.size()).putAll(value));
    }

    /** Appends nil: an element that reads back with {@link Value#isNil} true. */
    public ListBuilder addNil() {
        Slots.scalar(Slots.ONE, 0, Value.NIL, 0, 0);
        return push();
    }

    /**
     * Appends a GeoJSON document from its text, kept as that kind rather than a string. The text is not
     * checked here.
     */
    public ListBuilder addGeoJson(String doc) {
        byte[] d = LowLevel.bytes(doc);

        Slots.guest(Slots.ONE, 0, Value.GUEST_GEOJSON, d, d.length);
        return push();
    }

    /**
     * Appends every element of {@code items}, one by one: a stored list ({@link ListValue}), whose elements
     * go in by position, or a {@code java.util.List} or other iterable of {@code Long}, {@code Integer},
     * {@code Short}, {@code Byte}, {@code Double}, {@code Float}, {@code Boolean}, {@code String},
     * {@code byte[]}, {@link Value}, stored lists and maps, builders, nested {@code java.util} lists and
     * maps, and {@code null} for nil. Anything else throws {@link UdfException} naming its class, with the
     * elements before it already added.
     *
     * <pre>
     * List&lt;Object&gt; row = new ArrayList&lt;&gt;();
     * row.add("alice");
     * row.add(42);                          // an Integer goes in as an integer
     * row.add(null);                        // nil
     *
     * ListBuilder rows = Udf.list().add(row);                 // one nested list of three
     * ListBuilder flat = Udf.list().addAll(row).add(true);    // four elements
     * </pre>
     */
    public ListBuilder addAll(Iterable<?> items) {
        for (Object o : items) {
            Convert.add(this, o);
        }

        return this;
    }
}

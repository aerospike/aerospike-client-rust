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
            throw new UdfException("could not build a list");
        }

        return new ListBuilder(h);
    }

    /** The host's handle on the list. */
    int handle() {
        return handle;
    }

    /** How many elements have been added. */
    public int size() {
        return Natives.collSize(handle);
    }

    /** Whether nothing has been added. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /** The list as a value, to put where a {@link Value} is taken. */
    public Value asValue() {
        return new Value().assign(Value.HANDLE, 0, handle);
    }

    private ListBuilder check(int rc) {
        if (rc != LowLevel.OK) {
            throw new UdfException("could not add to the list (" + rc + ")");
        }

        return this;
    }

    /** Sends the one value written to {@link Slots#ONE}. */
    private ListBuilder push() {
        if (Natives.listPushValues(handle, Address.ofData(Slots.ONE), Slots.SIZE) != 1) {
            throw new UdfException("could not add to the list");
        }

        return this;
    }

    /** Appends an integer. */
    public ListBuilder add(long value) {
        return check(Natives.listPushInt(handle, value));
    }

    /** Appends a double. */
    public ListBuilder add(double value) {
        Slots.scalar(Slots.ONE, 0, Value.DOUBLE, 0, Double.doubleToRawLongBits(value));
        return push();
    }

    /** Appends a boolean. */
    public ListBuilder add(boolean value) {
        Slots.scalar(Slots.ONE, 0, Value.BOOL, 0, value ? 1 : 0);
        return push();
    }

    /** Appends a string. */
    public ListBuilder add(String value) {
        byte[] v = LowLevel.bytes(value);
        return check(Natives.listPushStr(handle, Address.ofData(v), v.length));
    }

    /** Appends a blob. */
    public ListBuilder add(byte[] value) {
        Slots.guest(Slots.ONE, 0, Value.GUEST_BLOB, value, value.length);
        return push();
    }

    /**
     * Appends a value of any kind: a scalar, a string or blob of the guest's, a stored value by position
     * (so a nested document goes in exactly as it is stored), a built collection.
     */
    public ListBuilder add(Value value) {
        if (value.kind() == Value.ABSENT) {
            throw new UdfException("could not add an absent value to the list");
        }

        // A built collection goes by the native that shares it, as add(ListBuilder) does.
        if (value.kind() == Value.HANDLE) {
            return check(Natives.listPush(handle, (int) value.payload()));
        }

        Slots.value(Slots.ONE, 0, value);
        return push();
    }

    /** Appends a built map, nested. */
    public ListBuilder add(MapBuilder value) {
        return check(Natives.listPush(handle, value.handle()));
    }

    /** Appends a built list, nested. */
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

    /** Appends nil. */
    public ListBuilder addNull() {
        Slots.scalar(Slots.ONE, 0, Value.NIL, 0, 0);
        return push();
    }

    /** Appends a GeoJSON document, kept as that kind rather than a string. */
    public ListBuilder addGeoJson(String doc) {
        byte[] d = LowLevel.bytes(doc);

        Slots.guest(Slots.ONE, 0, Value.GUEST_GEOJSON, d, d.length);
        return push();
    }

    /**
     * Appends every element of {@code items}, one by one: a stored list ({@link ListValue}), whose
     * elements go in by position, or a {@code java.util.List} or other iterable of {@code Long},
     * {@code Integer}, {@code Double}, {@code Boolean}, {@code String}, {@code byte[]}, {@link Value},
     * builders, nested {@code java.util} lists and maps, and {@code null} for nil. Anything else throws
     * {@link UdfException}.
     */
    public ListBuilder addAll(Iterable<?> items) {
        for (Object o : items) {
            Convert.add(this, o);
        }

        return this;
    }
}

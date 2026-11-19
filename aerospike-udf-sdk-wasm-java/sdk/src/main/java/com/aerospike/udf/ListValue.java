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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.teavm.interop.Address;

/**
 * A list the server holds - a list bin, an element of another collection, an argument - to read: its
 * size, an element by index, and every element in order. It is read where it is, so nothing is copied
 * until an element is asked for, and an element that is itself a list or map is another view.
 *
 * <pre>
 * ListValue tags = rec.getList("tags");
 *
 * for (Value tag : tags) {
 *     if (tag.asString().equals(wanted)) {
 *         break;            // leaving early is fine
 *     }
 * }
 *
 * Value first = tags.isEmpty() ? null : tags.get(0);
 * </pre>
 *
 * <p>The typed readers ({@link #getInt(int)} and the rest) answer null for an element that is nil and
 * throw {@link UdfException} naming the index, with the status {@link LowLevel#TYPE_MISMATCH}, when the
 * element is of another kind. An index past the end throws too ({@link LowLevel#NOT_FOUND}), as an index
 * out of range does on a {@code java.util.List}: {@link #size} is the test. A list is
 * read-only: to change one, build another ({@link ListBuilder}) and write that. {@link #toList} copies
 * the whole list out as a {@code java.util.List}.
 */
public final class ListValue implements Iterable<Value> {
    private static final Value SCRATCH = new Value();

    private final Value list;
    /** What {@link #asValue} answers for a view of nothing: an empty list, built when first asked for. */
    private Value empty;

    /** A view of {@code list}, which is a stored list, or of nothing at all, which reads as empty. */
    ListValue(Value list) {
        this.list = list;
    }

    /** How many elements the list has; known without a call to the host. */
    public int size() {
        return list.kind() == Value.LIST ? list.length() : 0;
    }

    /** Whether the list has no elements. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * The list as the one value it is, to hand on: into a bin, a builder or the result, by position, so the
     * server copies it without its elements passing through the guest. A view of nothing (the deleted bins of
     * a side that deleted none, say) answers an empty list, built for the purpose; never an absent value,
     * which no bin, builder or result takes.
     */
    public Value asValue() {
        if (list.kind() == Value.LIST) {
            return list;
        }

        if (empty == null) {
            empty = ListBuilder.create(0).asValue();
        }

        return empty;
    }

    //==========================================================
    // By index.
    //

    private Value at(int index, Value out) {
        if (index < 0 || index >= size()) {
            throw new UdfException(LowLevel.NOT_FOUND, "the list has no element " + index + " (it has " + size() + ")");
        }

        int rc = Natives.listGet(list.src(), list.at(), index, Address.ofData(Slots.ONE), Slots.SIZE);

        if (rc != LowLevel.OK) {
            throw new UdfException(rc, "element " + index + " of the list could not be read");
        }

        return out.fill(Slots.ONE, 0);
    }

    /** The scratch value when the element is of {@code kind}; null when it is nil; throws for another. */
    private Value at(int index, int kind, String what) {
        Value v = at(index, SCRATCH);

        if (v.kind() == kind) {
            return v;
        }

        if (v.kind() == Value.NIL) {
            return null;
        }

        throw new UdfException(LowLevel.TYPE_MISMATCH, "element " + index + " of the list is not " + what);
    }

    /**
     * The element at {@code index}, counted from 0, as a fresh {@link Value}; a nil element is a value whose
     * {@link Value#isNil} is true, not null. As {@code java.util.List.get} does, it throws for an index that
     * is negative or past the end, where the other SDKs answer nothing: an {@link UdfException} with the
     * status {@link LowLevel#NOT_FOUND}. Test {@link #size} first where the index is not known to be good.
     * One call to the host, which steps over the elements before it without decoding them: reading every
     * element this way is quadratic, and what the iterator is for.
     */
    public Value get(int index) {
        return at(index, new Value());
    }

    /**
     * The integer at {@code index}, counted from 0, or null when that element is nil. Throws
     * {@link UdfException} naming the index: {@link LowLevel#TYPE_MISMATCH} when the element is of another
     * kind, {@link LowLevel#NOT_FOUND} when the list has no such element, as {@link #get} does. Every typed
     * reader here answers these three ways.
     */
    public Long getInt(int index) {
        Value v = at(index, Value.INT, "an int");

        return v == null ? null : v.payload();
    }

    /**
     * The double at {@code index}, or null when that element is nil. Throws {@link UdfException} when the
     * element is of another kind (an integer is not widened) or the list has no such element.
     */
    public Double getDouble(int index) {
        Value v = at(index, Value.DOUBLE, "a double");

        return v == null ? null : v.doubleBits();
    }

    /**
     * The boolean at {@code index}, or null when that element is nil. Throws {@link UdfException} when the
     * element is of another kind or the list has no such element.
     */
    public Boolean getBool(int index) {
        Value v = at(index, Value.BOOL, "a bool");

        return v == null ? null : v.payload() != 0;
    }

    /**
     * The text of the string at {@code index}, decoded into a new {@code String}, or null when that element
     * is nil. Throws {@link UdfException} when the element is not a string or the list has no such element.
     */
    public String getString(int index) {
        Value v = at(index, Value.STR, "a string");

        return v == null ? null : v.asString();
    }

    /**
     * The bytes of the blob at {@code index}, copied into a new array, or null when that element is nil. It
     * is strict: throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the element is anything
     * but a blob, a string included; {@link #getBytes} takes those.
     */
    public byte[] getBlob(int index) {
        Value v = at(index, Value.BLOB, "a blob");

        return v == null ? null : v.asBytes();
    }

    /**
     * The raw bytes of the string, blob, GeoJSON or HLL at {@code index}, copied into a new array, or null
     * when that element is nil. Throws {@link UdfException} ({@link LowLevel#TYPE_MISMATCH}) when the element
     * is a number, a boolean, a list or a map, and when the list has no such element.
     */
    public byte[] getBytes(int index) {
        Value v = at(index, SCRATCH);

        if (v.kind() == Value.NIL) {
            return null;
        }

        if (!v.hasBytes()) {
            throw new UdfException(LowLevel.TYPE_MISMATCH,
                    "element " + index + " of the list is not a string, blob, GeoJSON or HLL");
        }

        return v.asBytes();
    }

    /**
     * The list at {@code index}, as another view: nothing is copied. Null when that element is nil. Throws
     * {@link UdfException} when the element is not a list or the list has no such element.
     */
    public ListValue getList(int index) {
        Value v = at(index, Value.LIST, "a list");

        return v == null ? null : new ListValue(v.copy());
    }

    /**
     * The map at {@code index}, as a view: nothing is copied. Null when that element is nil. Throws
     * {@link UdfException} when the element is not a map or the list has no such element.
     */
    public MapValue getMap(int index) {
        Value v = at(index, Value.MAP, "a map");

        return v == null ? null : new MapValue(v.copy());
    }

    //==========================================================
    // The whole list.
    //

    /**
     * Whether any element is the string {@code text}, of any length, compared byte for byte as UTF-8. It walks
     * the list from the start and stops at the first match, so it is linear in the list; an element of another kind never
     * matches, and a nested list is not searched.
     */
    public boolean contains(String text) {
        if (isEmpty()) {
            return false;
        }

        byte[] want = Names.of(text);
        byte[] have = new byte[want.length];
        Walk walk = new Walk(list);

        while (walk.more()) {
            Value v = walk.next(SCRATCH);

            if (v.kind() == Value.STR && v.length() == want.length && Maps.sameBytes(v, want, have)) {
                walk.close();
                return true;
            }
        }

        return false;
    }

    /**
     * Whether any element is the integer {@code i}. It walks the list from the start and stops at the first
     * match; a double of the same value does not match, and a nested list is not searched.
     */
    public boolean contains(long i) {
        if (isEmpty()) {
            return false;
        }

        Walk walk = new Walk(list);

        while (walk.more()) {
            Value v = walk.next(SCRATCH);

            if (v.kind() == Value.INT && v.payload() == i) {
                walk.close();
                return true;
            }
        }

        return false;
    }

    /**
     * The elements in order, a fresh {@link Value} each, so one can be kept after the loop moves on. The loop
     * may be left early. The elements come from the host 128 at a time, so a loop costs a call per 128
     * elements, not one per element. {@code next()} past the end throws {@code NoSuchElementException}.
     */
    @Override
    public Iterator<Value> iterator() {
        final Walk walk = new Walk(list.kind() == Value.LIST ? list : Maps.NOTHING);

        return new Iterator<Value>() {
            @Override
            public boolean hasNext() {
                return walk.more();
            }

            @Override
            public Value next() {
                if (!walk.more()) {
                    throw new NoSuchElementException();
                }

                return walk.next(new Value());
            }
        };
    }

    /**
     * The whole list copied out as a {@code java.util.List}, each element as {@link Value#toObject} gives
     * it; a nested list or map is copied too.
     */
    public List<Object> toList() {
        return Convert.toList(list);
    }
}

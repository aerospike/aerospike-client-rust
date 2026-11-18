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
 * long first = tags.getInt(0);
 * </pre>
 *
 * <p>The typed readers ({@link #getInt(int)} and the rest) throw {@link UdfException} naming the index
 * when the element is of another kind, and so does every reader for an index past the end. A list is
 * read-only: to change one, build another ({@link ListBuilder}) and write that. {@link #toList} copies
 * the whole list out as a {@code java.util.List}.
 */
public final class ListValue implements Iterable<Value> {
    private static final Value SCRATCH = new Value();

    private final Value list;

    /** A view of {@code list}, which is a stored list, or of nothing at all, which reads as empty. */
    ListValue(Value list) {
        this.list = list;
    }

    /** How many elements the list has. */
    public int size() {
        return list.kind() == Value.LIST ? list.length() : 0;
    }

    /** Whether the list has no elements. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /** The list as the one value it is, to hand on: into a bin, a builder or the result, by position. */
    public Value asValue() {
        return list;
    }

    //==========================================================
    // By index.
    //

    private Value at(int index, Value out) {
        if (index < 0 || index >= size() || Natives.listGet(list.src(), list.at(), index,
                Address.ofData(Slots.ONE), Slots.SIZE) != LowLevel.OK) {
            throw new UdfException("the list has no element " + index + " (it has " + size() + ")");
        }

        return out.fill(Slots.ONE, 0);
    }

    private Value at(int index, int kind, String what) {
        Value v = at(index, SCRATCH);

        if (v.kind() != kind) {
            throw new UdfException("element " + index + " of the list is not " + what);
        }

        return v;
    }

    /**
     * The element at {@code index}, counted from 0; throws {@link UdfException} when the list has no such
     * element. The server steps over the elements before it without decoding them.
     */
    public Value get(int index) {
        return at(index, new Value());
    }

    /** The integer at {@code index}; throws {@link UdfException} naming the index when it is not one. */
    public long getInt(int index) {
        return at(index, Value.INT, "an int").payload();
    }

    /** The double at {@code index}; an integer is not widened. */
    public double getDouble(int index) {
        return at(index, Value.DOUBLE, "a double").doubleBits();
    }

    /** The boolean at {@code index}. */
    public boolean getBool(int index) {
        return at(index, Value.BOOL, "a bool").payload() != 0;
    }

    /** The text of the string at {@code index}, copied out. */
    public String getString(int index) {
        return at(index, Value.STR, "a string").asString();
    }

    /** The bytes of the string, blob, GeoJSON or HLL at {@code index}, copied out. */
    public byte[] getBlob(int index) {
        Value v = at(index, SCRATCH);

        if (!v.isStored() || v.isList() || v.isMap()) {
            throw new UdfException("element " + index + " of the list is not a string or blob");
        }

        return v.asBlob();
    }

    /** The list at {@code index}. */
    public ListValue getList(int index) {
        return new ListValue(at(index, Value.LIST, "a list").copy());
    }

    /** The map at {@code index}. */
    public MapValue getMap(int index) {
        return new MapValue(at(index, Value.MAP, "a map").copy());
    }

    //==========================================================
    // The whole list.
    //

    /** Whether the list holds the string {@code text}. */
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

    /** Whether the list holds the integer {@code i}. */
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
     * The elements in order, a fresh {@link Value} each, so one can be kept after the loop moves on. The
     * loop may be left early.
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

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
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A walk over a stored list or map, the Java face of the Rust SDK's {@code Coll::iter} and
 * {@code entries}. A list cursor is an {@code Iterable<Value>}, a map cursor an {@code Iterable<Entry>},
 * so the natural form is a for-each, over a fresh element each time:
 *
 * <pre>
 * for (Value v : L.asList()) {
 *     total += v.asLong();
 * }
 *
 * for (var e : COUNTS.asMap()) {
 *     offer(e.key.asString(), e.value.asLong());
 * }
 * </pre>
 *
 * <p>The {@code while} form allocates nothing: {@link #next} steps, refilling the reused {@link #value}
 * (and, for a map, {@link #key}) from a window of 32 values per import, and a value to keep across steps
 * is copied with {@link Value#set}. Either way the cursor closes itself when the walk ends, and
 * {@link #close} closes it early, so an early {@code break} goes inside a try-with-resources:
 *
 * <pre>
 * try (Cursor&lt;Entry&gt; c = COUNTS.asMap()) {
 *     while (c.next()) {
 *         ... c.key, c.value ...
 *     }
 * }
 * </pre>
 *
 * <p>Cursors nest (a list inside a map), as long as the inner one is closed, by exhaustion or by hand,
 * before the outer one steps on: the host releases cursor slots last in, first out.
 *
 * @param <T> what the for-each yields: {@link Value} for a list, {@link Entry} for a map
 */
public final class Cursor<T> implements AutoCloseable, Iterable<T> {
    /** One entry of a stored map, as a map cursor's for-each yields it: a key and a value, both fresh. */
    public static final class Entry implements Map.Entry<Value, Value> {
        /** The entry's key. */
        public final Value key;
        /** The entry's value. */
        public final Value value;

        Entry(Value key, Value value) {
            this.key = key;
            this.value = value;
        }

        /** The key: the same object as {@link #key}. */
        @Override
        public Value getKey() {
            return key;
        }

        /** The value: the same object as {@link #value}. */
        @Override
        public Value getValue() {
            return value;
        }

        /** Entries are read-only. */
        @Override
        public Value setValue(Value v) {
            throw new UnsupportedOperationException("a stored entry is read-only");
        }
    }

    private static final int WINDOW = 32;

    /** The current element, or the current entry's value for a map. */
    public final Value value = new Value();
    /** The current entry's key, for a map; untouched for a list. */
    public final Value key = new Value();

    private final boolean map;
    private final Value[] window = new Value[WINDOW];
    private int cursor;
    private int got;
    private int at;
    private boolean last;
    private boolean prefetched;

    private Cursor(Value coll, boolean map) {
        this.map = map;
        cursor = Udf.enter(coll);

        if (cursor <= 0) {
            throw new UdfException("could not open the " + (map ? "map" : "list"));
        }
    }

    /** A cursor over a stored list value. */
    static Cursor<Value> list(Value coll) {
        return new Cursor<>(coll, false);
    }

    /** A cursor over a stored map value. */
    static Cursor<Entry> map(Value coll) {
        return new Cursor<>(coll, true);
    }

    /** Steps to the next element or entry; false at the end, when the cursor has closed itself. */
    public boolean next() {
        if (prefetched) {
            prefetched = false;
            return true;
        }

        return advance();
    }

    private boolean advance() {
        if (cursor <= 0) {
            return false;
        }

        if (at >= got) {
            if (last) {
                close();
                return false;
            }

            got = Udf.next(cursor, window);
            at = 0;
            last = got < WINDOW;

            if (got <= 0) {
                close();
                return false;
            }
        }

        if (map) {
            if (at + 1 >= got) {
                close();
                return false;
            }

            key.set(window[at].kind, window[at].len, window[at].payload);
            value.set(window[at + 1].kind, window[at + 1].len, window[at + 1].payload);
            at += 2;
        }
        else {
            value.set(window[at].kind, window[at].len, window[at].payload);
            at++;
        }

        return true;
    }

    /** Gives the host's cursor slot back; harmless when already closed. */
    @Override
    public void close() {
        if (cursor > 0) {
            Udf.close(cursor);
            cursor = 0;
        }
    }

    private static Value copy(Value v) {
        return new Value().set(v.kind, v.len, v.payload);
    }

    @SuppressWarnings("unchecked")
    private T current() {
        return map ? (T) new Entry(copy(key), copy(value)) : (T) copy(value);
    }

    /** The elements or entries, a fresh one each; the walk closes the cursor when it reaches the end. */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                if (!prefetched) {
                    prefetched = advance();
                }

                return prefetched;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                prefetched = false;

                return current();
            }
        };
    }
}

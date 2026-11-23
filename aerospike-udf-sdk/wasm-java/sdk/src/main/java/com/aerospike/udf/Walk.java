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
 * One walk over a stored list or map, behind the iterators of {@link ListValue} and {@link MapValue}: the
 * values come from the host a window at a time, {@link Slots#BATCH} per import, into a buffer this owns.
 *
 * <p><b>The host's cursor is given back as soon as the last window is read.</b> A walk made with
 * {@link #whole} reads every window before it hands out the first value, so it holds no cursor at all while
 * the values are used: that is what the {@code Iterable} iterators use, because a for-each left early has no
 * hook to close anything with. A windowed walk ({@code new Walk}) keeps its cursor between windows, and is
 * used only where the SDK closes it on every path out ({@code contains}, {@code forEach}, the
 * {@code java.util} conversions, the lookups).
 */
final class Walk {
    private final byte[] window;
    private int cursor;
    /** Values not read from the host yet. */
    private int remaining;
    private int got;
    private int at;

    /**
     * A walk whose values are all read, a window per import, before it returns: the cursor is closed
     * before the first value is handed out, however the walk is then used or left. The values are the
     * 16-byte positions the host hands out, not copies of the data, so it costs 16 bytes of the guest's
     * memory per list element, 32 per map entry; a value read from it is good for the call, as any stored
     * value is.
     */
    static Walk whole(Value coll) {
        int total = coll.kind() == Value.MAP ? coll.length() * 2 : coll.kind() == Value.LIST ? coll.length() : 0;

        if (total <= Slots.BATCH) {
            // One window: the windowed walk already closes the cursor before it hands anything out.
            return new Walk(coll);
        }

        Walk w = new Walk(new byte[total * Slots.SIZE]);
        int cursor = Natives.cursorEnterAt(coll.src(), coll.at());

        if (cursor <= 0) {
            throw new UdfException(LowLevel.ERR, "could not open the " + (coll.kind() == Value.MAP ? "map" : "list"));
        }

        byte[] window = new byte[Slots.BATCH * Slots.SIZE];
        int filled = 0;

        try {
            while (filled < total) {
                int want = Math.min(total - filled, Slots.BATCH) * Slots.SIZE;
                int n = Natives.cursorValues(cursor, Address.ofData(window), want);

                if (n <= 0) {
                    break;
                }

                System.arraycopy(window, 0, w.window, filled * Slots.SIZE, n * Slots.SIZE);
                filled += n;

                if (n * Slots.SIZE < want) {
                    break;
                }
            }
        }
        finally {
            Natives.cursorClose(cursor);
        }

        w.got = filled;

        return w;
    }

    /** A walk over values already read, with no cursor. */
    private Walk(byte[] values) {
        window = values;
    }

    /** A walk over {@code coll}, a stored list or map; a map yields a key and then its value. */
    Walk(Value coll) {
        remaining = coll.kind() == Value.MAP ? coll.length() * 2 : coll.length();
        window = new byte[Math.min(remaining, Slots.BATCH) * Slots.SIZE];

        if (remaining == 0) {
            return;
        }

        cursor = Natives.cursorEnterAt(coll.src(), coll.at());

        if (cursor <= 0) {
            // The host gives no reason: the value can no longer be read, or the call's memory is spent.
            throw new UdfException(LowLevel.ERR, "could not open the " + (coll.kind() == Value.MAP ? "map" : "list"));
        }

        fill();
    }

    private void fill() {
        got = Natives.cursorValues(cursor, Address.ofData(window), window.length);
        at = 0;

        if (got < 0) {
            got = 0;
        }

        remaining -= got;

        // The count came from the collection's own header, so this is the end exactly when it says so; a
        // short window is the end whatever it says.
        if (remaining <= 0 || got * Slots.SIZE < window.length) {
            close();
        }
    }

    /** Whether there is another value. */
    boolean more() {
        if (at < got) {
            return true;
        }

        if (cursor <= 0) {
            return false;
        }

        fill();

        return at < got;
    }

    /** The next value into {@code out}; call after {@link #more} said there is one. */
    Value next(Value out) {
        out.fill(window, at * Slots.SIZE);
        at++;

        return out;
    }

    /** Gives the host's cursor back; harmless when it already was. */
    void close() {
        if (cursor > 0) {
            Natives.cursorClose(cursor);
            cursor = 0;
        }

        remaining = 0;
    }
}

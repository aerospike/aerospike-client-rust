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
 * <p><b>The host's cursor is given back as soon as the last window is read</b>, which is before the walk
 * hands out its first element when the collection fits one window (128 values, 64 map entries). So a
 * for-each left early holds nothing in that case. A longer collection keeps its cursor between windows,
 * and a walk abandoned part way leaves it open; the host releases every handle of a call when the call
 * returns (per record, in a stream), so that costs one of the call's 256 handle slots until then and
 * nothing afterwards. There is no finalizer to do better with, and nothing for an entry point to release
 * that the host does not already.
 *
 * <p>The host gives a slot back only when it is the last one handed out, so walks nest (a list inside a
 * map) as loops do: the inner one finishes before the outer one steps on.
 */
final class Walk {
    private final byte[] window;
    private int cursor;
    /** Values not read from the host yet. */
    private int remaining;
    private int got;
    private int at;

    /** A walk over {@code coll}, a stored list or map; a map yields a key and then its value. */
    Walk(Value coll) {
        remaining = coll.kind() == Value.MAP ? coll.length() * 2 : coll.length();
        window = new byte[Math.min(remaining, Slots.BATCH) * Slots.SIZE];

        if (remaining == 0) {
            return;
        }

        cursor = Natives.cursorEnterAt(coll.src(), coll.at());

        if (cursor <= 0) {
            throw new UdfException("could not open the " + (coll.kind() == Value.MAP ? "map" : "list"));
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

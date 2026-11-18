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
 * How the SDK itself reads the description maps the server hands an event handler: an entry by its
 * string key as the type expected, with a fallback rather than an exception, since a description is the
 * server's and an entry it leaves out means "none". User code reads maps through {@link MapValue}.
 */
final class Maps {
    private Maps() {
    }

    /** A value that is not there, for a view of nothing; never written to. */
    static final Value NOTHING = new Value();

    private static final Value SCRATCH = new Value();

    /** The entry under {@code key} into {@code out}; false when there is none or {@code m} is not a map. */
    private static boolean find(Value m, String key, Value out) {
        if (m.kind() != Value.MAP) {
            return false;
        }

        byte[] k = Names.of(key);

        if (Natives.collGet(m.src(), m.at(), Address.ofData(k), k.length, Address.ofData(Slots.ONE), Slots.SIZE)
                != LowLevel.OK) {
            return false;
        }

        out.fill(Slots.ONE, 0);

        return true;
    }

    /** A map entry by string key, fresh: of ABSENT kind when there is none or {@code m} is not a map. */
    static Value get(Value m, String key) {
        Value v = new Value();

        find(m, key, v);

        return v;
    }

    /** An integer entry, or {@code dflt} when there is none or it is not an integer. */
    static long getInt(Value m, String key, long dflt) {
        return find(m, key, SCRATCH) && SCRATCH.kind() == Value.INT ? SCRATCH.payload() : dflt;
    }

    /** A string entry's text, or "" when there is none or it is not a string. */
    static String getString(Value m, String key) {
        return find(m, key, SCRATCH) && SCRATCH.kind() == Value.STR ? SCRATCH.asString() : "";
    }

    /** Whether an entry is the boolean true; false when there is none or it is of another kind. */
    static boolean getBool(Value m, String key) {
        return find(m, key, SCRATCH) && SCRATCH.kind() == Value.BOOL && SCRATCH.payload() != 0;
    }

    /** Whether a string entry reads as {@code text}. */
    static boolean is(Value m, String key, String text) {
        return text.equals(getString(m, key));
    }

    /**
     * Whether the stored string or blob {@code v}, already known to be as long as {@code want}, has its
     * bytes; {@code scratch} is at least that long and is overwritten.
     */
    static boolean sameBytes(Value v, byte[] want, byte[] scratch) {
        int n = Natives.cursorBytesAt(v.src(), v.at(), Address.ofData(scratch), scratch.length);

        if (n != want.length) {
            return false;
        }

        for (int i = 0; i < n; i++) {
            if (scratch[i] != want[i]) {
                return false;
            }
        }

        return true;
    }
}

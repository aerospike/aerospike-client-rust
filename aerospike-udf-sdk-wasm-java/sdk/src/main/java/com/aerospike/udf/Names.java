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

import java.nio.charset.StandardCharsets;

/**
 * Bin names and map keys as the UTF-8 bytes the host takes, encoded once per name rather than once per
 * call. A module names its bins with the same few string literals on every call, so the bytes of a name
 * are found by the string itself: {@code rec.getLong("ticks")} allocates nothing, and for the names a
 * call used last it costs a few comparisons of references.
 *
 * <p>There are two tiers. The eight names used most recently are held in fields and matched by identity,
 * which is all a literal needs and is as cheap as this backend gets: no array, no bounds check, and for
 * the first four no call.
 * Behind them is a table found by the string's hash, which also matches a name that is equal without
 * being the same object, and which refills the first tier.
 *
 * <p>The table is a fixed size and a name sits in one of two slots, so a module that takes its names from
 * its arguments cannot grow it: a name that finds both its slots taken replaces one, and is encoded again
 * if it comes back. Long names are not kept at all. The arrays handed out are shared: nothing may write
 * to one.
 */
final class Names {
    private Names() {
    }

    private static final int SLOTS = 128;
    /** A bin name is 15 bytes at most; this leaves room for the map keys a module is likely to repeat. */
    private static final int LONGEST = 64;

    private static final String[] KEYS = new String[SLOTS];
    private static final byte[][] BYTES = new byte[SLOTS][];

    // The first tier: the last eight names, by identity.
    private static String k0;
    private static String k1;
    private static String k2;
    private static String k3;
    private static String k4;
    private static String k5;
    private static String k6;
    private static String k7;
    private static byte[] b0;
    private static byte[] b1;
    private static byte[] b2;
    private static byte[] b3;
    private static byte[] b4;
    private static byte[] b5;
    private static byte[] b6;
    private static byte[] b7;
    /** Which of the eight the next name replaces. */
    private static int next;

    /** The UTF-8 bytes of {@code name}: the same array for the same name, as long as it stays in the table. */
    static byte[] of(String name) {
        if (name == k0) {
            return b0;
        }

        if (name == k1) {
            return b1;
        }

        if (name == k2) {
            return b2;
        }

        if (name == k3) {
            return b3;
        }

        return rest(name);
    }

    // The other four, apart: this backend puts a small method in line where it is called and leaves a
    // larger one a call, and a call costs more here than the four comparisons above.
    private static byte[] rest(String name) {
        if (name == k4) {
            return b4;
        }

        if (name == k5) {
            return b5;
        }

        if (name == k6) {
            return b6;
        }

        if (name == k7) {
            return b7;
        }

        return remember(name, find(name));
    }

    private static byte[] remember(String name, byte[] b) {
        // Only what the table kept: a name it turned away is one of a kind, and has no place here either.
        if (name.length() <= LONGEST) {
            switch (next) {
            case 0:
                k0 = name;
                b0 = b;
                break;
            case 1:
                k1 = name;
                b1 = b;
                break;
            case 2:
                k2 = name;
                b2 = b;
                break;
            case 3:
                k3 = name;
                b3 = b;
                break;
            case 4:
                k4 = name;
                b4 = b;
                break;
            case 5:
                k5 = name;
                b5 = b;
                break;
            case 6:
                k6 = name;
                b6 = b;
                break;
            default:
                k7 = name;
                b7 = b;
                break;
            }

            next = (next + 1) & 7;
        }

        return b;
    }

    private static byte[] find(String name) {
        int i = name.hashCode() & (SLOTS - 1);
        String k = KEYS[i];

        if (k == name || (k != null && k.equals(name))) {
            return BYTES[i];
        }

        int j = (i + 1) & (SLOTS - 1);

        k = KEYS[j];

        if (k == name || (k != null && k.equals(name))) {
            return BYTES[j];
        }

        byte[] b = name.getBytes(StandardCharsets.UTF_8);

        if (name.length() <= LONGEST) {
            int at = KEYS[i] == null || KEYS[j] != null ? i : j;

            KEYS[at] = name;
            BYTES[at] = b;
        }

        return b;
    }
}

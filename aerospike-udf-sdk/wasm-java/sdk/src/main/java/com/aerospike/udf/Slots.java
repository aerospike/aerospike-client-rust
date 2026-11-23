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
 * The 16-byte form a value crosses the boundary in: a kind and a length, 4 bytes each, and an 8-byte
 * payload, little-endian. The host writes one for every value it hands out, and takes them back, one or a
 * batch, for the values a guest pushes into a list or puts into a map.
 *
 * <p>Nothing here allocates. That is what lets a slot carry the address of a guest array: the address is
 * valid only until the next allocation, and between {@link #guest} writing it and the import reading it
 * there is none.
 */
final class Slots {
    private Slots() {
    }

    /** The size of one value. */
    static final int SIZE = 16;

    /** Values cross at most this many per import. */
    static final int BATCH = 128;

    /** One value, for a call that reads or sends a single one. */
    static final byte[] ONE = new byte[SIZE];

    /** A key and a value, for one map entry. */
    static final byte[] PAIR = new byte[2 * SIZE];

    /** A batch of values, for the calls that carry many. */
    static final byte[] MANY = new byte[BATCH * SIZE];

    static int le32(byte[] b, int off) {
        return (b[off] & 0xff) | (b[off + 1] & 0xff) << 8 | (b[off + 2] & 0xff) << 16 | (b[off + 3] & 0xff) << 24;
    }

    static long le64(byte[] b, int off) {
        return (le32(b, off) & 0xffffffffL) | (long) le32(b, off + 4) << 32;
    }

    static void putLe32(byte[] b, int off, int v) {
        b[off] = (byte) v;
        b[off + 1] = (byte) (v >>> 8);
        b[off + 2] = (byte) (v >>> 16);
        b[off + 3] = (byte) (v >>> 24);
    }

    static void putLe64(byte[] b, int off, long v) {
        putLe32(b, off, (int) v);
        putLe32(b, off + 4, (int) (v >>> 32));
    }

    /** Writes a value that is its payload: a scalar, a handle, or a stored value's position. */
    static void scalar(byte[] buf, int off, int kind, int len, long payload) {
        putLe32(buf, off, kind);
        putLe32(buf, off + 4, len);
        putLe64(buf, off + 8, payload);
    }

    /** Writes an integer. */
    static void integer(byte[] buf, int off, long i) {
        scalar(buf, off, Value.INT, 0, i);
    }

    /**
     * Writes a string, blob or GeoJSON of the guest's by the address of its bytes. Nothing may allocate
     * between this and the import that reads the slot.
     */
    static void guest(byte[] buf, int off, int kind, byte[] bytes, int len) {
        putLe32(buf, off, kind);
        putLe32(buf, off + 4, len);
        putLe64(buf, off + 8, len == 0 ? 0 : Address.ofData(bytes).toInt() & 0xffffffffL);
    }

    /** Writes any value: the guest kinds by address, everything else by its payload. */
    static void value(byte[] buf, int off, Value v) {
        int kind = v.kind();

        if (kind == Value.GUEST_STR || kind == Value.GUEST_BLOB || kind == Value.GUEST_GEOJSON) {
            guest(buf, off, kind, v.guestBytes(), v.guestLen());
        }
        else {
            scalar(buf, off, kind, v.length(), v.payload());
        }
    }
}

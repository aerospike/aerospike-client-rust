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

/**
 * What a typed read came to, shared by the readers of a record's bins ({@link Record},
 * {@link EventRecord}) and of a map's entries ({@link MapValue}), so that all three answer the same way:
 * the value, nothing (no such bin or key, or nil), a value of another kind, or a failed read. The message
 * for a problem is put together only when there is one, so a read that succeeds allocates nothing.
 */
final class Reads {
    private Reads() {
    }

    /** The value is there and of the kind asked for. */
    static final int HIT = 0;
    /** There is no such bin or key, or it holds nil. */
    static final int NONE = 1;
    /** The value is of another kind. */
    static final int WRONG = 2;
    /** The host could not read it. */
    static final int FAILED = 3;

    /** What a read with status {@code rc} into {@code v} came to, when {@code kind} was wanted. */
    static int state(int rc, Value v, int kind) {
        if (rc == LowLevel.NOT_FOUND) {
            return NONE;
        }

        if (rc != LowLevel.OK) {
            return FAILED;
        }

        if (v.kind() == Value.NIL || v.kind() == Value.ABSENT) {
            return NONE;
        }

        return v.kind() == kind ? HIT : WRONG;
    }

    /**
     * The exception for a read that did not hit, carrying the status of what it came to: {@code label} is
     * {@code "bin "} or {@code "key "}.
     */
    static UdfException problem(int state, String label, String name, String what) {
        switch (state) {
        case NONE:
            return new UdfException(LowLevel.NOT_FOUND, label + name + " is absent");
        case WRONG:
            return new UdfException(LowLevel.TYPE_MISMATCH, label + name + " is not " + what);
        default:
            return new UdfException(LowLevel.ERR, label + name + " could not be read");
        }
    }

    /** The exception for a write, a save or a build the host turned down with status {@code rc}. */
    static UdfException refused(int rc, String what) {
        return new UdfException(rc < 0 ? rc : LowLevel.ERR, what + ": " + why(rc));
    }

    /**
     * The status of a batch call that was to take one value or entry and answered {@code n}: its own status
     * when it gave one, and {@link LowLevel#REFUSED} when it took nothing, which is the host turning the
     * value down.
     */
    static int one(int n) {
        return n == 1 ? LowLevel.OK : n < 0 ? n : LowLevel.REFUSED;
    }

    /** Why the host turned a write down, for a message. */
    static String why(int rc) {
        switch (rc) {
        case LowLevel.REFUSED:
            return "the server refused it (the record is read-only here, or the write is one it does not take)";
        case LowLevel.TYPE_MISMATCH:
            return "the value is of a kind that cannot go there";
        case LowLevel.ERR:
            return "the host rejected the call";
        default:
            return "status " + rc;
        }
    }
}

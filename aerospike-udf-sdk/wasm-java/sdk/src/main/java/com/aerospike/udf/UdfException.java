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
 * Fails the call from inside a {@link Function}, {@link Stream} or {@link On} method: the generated export
 * catches it and delivers its message with {@link Udf#fail}, the way a hand-written export returns
 * {@code Udf.fail(message)}. {@link Params} throws it for an argument that does not fit its parameter.
 *
 * <pre>
 * {@literal @}Function("withdraw")
 * static long withdraw(Record rec, long amount) {
 *     long balance = rec.bin("balance").asInt();  // throws when the account has no balance
 *
 *     if (amount &gt; balance) {
 *         throw new UdfException("insufficient funds: " + balance);
 *     }
 *
 *     rec.set("balance", balance - amount);
 *     rec.save();
 *
 *     return balance - amount;
 * }
 * </pre>
 *
 * <p>The client sees the message as the call's error. It is unchecked, so no method declares it, and every
 * reader and every write of the SDK that fails throws it. A call that fails writes nothing: throwing before
 * {@link Record#save} leaves what was staged unwritten, and throwing after it undoes what the save wrote,
 * and what {@link Record#setInts} or {@link Record#setDoubles} wrote, as well.
 *
 * <p><b>It carries the host's status</b>, so code that catches one can tell what went wrong without reading
 * the message: {@link #status} is {@link LowLevel#NOT_FOUND} for a bin, key, element or argument that is
 * not there, {@link LowLevel#TYPE_MISMATCH} for a value of another kind, {@link LowLevel#REFUSED} for
 * something the server refuses (a write to a read-only record, a value nested too deep, a call that has
 * built as much as it may), a status at or below {@link LowLevel#NEEDED_BASE} for an array that was too
 * small, and {@link LowLevel#ERR} for everything else, which includes every exception user code makes with
 * {@link #UdfException(String)}.
 *
 * <pre>
 * try {
 *     rec.set("n", n);
 *     rec.save();
 * }
 * catch (UdfException e) {
 *     if (e.status() != LowLevel.REFUSED) {
 *         throw e;
 *     }
 *
 *     // a read-only record: answer without writing
 * }
 * </pre>
 */
public class UdfException extends RuntimeException {
    private final int status;

    /** A failure of the UDF's own, with the message the client is to see. Its status is {@link LowLevel#ERR}. */
    public UdfException(String message) {
        this(LowLevel.ERR, message);
    }

    /**
     * A failure that carries a host status: one of the {@link LowLevel} constants, or a status at or below
     * {@link LowLevel#NEEDED_BASE}. Every throw of the SDK uses this form.
     */
    public UdfException(int status, String message) {
        super(message);
        this.status = status;
    }

    /**
     * The host status behind the failure: {@link LowLevel#ERR}, {@link LowLevel#NOT_FOUND},
     * {@link LowLevel#TYPE_MISMATCH}, {@link LowLevel#REFUSED}, or, for an array that was too small, a status
     * at or below {@link LowLevel#NEEDED_BASE} whose {@code -(status) - 16} is the size needed. Never
     * {@link LowLevel#OK}.
     */
    public int status() {
        return status;
    }
}

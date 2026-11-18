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
 * What an {@code xdr-merge} event is about: the current record ({@link #local}, read by index 0), the
 * incoming XDR write ({@link #remote}, index 1), and the description. A handler must be pure and
 * convergent: both data centres merge the same pair with the sides swapped and must land on the same record.
 * {@link #remoteNewer} orders a bin's two sides by its own LUT and then source id, which is symmetric;
 * "mine first" is not.
 */
public final class Conflict {
    /** The whole description map, for anything not decoded here. */
    public Value description = new Value();
    /** The incoming bins that would lose under bin convergence, as a stored list. */
    public Value lost = new Value();
    /** The call is a dry run of the merge, not a live one. */
    public boolean dryRun;
    /** Bound with {@code trigger=conflict}: a bin would lose under bin convergence. */
    public boolean conflictTrigger;
    /** The policy stood in for is bin-lut rather than a plain apply. */
    public boolean binConvergence;
    /** The incoming write carries only changed bins. */
    public boolean remotePartial;
    /** The incoming write is a record delete; {@link #remote} has no bins. */
    public boolean remoteDeletedRecord;
    /** The current record is a tombstone: exists, no bins. */
    public boolean localTombstone;
    /** The current record, this node's, read by index 0. */
    public final EventRecord local = new EventRecord();
    /** The incoming XDR write, as shipped, read by index 1. */
    public final EventRecord remote = new EventRecord();

    /** Reads an event call's arguments. False when they are not an xdr-merge. */
    public boolean decode(int args, int argc) {
        Value[] a = new Value[1];

        if (LowLevel.args(args, argc, a) < 1 || a[0].kind() != Value.MAP) {
            return false;
        }

        description = a[0];
        lost = Maps.get(a[0], "lost");
        dryRun = Maps.is(a[0], "mode", "dry-run");
        conflictTrigger = Maps.is(a[0], "trigger", "conflict");
        binConvergence = Maps.is(a[0], "policy", "bin-lut");

        if (!local.decode(a[0], "current", 0) || !remote.decode(a[0], "incoming", 1)) {
            return false;
        }

        Value incoming = Maps.get(a[0], "incoming");

        remotePartial = Maps.getBoolean(incoming, "partial");
        remoteDeletedRecord = Maps.getBoolean(incoming, "deleted-record");
        localTombstone = Maps.getBoolean(Maps.get(a[0], "current"), "tombstone");

        return true;
    }

    /**
     * Whether the remote side wrote the bin later: by LUT, then source id - the order the server's bin
     * convergence uses, so both data centres answer the same.
     */
    public boolean remoteNewer(String name) {
        long ll = local.binLut(name);
        long rl = remote.binLut(name);

        return rl > ll || (rl == ll && remote.binSrc(name) > local.binSrc(name));
    }

    /**
     * Whether either side deleted a bin: the incoming write carries a bin delete, or the current record has
     * a bin tombstone. Such a bin is absent on that side, and resurrecting it from the other side is the
     * classic merge mistake.
     */
    public boolean involvesDelete() {
        return local.deletedAny() || remote.deletedAny();
    }

    /** The names of the bins the incoming write deletes; {@code remote.deletedBins()}. */
    public ListValue remoteDeleted() {
        return remote.deletedBins();
    }

    /** The two records side by side, for bin-wise merging. */
    public Pair pair() {
        return new Pair(local, remote);
    }

    // The verdicts. Each returns the status the handler returns.

    /**
     * Answers {@code keep-local}: the current record stays and the incoming write is dropped. Returns 0, the
     * status the handler returns.
     */
    public static int keepLocal() {
        Udf.resultStr("keep-local");
        return 0;
    }

    /** Answers {@code take-remote}: the incoming write is applied whole. Returns 0. */
    public static int takeRemote() {
        Udf.resultStr("take-remote");
        return 0;
    }

    /**
     * Answers {@code policy}: the namespace's policy decides, as it would have without the event. Returns 0.
     */
    public static int policy() {
        Udf.resultStr("policy");
        return 0;
    }

    /**
     * The final record: {@code bins} (a built map; a bin absent or nil is deleted) and a TTL with a write's
     * meaning, or -1 to keep the one the incoming write asked for.
     */
    public static int record(int bins, long ttl) {
        int out = LowLevel.newMap(3);

        if (out <= 0) {
            return Udf.fail("resolve: could not build the resolution");
        }

        MapBuilder answer = LowLevel.map(out).put("verdict", "record").put("bins", LowLevel.map(bins));

        if (ttl >= 0) {
            answer.put("ttl", ttl);
        }

        if (LowLevel.resultHandle(out) != LowLevel.OK) {
            return Udf.fail("resolve: could not return the record");
        }

        return 0;
    }

    /** The signature text an xdr-merge handler exports under {@code __udf_sig_<name>}, NUL-terminated. */
    public static byte[] signature(String name) {
        return Params.signatureBytes("event xdr-merge/1 " + name
                + "(current: record, incoming: record, conflict: conflict) -> resolution");
    }
}

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

package com.aerospike.udf.examples;

import com.aerospike.udf.Both;
import com.aerospike.udf.Conflict;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.On;
import com.aerospike.udf.Pair;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfList;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

/**
 * An XDR merge, the same as the Rust, C and Go resolvers: visits take the larger count, tags unite in LUT
 * order, the profile is whichever side wrote it last, a bin deleted by the later writer stays deleted, and
 * everything else is the newer side's. See aerospike-udf-sdk-wasm/examples/resolver for why each rule is
 * the way it is; this is that module line for line, over {@link Pair} and {@link Both}.
 */
public final class Resolver {
    @On.XdrMerge("merge_account")
    static Resolution mergeAccount(EventRecord local, EventRecord remote, Conflict conflict) {
        Pair pair = conflict.pair();
        UdfMap out = UdfMap.create(8);

        // A count each DC advanced on its own: both sides' increments are real, so the merge is the larger
        // of the two, not the sum (the sum would double-count the visits both already knew of).
        Long visits = pair.int64("visits").max();

        if (visits != null) {
            out.put("visits", visits);
        }

        // A set each DC added to: the union. Its order is part of the result, so it is the order both DCs
        // agree on - the older side's tags, then the newer side's new ones - never "mine first".
        Both<Value> sides = pair.list("tags");
        Value older = sides.remoteNewer() ? sides.local : sides.remote;
        Value newer = sides.remoteNewer() ? sides.remote : sides.local;
        UdfList tags = UdfList.create(16);

        if (older != null) {
            tags.addAll(older);
        }

        if (newer != null) {
            for (Value tag : newer.asList()) {
                if (older == null || !Udf.listHasStr(older, tag.asBytes())) {
                    tags.add(tag);
                }
            }
        }

        out.put("tags", tags);

        // A document one side rewrote whole: last writer wins, by LUT, which both DCs agree on.
        Value profile = pair.bin("profile").newer();

        if (profile != null) {
            out.put("profile", profile);
        }

        // A bin one side deleted stays deleted when the delete is the later write for that bin: a nil in
        // the returned record is "no such bin". Without this, the other side's copy would come back
        // through withNewer below - the resurrection every merge has to guard against.
        applyDeletes(pair, out, local, false);
        applyDeletes(pair, out, remote, true);

        // Everything else is the newer side's - the policy's answer for a bin this handler has no opinion
        // on. `local.withBins(out)` would read more naturally and be wrong: it prefers a side, and the
        // other DC prefers the other side.
        return Resolution.record(pair.withNewer(out));
    }

    private static void applyDeletes(Pair pair, UdfMap out, EventRecord side, boolean byRemote) {
        if (!side.deletedAny()) {
            return;
        }

        for (Value name : side.deleted.asList()) {
            String bin = name.asString();

            if (pair.remoteNewer(bin) == byRemote) {
                out.putNull(bin);
            }
        }
    }
}

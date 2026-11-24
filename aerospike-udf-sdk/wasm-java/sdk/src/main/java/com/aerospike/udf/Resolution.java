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
 * What an {@code xdr-merge} handler returns: the Java face of the Rust SDK's {@code Resolution}. Keep the
 * current record, apply the incoming write whole, do what the server would have done without the handler,
 * remove the record, or the final record.
 *
 * <pre>
 * return Resolution.keepLocal();                               // drop the incoming write
 * return Resolution.record(pair.withNewer(out));               // these bins
 * </pre>
 *
 * <p>No answer carries a TTL. The server gives the merged record the later of the two records' expirations,
 * and a record that never expires wins.
 *
 * <p>A resolution is checked by the server before it is taken, and one it does not accept is the handler's
 * failure, which the binding's {@code on-failure} setting decides.
 */
public final class Resolution {
    private final String verdict;
    private final int bins;

    private Resolution(String verdict, int bins) {
        this.verdict = verdict;
        this.bins = bins;
    }

    /**
     * Keep the local record; the incoming write is dropped, and the source cluster is told so in a way it
     * counts as success.
     */
    public static Resolution keepLocal() {
        return new Resolution("keep-local", 0);
    }

    /**
     * Apply the incoming write whole, as if it had won every bin: the incoming bins go over the current
     * record.
     */
    public static Resolution takeRemote() {
        return new Resolution("take-remote", 0);
    }

    /**
     * Do what the server would have done without the handler. A set bound to a handler never uses bin
     * convergence, so this applies the incoming write, as {@link #takeRemote} does.
     */
    public static Resolution policy() {
        return new Resolution("policy", 0);
    }

    /**
     * Remove the record on this cluster and leave a durable tombstone, so a late write cannot bring it back.
     * The delete is not shipped: the other cluster decides for itself, so a merge that deletes has to delete
     * on both sides for the two to agree. A record resolution with no bins left in it is not this: it is the
     * handler's failure.
     */
    public static Resolution delete() {
        return new Resolution("delete", 0);
    }

    /**
     * Remove the record on this cluster outright, with no tombstone, so a later write creates it again. Like
     * {@link #delete}, it is not shipped to the other cluster.
     */
    public static Resolution expunge() {
        return new Resolution("expunge", 0);
    }

    /**
     * The final record: these bins, whole - a bin not in the map, or set to nil, is deleted. The map's keys
     * are the bin names, so each is a string of 1 to 15 bytes. At least one bin must be left: an empty map,
     * or one whose values are all nil, is the handler's failure and not a deletion, which is what
     * {@link #delete} is for. The builder is not used up and may still be added to before the handler
     * returns; it is read after the handler returns, so it must not be closed (nor made in a
     * try-with-resources block) in the handler.
     */
    public static Resolution record(MapBuilder bins) {
        return new Resolution("record", bins.handle());
    }

    /**
     * Sets the resolution as the result of the call and returns the status the export returns: 0, or 1 after
     * failing the call when the answer could not be built. The generated export of a typed handler calls it;
     * a handler in the raw form {@code int name(int args, int argc)} ends with it, as in
     * {@code return Resolution.keepLocal().deliver()}.
     */
    public int deliver() {
        if (bins == 0) {
            Udf.resultString(verdict);
            return 0;
        }

        int out = LowLevel.newMap(2);

        if (out <= 0) {
            return Udf.fail("resolve: could not build the resolution");
        }

        // The result is the host's own reference, so the answer the handler never sees is given back at once.
        try (MapBuilder answer = LowLevel.map(out)) {
            answer.put("verdict", "record").put("bins", LowLevel.map(bins));

            if (LowLevel.resultHandle(out) != LowLevel.OK) {
                return Udf.fail("resolve: could not return the record");
            }
        }

        return 0;
    }

    /**
     * The verdict word as the server reads it: {@code keep-local}, {@code take-remote}, {@code policy},
     * {@code delete}, {@code expunge} or {@code record}.
     */
    public String verdict() {
        return verdict;
    }
}

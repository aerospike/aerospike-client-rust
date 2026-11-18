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

import com.aerospike.udf.Function;
import com.aerospike.udf.LowLevel;
import com.aerospike.udf.Stream;
import com.aerospike.udf.Udf;
import com.aerospike.udf.Value;

/**
 * Exact top-k over packed float vectors, as a WASM UDF in Java.
 *
 * <p>The same module as the Rust one (aerospike-udf-sdk-wasm/examples/vector), the C one and the Go one,
 * step for step - see the Rust module's documentation for the shared algorithm. In short: the host copies
 * the blob straight into a float array; distances accumulate in double from float elements in one fixed
 * order; larger is always worse; ties break on the digest; the arguments are read once per stream. The hot
 * path allocates nothing: every buffer and every hit is a field of the one instance, filled in place. That
 * is why this module is written on {@link LowLevel}, the SDK's calls one at a time with names as bytes and
 * values read into ones it owns, rather than on {@code Record}: a scan runs {@code record} once per record.
 *
 * <p>The class is the stream: {@code @Stream} names it, {@code record} and {@code end} are its two
 * halves, and the processor builds one instance per module instance - the state a stream keeps between
 * records is an ordinary field. Both halves keep the raw {@code (int args, int argc)} form, since the
 * arguments are read once, on the first record, and kept; {@code params} gives the signature.
 *
 * <pre>
 * topk("v", &lt;query blob&gt;, "cosine", 10)     stream: the 10 nearest, one partial per stream
 * vec_distance("v", &lt;query blob&gt;, "dot")    record: one number
 * </pre>
 */
@Stream(value = "topk", params = "(bin: str, query: bytes, metric: str, k?: int)")
public final class Vector {
    private static final int MAX_DIM = 2048;
    /** Bounded by the per-call handle table: each hit of the partial is one built map. */
    private static final int MAX_K = 200;
    private static final int DIGEST_LEN = 20;
    private static final int KEY_MAX = 64;

    private static final int COSINE = 0;
    private static final int DOT = 1;
    private static final int SQ_EUCLIDEAN = 2;
    private static final int EUCLIDEAN = 3;

    private static final class Hit {
        double score;
        final byte[] digest = new byte[DIGEST_LEN];
        int keyLen;
        final byte[] key = new byte[KEY_MAX];
    }

    // The stream's state.
    private boolean configured;
    private byte[] bin = new byte[0];
    private int metric;
    private int dim;
    private final float[] query = new float[MAX_DIM];
    private double qnorm;
    private int k;
    private int n;
    private final Hit[] hits = new Hit[MAX_K];
    private Hit spare = new Hit();
    private long skippedDim;
    private long skippedNone;

    /** The record's vector, copied by the host straight into the float array. */
    private final float[] vec = new float[MAX_DIM];

    private final byte[] binBuf = new byte[32];
    private final byte[] metricBuf = new byte[32];
    private final Value[] args = new Value[4];
    private final Value v = new Value();

    {
        for (int i = 0; i < MAX_K; i++) {
            hits[i] = new Hit();
        }
    }

    private static int parseMetric(String name) {
        switch (name) {
        case "":
        case "cosine":
            return COSINE;
        case "dot":
        case "ip":
        case "inner_product":
            return DOT;
        case "sqeuclidean":
        case "l2sq":
            return SQ_EUCLIDEAN;
        case "euclidean":
        case "l2":
            return EUCLIDEAN;
        default:
            return -1;
        }
    }

    /** Reads the arguments once: bin, query, metric, k. Returns null or the failure message. */
    private String configure(int handle, int argc) {
        int count = LowLevel.args(handle, argc, args);

        if (count < 2 || args[0].kind() != Value.STR) {
            return "topk: bin name missing";
        }

        int blen = LowLevel.bytes(args[0], binBuf);

        if (blen < 0) {
            return "topk: bin name too long";
        }

        bin = new byte[blen];
        System.arraycopy(binBuf, 0, bin, 0, blen);

        if (args[1].kind() != Value.BLOB) {
            return "topk: the query must be a blob of f32";
        }

        if (args[1].length() % 4 != 0 || args[1].length() / 4 > MAX_DIM) {
            return "topk: the query is not a vector of f32 this module can hold";
        }

        dim = args[1].length() / 4;

        if (LowLevel.floats(args[1], query) < 0) {
            return "topk: could not read the query";
        }

        double qn = 0.0;

        for (int i = 0; i < dim; i++) {
            double b = query[i];
            qn += b * b;
        }

        qnorm = Math.sqrt(qn);

        String name = "";

        if (count > 2 && args[2].kind() == Value.STR) {
            int mlen = LowLevel.bytes(args[2], metricBuf);

            if (mlen < 0) {
                return "topk: unknown metric";
            }

            name = LowLevel.string(metricBuf, mlen);
        }

        metric = parseMetric(name);

        if (metric < 0) {
            return "topk: unknown metric";
        }

        k = 10;

        if (count > 3 && args[3].kind() == Value.INT) {
            long kk = args[3].asInt();

            if (kk < 1) {
                kk = 1;
            }

            if (kk > MAX_K) {
                kk = MAX_K;
            }

            k = (int) kk;
        }

        n = 0;
        configured = true;

        return null;
    }

    /**
     * The measured distance (or similarity, for dot) between the record's vector and the query.
     *
     * <p>Four accumulators, lane k holding the elements whose index is k mod 4, folded in one fixed order at
     * the end: the same order as the Rust, C and Go modules and the test's local computation, which keeps
     * every module bit-identical. (Java emits no SIMD here; the lanes keep the arithmetic identical.)
     */
    private double measure() {
        int len = dim;
        int full = len - len % 4;

        switch (metric) {
        case COSINE: {
            double d0 = 0, d1 = 0, d2 = 0, d3 = 0;
            double s0 = 0, s1 = 0, s2 = 0, s3 = 0;

            for (int i = 0; i < full; i += 4) {
                double a0 = vec[i], a1 = vec[i + 1], a2 = vec[i + 2], a3 = vec[i + 3];
                double b0 = query[i], b1 = query[i + 1], b2 = query[i + 2], b3 = query[i + 3];
                d0 += a0 * b0;
                d1 += a1 * b1;
                d2 += a2 * b2;
                d3 += a3 * b3;
                s0 += a0 * a0;
                s1 += a1 * a1;
                s2 += a2 * a2;
                s3 += a3 * a3;
            }

            for (int kk = 0; kk < len - full; kk++) {
                double a = vec[full + kk];
                double b = query[full + kk];

                switch (kk) {
                case 0: d0 += a * b; s0 += a * a; break;
                case 1: d1 += a * b; s1 += a * a; break;
                case 2: d2 += a * b; s2 += a * a; break;
                default: d3 += a * b; s3 += a * a; break;
                }
            }

            double dot = ((d0 + d1) + d2) + d3;
            double na = ((s0 + s1) + s2) + s3;

            if (na == 0 || qnorm == 0) {
                return 1.0;
            }

            return 1.0 - dot / (Math.sqrt(na) * qnorm);
        }
        case DOT: {
            double d0 = 0, d1 = 0, d2 = 0, d3 = 0;

            for (int i = 0; i < full; i += 4) {
                d0 += (double) vec[i] * query[i];
                d1 += (double) vec[i + 1] * query[i + 1];
                d2 += (double) vec[i + 2] * query[i + 2];
                d3 += (double) vec[i + 3] * query[i + 3];
            }

            for (int kk = 0; kk < len - full; kk++) {
                double p = (double) vec[full + kk] * query[full + kk];

                switch (kk) {
                case 0: d0 += p; break;
                case 1: d1 += p; break;
                case 2: d2 += p; break;
                default: d3 += p; break;
                }
            }

            return ((d0 + d1) + d2) + d3;
        }
        default: {
            double s0 = 0, s1 = 0, s2 = 0, s3 = 0;

            for (int i = 0; i < full; i += 4) {
                double e0 = (double) vec[i] - query[i];
                double e1 = (double) vec[i + 1] - query[i + 1];
                double e2 = (double) vec[i + 2] - query[i + 2];
                double e3 = (double) vec[i + 3] - query[i + 3];
                s0 += e0 * e0;
                s1 += e1 * e1;
                s2 += e2 * e2;
                s3 += e3 * e3;
            }

            for (int kk = 0; kk < len - full; kk++) {
                double e = (double) vec[full + kk] - query[full + kk];

                switch (kk) {
                case 0: s0 += e * e; break;
                case 1: s1 += e * e; break;
                case 2: s2 += e * e; break;
                default: s3 += e * e; break;
                }
            }

            double sum = ((s0 + s1) + s2) + s3;

            return metric == EUCLIDEAN ? Math.sqrt(sum) : sum;
        }
        }
    }

    /** Larger is worse. */
    private double scoreOf(double measured) {
        return metric == DOT ? -measured : measured;
    }

    /** The record's vector into vec: 1 read, 0 skipped and counted, -1 a failure. */
    private int readVector() {
        int rc = LowLevel.bin(bin, v);

        if (rc == LowLevel.NOT_FOUND || (rc == LowLevel.OK && v.kind() != Value.BLOB)) {
            skippedNone++;
            return 0;
        }

        if (rc != LowLevel.OK) {
            return -1;
        }

        if (v.length() != dim * 4) {
            skippedDim++;
            return 0;
        }

        if (LowLevel.floats(v, vec) < 0) {
            return -1;
        }

        return 1;
    }

    private static int digestCmp(byte[] a, byte[] b) {
        for (int i = 0; i < DIGEST_LEN; i++) {
            if (a[i] != b[i]) {
                return (a[i] & 0xff) < (b[i] & 0xff) ? -1 : 1;
            }
        }

        return 0;
    }

    /** (score, digest) &lt; (score, digest) */
    private static boolean better(Hit a, Hit b) {
        if (a.score != b.score) {
            return a.score < b.score;
        }

        return digestCmp(a.digest, b.digest) < 0;
    }

    /** Offers a candidate: kept if it beats the worst of the k, ties broken on the digest. */
    private boolean offer(double score) {
        if (n == k && score > hits[n - 1].score) {
            return true;
        }

        Hit h = spare;
        h.score = score;

        if (LowLevel.digest(h.digest) != DIGEST_LEN) {
            return false;
        }

        if (n == k && score == hits[n - 1].score && digestCmp(h.digest, hits[n - 1].digest) >= 0) {
            return true;
        }

        h.keyLen = 0;

        switch (LowLevel.keyType()) {
        case Value.INT: {
            byte[] text = LowLevel.bytes(Long.toString(LowLevel.keyInt()));
            h.keyLen = Math.min(text.length, KEY_MAX);
            System.arraycopy(text, 0, h.key, 0, h.keyLen);
            break;
        }
        case Value.STR:
        case Value.BLOB: {
            int kl = LowLevel.keyBytes(h.key);

            if (kl > 0) {
                h.keyLen = kl;
            }

            break;
        }
        default:
            break;
        }

        int at = n;

        while (at > 0 && better(h, hits[at - 1])) {
            at--;
        }

        int last = n == k ? k - 1 : n;

        if (at > last) {
            return true;
        }

        Hit evicted = hits[last];

        for (int i = last; i > at; i--) {
            hits[i] = hits[i - 1];
        }

        hits[at] = h;
        spare = evicted;

        if (n < k) {
            n++;
        }

        return true;
    }

    // ANCHOR: stream
    /** The per-record half, in the raw form: see the class comment. */
    int record(int handle, int argc) {
        if (!configured) {
            String msg = configure(handle, argc);

            if (msg != null) {
                return Udf.fail(msg);
            }
        }

        switch (readVector()) {
        case -1:
            return Udf.fail("topk: could not read the vector");
        case 0:
            return 0;
        default:
            break;
        }

        if (!offer(scoreOf(measure()))) {
            return Udf.fail("topk: could not read the digest");
        }

        return 0;
    }

    private static final byte[] KEY_SCORE = LowLevel.bytes("score");
    private static final byte[] KEY_DIGEST = LowLevel.bytes("digest");
    private static final byte[] KEY_KEY = LowLevel.bytes("key");
    private static final byte[] KEY_HITS = LowLevel.bytes("hits");
    private static final byte[] KEY_SKIPPED_DIM = LowLevel.bytes("skipped_dim");
    private static final byte[] KEY_SKIPPED_NONE = LowLevel.bytes("skipped_none");
    private final Value[] pairs = { new Value(), new Value(), new Value(), new Value(), new Value(), new Value() };

    /** The closing half: the partial, k hits at most. */
    int end(int handle, int argc) {
        int out = LowLevel.newMap(3);
        int list = LowLevel.newList(n);

        if (out <= 0 || list <= 0) {
            return Udf.fail("topk: could not build the partial");
        }

        for (int i = 0; i < n; i++) {
            Hit h = hits[i];
            int entry = LowLevel.newMap(3);

            LowLevel.setStr(pairs[0], KEY_SCORE, KEY_SCORE.length);
            LowLevel.setDouble(pairs[1], h.score);
            LowLevel.setStr(pairs[2], KEY_DIGEST, KEY_DIGEST.length);
            LowLevel.setBlob(pairs[3], h.digest, DIGEST_LEN);
            LowLevel.setStr(pairs[4], KEY_KEY, KEY_KEY.length);
            LowLevel.setStr(pairs[5], h.key, h.keyLen);

            if (entry <= 0 || LowLevel.mapPutEntries(entry, pairs, 6) != 3
                    || LowLevel.listPushHandle(list, entry) != LowLevel.OK) {
                return Udf.fail("topk: could not build a hit");
            }
        }

        LowLevel.mapPutHandle(out, KEY_HITS, list);
        LowLevel.mapPutInt(out, KEY_SKIPPED_DIM, skippedDim);
        LowLevel.mapPutInt(out, KEY_SKIPPED_NONE, skippedNone);

        if (LowLevel.resultHandle(out) != LowLevel.OK) {
            return Udf.fail("topk: could not return the partial");
        }

        return 0;
    }

    // ANCHOR_END: stream

    // ANCHOR: record
    /** The distance from this record's vector to the query: one record, one number, nil for no such vector. */
    @Function(value = "vec_distance", params = "(bin: str, query: bytes, metric: str)")
    int vecDistance(int handle, int argc) {
        String msg = configure(handle, argc);

        if (msg != null) {
            return Udf.fail(msg);
        }

        switch (readVector()) {
        case -1:
            return Udf.fail("vec_distance: could not read the vector");
        case 0:
            Udf.resultNil();
            break;
        default:
            Udf.resultDouble(measure());
            break;
        }

        return 0;
    }

    // ANCHOR_END: record

}

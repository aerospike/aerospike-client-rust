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
 * The events other than the merge. A handler is an exported {@code int name(int args, int argc)};
 * {@link #desc} reads the description, the {@code *Info} classes decode its parts, {@link EventRecord}
 * reads a record (index 0 for the first record the event hands over, 1 for the second), and the answer
 * methods deliver the verdict. The signature {@link #signature} gives goes under {@code __udf_sig_<name>}.
 */
public final class Events {
    private Events() {
    }

    /** The description map of an event call, or null when the arguments are not one. */
    public static Value desc(int args, int argc) {
        Value[] a = new Value[1];

        if (LowLevel.args(args, argc, a) < 1 || a[0].kind() != Value.MAP) {
            return null;
        }

        return a[0];
    }

    /**
     * The record under the description's entry {@code key} ({@code before}, {@code after}, {@code record}),
     * or null when the description has none; {@code which} is the index the record is read by.
     */
    public static EventRecord side(Value desc, String key, int which) {
        EventRecord out = new EventRecord();

        return desc != null && out.decode(desc, key, which) ? out : null;
    }

    /** What a before-write or after-write handler is asked about. */
    public static final class WriteInfo {
        /** client, batch, udf, ops, xdr. */
        public String source = "";
        /** The generation the client asked to match, 0 for none. */
        public long generation;
        /** The TTL the write asked for, with a write's meaning. */
        public long ttl;
        /** The record did not exist. */
        public boolean create;
        /** The write asked for durable delete. */
        public boolean durableDelete;

        /** Fills this from the description's {@code write} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "write");

            if (m.kind() != Value.MAP) {
                return false;
            }

            source = Maps.getString(m, "source");
            generation = Maps.getLong(m, "generation", 0);
            ttl = Maps.getLong(m, "ttl", 0);
            create = Maps.getBoolean(m, "create");
            durableDelete = Maps.getBoolean(m, "durable-delete");

            return true;
        }
    }

    /** What a before-delete or after-delete handler is asked about. */
    public static final class DeleteInfo {
        /** Where the delete came from, named as {@code WriteInfo.source} names it. */
        public String source = "";
        /** The delete is a durable one. */
        public boolean durable;

        /** Fills this from the description's {@code delete} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "delete");

            if (m.kind() != Value.MAP) {
                return false;
            }

            source = Maps.getString(m, "source");
            durable = Maps.getBoolean(m, "durable");

            return true;
        }
    }

    /** What a before-truncate or after-truncate handler is asked about. */
    public static final class TruncateInfo {
        /** The namespace being truncated. */
        public String namespace = "";
        /** "" for the whole namespace. */
        public String set = "";
        /** Records last updated before this (ms since the epoch) go. */
        public long lut;

        /** Fills this from the description's {@code truncate} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "truncate");

            if (m.kind() != Value.MAP) {
                return false;
            }

            namespace = Maps.getString(m, "namespace");
            set = Maps.getString(m, "set");
            lut = Maps.getLong(m, "lut", 0);

            return true;
        }
    }

    /** An expiry (before-expire) or an eviction (before-evict). */
    public static final class ExpireInfo {
        /** When the record was due, seconds since the Aerospike epoch. */
        public long voidTime;
        /** The supervisor's clock, in the same units. */
        public long now;
        /** before-evict: records due before this are being evicted. */
        public long evictVoidTime;

        /**
         * Fills this from the description's {@code expire} entry, or its {@code evict} entry. False when it has
         * neither.
         */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "expire");

            if (m.kind() != Value.MAP) {
                m = Maps.get(desc, "evict");
            }

            if (m.kind() != Value.MAP) {
                return false;
            }

            voidTime = Maps.getLong(m, "void-time", 0);
            now = Maps.getLong(m, "now", 0);
            evictVoidTime = Maps.getLong(m, "evict-void-time", 0);

            return true;
        }
    }

    /** What an after-read handler is asked about. */
    public static final class ReadInfo {
        /** Where the read came from. */
        public String source = "";
        /** Every bin was asked for, not named ones. */
        public boolean all;

        /** Fills this from the description's {@code read} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "read");

            if (m.kind() != Value.MAP) {
                return false;
            }

            source = Maps.getString(m, "source");
            all = Maps.getBoolean(m, "all");

            return true;
        }
    }

    /** What a before-ship handler is asked about. */
    public static final class ShipInfo {
        /** The destination DC's name. */
        public String dc = "";

        /** Fills this from the description's {@code ship} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "ship");

            if (m.kind() != Value.MAP) {
                return false;
            }

            dc = Maps.getString(m, "dc");

            return true;
        }
    }

    /** What a node-lifecycle handler is told. */
    public static final class LifecycleInfo {
        /** service-ready, shutdown, binding-deployed. */
        public String phase = "";
        /** This node's id, hex. */
        public String node = "";
        /** binding-deployed: a map of event, namespace, set, name, module, function; else ABSENT. */
        public Value binding = new Value();

        /** Fills this from the description's {@code lifecycle} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "lifecycle");

            if (m.kind() != Value.MAP) {
                return false;
            }

            phase = Maps.getString(m, "phase");
            node = Maps.getString(m, "node");
            binding = Maps.get(m, "binding");

            return true;
        }
    }

    /** What a cluster-change handler is told: the cluster after a rebalance. */
    public static final class ClusterInfo {
        /** The cluster key, in hex. */
        public String key = "";
        /** The cluster's size, in nodes. */
        public long size;
        /** This node's id. */
        public String self = "";
        /** The list of hex node ids in succession order. */
        public Value nodes = new Value();

        /** Fills this from the description's {@code cluster} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "cluster");

            if (m.kind() != Value.MAP) {
                return false;
            }

            key = Maps.getString(m, "key");
            self = Maps.getString(m, "self");
            size = Maps.getLong(m, "size", 0);
            nodes = Maps.get(m, "nodes");

            return true;
        }
    }

    /** What a partition-transfer handler is told: a partition arrived at this node or departed from it. */
    public static final class TransferInfo {
        /** The partition's namespace. */
        public String namespace = "";
        /** The partition's id. */
        public long partition;
        /** true: arrived here; false: departed. */
        public boolean arrived;
        /** The other node, in hex. */
        public String peer = "";

        /** Fills this from the description's {@code transfer} entry. False when it has none. */
        public boolean decode(Value desc) {
            Value m = Maps.get(desc, "transfer");

            if (m.kind() != Value.MAP) {
                return false;
            }

            namespace = Maps.getString(m, "namespace");
            partition = Maps.getLong(m, "partition", 0);
            arrived = "arrived".equals(Maps.getString(m, "direction"));
            peer = Maps.getString(m, "peer");

            return true;
        }
    }

    /** A timer handler's argument: the timer that came due. The handler runs as a record UDF. */
    public static final class TimerInfo {
        /** The name the timer was set under. */
        public String name = "";
        /** When the timer was due, in ms since the epoch. */
        public long due;
        /** When the timer fired, in ms since the epoch. */
        public long now;

        /** Reads a timer call's arguments. False when they are not a timer's. */
        public boolean decode(int args, int argc) {
            Value[] a = new Value[1];

            if (LowLevel.args(args, argc, a) < 1 || a[0].kind() != Value.MAP) {
                return false;
            }

            name = Maps.getString(a[0], "name");
            due = Maps.getLong(a[0], "due", 0);
            now = Maps.getLong(a[0], "now", 0);

            return true;
        }
    }

    /**
     * Delivers a verdict word with a code, a message, a bins map and a TTL where the kind takes them (0,
     * null, 0 and -1 for none). Returns the status the handler returns.
     */
    public static int answer(String verdict, long code, String message, int bins, long ttl) {
        if (code == 0 && (message == null || message.isEmpty()) && bins == 0 && ttl < 0) {
            Udf.resultStr(verdict);
            return 0;
        }

        int out = LowLevel.newMap(5);

        if (out <= 0) {
            return Udf.fail("answer: could not build the answer");
        }

        MapBuilder answer = LowLevel.map(out).put("verdict", verdict);

        if (code != 0) {
            answer.put("code", code);
        }

        if (message != null && !message.isEmpty()) {
            answer.put("message", message);
        }

        if (bins != 0) {
            answer.put("bins", LowLevel.map(bins));
        }

        if (ttl >= 0) {
            answer.put("ttl", ttl);
        }

        if (LowLevel.resultHandle(out) != LowLevel.OK) {
            return Udf.fail("answer: could not return the answer");
        }

        return 0;
    }

    // The words each kind takes. before-write: accept / reject / record; before-delete and before-truncate:
    // allow / refuse; before-expire: expire / keep; before-evict: evict / protect; after-read: unchanged /
    // record; before-ship: ship / skip / record; the after- and once-per-node events and timers: done.

    /**
     * Answers {@code accept} from a before-write handler: the write goes through as it is. Returns the status
     * the handler returns.
     */
    public static int accept() {
        return answer("accept", 0, null, 0, -1);
    }

    /**
     * Answers {@code reject} from a before-write handler: the write fails with a server result code the client
     * sees and a message, and nothing is written. Returns the status the handler returns.
     */
    public static int reject(long code, String message) {
        return answer("reject", code, message, 0, -1);
    }

    /**
     * Answers {@code record}: {@code bins} (a built map) is the whole record, a bin absent from it or nil being
     * deleted, with a TTL in seconds that has a write's meaning, or a negative one to send none. Returns the
     * status the handler returns.
     */
    public static int record(int bins, long ttl) {
        return answer("record", 0, null, bins, ttl);
    }

    /**
     * Answers {@code allow} from a before-delete or before-truncate handler: it goes ahead. Returns the status
     * the handler returns.
     */
    public static int allow() {
        return answer("allow", 0, null, 0, -1);
    }

    /**
     * Answers {@code refuse} from a before-delete or before-truncate handler: it fails with a server result
     * code the client sees and a message. Returns the status the handler returns.
     */
    public static int refuse(long code, String message) {
        return answer("refuse", code, message, 0, -1);
    }

    /**
     * Answers {@code expire} from a before-expire handler: the record expires. Returns the status the handler
     * returns.
     */
    public static int expire() {
        return answer("expire", 0, null, 0, -1);
    }

    /**
     * Answers {@code keep} from a before-expire handler: the record is kept for {@code ttl} more seconds, or
     * for the namespace's default TTL when {@code ttl} is negative. Returns the status the handler returns.
     */
    public static int keep(long ttl) {
        return answer("keep", 0, null, 0, ttl);
    }

    /**
     * Answers {@code evict} from a before-evict handler: the record is evicted. Returns the status the handler
     * returns.
     */
    public static int evict() {
        return answer("evict", 0, null, 0, -1);
    }

    /**
     * Answers {@code protect} from a before-evict handler: the record is left alone. Returns the status the
     * handler returns.
     */
    public static int protect() {
        return answer("protect", 0, null, 0, -1);
    }

    /**
     * Answers {@code unchanged} from a after-read handler: the read is answered with the record as it was read.
     * Returns the status the handler returns.
     */
    public static int unchanged() {
        return answer("unchanged", 0, null, 0, -1);
    }

    /**
     * Answers {@code ship} from a before-ship handler: the record ships as it is. Returns the status the
     * handler returns.
     */
    public static int ship() {
        return answer("ship", 0, null, 0, -1);
    }

    /**
     * Answers {@code skip} from a before-ship handler: the record is not shipped. Returns the status the
     * handler returns.
     */
    public static int skip() {
        return answer("skip", 0, null, 0, -1);
    }

    /**
     * Answers {@code ok}, for the kinds that take no verdict: the after events, the once-per-node events and
     * timers. Returns the status the handler returns.
     */
    public static int done() {
        return answer("ok", 0, null, 0, -1);
    }

    /**
     * The signature text for a handler of {@code kind} with the given parameter text, for example
     * {@code signature("before-write", "guard", PARAMS_BEFORE_WRITE)}; NUL-terminated, for a static field
     * served by {@link Params#signature}.
     */
    public static byte[] signature(String kind, String name, String params) {
        return Params.signatureBytes("event " + kind + "/1 " + name + params);
    }

    // The parameter text of each kind, for signature().
    /** The parameter text of a {@code before-write} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_WRITE = "(before: record, after: record, write: write) -> decision";
    /** The parameter text of a {@code before-delete} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_DELETE = "(record: record, delete: delete) -> gate";
    /** The parameter text of an {@code after-write} handler, for {@link #signature}. */
    public static final String PARAMS_AFTER_WRITE = "(before: record, after: record, write: write) -> ()";
    /** The parameter text of an {@code after-delete} handler, for {@link #signature}. */
    public static final String PARAMS_AFTER_DELETE = "(record: record, delete: delete) -> ()";
    /** The parameter text of a {@code before-expire} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_EXPIRE = "(record: record, expire: expire) -> expiry";
    /** The parameter text of a {@code before-evict} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_EVICT = "(record: record, evict: evict) -> eviction";
    /** The parameter text of an {@code after-read} handler, for {@link #signature}. */
    public static final String PARAMS_AFTER_READ = "(record: record, read: read) -> read-result";
    /** The parameter text of a {@code before-truncate} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_TRUNCATE = "(truncate: truncate) -> gate";
    /** The parameter text of an {@code after-truncate} handler, for {@link #signature}. */
    public static final String PARAMS_AFTER_TRUNCATE = "(truncate: truncate) -> ()";
    /** The parameter text of a {@code before-ship} handler, for {@link #signature}. */
    public static final String PARAMS_BEFORE_SHIP = "(record: record, ship: ship) -> ship-decision";
    /** The parameter text of a {@code partition-transfer} handler, for {@link #signature}. */
    public static final String PARAMS_PARTITION_TRANSFER = "(transfer: transfer) -> ()";
    /** The parameter text of a {@code cluster-change} handler, for {@link #signature}. */
    public static final String PARAMS_CLUSTER_CHANGE = "(cluster: cluster) -> ()";
    /** The parameter text of a {@code node-lifecycle} handler, for {@link #signature}. */
    public static final String PARAMS_NODE_LIFECYCLE = "(lifecycle: lifecycle) -> ()";
    /** The parameter text of a {@code timer} handler, for {@link #signature}. */
    public static final String PARAMS_TIMER = "(timer: timer) -> ()";
}

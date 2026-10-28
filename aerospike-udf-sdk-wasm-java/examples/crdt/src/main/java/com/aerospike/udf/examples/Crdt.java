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

import com.aerospike.udf.Conflict;
import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.On;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.UdfMap;
import com.aerospike.udf.Value;

/**
 * An active-active CRDT in Java: the same module as aerospike-udf-sdk-wasm/examples/crdt, rule for rule.
 * A last-writer-wins map of devices inside one record, merged across data centres by the xdr-merge
 * event, with the write UDFs that keep it well-formed.
 *
 * <p>The record keeps a subscriber's devices in one bin, {@code devices}, a list of maps each with a
 * {@code deviceId} and a {@code version} the application raises on every change. A second bin,
 * {@code devicesRemoved}, holds a removal marker per removed device ({@code {offAt, version}} under
 * {@code markers}), the purge horizon {@code purgedBefore}, and, once a purge has dropped a marker, the
 * version floor {@code versionFloor}. Both data centres take writes; when a write from one arrives at the
 * other, {@code xdr_merge} decides each device from values in the data alone - the version, whether it is
 * a removal, and finally the stored bytes - so both sides compute the same record without a clock, a LUT
 * or a writer id, in any delivery order.
 *
 * <pre>
 * event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
 * </pre>
 *
 * <p>Every application write goes through {@code update_device}, {@code remove_device} and
 * {@code purge_devices}, which keep the list sorted by {@code deviceId} and every version above the last one
 * seen, live or removed.
 */
public final class Crdt {
    /**
     * How long a removal marker outlives its removal, in ms. Longer than the longest outage between the
     * clusters plus the replication lag, or a purged device can come back on one side.
     */
    private static final long PURGE_AGE_MS = 7L * 24 * 60 * 60 * 1000;

    private static final int MAX_DEVICES = 128;
    private static final int ID_MAX = 64;

    private static final byte[] DEVICES = Udf.bytes("devices");
    private static final byte[] DEVICES_REMOVED = Udf.bytes("devicesRemoved");
    private static final byte[] DEVICE_ID = Udf.bytes("deviceId");
    private static final byte[] VERSION = Udf.bytes("version");
    private static final byte[] MARKERS = Udf.bytes("markers");
    private static final byte[] PURGED_BEFORE = Udf.bytes("purgedBefore");
    private static final byte[] VERSION_FLOOR = Udf.bytes("versionFloor");
    private static final byte[] OFF_AT = Udf.bytes("offAt");

    /** One side's candidate for a device: its live entry, or its removal marker. */
    private static final class Candidate {
        final byte[] id = new byte[ID_MAX];
        int idLen;
        long version;
        boolean removed;
        /** The entry or the marker, where it sits in the record it came from. */
        final Value value = new Value();
        /** A marker's removal time; 0 for a live entry. */
        long offAt;

        void copyFrom(Candidate o) {
            System.arraycopy(o.id, 0, id, 0, o.idLen);
            idLen = o.idLen;
            version = o.version;
            removed = o.removed;
            value.set(o.value.kind, o.value.len, o.value.payload);
            offAt = o.offAt;
        }
    }

    private static int cmpBytes(byte[] a, int alen, byte[] b, int blen) {
        int n = Math.min(alen, blen);

        for (int i = 0; i < n; i++) {
            int x = a[i] & 0xff;
            int y = b[i] & 0xff;

            if (x != y) {
                return x < y ? -1 : 1;
            }
        }

        return Integer.compare(alen, blen);
    }

    private static boolean sameId(Candidate a, byte[] id, int idLen) {
        return a.idLen == idLen && cmpBytes(a.id, a.idLen, id, idLen) == 0;
    }

    /**
     * {@code higher()}: the one of two candidates for a device that wins. The first rule that tells them
     * apart decides, and every rule reads the data alone. Two different changes at one version are
     * concurrent: the stored bytes pick one, the same on both clusters, since each side compares the same
     * two byte strings.
     */
    private static Candidate higher(Candidate a, Candidate b) {
        if (a.version != b.version) {
            return a.version > b.version ? a : b;
        }

        if (a.removed != b.removed) {
            return a.removed ? a : b;
        }

        return Udf.cmpStored(a.value, b.value) >= 0 ? a : b;
    }

    /** The candidates seen so far, one per device: {@link #offer} keeps the higher. */
    private static final class Best {
        final Candidate[] items = new Candidate[MAX_DEVICES];
        int n;
        final Candidate scratch = new Candidate();

        Best() {
            for (int i = 0; i < MAX_DEVICES; i++) {
                items[i] = new Candidate();
            }
        }

        /** Offers {@link #scratch}; false when the table is full. */
        boolean offer() {
            for (int i = 0; i < n; i++) {
                if (sameId(items[i], scratch.id, scratch.idLen)) {
                    if (higher(items[i], scratch) == scratch) {
                        items[i].copyFrom(scratch);
                    }

                    return true;
                }
            }

            if (n == MAX_DEVICES) {
                return false;
            }

            items[n++].copyFrom(scratch);

            return true;
        }

        /**
         * In deviceId order, by UTF-8 bytes - the order the server keeps a key-ordered map's string keys in,
         * so the list and the markers agree.
         */
        void sort() {
            for (int i = 1; i < n; i++) {
                int j = i;

                while (j > 0 && cmpBytes(items[j - 1].id, items[j - 1].idLen, items[j].id, items[j].idLen) > 0) {
                    Candidate t = items[j - 1];
                    items[j - 1] = items[j];
                    items[j] = t;
                    j--;
                }
            }
        }
    }

    private static final Value[] w = new Value[64];
    private static final Value tmp = new Value();
    private static final Value tmp2 = new Value();

    /** Every live entry of a devices list as a candidate. */
    private static boolean offerLive(Best best, Value devices) {
        int cur = Udf.enter(devices);

        if (cur <= 0) {
            return false;
        }

        for (;;) {
            int n = Udf.next(cur, w);

            for (int i = 0; i < n; i++) {
                Candidate c = best.scratch;

                if (w[i].kind != Value.MAP || Udf.get(w[i], DEVICE_ID, tmp) != Udf.OK || tmp.kind != Value.STR) {
                    Udf.close(cur);
                    return false;
                }

                c.idLen = Udf.bytes(tmp, c.id);

                if (c.idLen < 0 || Udf.get(w[i], VERSION, tmp) != Udf.OK || tmp.kind != Value.INT) {
                    Udf.close(cur);
                    return false;
                }

                c.version = tmp.payload;
                c.removed = false;
                c.value.set(w[i].kind, w[i].len, w[i].payload);
                c.offAt = 0;

                if (!best.offer()) {
                    Udf.close(cur);
                    return false;
                }
            }

            if (n < w.length) {
                break;
            }
        }

        Udf.close(cur);

        return true;
    }

    private static long horizon;
    private static long floor;

    /** Every marker of a devicesRemoved bin as a candidate; the horizon and the floor it carries. */
    private static boolean offerMarkers(Best best, Value removed) {
        if (removed.kind != Value.MAP) {
            return true;
        }

        if (Udf.get(removed, PURGED_BEFORE, tmp) == Udf.OK && tmp.kind == Value.INT) {
            horizon = Math.max(horizon, tmp.payload);
        }

        if (Udf.get(removed, VERSION_FLOOR, tmp) == Udf.OK && tmp.kind == Value.INT) {
            floor = Math.max(floor, tmp.payload);
        }

        if (Udf.get(removed, MARKERS, tmp) != Udf.OK || tmp.kind != Value.MAP) {
            return true;
        }

        int cur = Udf.enter(tmp);

        if (cur <= 0) {
            return false;
        }

        for (;;) {
            int n = Udf.next(cur, w);

            for (int i = 0; i + 1 < n; i += 2) {
                Candidate c = best.scratch;
                Value key = w[i];
                Value marker = w[i + 1];

                if (key.kind != Value.STR || marker.kind != Value.MAP) {
                    Udf.close(cur);
                    return false;
                }

                c.idLen = Udf.bytes(key, c.id);

                if (c.idLen < 0 || Udf.get(marker, VERSION, tmp2) != Udf.OK || tmp2.kind != Value.INT) {
                    Udf.close(cur);
                    return false;
                }

                c.version = tmp2.payload;

                if (Udf.get(marker, OFF_AT, tmp2) != Udf.OK || tmp2.kind != Value.INT) {
                    Udf.close(cur);
                    return false;
                }

                c.offAt = tmp2.payload;
                c.removed = true;
                c.value.set(marker.kind, marker.len, marker.payload);

                if (!best.offer()) {
                    Udf.close(cur);
                    return false;
                }
            }

            if (n < w.length) {
                break;
            }
        }

        Udf.close(cur);

        return true;
    }

    private static boolean hasBin(EventRecord rec, byte[] name, int kind) {
        return rec.bin(name, tmp) == Udf.OK && tmp.kind == kind;
    }

    /**
     * The merge: every device from both sides goes in as a candidate, live entry or marker, the higher wins
     * per device, and the result is the sorted live list plus the markers still inside the purge horizon.
     * Every other bin is the incoming one's, as XDR would have applied it.
     */
    @On.XdrMerge("xdr_merge")
    static Resolution xdrMerge(EventRecord current, EventRecord incoming, Conflict c) {
        int out = Udf.newMap(8);

        if (out <= 0) {
            throw new UdfException("xdr_merge: could not build the record");
        }

        // Other bins: as XDR does today.
        Udf.eachStr(incoming.bins, (text, len, v) -> {
            byte[] name = new byte[len];
            System.arraycopy(text, 0, name, 0, len);

            if (!Udf.equalBytes(name, DEVICES, len, DEVICES.length)
                    && !Udf.equalBytes(name, DEVICES_REMOVED, len, DEVICES_REMOVED.length)) {
                Value val = new Value();

                if (incoming.bin(name, val) == Udf.OK) {
                    Udf.mapPutValue(out, name, val);
                }
            }
        });

        boolean incomingDevices = hasBin(incoming, DEVICES, Value.LIST);
        boolean incomingRemoved = hasBin(incoming, DEVICES_REMOVED, Value.MAP);

        if (incomingDevices || incomingRemoved) {
            Best best = new Best();
            horizon = 0;
            floor = 0;
            Value side = new Value();
            boolean anyDevices = false;

            for (int which = 0; which < 2; which++) {
                EventRecord rec = which == 0 ? current : incoming;

                if (rec.bin(DEVICES, side) == Udf.OK && side.kind == Value.LIST) {
                    anyDevices = true;

                    if (!offerLive(best, side)) {
                        throw new UdfException("xdr_merge: malformed devices, or too many");
                    }
                }

                if (rec.bin(DEVICES_REMOVED, side) == Udf.OK && side.kind == Value.MAP && !offerMarkers(best, side)) {
                    throw new UdfException("xdr_merge: malformed devicesRemoved, or too many");
                }
            }

            best.sort();

            int live = Udf.newList(best.n);
            int markers = Udf.newMap(best.n);
            int nMarkers = 0;

            if (live <= 0 || markers <= 0) {
                throw new UdfException("xdr_merge: could not build the result");
            }

            for (int i = 0; i < best.n; i++) {
                Candidate cand = best.items[i];

                if (!cand.removed) {
                    Udf.listPushValue(live, cand.value);
                }
                else if (cand.offAt >= horizon) {
                    byte[] id = new byte[cand.idLen];
                    System.arraycopy(cand.id, 0, id, 0, cand.idLen);
                    Udf.mapPutValue(markers, id, cand.value);
                    nMarkers++;
                }
                else {
                    // Dropped by the other side's horizon before this side purged it: its version still
                    // has to be the floor, or a re-add here could start below a copy of the marker that
                    // is still on its way.
                    floor = Math.max(floor, cand.version);
                }
            }

            if (anyDevices) {
                Udf.mapPutHandle(out, DEVICES, live);
            }

            if (nMarkers > 0 || horizon > 0) {
                int removed = Udf.newMap(3);

                Udf.mapPutHandle(removed, MARKERS, markers);
                Udf.mapPutInt(removed, PURGED_BEFORE, horizon);

                if (floor > 0) {
                    Udf.mapPutInt(removed, VERSION_FLOOR, floor);
                }

                Udf.mapPutHandle(out, DEVICES_REMOVED, removed);
            }
            else {
                // The merge has no removals left: a nil deletes the bin, so a stale copy cannot come back.
                Udf.mapPutHandle(out, DEVICES_REMOVED, Udf.nilHandle());
            }
        }

        // Every bin of the current record not decided above, by position: `with_bins`.
        Udf.eachStr(current.bins, (text, len, v) -> {
            byte[] name = new byte[len];
            System.arraycopy(text, 0, name, 0, len);

            if (!Udf.mapHasKey(out, name)) {
                Value val = new Value();

                if (current.bin(name, val) == Udf.OK) {
                    Udf.mapPutValue(out, name, val);
                }
            }
        });

        return Resolution.record(UdfMap.wrap(out));
    }

    //==========================================================
    // The write UDFs: every application write to the devices goes through these, so each change moves one
    // device strictly up the order the merge uses.
    //

    /** A live entry as the write UDFs hold it: its id and where it sits, or the map built for it. */
    private static final class Entry {
        final byte[] id = new byte[ID_MAX];
        int idLen;
        final Value value = new Value();
        int handle;
    }

    private static final Entry[] entries = new Entry[MAX_DEVICES];
    private static final Entry[] sorted = new Entry[MAX_DEVICES + 1];
    private static final Entry added = new Entry();

    static {
        for (int i = 0; i < MAX_DEVICES; i++) {
            entries[i] = new Entry();
        }
    }

    /** The live entries, in stored order. -1 for too many or malformed. */
    private static int liveEntries() {
        Value devices = new Value();

        if (Udf.bin(DEVICES, devices) != Udf.OK || devices.kind != Value.LIST) {
            return 0;
        }

        int cur = Udf.enter(devices);

        if (cur <= 0) {
            return -1;
        }

        int count = 0;

        for (;;) {
            int n = Udf.next(cur, w);

            for (int i = 0; i < n; i++) {
                if (count == MAX_DEVICES || w[i].kind != Value.MAP || Udf.get(w[i], DEVICE_ID, tmp) != Udf.OK
                        || tmp.kind != Value.STR) {
                    Udf.close(cur);
                    return -1;
                }

                Entry e = entries[count];
                e.idLen = Udf.bytes(tmp, e.id);

                if (e.idLen < 0) {
                    Udf.close(cur);
                    return -1;
                }

                e.value.set(w[i].kind, w[i].len, w[i].payload);
                e.handle = 0;
                count++;
            }

            if (n < w.length) {
                break;
            }
        }

        Udf.close(cur);

        return count;
    }

    /** A marker's version, or -1 when the device has none. */
    private static long markerVersion(byte[] id, int idLen) {
        Value removed = new Value();
        Value markers = new Value();
        Value marker = new Value();
        byte[] key = new byte[idLen];
        System.arraycopy(id, 0, key, 0, idLen);

        if (Udf.bin(DEVICES_REMOVED, removed) != Udf.OK || removed.kind != Value.MAP
                || Udf.get(removed, MARKERS, markers) != Udf.OK || markers.kind != Value.MAP
                || Udf.get(markers, key, marker) != Udf.OK || marker.kind != Value.MAP
                || Udf.get(marker, VERSION, tmp) != Udf.OK || tmp.kind != Value.INT) {
            return -1;
        }

        return tmp.payload;
    }

    private static long removedInt(byte[] key) {
        Value removed = new Value();

        if (Udf.bin(DEVICES_REMOVED, removed) != Udf.OK || removed.kind != Value.MAP
                || Udf.get(removed, key, tmp) != Udf.OK || tmp.kind != Value.INT) {
            return 0;
        }

        return tmp.payload;
    }

    /** The device's latest version, live or removed, and never below the version floor. */
    private static long lastVersion(int n, byte[] id, int idLen) {
        long v = removedInt(VERSION_FLOOR);

        for (int i = 0; i < n; i++) {
            Entry e = entries[i];

            if (e.idLen == idLen && cmpBytes(e.id, e.idLen, id, idLen) == 0
                    && Udf.get(e.value, VERSION, tmp) == Udf.OK && tmp.kind == Value.INT) {
                v = Math.max(v, tmp.payload);
            }
        }

        return Math.max(v, markerVersion(id, idLen));
    }

    /** Stores the entries plus an optional built entry, sorted by id, less any entry under {@code without}. */
    private static boolean storeDevices(int n, byte[] without, int withoutLen, Entry add) {
        int count = 0;

        for (int i = 0; i < n; i++) {
            Entry e = entries[i];

            if (!(e.idLen == withoutLen && cmpBytes(e.id, e.idLen, without, withoutLen) == 0)) {
                sorted[count++] = e;
            }
        }

        if (add != null) {
            sorted[count++] = add;
        }

        for (int i = 1; i < count; i++) {
            int j = i;

            while (j > 0 && cmpBytes(sorted[j - 1].id, sorted[j - 1].idLen, sorted[j].id, sorted[j].idLen) > 0) {
                Entry t = sorted[j - 1];
                sorted[j - 1] = sorted[j];
                sorted[j] = t;
                j--;
            }
        }

        int list = Udf.newList(count);

        if (list <= 0) {
            return false;
        }

        for (int i = 0; i < count; i++) {
            Entry e = sorted[i];
            int rc = e.handle != 0 ? Udf.listPushHandle(list, e.handle) : Udf.listPushValue(list, e.value);

            if (rc != Udf.OK) {
                return false;
            }
        }

        return Udf.setHandle(DEVICES, list) == Udf.OK;
    }

    /**
     * Rebuilds devicesRemoved from the current one with one marker removed ({@code drop}), one added or
     * replaced ({@code addId} with {@code addOffAt}, {@code addVersion}), and a purge at {@code now} (when
     * {@code now} is not negative). The bin is left out while it holds nothing.
     */
    private static boolean storeRemoved(byte[] drop, int dropLen, byte[] addId, int addLen, long addOffAt,
            long addVersion, long now) {
        long hz = removedInt(PURGED_BEFORE);
        long fl = removedInt(VERSION_FLOOR);

        if (now >= 0) {
            hz = Math.max(hz, now - PURGE_AGE_MS);
        }

        int markers = Udf.newMap(8);

        if (markers <= 0) {
            return false;
        }

        int nMarkers = 0;
        Value removed = new Value();
        Value existing = new Value();

        if (Udf.bin(DEVICES_REMOVED, removed) == Udf.OK && removed.kind == Value.MAP
                && Udf.get(removed, MARKERS, existing) == Udf.OK && existing.kind == Value.MAP) {
            int cur = Udf.enter(existing);

            if (cur <= 0) {
                return false;
            }

            byte[] idBuf = new byte[ID_MAX];

            for (;;) {
                int n = Udf.next(cur, w);

                for (int i = 0; i + 1 < n; i += 2) {
                    Value key = w[i];
                    Value marker = w[i + 1];
                    int idLen = Udf.bytes(key, idBuf);

                    if (idLen < 0 || marker.kind != Value.MAP) {
                        Udf.close(cur);
                        return false;
                    }

                    if ((drop != null && idLen == dropLen && cmpBytes(idBuf, idLen, drop, dropLen) == 0)
                            || (addId != null && idLen == addLen && cmpBytes(idBuf, idLen, addId, addLen) == 0)) {
                        continue;
                    }

                    if (Udf.get(marker, OFF_AT, tmp2) != Udf.OK || tmp2.kind != Value.INT) {
                        Udf.close(cur);
                        return false;
                    }

                    if (tmp2.payload < hz) {
                        // Purged: its version becomes the floor every later version starts above.
                        if (Udf.get(marker, VERSION, tmp2) == Udf.OK && tmp2.kind == Value.INT) {
                            fl = Math.max(fl, tmp2.payload);
                        }

                        continue;
                    }

                    byte[] id = new byte[idLen];
                    System.arraycopy(idBuf, 0, id, 0, idLen);
                    Udf.mapPutValue(markers, id, marker);
                    nMarkers++;
                }

                if (n < w.length) {
                    break;
                }
            }

            Udf.close(cur);
        }

        if (addId != null) {
            int marker = Udf.newMap(2);
            byte[] id = new byte[addLen];
            System.arraycopy(addId, 0, id, 0, addLen);

            Udf.mapPutInt(marker, OFF_AT, addOffAt);
            Udf.mapPutInt(marker, VERSION, addVersion);
            Udf.mapPutHandle(markers, id, marker);
            nMarkers++;
        }

        if (nMarkers == 0 && hz == 0) {
            if (Udf.bin(DEVICES_REMOVED, removed) == Udf.OK) {
                return Udf.remove(DEVICES_REMOVED) == Udf.OK;
            }

            return true;
        }

        int out = Udf.newMap(3);

        Udf.mapPutHandle(out, MARKERS, markers);
        Udf.mapPutInt(out, PURGED_BEFORE, hz);

        if (fl > 0) {
            Udf.mapPutInt(out, VERSION_FLOOR, fl);
        }

        return Udf.setHandle(DEVICES_REMOVED, out) == Udf.OK;
    }

    private static int commit() {
        return Udf.exists() ? Udf.update() : Udf.create();
    }

    private static final byte[] idBuf = new byte[ID_MAX];

    /** The id's bytes into idBuf; its length. */
    private static int idBytes(String id, String fn) {
        byte[] b = Udf.bytes(id);

        if (b.length > ID_MAX) {
            throw new UdfException(fn + ": the id is too long");
        }

        System.arraycopy(b, 0, idBuf, 0, b.length);

        return b.length;
    }

    /** Copies every entry of a stored map into a built map, by position. */
    private static boolean copyEntries(Value map, int into) {
        int cur = Udf.enter(map);

        if (cur <= 0) {
            return false;
        }

        byte[] keyBuf = new byte[ID_MAX];

        for (;;) {
            int n = Udf.next(cur, w);

            for (int i = 0; i + 1 < n; i += 2) {
                int klen = Udf.bytes(w[i], keyBuf);

                if (klen < 0) {
                    Udf.close(cur);
                    return false;
                }

                byte[] key = new byte[klen];
                System.arraycopy(keyBuf, 0, key, 0, klen);
                Udf.mapPutValue(into, key, w[i + 1]);
            }

            if (n < w.length) {
                break;
            }
        }

        Udf.close(cur);

        return true;
    }

    /**
     * Adds a device, updates one, or adds a removed one back: the entry becomes the old one (if any) with
     * {@code changes} on top and a version one above the device's last, live or removed. Returns the version.
     */
    @Function("update_device")
    static long updateDevice(String id, Value changes) {
        if (changes.kind != Value.MAP) {
            throw new UdfException("update_device: changes is not a map");
        }

        int idLen = idBytes(id, "update_device");
        int n = liveEntries();

        if (n < 0) {
            throw new UdfException("update_device: malformed devices, or too many");
        }

        long version = lastVersion(n, idBuf, idLen) + 1;
        int entry = Udf.newMap(8);

        if (entry <= 0) {
            throw new UdfException("update_device: could not build the entry");
        }

        for (int i = 0; i < n; i++) {
            Entry e = entries[i];

            if (e.idLen == idLen && cmpBytes(e.id, e.idLen, idBuf, idLen) == 0 && !copyEntries(e.value, entry)) {
                throw new UdfException("update_device: could not read the entry");
            }
        }

        if (!copyEntries(changes, entry)) {
            throw new UdfException("update_device: could not read the changes");
        }

        byte[] idCopy = new byte[idLen];
        System.arraycopy(idBuf, 0, idCopy, 0, idLen);
        Udf.mapPutStr(entry, DEVICE_ID, idCopy, idLen);
        Udf.mapPutInt(entry, VERSION, version);

        System.arraycopy(idBuf, 0, added.id, 0, idLen);
        added.idLen = idLen;
        added.handle = entry;

        if (!storeDevices(n, idBuf, idLen, added)) {
            throw new UdfException("update_device: could not store the devices");
        }

        if (markerVersion(idBuf, idLen) >= 0 && !storeRemoved(idBuf, idLen, null, 0, 0, 0, -1)) {
            throw new UdfException("update_device: could not drop the marker");
        }

        if (commit() != Udf.OK) {
            throw new UdfException("update_device: could not commit");
        }

        return version;
    }

    /** Removes a device, leaving a marker with the application's time {@code now} (ms) and the next version. */
    @Function("remove_device")
    static long removeDevice(String id, long now) {
        int idLen = idBytes(id, "remove_device");
        int n = liveEntries();

        if (n < 0) {
            throw new UdfException("remove_device: malformed devices, or too many");
        }

        long version = lastVersion(n, idBuf, idLen) + 1;

        if (!storeDevices(n, idBuf, idLen, null)) {
            throw new UdfException("remove_device: could not store the devices");
        }

        if (!storeRemoved(null, 0, idBuf, idLen, now, version, now)) {
            throw new UdfException("remove_device: could not store the marker");
        }

        if (commit() != Udf.OK) {
            throw new UdfException("remove_device: could not commit");
        }

        return version;
    }

    /** Drops the markers older than the purge age before {@code now}, and stores the list sorted. */
    @Function("purge_devices")
    static long purgeDevices(long now) {
        int n = liveEntries();

        if (n < 0) {
            throw new UdfException("purge_devices: malformed devices, or too many");
        }

        if (n > 0 && !storeDevices(n, null, -1, null)) {
            throw new UdfException("purge_devices: could not store the devices");
        }

        if (!storeRemoved(null, 0, null, 0, 0, 0, now)) {
            throw new UdfException("purge_devices: could not store the markers");
        }

        if (commit() != Udf.OK) {
            throw new UdfException("purge_devices: could not commit");
        }

        return removedInt(PURGED_BEFORE);
    }

}

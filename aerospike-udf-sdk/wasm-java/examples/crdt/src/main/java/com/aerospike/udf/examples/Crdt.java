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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aerospike.udf.EventRecord;
import com.aerospike.udf.Function;
import com.aerospike.udf.ListBuilder;
import com.aerospike.udf.ListValue;
import com.aerospike.udf.MapBuilder;
import com.aerospike.udf.MapValue;
import com.aerospike.udf.On;
import com.aerospike.udf.Record;
import com.aerospike.udf.Resolution;
import com.aerospike.udf.Udf;
import com.aerospike.udf.UdfException;
import com.aerospike.udf.Value;

/**
 * An active-active CRDT in Java: the same module as aerospike-udf-sdk/wasm-rust/examples/crdt, rule for rule.
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
    /** The longest device id, in UTF-8 bytes. */
    private static final int ID_MAX = 64;

    /** What a malformed record, or one with too many devices, ends a helper with; the caller says where. */
    private static final class Malformed extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /** One side's candidate for a device: its live entry, or its removal marker. */
    private static final class Candidate {
        final String id;
        final long version;
        final boolean removed;
        /** The entry or the marker, where it sits in the record it came from. */
        final Value value;
        /** A marker's removal time; 0 for a live entry. */
        final long offAt;

        Candidate(String id, long version, boolean removed, Value value, long offAt) {
            this.id = id;
            this.version = version;
            this.removed = removed;
            this.value = value;
            this.offAt = offAt;
        }
    }

    /**
     * Orders two ids by their UTF-8 bytes - the order the server keeps a key-ordered map's string keys in,
     * so the list and the markers agree. That is code point order, which {@code String.compareTo} is not
     * once a character outside the basic plane meets one near its top: surrogates sort below U+E000 in
     * UTF-16 and above it in UTF-8, and this puts them where UTF-8 does.
     */
    private static int compareIds(String a, String b) {
        int n = Math.min(a.length(), b.length());

        for (int i = 0; i < n; i++) {
            char x = a.charAt(i);
            char y = b.charAt(i);

            if (x != y) {
                boolean xs = Character.isSurrogate(x);
                boolean ys = Character.isSurrogate(y);

                return xs == ys ? Character.compare(x, y) : xs ? 1 : -1;
            }
        }

        return Integer.compare(a.length(), b.length());
    }

    /** A device id read from a record: a string of at most {@link #ID_MAX} bytes, or the record is malformed. */
    private static String idOf(Value v) {
        if (v == null || !v.isString() || v.length() > ID_MAX) {
            throw new Malformed();
        }

        return v.asString();
    }

    /** An integer a record must hold, or the record is malformed. */
    private static long intOf(Value v) {
        if (v == null || !v.isInt()) {
            throw new Malformed();
        }

        return v.asInt();
    }

    /** An integer entry of a map, or {@code dflt} when it has none or holds something else there. */
    private static long intOr(MapValue m, String key, long dflt) {
        Value v = m == null ? null : m.get(key);

        return v != null && v.isInt() ? v.asInt() : dflt;
    }

    /** A map entry of a map, or null when it has none or holds something else there. */
    private static MapValue mapOr(MapValue m, String key) {
        Value v = m == null ? null : m.get(key);

        return v != null && v.isMap() ? v.asMap() : null;
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

        return a.value.cmpStored(b.value) >= 0 ? a : b;
    }

    /** The candidates seen so far, one per device: {@link #offer} keeps the higher. */
    private static final class Best {
        final List<Candidate> items = new ArrayList<>();
        /** The latest purge horizon either side carries, and the highest version floor. */
        long horizon;
        long floor;

        void offer(Candidate c) {
            for (int i = 0; i < items.size(); i++) {
                if (items.get(i).id.equals(c.id)) {
                    items.set(i, higher(items.get(i), c));
                    return;
                }
            }

            if (items.size() == MAX_DEVICES) {
                throw new Malformed();
            }

            items.add(c);
        }

        /** Every live entry of a devices list as a candidate. */
        void offerLive(ListValue devices) {
            for (Value device : devices) {
                if (!device.isMap()) {
                    throw new Malformed();
                }

                MapValue entry = device.asMap();

                offer(new Candidate(idOf(entry.get("deviceId")), intOf(entry.get("version")), false, device, 0));
            }
        }

        /** Every marker of a devicesRemoved bin as a candidate; the horizon and the floor it carries. */
        void offerMarkers(MapValue removed) {
            horizon = Math.max(horizon, intOr(removed, "purgedBefore", 0));
            floor = Math.max(floor, intOr(removed, "versionFloor", 0));

            MapValue markers = mapOr(removed, "markers");

            if (markers == null) {
                return;
            }

            for (MapValue.Entry e : markers) {
                if (!e.getValue().isMap()) {
                    throw new Malformed();
                }

                MapValue marker = e.getValue().asMap();

                offer(new Candidate(idOf(e.getKey()), intOf(marker.get("version")), true, e.getValue(),
                        intOf(marker.get("offAt"))));
            }
        }

        void sort() {
            Collections.sort(items, (a, b) -> compareIds(a.id, b.id));
        }
    }

    /** A list or map bin of an event's record, or null when the record has none of that kind there. */
    private static Value binOfKind(EventRecord rec, String name, int kind) {
        Value v = rec.get(name);

        return v != null && v.kind() == kind ? v : null;
    }

    /**
     * The merge: every device from both sides goes in as a candidate, live entry or marker, the higher wins
     * per device, and the result is the sorted live list plus the markers still inside the purge horizon.
     * Every other bin is the incoming one's, as XDR would have applied it.
     */
    @On.XdrMerge("xdr_merge")
    static Resolution xdrMerge(EventRecord current, EventRecord incoming) {
        MapBuilder out = Udf.map(8);

        // Other bins: as XDR does today.
        for (Value name : incoming.binNames()) {
            String bin = name.asString();

            if (!bin.equals("devices") && !bin.equals("devicesRemoved")) {
                Value val = incoming.get(bin);

                if (val != null) {
                    out.put(bin, val);
                }
            }
        }

        if (binOfKind(incoming, "devices", Value.LIST) != null
                || binOfKind(incoming, "devicesRemoved", Value.MAP) != null) {
            Best best = new Best();
            boolean anyDevices = false;

            for (EventRecord rec : new EventRecord[] { current, incoming }) {
                Value devices = binOfKind(rec, "devices", Value.LIST);
                Value removed = binOfKind(rec, "devicesRemoved", Value.MAP);

                if (devices != null) {
                    anyDevices = true;

                    try {
                        best.offerLive(devices.asList());
                    }
                    catch (Malformed e) {
                        throw new UdfException("xdr_merge: malformed devices, or too many");
                    }
                }

                if (removed != null) {
                    try {
                        best.offerMarkers(removed.asMap());
                    }
                    catch (Malformed e) {
                        throw new UdfException("xdr_merge: malformed devicesRemoved, or too many");
                    }
                }
            }

            best.sort();

            ListBuilder live = Udf.list(best.items.size());
            MapBuilder markers = Udf.map(best.items.size());
            long floor = best.floor;

            for (Candidate cand : best.items) {
                if (!cand.removed) {
                    live.add(cand.value);
                }
                else if (cand.offAt >= best.horizon) {
                    markers.put(cand.id, cand.value);
                }
                else {
                    // Dropped by the other side's horizon before this side purged it: its version still
                    // has to be the floor, or a re-add here could start below a copy of the marker that
                    // is still on its way.
                    floor = Math.max(floor, cand.version);
                }
            }

            if (anyDevices) {
                out.put("devices", live);
            }

            if (!markers.isEmpty() || best.horizon > 0) {
                MapBuilder removed = Udf.map(3).put("markers", markers).put("purgedBefore", best.horizon);

                if (floor > 0) {
                    removed.put("versionFloor", floor);
                }

                out.put("devicesRemoved", removed);
            }
            else {
                // The merge has no removals left: a nil deletes the bin, so a stale copy cannot come back.
                out.putNil("devicesRemoved");
            }
        }

        // Every bin of the current record not decided above, by position.
        return Resolution.record(current.withBins(out));
    }

    //==========================================================
    // The write UDFs: every application write to the devices goes through these, so each change moves one
    // device strictly up the order the merge uses.
    //

    /** A live entry as the write UDFs hold it: its id, and the stored entry or the map built to replace it. */
    private static final class Entry {
        final String id;
        final Value value;

        Entry(String id, Value value) {
            this.id = id;
            this.value = value;
        }
    }

    /** The record's live entries, in stored order. */
    private static List<Entry> liveEntries(Record rec, String fn) {
        List<Entry> entries = new ArrayList<>();
        Value devices = rec.get("devices");

        if (devices == null || !devices.isList()) {
            return entries;
        }

        try {
            for (Value device : devices.asList()) {
                if (entries.size() == MAX_DEVICES || !device.isMap()) {
                    throw new Malformed();
                }

                entries.add(new Entry(idOf(device.asMap().get("deviceId")), device));
            }
        }
        catch (Malformed e) {
            throw new UdfException(fn + ": malformed devices, or too many");
        }

        return entries;
    }

    /** The record's devicesRemoved map, or null when it has none. */
    private static MapValue removedOf(Record rec) {
        Value removed = rec.get("devicesRemoved");

        return removed != null && removed.isMap() ? removed.asMap() : null;
    }

    /** A marker's version, or -1 when the device has none. */
    private static long markerVersion(Record rec, String id) {
        return intOr(mapOr(mapOr(removedOf(rec), "markers"), id), "version", -1);
    }

    /** The device's latest version, live or removed, and never below the version floor. */
    private static long lastVersion(Record rec, List<Entry> entries, String id) {
        long v = intOr(removedOf(rec), "versionFloor", 0);

        for (Entry e : entries) {
            if (e.id.equals(id)) {
                v = Math.max(v, intOr(e.value.asMap(), "version", v));
            }
        }

        return Math.max(v, markerVersion(rec, id));
    }

    /** Stores the entries plus an optional new one, sorted by id, less any entry under {@code without}. */
    private static void storeDevices(Record rec, List<Entry> entries, String without, Entry add) {
        List<Entry> sorted = new ArrayList<>();

        for (Entry e : entries) {
            if (!e.id.equals(without)) {
                sorted.add(e);
            }
        }

        if (add != null) {
            sorted.add(add);
        }

        Collections.sort(sorted, (a, b) -> compareIds(a.id, b.id));

        ListBuilder list = Udf.list(sorted.size());

        for (Entry e : sorted) {
            list.add(e.value);
        }

        rec.set("devices", list);
    }

    /**
     * Rebuilds devicesRemoved from the current one with one marker removed ({@code drop}), one added or
     * replaced ({@code addId} with {@code addOffAt}, {@code addVersion}), and a purge at {@code now} (when
     * {@code now} is not negative). The bin is left out while it holds nothing. Returns the purge horizon.
     */
    private static long storeRemoved(Record rec, String fn, String drop, String addId, long addOffAt,
            long addVersion, long now) {
        MapValue removed = removedOf(rec);
        long horizon = intOr(removed, "purgedBefore", 0);
        long floor = intOr(removed, "versionFloor", 0);

        if (now >= 0) {
            horizon = Math.max(horizon, now - PURGE_AGE_MS);
        }

        MapBuilder markers = Udf.map(8);
        MapValue existing = mapOr(removed, "markers");

        if (existing != null) {
            try {
                for (MapValue.Entry e : existing) {
                    Value key = e.getKey();

                    if (!key.isString() || key.length() > ID_MAX || !e.getValue().isMap()) {
                        throw new Malformed();
                    }

                    String id = key.asString();
                    MapValue marker = e.getValue().asMap();

                    if (id.equals(drop) || id.equals(addId)) {
                        continue;
                    }

                    if (intOf(marker.get("offAt")) < horizon) {
                        // Purged: its version becomes the floor every later version starts above.
                        floor = Math.max(floor, intOr(marker, "version", floor));
                        continue;
                    }

                    markers.put(id, e.getValue());
                }
            }
            catch (Malformed e) {
                throw new UdfException(fn + ": malformed devicesRemoved");
            }
        }

        if (addId != null) {
            markers.put(addId, Udf.map(2).put("offAt", addOffAt).put("version", addVersion));
        }

        if (markers.isEmpty() && horizon == 0) {
            if (rec.has("devicesRemoved")) {
                rec.remove("devicesRemoved");
            }

            return horizon;
        }

        MapBuilder out = Udf.map(3).put("markers", markers).put("purgedBefore", horizon);

        if (floor > 0) {
            out.put("versionFloor", floor);
        }

        rec.set("devicesRemoved", out);

        return horizon;
    }

    /** The Rust module bounds an id with a Str<64>; the same limit, as a check. */
    private static void checkId(String id, String fn) {
        if (id.getBytes(StandardCharsets.UTF_8).length > ID_MAX) {
            throw new UdfException(fn + ": the id is too long");
        }
    }

    /**
     * Adds a device, updates one, or adds a removed one back: the entry becomes the old one (if any) with
     * {@code changes} on top and a version one above the device's last, live or removed. Returns the version.
     */
    @Function("update_device")
    static long updateDevice(Record rec, String id, Value changes) {
        if (!changes.isMap()) {
            throw new UdfException("update_device: changes is not a map");
        }

        checkId(id, "update_device");

        List<Entry> entries = liveEntries(rec, "update_device");
        long version = lastVersion(rec, entries, id) + 1;
        MapBuilder entry = Udf.map(8);

        for (Entry e : entries) {
            if (e.id.equals(id)) {
                entry.putAll(e.value.asMap());
            }
        }

        entry.putAll(changes.asMap()).put("deviceId", id).put("version", version);

        storeDevices(rec, entries, id, new Entry(id, entry.asValue()));

        if (markerVersion(rec, id) >= 0) {
            storeRemoved(rec, "update_device", id, null, 0, 0, -1);
        }

        rec.save();

        return version;
    }

    /** Removes a device, leaving a marker with the application's time {@code now} (ms) and the next version. */
    @Function("remove_device")
    static long removeDevice(Record rec, String id, long now) {
        checkId(id, "remove_device");

        List<Entry> entries = liveEntries(rec, "remove_device");
        long version = lastVersion(rec, entries, id) + 1;

        storeDevices(rec, entries, id, null);
        storeRemoved(rec, "remove_device", null, id, now, version, now);
        rec.save();

        return version;
    }

    /** Drops the markers older than the purge age before {@code now}, and stores the list sorted. */
    @Function("purge_devices")
    static long purgeDevices(Record rec, long now) {
        List<Entry> entries = liveEntries(rec, "purge_devices");

        if (!entries.isEmpty()) {
            storeDevices(rec, entries, null, null);
        }

        long horizon = storeRemoved(rec, "purge_devices", null, null, 0, 0, now);

        rec.save();

        return horizon;
    }
}

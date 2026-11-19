/*
 * Copyright 2015-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// An active-active CRDT in C++: the same module as aerospike-udf-sdk-wasm/examples/crdt, rule for rule. A
// last-writer-wins map of devices inside one record, merged across data centres by the xdr-merge event,
// with the write UDFs that keep it well-formed.
//
// The record keeps a subscriber's devices in one bin, devices, a list of maps each with a deviceId and a
// version the application raises on every change. A second bin, devicesRemoved, holds a removal marker
// per removed device ({offAt, version} under markers), the purge horizon purgedBefore, and, once a purge
// has dropped a marker, the version floor versionFloor. Both data centres take writes; when a write from
// one arrives at the other, xdr_merge decides each device from values in the data alone - the version,
// whether it is a removal, and finally the stored bytes - so both sides compute the same record without a
// clock, a LUT or a writer id, in any delivery order.
//
//   event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
//
// Every application write goes through update_device, remove_device and purge_devices, which keep the
// list sorted by deviceId and every version above the last one seen, live or removed.

#include "aerospike_udf.hpp"

// How long a removal marker outlives its removal, in ms. Longer than the longest outage between the
// clusters plus the replication lag, or a purged device can come back on one side.
static constexpr int64_t PURGE_AGE_MS = 7LL * 24 * 60 * 60 * 1000;
static constexpr int MAX_DEVICES = 128;

static constexpr udf::StrView DEVICES{"devices"};
static constexpr udf::StrView DEVICES_REMOVED{"devicesRemoved"};

using Id = udf::Text<64>;

// One side's candidate for a device: its live entry, or its removal marker.
struct Candidate {
	Id id;
	int64_t version = 0;
	bool removed = false;
	// The entry or the marker, where it sits in the record it came from.
	udf::Value value;
	// A marker's removal time; 0 for a live entry.
	int64_t off_at = 0;
};

// higher() is the one of two candidates for a device that wins. The first rule that tells them apart
// decides, and every rule reads the data alone.
static const Candidate& higher(const Candidate& a, const Candidate& b)
{
	if (a.version != b.version) return a.version > b.version ? a : b;
	if (a.removed != b.removed) return a.removed ? a : b;

	// Two different changes at one version are concurrent: the stored bytes pick one, the same on both
	// clusters, since each side compares the same two byte strings.
	// Both values are entries read from a record, so they are stored and the comparison cannot fail; were it
	// to, the first candidate would stand.
	udf::Result<int32_t> order = a.value.cmp_stored(b.value);
	return !order || *order >= 0 ? a : b;
}

// The candidates seen so far, one per device: offer keeps the higher. Static, since a module has no
// allocator and 128 candidates are more than a stack frame should hold.
struct Best {
	Candidate items[MAX_DEVICES];
	int n = 0;

	udf::Result<void> offer(const Candidate& c)
	{
		for (int i = 0; i < n; i++) {
			if (items[i].id == c.id.view()) {
				items[i] = higher(items[i], c);
				return {};
			}
		}

		if (n == MAX_DEVICES) return udf::error(AS_UDF_ERR, "too many devices");

		items[n++] = c;

		return {};
	}

	// In deviceId order, by UTF-8 bytes - the order the server keeps a key-ordered map's string keys in,
	// so the list and the markers agree.
	void sort()
	{
		for (int i = 1; i < n; i++) {
			for (int j = i; j > 0 && items[j].id < items[j - 1].id; j--) {
				Candidate tmp = items[j];
				items[j] = items[j - 1];
				items[j - 1] = tmp;
			}
		}
	}
};

static Best best;

// A device's id, from its entry.
static udf::Result<Id> id_of(const udf::Value& entry)
{
	udf::Optional<udf::Value> id = entry.get("deviceId");

	if (!id) return udf::error(AS_UDF_NOT_FOUND, "an entry has no deviceId");

	return udf::required(id->as_string<64>(), "an entry's deviceId is nil");
}

static udf::Result<int64_t> int_field(const udf::Value& m, udf::StrView key)
{
	udf::Optional<udf::Value> v = m.get(key);

	if (!v) return udf::error(AS_UDF_NOT_FOUND, "a map lacks a field");

	return udf::required(v->as_int(), "a map field is nil");
}

// Every live entry of a devices list as a candidate.
static udf::Result<void> offer_live(const udf::Value& list)
{
	for (udf::Value item : list.list()) {
		Candidate c;

		c.id = UDF_TRY(id_of(item));
		c.version = UDF_TRY(int_field(item, "version"));
		c.value = item;

		UDF_TRY(best.offer(c));
	}

	return {};
}

// Every marker of a devicesRemoved bin as a candidate; the horizon and the floor it carries.
static udf::Result<void> offer_markers(const udf::Value& removed, int64_t& horizon, int64_t& floor)
{
	if (udf::Optional<udf::Value> h = removed.get("purgedBefore")) {
		int64_t n = UDF_TRY(udf::required(h->as_int(), "purgedBefore is nil"));
		if (n > horizon) horizon = n;
	}

	if (udf::Optional<udf::Value> f = removed.get("versionFloor")) {
		int64_t n = UDF_TRY(udf::required(f->as_int(), "versionFloor is nil"));
		if (n > floor) floor = n;
	}

	udf::Optional<udf::Value> markers = removed.get("markers");

	if (!markers) return {};

	for (udf::Entry e : markers->map()) {
		Candidate c;

		c.id = UDF_TRY(udf::required(e.key.as_string<64>(), "a marker's key is nil"));
		c.version = UDF_TRY(int_field(e.value, "version"));
		c.removed = true;
		c.value = e.value;
		c.off_at = UDF_TRY(int_field(e.value, "offAt"));

		UDF_TRY(best.offer(c));
	}

	return {};
}

// The merge: every device from both sides goes in as a candidate, live entry or marker, the higher wins
// per device, and the result is the sorted live list plus the markers still inside the purge horizon.
// Every other bin is the incoming one's, as XDR would have applied it.
UDF_ON_XDR_MERGE(xdr_merge)
{
	udf::Map out(8);

	// Other bins: as XDR does today.
	for (udf::Value name : incoming.bin_names()) {
		udf::Text<64> n = UDF_TRY(udf::required(name.as_string<64>(), "a bin name is nil"));

		if (n == "devices" || n == "devicesRemoved") continue;

		udf::Value v;

		if (as_udf_event_bin_value(incoming.which, n.buf, n.len, &v, (int32_t)sizeof(v)) == AS_UDF_OK) {
			out.put(n.view(), v);
		}
	}

	bool touched = incoming.get("devices").has_value() || incoming.get("devicesRemoved").has_value();

	if (touched) {
		best.n = 0;
		int64_t horizon = 0;
		int64_t floor = 0;

		const udf::EventRecord* sides[] = {&current, &incoming};

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> list = side->get("devices"); list && list->is_list()) {
				UDF_TRY(offer_live(*list));
			}
			if (udf::Optional<udf::Value> removed = side->get("devicesRemoved"); removed && removed->is_map()) {
				UDF_TRY(offer_markers(*removed, horizon, floor));
			}
		}

		best.sort();

		udf::List live(best.n > 0 ? best.n : 1);
		udf::Map markers(best.n > 0 ? best.n : 1);
		int n_markers = 0;

		for (int i = 0; i < best.n; i++) {
			const Candidate& c = best.items[i];

			if (!c.removed) {
				live.add(c.value);
			}
			else if (c.off_at >= horizon) {
				markers.put(c.id.view(), c.value);
				n_markers++;
			}
			else if (c.version > floor) {
				// Dropped by the other side's horizon before this side purged it: its version still has to
				// be the floor, or a re-add here could start below a copy of the marker that is still on
				// its way.
				floor = c.version;
			}
		}

		if (current.get("devices").has_value() || incoming.get("devices").has_value()) {
			out.put("devices", live);
		}

		if (n_markers > 0 || horizon > 0) {
			udf::Map removed(3);

			removed.put("markers", markers).put("purgedBefore", horizon);

			if (floor > 0) removed.put("versionFloor", floor);

			out.put("devicesRemoved", removed);
		}
		else {
			// The merge has no removals left: the bin stays out, so with_bins below cannot bring a stale
			// copy back.
			out.put_nil("devicesRemoved");
		}
	}

	return udf::Resolution::record(current.with_bins(out));
}

//==========================================================
// The write UDFs: every application write to the devices goes through these, so each change moves one
// device strictly up the order the merge uses.
//

struct Entry {
	Id id;
	udf::Value value;
};

static Entry entries[MAX_DEVICES];
static Entry all[MAX_DEVICES + 1];

// The live entries, in stored order; how many.
static udf::Result<int> live_entries(const udf::Record& rec)
{
	int n = 0;

	for (udf::Value item : UDF_TRY(rec.list(DEVICES)).value_or(udf::ListRange())) {
		if (n == MAX_DEVICES) return udf::error(AS_UDF_ERR, "too many devices");

		entries[n].id = UDF_TRY(id_of(item));
		entries[n].value = item;
		n++;
	}

	return n;
}

// A marker's version, if the device has one.
static udf::Optional<int64_t> marker_version(const udf::Record& rec, udf::StrView id)
{
	udf::Optional<udf::Value> removed = rec.get(DEVICES_REMOVED);
	if (!removed) return {};
	udf::Optional<udf::Value> markers = removed->get("markers");
	if (!markers) return {};
	udf::Optional<udf::Value> marker = markers->get(id);
	if (!marker) return {};
	udf::Result<int64_t> v = int_field(*marker, "version");
	if (!v) return {};
	return *v;
}

static int64_t removed_int(const udf::Record& rec, udf::StrView key)
{
	udf::Optional<udf::Value> removed = rec.get(DEVICES_REMOVED);
	if (!removed) return 0;
	udf::Optional<udf::Value> v = removed->get(key);
	if (!v || !v->is_int()) return 0;
	return v->payload;
}

// The device's latest version, live or removed, and never below the version floor.
static udf::Result<int64_t> last_version(const udf::Record& rec, int n, udf::StrView id)
{
	int64_t v = removed_int(rec, "versionFloor");

	for (int i = 0; i < n; i++) {
		if (entries[i].id == id) {
			int64_t version = UDF_TRY(int_field(entries[i].value, "version"));
			if (version > v) v = version;
		}
	}

	if (udf::Optional<int64_t> mv = marker_version(rec, id); mv && *mv > v) v = *mv;

	return v;
}

// Stores the entries plus an optional built entry under id, sorted by id, less any entry under without.
static udf::Result<void> store_devices(udf::Record& rec, int n, udf::StrView without, const Entry* add)
{
	int count = 0;

	for (int i = 0; i < n; i++) {
		if (entries[i].id != without) all[count++] = entries[i];
	}

	if (add != nullptr) all[count++] = *add;

	for (int i = 1; i < count; i++) {
		for (int j = i; j > 0 && all[j].id < all[j - 1].id; j--) {
			Entry tmp = all[j];
			all[j] = all[j - 1];
			all[j - 1] = tmp;
		}
	}

	udf::List list(count > 0 ? count : 1);

	for (int i = 0; i < count; i++) list.add(all[i].value);

	return rec.set(DEVICES, list);
}

// A marker to add: its device, removal time and version.
struct Marker {
	udf::StrView id;
	int64_t off_at;
	int64_t version;
};

// Rebuilds devicesRemoved from the current one with one marker dropped (drop non-empty), one added or
// replaced, and a purge at now (when given). The bin is left out while it holds nothing.
static udf::Result<void> store_removed(udf::Record& rec, udf::StrView drop, const Marker* add, const int64_t* now)
{
	int64_t horizon = removed_int(rec, "purgedBefore");
	int64_t floor = removed_int(rec, "versionFloor");

	if (now != nullptr && *now - PURGE_AGE_MS > horizon) horizon = *now - PURGE_AGE_MS;

	udf::Map markers(8);
	int n = 0;

	if (udf::Optional<udf::Value> removed = rec.get(DEVICES_REMOVED)) {
		if (udf::Optional<udf::Value> existing = removed->get("markers")) {
			for (udf::Entry e : existing->map()) {
				Id id = UDF_TRY(udf::required(e.key.as_string<64>(), "a marker's key is nil"));

				if ((drop.len > 0 && id == drop) || (add != nullptr && id == add->id)) continue;

				int64_t off_at = UDF_TRY(int_field(e.value, "offAt"));

				if (off_at < horizon) {
					// Purged: its version becomes the floor every later version starts above.
					int64_t version = UDF_TRY(int_field(e.value, "version"));
					if (version > floor) floor = version;
					continue;
				}

				markers.put(id.view(), e.value);
				n++;
			}
		}
	}

	if (add != nullptr) {
		udf::Map marker(2);

		marker.put("offAt", add->off_at).put("version", add->version);
		markers.put(add->id, marker);
		n++;
	}

	if (n == 0 && horizon == 0) {
		if (rec.has(DEVICES_REMOVED)) return rec.remove(DEVICES_REMOVED);
		return {};
	}

	udf::Map removed(3);

	removed.put("markers", markers).put("purgedBefore", horizon);

	if (floor > 0) removed.put("versionFloor", floor);

	return rec.set(DEVICES_REMOVED, removed);
}

// Adds a device, updates one, or adds a removed one back: the entry becomes the old one (if any) with
// changes on top and a version one above the device's last, live or removed. Returns the version.
UDF_FUNCTION(update_device, udf::Result<int64_t>, udf::Record& rec, Id id, udf::Value changes)
{
	if (!changes.is_map()) return udf::error(AS_UDF_TYPE_MISMATCH, "update_device: changes is not a map");

	int n = UDF_TRY(live_entries(rec));
	int64_t version = UDF_TRY(last_version(rec, n, id.view())) + 1;
	udf::Map e(8);

	for (int i = 0; i < n; i++) {
		if (entries[i].id == id.view()) e.put_all(entries[i].value);
	}

	e.put_all(changes);
	e.put("deviceId", id.view()).put("version", version);
	UDF_TRY(e.result());

	Entry added;

	added.id = id;
	added.value = e.value();

	UDF_TRY(store_devices(rec, n, id.view(), &added));

	if (marker_version(rec, id.view())) {
		UDF_TRY(store_removed(rec, id.view(), nullptr, nullptr));
	}

	UDF_TRY(rec.save());

	return version;
}

// Removes a device, leaving a marker with the application's time now (ms) and the next version.
UDF_FUNCTION(remove_device, udf::Result<int64_t>, udf::Record& rec, Id id, int64_t now)
{
	int n = UDF_TRY(live_entries(rec));
	int64_t version = UDF_TRY(last_version(rec, n, id.view())) + 1;

	UDF_TRY(store_devices(rec, n, id.view(), nullptr));

	Marker marker{id.view(), now, version};

	UDF_TRY(store_removed(rec, "", &marker, &now));
	UDF_TRY(rec.save());

	return version;
}

// Drops the markers older than the purge age before now, and stores the list sorted.
UDF_FUNCTION(purge_devices, udf::Result<int64_t>, udf::Record& rec, int64_t now)
{
	int n = UDF_TRY(live_entries(rec));

	if (n > 0) {
		UDF_TRY(store_devices(rec, n, "", nullptr));
	}

	UDF_TRY(store_removed(rec, "", nullptr, &now));
	UDF_TRY(rec.save());

	return removed_int(rec, "purgedBefore");
}

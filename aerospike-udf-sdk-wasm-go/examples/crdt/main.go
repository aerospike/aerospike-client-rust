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

// An active-active CRDT in Go: the same module as aerospike-udf-sdk-wasm/examples/crdt, rule for rule. A
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
//	event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
//
// Every application write goes through update_device, remove_device and purge_devices, which keep the
// list sorted by deviceId and every version above the last one seen, live or removed.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"errors"
	"sort"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

// How long a removal marker outlives its removal, in ms. Longer than the longest outage between the
// clusters plus the replication lag, or a purged device can come back on one side.
const purgeAgeMs = 7 * 24 * 60 * 60 * 1000

// One side's candidate for a device: its live entry, or its removal marker.
type candidate struct {
	id      string
	version int64
	removed bool
	// The entry or the marker, where it sits in the record it came from.
	value udf.Value
	// A marker's removal time; 0 for a live entry.
	offAt int64
}

// higher is the one of two candidates for a device that wins. The first rule that tells them apart
// decides, and every rule reads the data alone.
func higher(a, b candidate) (candidate, error) {
	if a.version != b.version {
		if a.version > b.version {
			return a, nil
		}
		return b, nil
	}

	if a.removed != b.removed {
		if a.removed {
			return a, nil
		}
		return b, nil
	}

	// Two different changes at one version are concurrent: the stored bytes pick one, the same on both
	// clusters, since each side compares the same two byte strings.
	order, err := udf.CmpStored(a.value, b.value)
	if err != nil {
		return candidate{}, err
	}
	if order >= 0 {
		return a, nil
	}
	return b, nil
}

// best is the candidates seen so far, one per device; offer keeps the higher.
type best map[string]candidate

func (b best) offer(c candidate) error {
	have, ok := b[c.id]
	if !ok {
		b[c.id] = c
		return nil
	}

	winner, err := higher(have, c)
	if err != nil {
		return err
	}
	b[c.id] = winner
	return nil
}

// sorted is the candidates in deviceId order, by UTF-8 bytes - the order the server keeps a key-ordered
// map's string keys in, so the list and the markers agree.
func (b best) sorted() []candidate {
	out := make([]candidate, 0, len(b))
	for _, c := range b {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// idOf is a device's id, from its entry.
func idOf(entry udf.Value) (string, error) {
	id, ok, err := entry.Lookup("deviceId")
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("an entry has no deviceId")
	}
	return id.String()
}

func intField(m udf.Value, key string) (int64, error) {
	v, ok, err := m.Lookup(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("a map has no " + key)
	}
	return v.Int()
}

// offerLive offers every live entry of a devices list.
func offerLive(b best, list udf.Value) error {
	for item := range list.List() {
		id, err := idOf(item)
		if err != nil {
			return err
		}
		version, err := intField(item, "version")
		if err != nil {
			return err
		}
		if err := b.offer(candidate{id: id, version: version, value: item}); err != nil {
			return err
		}
	}
	return nil
}

// offerMarkers offers every marker of a devicesRemoved bin, and raises the horizon and the floor to what
// it carries.
func offerMarkers(b best, removed udf.Value, horizon, floor *int64) error {
	if h, ok, err := removed.Lookup("purgedBefore"); err != nil {
		return err
	} else if ok {
		n, err := h.Int()
		if err != nil {
			return err
		}
		*horizon = max(*horizon, n)
	}

	if f, ok, err := removed.Lookup("versionFloor"); err != nil {
		return err
	} else if ok {
		n, err := f.Int()
		if err != nil {
			return err
		}
		*floor = max(*floor, n)
	}

	markers, ok, err := removed.Lookup("markers")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	for key, marker := range markers.Map() {
		id, err := key.String()
		if err != nil {
			return err
		}
		version, err := intField(marker, "version")
		if err != nil {
			return err
		}
		offAt, err := intField(marker, "offAt")
		if err != nil {
			return err
		}
		if err := b.offer(candidate{id: id, version: version, removed: true, value: marker, offAt: offAt}); err != nil {
			return err
		}
	}
	return nil
}

// The merge: every device from both sides goes in as a candidate, live entry or marker, the higher wins
// per device, and the result is the sorted live list plus the markers still inside the purge horizon.
// Every other bin is the incoming one's, as XDR would have applied it.
//
//udf:on xdr-merge xdr_merge
func xdrMerge(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(8)

	// Other bins: as XDR does today.
	for name := range incoming.BinNames() {
		if name == "devices" || name == "devicesRemoved" {
			continue
		}
		if v, err := incoming.Bin(name); err == nil {
			out.Put(name, v)
		}
	}

	_, curHas, err := incoming.Get("devices")
	if err != nil {
		return udf.Resolution{}, err
	}
	_, incHasRemoved, err := incoming.Get("devicesRemoved")
	if err != nil {
		return udf.Resolution{}, err
	}

	if curHas || incHasRemoved {
		b := best{}
		var horizon, floor int64

		for _, side := range []*udf.EventRecord{current, incoming} {
			list, ok, err := side.Get("devices")
			if err != nil {
				return udf.Resolution{}, err
			}
			if ok && list.IsList() {
				if err := offerLive(b, list); err != nil {
					return udf.Resolution{}, err
				}
			}

			removed, ok, err := side.Get("devicesRemoved")
			if err != nil {
				return udf.Resolution{}, err
			}
			if ok && removed.IsMap() {
				if err := offerMarkers(b, removed, &horizon, &floor); err != nil {
					return udf.Resolution{}, err
				}
			}
		}

		live := udf.NewList(len(b))
		markers := udf.NewMap(len(b))
		nMarkers := 0

		for _, c := range b.sorted() {
			switch {
			case !c.removed:
				live.Add(c.value)
			case c.offAt >= horizon:
				markers.Put(c.id, c.value)
				nMarkers++
			default:
				// Dropped by the other side's horizon before this side purged it: its version still has to
				// be the floor, or a re-add here could start below a copy of the marker that is still on
				// its way.
				floor = max(floor, c.version)
			}
		}

		_, curList, err := current.Get("devices")
		if err != nil {
			return udf.Resolution{}, err
		}
		if curList || curHas {
			out.Put("devices", live)
		}

		if nMarkers > 0 || horizon > 0 {
			removed := udf.NewMap(3).Put("markers", markers).Put("purgedBefore", horizon)
			if floor > 0 {
				removed.Put("versionFloor", floor)
			}
			out.Put("devicesRemoved", removed)
		} else {
			// The merge has no removals left: the bin stays out, so WithBins below cannot bring a stale
			// copy back.
			out.Put("devicesRemoved", nil)
		}
	}

	return udf.Resolve(current.WithBins(out)), out.Err()
}

//==========================================================
// The write UDFs: every application write to the devices goes through these, so each change moves one
// device strictly up the order the merge uses.
//

// An entry of the live list, with its id.
type entry struct {
	id    string
	value udf.Value
}

// liveEntries is the live entries in stored order.
func liveEntries(rec *udf.Record) ([]entry, error) {
	devices, _, err := rec.List("devices")
	if err != nil {
		return nil, err
	}

	var out []entry
	for item := range devices.List() {
		id, err := idOf(item)
		if err != nil {
			return nil, err
		}
		out = append(out, entry{id: id, value: item})
	}
	return out, nil
}

// removedBin is the devicesRemoved bin; a Value with nothing in it when the record has none, which
// every lookup below answers "not there" for.
func removedBin(rec *udf.Record) (udf.Value, error) {
	removed, _, err := rec.Map("devicesRemoved")
	return removed, err
}

// markerVersion is a marker's version, if the device has one.
func markerVersion(rec *udf.Record, id string) (int64, bool, error) {
	removed, err := removedBin(rec)
	if err != nil {
		return 0, false, err
	}
	markers, _, err := removed.Lookup("markers")
	if err != nil {
		return 0, false, err
	}
	marker, ok, err := markers.Lookup(id)
	if err != nil || !ok {
		return 0, false, err
	}
	v, err := intField(marker, "version")
	return v, err == nil, err
}

// removedInt is an integer field of devicesRemoved, 0 when there is none.
func removedInt(rec *udf.Record, key string) (int64, error) {
	removed, err := removedBin(rec)
	if err != nil {
		return 0, err
	}
	v, ok, err := removed.Lookup(key)
	if err != nil || !ok {
		return 0, err
	}
	return v.Int()
}

// lastVersion is the device's latest version, live or removed, and never below the version floor.
func lastVersion(rec *udf.Record, entries []entry, id string) (int64, error) {
	v, err := removedInt(rec, "versionFloor")
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.id == id {
			n, err := intField(e.value, "version")
			if err != nil {
				return 0, err
			}
			v = max(v, n)
		}
	}
	mv, ok, err := markerVersion(rec, id)
	if err != nil {
		return 0, err
	}
	if ok {
		v = max(v, mv)
	}
	return v, nil
}

// storeDevices stores entries plus an optional built entry under id, sorted by id, less any entry under
// without.
func storeDevices(rec *udf.Record, entries []entry, without string, add *entry) error {
	all := make([]entry, 0, len(entries)+1)
	for _, e := range entries {
		if e.id != without {
			all = append(all, e)
		}
	}
	if add != nil {
		all = append(all, *add)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].id < all[j].id })

	list := udf.NewList(len(all))
	for _, e := range all {
		list.Add(e.value)
	}
	return rec.Set("devices", list)
}

// A marker to add: its device, removal time and version.
type marker struct {
	id      string
	offAt   int64
	version int64
}

// storeRemoved rebuilds devicesRemoved from the current one with one marker dropped (drop != ""), one
// added or replaced, and a purge at now (when now != nil). The bin is left out while it holds nothing.
func storeRemoved(rec *udf.Record, drop string, add *marker, now *int64) error {
	horizon, err := removedInt(rec, "purgedBefore")
	if err != nil {
		return err
	}
	floor, err := removedInt(rec, "versionFloor")
	if err != nil {
		return err
	}
	if now != nil {
		horizon = max(horizon, *now-purgeAgeMs)
	}

	markers := udf.NewMap(8)
	n := 0

	removed, err := removedBin(rec)
	if err != nil {
		return err
	}
	existing, _, err := removed.Lookup("markers")
	if err != nil {
		return err
	}

	for key, m := range existing.Map() {
		id, err := key.String()
		if err != nil {
			return err
		}
		if (drop != "" && id == drop) || (add != nil && add.id == id) {
			continue
		}
		offAt, err := intField(m, "offAt")
		if err != nil {
			return err
		}
		if offAt < horizon {
			// Purged: its version becomes the floor every later version starts above.
			version, err := intField(m, "version")
			if err != nil {
				return err
			}
			floor = max(floor, version)
			continue
		}
		markers.Put(id, m)
		n++
	}

	if add != nil {
		markers.Put(add.id, udf.NewMap(2).Put("offAt", add.offAt).Put("version", add.version))
		n++
	}

	if n == 0 && horizon == 0 {
		if rec.Has("devicesRemoved") {
			return rec.Remove("devicesRemoved")
		}
		return nil
	}

	rebuilt := udf.NewMap(3).Put("markers", markers).Put("purgedBefore", horizon)
	if floor > 0 {
		rebuilt.Put("versionFloor", floor)
	}
	return rec.Set("devicesRemoved", rebuilt)
}

// Adds a device, updates one, or adds a removed one back: the entry becomes the old one (if any) with
// changes on top and a version one above the device's last, live or removed. Returns the version.
//
//udf:function update_device
func updateDevice(rec *udf.Record, id string, changes udf.Value) (int64, error) {
	if !changes.IsMap() {
		return 0, errors.New("update_device: changes is not a map")
	}

	entries, err := liveEntries(rec)
	if err != nil {
		return 0, err
	}
	version, err := lastVersion(rec, entries, id)
	if err != nil {
		return 0, err
	}
	version++

	e := udf.NewMap(8)
	for _, old := range entries {
		if old.id == id {
			e.PutAll(old.value)
		}
	}
	e.PutAll(changes)
	e.Put("deviceId", id).Put("version", version)
	if err := e.Err(); err != nil {
		return 0, err
	}

	if err := storeDevices(rec, entries, id, &entry{id: id, value: e.Value()}); err != nil {
		return 0, err
	}
	_, has, err := markerVersion(rec, id)
	if err != nil {
		return 0, err
	}
	if has {
		if err := storeRemoved(rec, id, nil, nil); err != nil {
			return 0, err
		}
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return version, nil
}

// Removes a device, leaving a marker with the application's time now (ms) and the next version.
//
//udf:function remove_device
func removeDevice(rec *udf.Record, id string, now int64) (int64, error) {
	entries, err := liveEntries(rec)
	if err != nil {
		return 0, err
	}
	version, err := lastVersion(rec, entries, id)
	if err != nil {
		return 0, err
	}
	version++

	if err := storeDevices(rec, entries, id, nil); err != nil {
		return 0, err
	}
	if err := storeRemoved(rec, "", &marker{id: id, offAt: now, version: version}, &now); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return version, nil
}

// Drops the markers older than the purge age before now, and stores the list sorted.
//
//udf:function purge_devices
func purgeDevices(rec *udf.Record, now int64) (int64, error) {
	entries, err := liveEntries(rec)
	if err != nil {
		return 0, err
	}
	if len(entries) > 0 {
		if err := storeDevices(rec, entries, "", nil); err != nil {
			return 0, err
		}
	}
	if err := storeRemoved(rec, "", nil, &now); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return removedInt(rec, "purgedBefore")
}

func main() {}

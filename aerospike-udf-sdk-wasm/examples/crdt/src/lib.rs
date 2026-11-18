//! An active-active CRDT: a last-writer-wins map of devices inside one record, merged across data centres
//! by the `xdr-merge` event, with the write UDFs that keep it well-formed.
//!
//! The record keeps a subscriber's devices in one bin, `devices`, a list of maps each with a `deviceId`
//! and a `version` the application raises on every change. A second bin, `devicesRemoved`, holds a
//! removal marker per removed device (`{offAt, version}` under `markers`), the purge horizon
//! `purgedBefore`, and, once a purge has dropped a marker, the version floor `versionFloor`. Both data
//! centres take writes; when a write from one arrives at the other, `xdr_merge` decides each device from
//! values in the data alone - the version, whether it is a removal, and finally the stored bytes - so both
//! sides compute the same record without a clock, a LUT or a writer id, in any delivery order.
//!
//! Bindings:
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
//! ```
//!
//! and every application write goes through `update_device`, `remove_device` and `purge_devices`, which
//! keep the list sorted by `deviceId` and every version above the last one seen, live or removed. The
//! server's dry run (`execute_udf(key, "__event__", "xdr-merge", [incoming bins, meta, other?])`) reports
//! whether the merge is symmetric, idempotent and associative on real bytes; `tests/src/events.rs` runs
//! the eight scenarios of the design note through it.

#![no_std]

use core::cmp::Ordering;

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Coll, Error, Record, Resolution, Str, Value};

/// How long a removal marker outlives its removal, in ms. Longer than the longest outage between the
/// clusters plus the replication lag, or a purged device can come back on one side.
const PURGE_AGE_MS: i64 = 7 * 24 * 60 * 60 * 1000;

const MAX_DEVICES: usize = 128;

type Id = Str<64>;

/// One side's candidate for a device: its live entry, or its removal marker.
#[derive(Clone, Copy)]
struct Candidate {
    id: Id,
    version: i64,
    removed: bool,
    /// The entry or the marker, where it sits in the record it came from.
    value: Value<'static>,
    /// A marker's removal time; 0 for a live entry.
    off_at: i64,
}

/// `higher()`: the one of two candidates for a device that wins. The first rule that tells them apart
/// decides, and every rule reads the data alone.
fn higher(a: Candidate, b: Candidate) -> Result<Candidate, Error> {
    if a.version != b.version {
        return Ok(if a.version > b.version { a } else { b });
    }

    if a.removed != b.removed {
        return Ok(if a.removed { a } else { b });
    }

    // Two different changes at one version are concurrent: the stored bytes pick one, the same on both
    // clusters, since each side compares the same two byte strings.
    Ok(if a.value.cmp_stored(&b.value)? != Ordering::Less { a } else { b })
}

/// The candidates seen so far, one per device: `offer` keeps the higher.
struct Best {
    items: [Option<Candidate>; MAX_DEVICES],
    n: usize,
}

impl Best {
    const fn new() -> Self {
        Best { items: [None; MAX_DEVICES], n: 0 }
    }

    fn offer(&mut self, c: Candidate) -> Result<(), Error> {
        for slot in self.items[..self.n].iter_mut() {
            if let Some(have) = slot {
                if have.id == c.id {
                    *slot = Some(higher(*have, c)?);
                    return Ok(());
                }
            }
        }

        if self.n == MAX_DEVICES {
            return Err(Error::Custom("too many devices"));
        }

        self.items[self.n] = Some(c);
        self.n += 1;

        Ok(())
    }

    /// In `deviceId` order, by UTF-8 bytes - the order the server keeps a key-ordered map's string keys
    /// in, so the list and the markers agree.
    fn sort(&mut self) {
        for i in 1..self.n {
            let mut j = i;

            while j > 0 && id_cmp(&self.items[j - 1], &self.items[j]) == Ordering::Greater {
                self.items.swap(j - 1, j);
                j -= 1;
            }
        }
    }
}

fn id_cmp(a: &Option<Candidate>, b: &Option<Candidate>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.id.as_str().as_bytes().cmp(b.id.as_str().as_bytes()),
        _ => Ordering::Equal,
    }
}

/// A device's id, from its entry.
fn id_of(entry: &Coll) -> Result<Id, Error> {
    let mut buf = [0u8; 64];

    Id::new(entry.get("deviceId")?.as_str(&mut buf)?)
}

/// Every live entry of a `devices` list as a candidate.
fn offer_live(best: &mut Best, devices: Coll) -> Result<(), Error> {
    for item in devices.iter() {
        let item = item?;
        let entry = item.map()?;

        best.offer(Candidate {
            id: id_of(&entry)?,
            version: entry.get("version")?.int()?,
            removed: false,
            value: item,
            off_at: 0,
        })?;
    }

    Ok(())
}

/// Every marker of a `devicesRemoved` bin as a candidate; the horizon and the floor it carries.
fn offer_markers(best: &mut Best, removed: Option<Coll>, horizon: &mut i64, floor: &mut i64) -> Result<(), Error> {
    let Some(removed) = removed else { return Ok(()) };

    if let Ok(h) = removed.get("purgedBefore") {
        *horizon = (*horizon).max(h.int()?);
    }

    if let Ok(f) = removed.get("versionFloor") {
        *floor = (*floor).max(f.int()?);
    }

    if let Ok(markers) = removed.get("markers") {
        let mut buf = [0u8; 64];

        for entry in markers.map()?.entries() {
            let (key, marker) = entry?;
            let m = marker.map()?;

            best.offer(Candidate {
                id: Id::new(key.as_str(&mut buf)?)?,
                version: m.get("version")?.int()?,
                removed: true,
                value: marker,
                off_at: m.get("offAt")?.int()?,
            })?;
        }
    }

    Ok(())
}

event! {
    /// The merge: every device from both sides goes in as a candidate, live entry or marker, the higher
    /// wins per device, and the result is the sorted live list plus the markers still inside the purge
    /// horizon. Every other bin is the incoming one's, as XDR would have applied it.
    on xdr_merge
    fn xdr_merge(current: EventRecord, incoming: EventRecord, _conflict: Conflict) -> Result<Resolution, Error> {
        let out = as_udf::map(8)?;

        // Other bins: as XDR does today.
        let mut nbuf = [0u8; 32];

        for name in incoming.bins().iter() {
            let name = name?;
            let name = name.as_str(&mut nbuf)?;

            if name != "devices" && name != "devicesRemoved" {
                out.insert(name, incoming.bin(name)?)?;
            }
        }

        let touched = incoming.list("devices")?.is_some() || incoming.map("devicesRemoved")?.is_some();

        if touched {
            let mut best = Best::new();
            let mut horizon = 0;
            let mut floor = 0;

            for side in [&current, &incoming] {
                if let Some(devices) = side.list("devices")? {
                    offer_live(&mut best, devices)?;
                }

                offer_markers(&mut best, side.map("devicesRemoved")?, &mut horizon, &mut floor)?;
            }

            best.sort();

            let live = as_udf::list(best.n)?;
            let markers = as_udf::map(best.n)?;
            let mut n_markers = 0;

            for c in best.items[..best.n].iter().flatten() {
                if !c.removed {
                    live.push(c.value)?;
                }
                else if c.off_at >= horizon {
                    markers.insert(c.id.as_str(), c.value)?;
                    n_markers += 1;
                }
                else {
                    // Dropped by the other side's horizon before this side purged it: its version still
                    // has to be the floor, or a re-add here could start below a copy of the marker that
                    // is still on its way.
                    floor = floor.max(c.version);
                }
            }

            if current.list("devices")?.is_some() || incoming.list("devices")?.is_some() {
                out.insert("devices", live)?;
            }

            if n_markers > 0 || horizon > 0 {
                let removed = as_udf::map(3)?;

                removed.insert("markers", markers)?.insert("purgedBefore", horizon)?;

                if floor > 0 {
                    removed.insert("versionFloor", floor)?;
                }

                out.insert("devicesRemoved", removed)?;
            }
            else {
                // The merge has no removals left: the bin stays out, so with_bins below cannot bring a
                // stale copy back.
                out.insert("devicesRemoved", Value::Nil)?;
            }
        }

        Ok(Resolution::Record { bins: current.with_bins(out)?, ttl: None })
    }
}

//==========================================================
// The write UDFs: every application write to the devices goes through these, so each change moves one
// device strictly up the order the merge uses.
//

/// The live entries as (id, entry) pairs, in stored order.
fn live_entries(rec: &Record, buf: &mut [Option<(Id, Value<'static>)>; MAX_DEVICES]) -> Result<usize, Error> {
    let mut n = 0;

    if let Some(devices) = rec.list("devices")? {
        for item in devices.iter() {
            let item = item?;

            if n == MAX_DEVICES {
                return Err(Error::Custom("too many devices"));
            }

            buf[n] = Some((id_of(&item.map()?)?, item));
            n += 1;
        }
    }

    Ok(n)
}

/// A marker's version, if the device has one.
fn marker_version(rec: &Record, id: &str) -> Result<Option<i64>, Error> {
    let Some(removed) = rec.map("devicesRemoved")? else { return Ok(None) };

    match removed.get("markers") {
        Ok(markers) => match markers.map()?.get(id) {
            Ok(marker) => Ok(Some(marker.map()?.get("version")?.int()?)),
            Err(Error::Absent) => Ok(None),
            Err(e) => Err(e),
        },
        Err(Error::Absent) => Ok(None),
        Err(e) => Err(e),
    }
}

fn removed_int(rec: &Record, key: &str) -> Result<i64, Error> {
    let Some(removed) = rec.map("devicesRemoved")? else { return Ok(0) };

    match removed.get(key) {
        Ok(v) => v.int(),
        Err(Error::Absent) => Ok(0),
        Err(e) => Err(e),
    }
}

/// The device's latest version, live or removed, and never below the version floor.
fn last_version(rec: &Record, entries: &[Option<(Id, Value<'static>)>], id: &str) -> Result<i64, Error> {
    let mut v = removed_int(rec, "versionFloor")?;

    for (eid, entry) in entries.iter().flatten() {
        if eid.as_str() == id {
            v = v.max(entry.map()?.get("version")?.int()?);
        }
    }

    if let Some(mv) = marker_version(rec, id)? {
        v = v.max(mv);
    }

    Ok(v)
}

/// Store `entries` plus an optional built entry under `id`, sorted by id, less any entry under `id`.
fn store_devices(
    rec: &mut Record,
    entries: &[Option<(Id, Value<'static>)>],
    without: &str,
    add: Option<(Id, Value<'static>)>,
) -> Result<(), Error> {
    let mut all: [Option<(Id, Value<'static>)>; MAX_DEVICES] = [None; MAX_DEVICES];
    let mut n = 0;

    for e in entries.iter().flatten() {
        if e.0.as_str() != without {
            all[n] = Some(*e);
            n += 1;
        }
    }

    if let Some(a) = add {
        if n == MAX_DEVICES {
            return Err(Error::Custom("too many devices"));
        }

        all[n] = Some(a);
        n += 1;
    }

    for i in 1..n {
        let mut j = i;

        while j > 0 && all[j - 1].as_ref().map(|e| e.0.as_str().as_bytes()) > all[j].as_ref().map(|e| e.0.as_str().as_bytes()) {
            all.swap(j - 1, j);
            j -= 1;
        }
    }

    let list = as_udf::list(n)?;

    for (_, value) in all[..n].iter().flatten() {
        list.push(*value)?;
    }

    rec.set("devices", list)
}

/// Rebuild `devicesRemoved` from the current one with one marker removed, one added or replaced, and a
/// purge at `now` (when `now` is Some). The bin is left out while it holds nothing.
fn store_removed(
    rec: &mut Record,
    drop: Option<&str>,
    add: Option<(&str, i64, i64)>,
    now: Option<i64>,
) -> Result<(), Error> {
    let mut horizon = removed_int(rec, "purgedBefore")?;
    let mut floor = removed_int(rec, "versionFloor")?;

    if let Some(now) = now {
        horizon = horizon.max(now - PURGE_AGE_MS);
    }

    let markers = as_udf::map(8)?;
    let mut n_markers = 0;
    let mut kbuf = [0u8; 64];

    if let Some(removed) = rec.map("devicesRemoved")? {
        if let Ok(existing) = removed.get("markers") {
            for entry in existing.map()?.entries() {
                let (key, marker) = entry?;
                let id = key.as_str(&mut kbuf)?;
                let m = marker.map()?;

                if Some(id) == drop || add.is_some_and(|(aid, _, _)| aid == id) {
                    continue;
                }

                if m.get("offAt")?.int()? < horizon {
                    // Purged: its version becomes the floor every later version starts above.
                    floor = floor.max(m.get("version")?.int()?);
                    continue;
                }

                markers.insert(id, marker)?;
                n_markers += 1;
            }
        }
    }

    if let Some((id, off_at, version)) = add {
        let marker = as_udf::map(2)?;

        marker.insert("offAt", off_at)?.insert("version", version)?;
        markers.insert(id, marker)?;
        n_markers += 1;
    }

    if n_markers == 0 && horizon == 0 {
        return match rec.bin("devicesRemoved") {
            Ok(_) => rec.remove("devicesRemoved"),
            Err(_) => Ok(()),
        };
    }

    let removed = as_udf::map(3)?;

    removed.insert("markers", markers)?.insert("purgedBefore", horizon)?;

    if floor > 0 {
        removed.insert("versionFloor", floor)?;
    }

    rec.set("devicesRemoved", removed)
}

export! {
    /// Add a device, update one, or add a removed one back: the entry becomes the old one (if any) with
    /// `changes` on top and a version one above the device's last, live or removed. Returns the version.
    fn update_device(rec: &mut Record, id: &str, changes: Coll) -> Result<Value, Error> {
        let mut entries: [Option<(Id, Value<'static>)>; MAX_DEVICES] = [None; MAX_DEVICES];
        let n = live_entries(rec, &mut entries)?;
        let version = last_version(rec, &entries[..n], id)? + 1;

        let entry = as_udf::map(8)?;

        for (eid, old) in entries[..n].iter().flatten() {
            if eid.as_str() == id {
                for pair in old.map()?.entries() {
                    let mut kbuf = [0u8; 64];
                    let (k, v) = pair?;

                    entry.insert(k.as_str(&mut kbuf)?, v)?;
                }
            }
        }

        for pair in changes.entries() {
            let mut kbuf = [0u8; 64];
            let (k, v) = pair?;

            entry.insert(k.as_str(&mut kbuf)?, v)?;
        }

        entry.insert("deviceId", id)?.insert("version", version)?;
        store_devices(rec, &entries[..n], id, Some((Id::new(id)?, entry.into_value())))?;

        if marker_version(rec, id)?.is_some() {
            store_removed(rec, Some(id), None, None)?;
        }

        rec.save()?;

        Ok(Value::Int(version))
    }

    /// Remove a device, leaving a marker with the application's time `now` (ms) and the next version.
    fn remove_device(rec: &mut Record, id: &str, now: i64) -> Result<Value, Error> {
        let mut entries: [Option<(Id, Value<'static>)>; MAX_DEVICES] = [None; MAX_DEVICES];
        let n = live_entries(rec, &mut entries)?;
        let version = last_version(rec, &entries[..n], id)? + 1;

        store_devices(rec, &entries[..n], id, None)?;
        store_removed(rec, None, Some((id, now, version)), Some(now))?;
        rec.save()?;

        Ok(Value::Int(version))
    }

    /// Drop the markers older than the purge age before `now`, and store the list sorted.
    fn purge_devices(rec: &mut Record, now: i64) -> Result<Value, Error> {
        let mut entries: [Option<(Id, Value<'static>)>; MAX_DEVICES] = [None; MAX_DEVICES];
        let n = live_entries(rec, &mut entries)?;

        if n > 0 {
            store_devices(rec, &entries[..n], "", None)?;
        }

        store_removed(rec, None, None, Some(now))?;
        rec.save()?;

        Ok(Value::Int(removed_int(rec, "purgedBefore")?))
    }
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

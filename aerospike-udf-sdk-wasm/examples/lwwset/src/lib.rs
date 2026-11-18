//! A set with re-adds: the LWW-Element-Set (last-writer-wins element set), merged across data centres by
//! the `xdr-merge` event.
//!
//! The 2P-Set cannot add a tag back once it is removed, because its `removed` list never forgets. This
//! module remembers *when* instead of *whether*: two map bins, `addedAt` and `removedAt`, hold a
//! timestamp per tag, the server's clock in milliseconds at the write. A tag is a member when its add is
//! later than its removal; at equal stamps the removal wins, which is the bias every LWW set has to
//! choose. Adding a tag back after a removal is just a later stamp in `addedAt`.
//!
//! The merge is the PN-Counter's: for every tag of each map, the larger stamp. A stamp only ever grows
//! for a tag on the site that writes it, so the maximum is the latest, and the two maps converge for the
//! reason the counters did. The merge itself never reads a clock, and could not: the server refuses the
//! clock import inside a handler, because a decision that reads it cannot converge. Only the write UDFs
//! stamp, and they bump the stamp above the tag's last one when the clock has not moved, so two writes to
//! one tag on one site never tie.
//!
//! The limits are the clock and the tombstones. Two sites' clocks disagree, so "last" means last by the
//! wall clock that stamped it, which can be the earlier write; and `removedAt` keeps a stamp per removed
//! tag forever. The device map of the last step replaces the clock with a version the application raises
//! and purges its markers behind a horizon.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=tags;module=lwwset;function=merge_lwwset;on-failure=abandon
//! ```

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Coll, Error, List, Map, Record, Resolution, Str, Value};

const MAX_TAGS: usize = 64;

type Tag = Str<32>;

/// The stamps of one map, gathered from one or both sides.
struct Stamps {
    tags: [Option<(Tag, i64)>; MAX_TAGS],
    n: usize,
}

impl Stamps {
    const fn new() -> Self {
        Stamps { tags: [None; MAX_TAGS], n: 0 }
    }

    fn get(&self, tag: &str) -> Option<i64> {
        self.tags[..self.n].iter().flatten().find(|(t, _)| t == tag).map(|(_, at)| *at)
    }

    /// Keeps the later stamp for a tag.
    fn offer(&mut self, tag: Tag, at: i64) -> Result<(), Error> {
        for slot in self.tags[..self.n].iter_mut().flatten() {
            if slot.0 == tag {
                if at > slot.1 {
                    slot.1 = at;
                }

                return Ok(());
            }
        }

        if self.n == MAX_TAGS {
            return Err(Error::Custom("too many tags"));
        }

        self.tags[self.n] = Some((tag, at));
        self.n += 1;

        Ok(())
    }

    /// Every entry of a stored map.
    fn offer_all(&mut self, stamps: Option<Coll>) -> Result<(), Error> {
        let Some(stamps) = stamps else { return Ok(()) };
        let mut buf = [0u8; 32];

        for entry in stamps.entries() {
            let (tag, at) = entry?;

            self.offer(Tag::new(tag.as_str(&mut buf)?)?, at.int()?)?;
        }

        Ok(())
    }

    fn into_map(&self) -> Result<Map, Error> {
        let m = as_udf::map(self.n.max(1))?;

        for (tag, at) in self.tags[..self.n].iter().flatten() {
            m.insert(tag.as_str(), *at)?;
        }

        Ok(m)
    }
}

event! {
    /// The merge: for every tag of each map, the later stamp. Every other bin is the incoming one's.
    on xdr_merge
    fn merge_lwwset(current: EventRecord, incoming: EventRecord, _conflict: Conflict) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["addedAt", "removedAt"] {
            let mut stamps = Stamps::new();

            stamps.offer_all(current.map(bin)?)?;
            stamps.offer_all(incoming.map(bin)?)?;
            out.insert(bin, stamps.into_map()?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)?, ttl: None })
    }
}

/// One of the record's maps, or none.
fn stored(rec: &Record, bin: &str) -> Result<Stamps, Error> {
    let mut stamps = Stamps::new();

    stamps.offer_all(rec.map(bin)?)?;

    Ok(stamps)
}

/// Whether a tag is a member: added, and later than removed. A tie goes to the removal.
fn is_member(added: &Stamps, removed: &Stamps, tag: &str) -> bool {
    match (added.get(tag), removed.get(tag)) {
        (Some(a), Some(r)) => a > r,
        (Some(_), None) => true,
        _ => false,
    }
}

/// The members, in order, as a built list.
fn members_of(rec: &Record) -> Result<Value<'static>, Error> {
    let added = stored(rec, "addedAt")?;
    let removed = stored(rec, "removedAt")?;
    let l: List = as_udf::list(added.n.max(1))?;
    let mut done = [false; MAX_TAGS];

    // The stored map comes out key-ordered, but a tag added on this site sits at the end: pick the
    // smallest remaining each time, so the answer is sorted.
    for _ in 0..added.n {
        let mut next: Option<usize> = None;

        for (i, (tag, _)) in added.tags[..added.n].iter().flatten().enumerate() {
            if !done[i] && next.is_none_or(|j| tag.as_bytes() < added.tags[j].as_ref().unwrap().0.as_bytes()) {
                next = Some(i);
            }
        }

        let Some(i) = next else { break };

        done[i] = true;

        let tag = added.tags[i].as_ref().unwrap().0;

        if is_member(&added, &removed, tag.as_str()) {
            l.push(tag.as_str())?;
        }
    }

    Ok(l.into_value())
}

/// The stamp for a write to `tag` now: the clock, or one past the tag's last stamp when the clock has
/// not moved since.
fn stamp_for(rec: &Record, tag: &str) -> Result<i64, Error> {
    let last =
        stored(rec, "addedAt")?.get(tag).unwrap_or(0).max(stored(rec, "removedAt")?.get(tag).unwrap_or(0));

    Ok(as_udf::now()?.max(last + 1))
}

/// Stamps `tag` in one of the maps and saves.
fn stamp(rec: &mut Record, bin: &str, tag: Tag) -> Result<(), Error> {
    let at = stamp_for(rec, tag.as_str())?;
    let mut stamps = stored(rec, bin)?;

    stamps.offer(tag, at)?;
    rec.set(bin, stamps.into_map()?)?;

    rec.save()?;

    Ok(())
}

export! {
    /// Adds a tag, or adds it back. Returns the members.
    fn add(rec: &mut Record, tag: &str) -> Result<Value, Error> {
        stamp(rec, "addedAt", Tag::new(tag)?)?;
        members_of(rec)
    }

    /// Removes a tag that is a member. Returns the members.
    fn remove(rec: &mut Record, tag: &str) -> Result<Value, Error> {
        if !is_member(&stored(rec, "addedAt")?, &stored(rec, "removedAt")?, tag) {
            return Err(Error::Custom("not a member"));
        }

        stamp(rec, "removedAt", Tag::new(tag)?)?;
        members_of(rec)
    }

    /// The members, in order.
    fn members(rec: &Record) -> Result<Value, Error> {
        members_of(rec)
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

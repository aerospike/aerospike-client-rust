//! A set with removals: the 2P-Set (two-phase set), merged across data centres by the `xdr-merge` event.
//!
//! The counters merge with a maximum. A set merges with a union, and a union, like a maximum, is
//! commutative, idempotent and associative by construction: a grow-only set of tags converges with no
//! metadata at all. What a union cannot do is remove, because a replica that still holds the tag puts it
//! back at the next merge. So this module applies the PN-Counter's trick to sets: it never removes, it
//! adds to a second set instead. Two list bins, `added` and `removed`, both only ever grow; a tag is a
//! member when it is in `added` and not in `removed`; `merge_twopset` takes the union of each. A tag in
//! `removed` is a tombstone: it beats a concurrent add on the other side, and it stays forever.
//!
//! That is also the limit. A tag removed once can never be added back, because `removed` never shrinks,
//! and the tombstones accumulate for as long as the record lives. The next step lifts both with a stamp
//! per tag.
//!
//! Both lists are kept sorted and without duplicates, so the same set is always the same bytes: the
//! server's dry run compares the stored bytes of the merged records, and so does its "unchanged" check.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=tags;module=twopset;function=merge_twopset;on-failure=abandon
//! ```

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Coll, Error, List, Record, Resolution, Str};

const MAX_TAGS: usize = 64;

type Tag = Str<32>;

/// A sorted set of tags with no duplicates, gathered from one or both sides.
struct Tags {
    tags: [Option<Tag>; MAX_TAGS],
    n: usize,
}

impl Tags {
    const fn new() -> Self {
        Tags { tags: [None; MAX_TAGS], n: 0 }
    }

    /// Where `tag` is, or where it would go.
    fn position(&self, tag: &[u8]) -> (usize, bool) {
        for (i, t) in self.tags[..self.n].iter().flatten().enumerate() {
            match t.as_bytes().cmp(tag) {
                core::cmp::Ordering::Equal => return (i, true),
                core::cmp::Ordering::Greater => return (i, false),
                core::cmp::Ordering::Less => {}
            }
        }

        (self.n, false)
    }

    fn contains(&self, tag: &str) -> bool {
        self.position(tag.as_bytes()).1
    }

    /// Adds a tag in order; nothing when it is there already.
    fn insert(&mut self, tag: Tag) -> Result<(), Error> {
        let (at, found) = self.position(tag.as_bytes());

        if found {
            return Ok(());
        }

        if self.n == MAX_TAGS {
            return Err(Error::Custom("too many tags"));
        }

        self.tags.copy_within(at..self.n, at + 1);
        self.tags[at] = Some(tag);
        self.n += 1;

        Ok(())
    }

    /// Every item of a stored list.
    fn offer_all(&mut self, list: Option<Coll>) -> Result<(), Error> {
        let Some(list) = list else { return Ok(()) };
        let mut buf = [0u8; 32];

        for item in list.iter() {
            self.insert(Tag::new(item?.as_str(&mut buf)?)?)?;
        }

        Ok(())
    }

    /// The tags not in `except`, as a built list, in order.
    fn into_list(&self, except: Option<&Tags>) -> Result<List, Error> {
        let l = as_udf::list(self.n.max(1))?;

        for tag in self.tags[..self.n].iter().flatten() {
            if except.is_none_or(|e| !e.contains(tag.as_str())) {
                l.push(tag.as_str())?;
            }
        }

        Ok(l)
    }
}

event! {
    /// The merge: the union of each list. Every other bin is the incoming one's.
    on xdr_merge
    fn merge_twopset(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["added", "removed"] {
            let mut tags = Tags::new();

            tags.offer_all(current.list(bin)?)?;
            tags.offer_all(incoming.list(bin)?)?;
            out.insert(bin, &tags.into_list(None)?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}

/// One of the record's lists, or none.
fn stored(rec: &Record, bin: &str) -> Result<Tags, Error> {
    let mut tags = Tags::new();

    tags.offer_all(rec.list(bin)?)?;

    Ok(tags)
}

/// The members: added and not removed.
fn members_of(rec: &Record) -> Result<List, Error> {
    let added = stored(rec, "added")?;
    let removed = stored(rec, "removed")?;

    added.into_list(Some(&removed))
}

/// Adds `tag` to one of the lists and saves.
fn grow(rec: &mut Record, bin: &str, tag: Tag) -> Result<(), Error> {
    let mut tags = stored(rec, bin)?;

    if tags.contains(tag.as_str()) {
        return Ok(());
    }

    tags.insert(tag)?;
    rec.set(bin, &tags.into_list(None)?)?;

    rec.save()?;

    Ok(())
}

export! {
    /// Adds a tag. Returns the members. A tag removed before stays removed: `added` already has it, and
    /// `removed` never forgets it.
    fn add(rec: &mut Record, tag: &str) -> Result<List, Error> {
        grow(rec, "added", Tag::new(tag)?)?;
        members_of(rec)
    }

    /// Removes a tag that is a member, forever. Returns the members.
    fn remove(rec: &mut Record, tag: &str) -> Result<List, Error> {
        if !stored(rec, "added")?.contains(tag) || stored(rec, "removed")?.contains(tag) {
            return Err(Error::Custom("not a member"));
        }

        grow(rec, "removed", Tag::new(tag)?)?;
        members_of(rec)
    }

    /// The members, in order.
    fn members(rec: &Record) -> Result<List, Error> {
        members_of(rec)
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

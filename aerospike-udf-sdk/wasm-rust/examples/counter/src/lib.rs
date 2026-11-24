//! The smallest CRDT: a grow-only counter, merged across data centres by the `xdr-merge` event.
//!
//! One bin, `counts`, holds a map of `site -> count`. Each data centre adds only to its own entry
//! (`increment("dc1", 3)`), and the counter's value is the sum of every entry (`value()`). When a write
//! from one site arrives at the other, `merge_counter` keeps, for every site, the larger of the two counts.
//! A site's count only ever grows, so the larger one is always the later one: the merge needs no clock, no
//! last update time and no writer id, and it is commutative, idempotent and associative by construction,
//! which the server's dry run checks on real bytes.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=counters;module=counter;function=merge_counter;on-failure=abandon
//! ```
//!
//! This is the module to read before `examples/crdt`, which keeps the same shape (a merge that decides on
//! the data alone, plus the write UDFs that keep the data well-formed) for a much richer structure.

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Coll, Error, Map, Record, Resolution, Str, Value};

const MAX_SITES: usize = 64;

type Site = Str<32>;

/// The per-site counts gathered from one or both sides.
struct Counts {
    sites: [Option<(Site, i64)>; MAX_SITES],
    n: usize,
}

impl Counts {
    const fn new() -> Self {
        Counts { sites: [None; MAX_SITES], n: 0 }
    }

    /// Keeps the larger count for a site.
    fn offer(&mut self, site: Site, count: i64) -> Result<(), Error> {
        for slot in self.sites[..self.n].iter_mut().flatten() {
            if slot.0 == site {
                if count > slot.1 {
                    slot.1 = count;
                }

                return Ok(());
            }
        }

        if self.n == MAX_SITES {
            return Err(Error::Custom("too many sites"));
        }

        self.sites[self.n] = Some((site, count));
        self.n += 1;

        Ok(())
    }

    /// Every entry of a stored `counts` map.
    fn offer_all(&mut self, counts: Option<Coll>) -> Result<(), Error> {
        let Some(counts) = counts else { return Ok(()) };
        let mut buf = [0u8; 32];

        for entry in counts.entries() {
            let (site, count) = entry?;

            self.offer(Site::new(site.as_str(&mut buf)?)?, count.int()?)?;
        }

        Ok(())
    }

    fn total(&self) -> i64 {
        self.sites[..self.n].iter().flatten().map(|(_, c)| *c).sum()
    }

    fn into_map(&self) -> Result<Map, Error> {
        let m = as_udf::map(self.n)?;

        for (site, count) in self.sites[..self.n].iter().flatten() {
            m.insert(site.as_str(), *count)?;
        }

        Ok(m)
    }
}

event! {
    /// The merge: for every site, the larger of the two counts. Every other bin is the incoming one's.
    on xdr_merge
    fn merge_counter(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let mut counts = Counts::new();

        counts.offer_all(current.map("counts")?)?;
        counts.offer_all(incoming.map("counts")?)?;

        let out = as_udf::map(1)?;

        out.insert("counts", &counts.into_map()?)?;

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}

/// The record's counts, or none.
fn stored(rec: &Record) -> Result<Counts, Error> {
    let mut counts = Counts::new();

    counts.offer_all(rec.map("counts")?)?;

    Ok(counts)
}

export! {
    /// Adds `n` to this site's count. Returns the counter's value.
    fn increment(rec: &mut Record, site: &str, n: i64) -> Result<Value, Error> {
        let mut counts = stored(rec)?;
        let site = Site::new(site)?;
        let mut found = false;

        for slot in counts.sites[..counts.n].iter_mut().flatten() {
            if slot.0 == site {
                slot.1 += n;
                found = true;
            }
        }

        if !found {
            counts.offer(site, n)?;
        }

        rec.set("counts", &counts.into_map()?)?;

        rec.save()?;

        Ok(Value::Int(counts.total()))
    }

    /// The counter's value: the sum of every site's count.
    fn value(rec: &Record) -> Result<Value, Error> {
        Ok(Value::Int(stored(rec)?.total()))
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

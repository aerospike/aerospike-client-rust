//! A counter that can go down: the PN-Counter, two grow-only counters glued together, merged across data
//! centres by the `xdr-merge` event.
//!
//! The grow-only counter of `examples/counter` cannot subtract: a decrement would shrink a site's entry,
//! and the per-site maximum would throw the shrink away. So this module never subtracts. The record keeps
//! two maps of `site -> amount`, `inc` and `dec`, and both only ever grow: `increment("dc1", 3)` adds to
//! `inc["dc1"]`, `decrement("dc1", 1)` adds to `dec["dc1"]`, and the counter's value is the sum of `inc`
//! minus the sum of `dec`. The merge is the grow-only counter's, run over each map on its own: for every
//! site, the larger amount. Both maps converge for the reason the single map did, so their difference
//! does too.
//!
//! What it cannot do is hold a floor: two sites that each see a stock of one and each take one merge to
//! minus one. A bound is not a property a merge can keep; it needs coordination, or a budget of
//! decrements handed to each site in advance.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=stock;module=pncounter;function=merge_pncounter;on-failure=abandon
//! ```

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Coll, Error, Map, Record, Resolution, Str, Value};

const MAX_SITES: usize = 64;

type Site = Str<32>;

/// The per-site amounts of one map, gathered from one or both sides.
struct Counts {
    sites: [Option<(Site, i64)>; MAX_SITES],
    n: usize,
}

impl Counts {
    const fn new() -> Self {
        Counts { sites: [None; MAX_SITES], n: 0 }
    }

    /// Keeps the larger amount for a site.
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

    /// Every entry of a stored map.
    fn offer_all(&mut self, counts: Option<Coll>) -> Result<(), Error> {
        let Some(counts) = counts else { return Ok(()) };
        let mut buf = [0u8; 32];

        for entry in counts.entries() {
            let (site, count) = entry?;

            self.offer(Site::new(site.as_str(&mut buf)?)?, count.int()?)?;
        }

        Ok(())
    }

    /// Adds `n` to a site's amount, creating the site if it is new.
    fn add(&mut self, site: Site, n: i64) -> Result<(), Error> {
        for slot in self.sites[..self.n].iter_mut().flatten() {
            if slot.0 == site {
                slot.1 += n;

                return Ok(());
            }
        }

        self.offer(site, n)
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
    /// The merge: for every site of each map, the larger amount. Every other bin is the incoming one's.
    on xdr_merge
    fn merge_pncounter(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["inc", "dec"] {
            let mut counts = Counts::new();

            counts.offer_all(current.map(bin)?)?;
            counts.offer_all(incoming.map(bin)?)?;
            out.insert(bin, &counts.into_map()?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}

/// One of the record's maps, or none.
fn stored(rec: &Record, bin: &str) -> Result<Counts, Error> {
    let mut counts = Counts::new();

    counts.offer_all(rec.map(bin)?)?;

    Ok(counts)
}

/// The counter's value: everything added minus everything taken.
fn value_of(rec: &Record) -> Result<i64, Error> {
    Ok(stored(rec, "inc")?.total() - stored(rec, "dec")?.total())
}

/// Adds `n` to this site's entry of one map and saves; answers the counter's value.
fn grow(rec: &mut Record, bin: &str, site: &str, n: i64) -> Result<i64, Error> {
    if n < 0 {
        return Err(Error::Custom("the amount must not be negative: an entry only grows"));
    }

    let mut counts = stored(rec, bin)?;

    counts.add(Site::new(site)?, n)?;
    rec.set(bin, &counts.into_map()?)?;

    rec.save()?;

    value_of(rec)
}

export! {
    /// Adds `n` to this site's increments. Returns the counter's value.
    fn increment(rec: &mut Record, site: &str, n: i64) -> Result<Value, Error> {
        Ok(Value::Int(grow(rec, "inc", site, n)?))
    }

    /// Adds `n` to this site's decrements. Returns the counter's value.
    fn decrement(rec: &mut Record, site: &str, n: i64) -> Result<Value, Error> {
        Ok(Value::Int(grow(rec, "dec", site, n)?))
    }

    /// The counter's value: the sum of every site's increments minus the sum of every site's decrements.
    fn value(rec: &Record) -> Result<Value, Error> {
        Ok(Value::Int(value_of(rec)?))
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

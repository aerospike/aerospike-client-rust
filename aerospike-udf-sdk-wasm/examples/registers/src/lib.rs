//! A record of registers: every bin is a last-writer-wins register, and the stamps are the server's.
//!
//! `examples/lwwset` keeps a timestamp per element in the record, written by its own UDFs. On a namespace
//! with bin convergence (`conflict-resolve-writes true`) the server already keeps one per bin: when the
//! bin was last written, and by which cluster. An `xdr-merge` handler reads both
//! (`bin_last_update_time`, `bin_source_id`), and that pair is the stamp of a register: the later write
//! wins, and of two writes in the same millisecond, the one from the cluster with the larger id.
//!
//! So a register needs no write UDF. A client writes `theme`, `lang` or `email` with a plain put, and
//! deletes one by writing nil to it; the server stamps the bin, and `merge_settings` picks the later side
//! bin by bin. A deleted bin keeps its stamp too, so a delete that is the later write stays a delete, and
//! one that is the earlier write does not undo what came after it.
//!
//! That much the server does on its own under bin convergence. The handler is here for the bin that is
//! not a register: `seen`, the highest message number this user has read, on any device. Its merge is the
//! larger number, whichever side wrote last, and no policy that picks a side can say that. It has the one
//! write UDF, `saw(n)`, which never lowers it: a merge can only keep a maximum that the writes kept too.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=testbc;set=settings;module=registers;function=merge_settings;on-failure=abandon
//! ```

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, event, export, Error, EventRecord, Record, Resolution, Value};

/// Whether this side deleted the bin and still remembers that it did.
fn deleted(side: &EventRecord, name: &str) -> Result<bool, Error> {
    let mut buf = [0u8; 32];

    for bin in side.deleted_bins().iter() {
        if bin?.as_str(&mut buf)? == name {
            return Ok(true);
        }
    }

    Ok(false)
}

/// A register's stamp on one side: when the bin was last written there, and by which cluster. None when
/// this side has nothing to say about the bin, which is less than any stamp.
fn stamp(side: &EventRecord, name: &str) -> Result<Option<(i64, i64)>, Error> {
    if side.has(name) || deleted(side, name)? {
        Ok(Some((side.bin_last_update_time(name), side.bin_source_id(name))))
    } else {
        Ok(None)
    }
}

event! {
    /// The merge: `seen` is the larger number; every other bin is the side that wrote it last.
    on xdr_merge
    fn merge_settings(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        // Without bin convergence there is no stamp per bin, and a merge that pretended otherwise would
        // decide every bin by the record's time. Failing hands the write to the binding's `on-failure`.
        if !current.uses_bin_convergence() {
            return Err(Error::Custom("registers need a namespace with conflict-resolve-writes"));
        }

        let out = as_udf::map(16)?;

        if let Some(seen) = current.int("seen")?.max(incoming.int("seen")?) {
            out.insert("seen", seen)?;
        }

        let mut buf = [0u8; 32];

        for (side, is_incoming) in [(&current, false), (&incoming, true)] {
            for names in [side.bin_names(), side.deleted_bins()] {
                for name in names.iter() {
                    let name = name?;
                    let name = name.as_str(&mut buf)?;

                    // The first pass decided every bin the current side knows of.
                    if name == "seen" || (is_incoming && stamp(&current, name)?.is_some()) {
                        continue;
                    }

                    let winner = if stamp(&incoming, name)? > stamp(&current, name)? { &incoming } else { &current };

                    // A nil in the answer is "no such bin": the winner's delete, kept.
                    out.insert(name, winner.get(name)?.unwrap_or(Value::Nil))?;
                }
            }
        }

        Ok(Resolution::Record { bins: out, ttl: None })
    }
}

export! {
    /// Records that message `n` was read. Returns the highest number read so far.
    fn saw(rec: &mut Record, n: i64) -> Result<Value, Error> {
        let seen = rec.int_or("seen", 0)?;

        if n > seen {
            rec.set("seen", n)?;
            rec.save()?;
        }

        Ok(Value::Int(seen.max(n)))
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

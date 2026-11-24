//! An XDR conflict resolver: the event handler a service with accounts written in two data centres might
//! ship.
//!
//! Under `conflict-resolve-writes`, an XDR write whose bins are older than the current record's loses
//! them, bin by bin, to the current record. That is right for a bin that is a plain value and wrong for one that is
//! a count or a set: the two DCs each saw some of the visits, and each tagged some of the tags, and the
//! merged record should have all of them. This module is bound to the namespace (or set) with
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=accounts;module=resolver;function=merge_account
//! ```
//!
//! after which every conflicting XDR write to an account runs `merge_account` with the current record and
//! the incoming one, and writes whatever it returns. Build it with
//! `cargo build --release --target wasm32-unknown-unknown` and register the `.wasm` with `UdfLang::Wasm`.
//!
//! The record: `visits` (an integer), `tags` (a list of strings), `profile` (a map), `balance` (an integer
//! that must not be merged: it is authoritative in one DC, and the policy's answer is the right one).
//!
//! The same merge is written in C (`aerospike-udf-sdk/wasm-c/examples/resolver`) and Go
//! (`aerospike-udf-sdk/wasm-go/examples/resolver`); `tests/src/events.rs` holds the three to the same
//! answer.
//!
//! # What a resolver must be
//!
//! **Pure.** It sees two records and returns one record. It does not read the clock, the
//! database or anything else, because it has to give the same answer on every node that sees the pair.
//!
//! **Symmetric.** DC A resolves (A's record, B's write) while DC B resolves (B's record, A's write), and the
//! two must land on the same record or the DCs diverge for good. `max`, union and "the newer one by last
//! update time" are symmetric; "prefer current" is not. The server's dry run (`execute_udf` on package
//! `__event__`) runs a handler both ways and reports whether the answers agree.

#![no_std]

// `EventRecord` is not imported: the parameter types in the `event!` arm are part of the macro's grammar,
// not names resolved here.
use aerospike_udf_sdk_wasm::{self as as_udf, event, Error, Pair, Resolution, Value};

event! {
    /// The merge: visits take the larger count, tags unite, the profile is whichever side wrote it last,
    /// and everything else - `balance` included - is the newer side's, which is the policy's own answer.
    on xdr_merge
    fn merge_account(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let pair = Pair::of(current, incoming);
        let out = as_udf::map(8)?;

        // A count each DC advanced on its own: both sides' increments are real, so the merge is the larger
        // of the two, not the sum (the sum would double-count the visits both already knew of).
        if let Some(visits) = pair.int("visits")?.max() {
            out.insert("visits", visits)?;
        }

        // A set each DC added to: the union. Its order is part of the result, so it is the order both DCs
        // agree on - the older side's tags, then the newer side's new ones - never "mine first".
        let (older, newer) = pair.list("tags")?.ordered();
        let tags = as_udf::list(16)?;
        let mut n_older = 0usize;

        if let Some(first) = older {
            tags.extend(first.iter())?;
            n_older = first.len();
        }

        if let Some(second) = newer {
            let mut buf = [0u8; 64];

            for tag in second.iter() {
                let tag = tag?;
                let text = tag.as_str(&mut buf)?;

                if !has_str(&tags, n_older, text)? {
                    tags.push(text)?;
                }
            }
        }

        out.insert("tags", &tags)?;

        // A document one side rewrote whole: last writer wins, by last update time, which both DCs
        // agree on.
        if let Some(profile) = pair.bin("profile")?.newer() {
            out.insert("profile", profile)?;
        }

        // A bin one side deleted stays deleted when the delete is the later write for that bin: a nil in
        // the returned record is "no such bin". Without this, the other side's copy would come back
        // through `with_newer` below - the resurrection every merge has to guard against.
        let mut nbuf = [0u8; 32];

        for (deleted, by_incoming) in [(current.deleted_bins(), false), (incoming.deleted_bins(), true)] {
            for name in deleted.iter() {
                let name = name?;
                let name = name.as_str(&mut nbuf)?;

                if pair.incoming_is_newer(name) == by_incoming {
                    out.insert(name, Value::Nil)?;
                }
            }
        }

        // Everything else is the newer side's - the policy's answer for a bin this handler has no opinion
        // on. `current.with_bins(out)` would read more naturally and be wrong: it prefers a side, and the
        // other DC prefers the other side.
        Ok(Resolution::Record { bins: pair.with_newer(out)? })
    }
}

/// Whether one of the first `n` strings in a list under construction is `text`.
fn has_str(list: &as_udf::List, n: usize, text: &str) -> Result<bool, Error> {
    let mut buf = [0u8; 64];

    for (i, item) in list.as_value().list()?.iter().enumerate() {
        if i >= n {
            break;
        }

        let item = item?;

        if let Value::String(_) = item {
            if item.as_str(&mut buf)? == text {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

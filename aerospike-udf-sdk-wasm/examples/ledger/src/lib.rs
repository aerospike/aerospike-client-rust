//! A ledger: the UDFs a payments service might ship, written to be read.
//!
//! The fixture beside this (`examples/udf`) exists to be asserted against by the test suite, which makes it
//! a poor introduction. This module does three ordinary things an account record needs - a guarded
//! withdrawal, a bounded history, a summary - and one aggregation, and says why each is written the way it
//! is. Build it with `cargo build --release --target wasm32-unknown-unknown` and register the `.wasm` with
//! `UdfLang::Wasm`.
//!
//! The record: `balance` (an integer, in cents), `postings` (a list of `{"amount": int, "memo": str}`).

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, export, stream, Error, Record, Str, Value};

/// The balance, or zero for an account that has none yet. A balance that is not an integer is the
/// caller's data problem, reported as such.
fn balance(rec: &Record) -> Result<i64, Error> {
    Ok(rec.int("balance")?.unwrap_or(0))
}

export! {
    /// Move `amount` out of the balance, unless `dry_run`. Refused, leaving the record untouched, when the
    /// balance would go negative - the client sees the message.
    ///
    /// `amount: i64` and `dry_run: Option<bool>` are checked before this body runs: a string where the
    /// amount should be is rejected as `withdraw: argument 0 (amount): expected an integer, got a string`.
    fn withdraw(rec: &mut Record, amount: i64, dry_run: Option<bool>) -> Result<Value, Error> {
        let balance = balance(rec)?;

        if amount < 0 {
            return Err(Error::Custom("withdraw: the amount must not be negative"));
        }

        if balance < amount {
            return Err(Error::Custom("withdraw: insufficient funds"));
        }

        if dry_run != Some(true) {
            // A `set` is staged until `save`, which is the one way a UDF looks right and changes nothing.
            rec.set("balance", balance - amount)?;
            rec.save()?;
        }

        Ok(Value::Int(balance - amount))
    }

    /// Record a posting - `{"amount": amount, "memo": memo}` - and apply it to the balance, keeping only the
    /// last `keep` postings (100 by default).
    ///
    /// The history is trimmed **on the host, by position**: the postings that stay are handed from the
    /// stored list into the new one in one import call per 128, and the guest never copies a posting it
    /// merely keeps. `memo: Str<64>` is the memo's limit; a longer one is rejected before this runs.
    fn post(rec: &mut Record, amount: i64, memo: Str<64>, keep: Option<i64>) -> Result<Value, Error> {
        let keep = keep.unwrap_or(100).max(1) as usize;
        let history = as_udf::list(keep)?;

        if let Some(existing) = rec.list("postings")? {
            let skip = existing.len().saturating_sub(keep - 1);

            history.extend(existing.iter().skip(skip))?;
        }

        let posting = as_udf::map(2)?;

        posting.insert("amount", amount)?.insert("memo", &memo)?;
        history.push(posting)?;

        let kept = history.into_value().list()?.len() as i64;

        let balance = balance(rec)?;

        rec.set("postings", history)?;
        rec.set("balance", balance + amount)?;
        rec.save()?;

        Ok(Value::Int(kept))
    }

    /// The account as one map: the balance, how many postings it keeps, and the largest of them. Read-only,
    /// and built on the host - a map the guest fills by key and returns as a value.
    fn summary(rec: &Record) -> Result<Value, Error> {
        let mut count = 0i64;
        let mut largest = 0i64;

        if let Some(postings) = rec.list("postings")? {
            for posting in postings.iter() {
                let amount = posting?.map()?.get("amount")?.int()?;

                count += 1;
                largest = largest.max(amount);
            }
        }

        let out = as_udf::map(3)?;

        out.insert("balance", balance(rec)?)?
            .insert("postings", count)?
            .insert("largest", largest)?;

        Ok(out.into_value())
    }
}

// An aggregation shares one instance across the records a node streams through it, so its running totals
// are plain statics. `end` returns the node's partial; a client sums the partials.
static mut TOTAL: i64 = 0;
static mut ACCOUNTS: i64 = 0;

stream! {
    /// The total balance across the accounts the query reaches, and how many there were: one
    /// `{"total": int, "accounts": int}` per node.
    fn balances {
        record(rec: &Record) -> Result<(), Error> {
            let balance = balance(rec)?;

            unsafe {
                TOTAL += balance;
                ACCOUNTS += 1;
            }

            Ok(())
        }
        end() -> Result<Value, Error> {
            let out = as_udf::map(2)?;

            out.insert("total", unsafe { TOTAL })?.insert("accounts", unsafe { ACCOUNTS })?;

            Ok(out.into_value())
        }
    }
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

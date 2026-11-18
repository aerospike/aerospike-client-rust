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

//! The Rust side of the book's tutorials. The page `record-udfs/tutorial.md` includes the code between
//! the `ANCHOR` comments, so what the book shows is what compiles here.

#![no_std]

// ANCHOR: setup
use aerospike_udf_sdk_wasm::{self as as_udf, export, Error, Record, Value};
// ANCHOR_END: setup

export! {
    // ANCHOR: visit
    fn visit(rec: &mut Record, by: i64) -> Result<Value, Error> {
        // None when there is no such bin yet; WrongType when it holds something else.
        let visits = rec.int("visits")?.unwrap_or(0) + by;

        rec.set("visits", visits)?;       // staged, not yet written
        rec.save()?;                      // written, and the record created if it was not there

        Ok(Value::Int(visits))
    }
    // ANCHOR_END: visit

    // ANCHOR: withdraw
    fn withdraw(rec: &mut Record, amount: i64) -> Result<Value, Error> {
        let balance = rec.bin("balance")?.int()?;       // Absent or WrongType fails the call

        if amount > balance {
            return Err(Error::Custom("insufficient funds"));
        }

        rec.set("balance", balance - amount)?;
        rec.save()?;

        Ok(Value::Int(balance - amount))
    }
    // ANCHOR_END: withdraw

    // ANCHOR: rename
    fn rename(rec: &mut Record, name: &str) -> Result<Value, Error> {
        let mut buf = [0u8; 64];
        let owner = rec.bin("owner")?.text()?;          // the string, still in the record

        if owner.as_str(&mut buf)? == name {            // its bytes, copied into `buf`
            return Ok(Value::Bool(false));
        }

        rec.set("owner", name)?;
        rec.save()?;

        Ok(Value::Bool(true))
    }
    // ANCHOR_END: rename

    // ANCHOR: total
    fn total(rec: &Record) -> Result<Value, Error> {
        let mut sum = 0;

        for payment in rec.bin("history")?.list()?.iter() {
            sum += payment?.int()?;
        }

        Ok(Value::Int(sum))
    }
    // ANCHOR_END: total

    // ANCHOR: limit_for
    fn limit_for(rec: &Record, kind: &str) -> Result<Value, Error> {
        let limits = rec.bin("limits")?.map()?;

        match limits.get(kind) {
            Ok(limit) => Ok(Value::Int(limit.int()?)),
            Err(Error::Absent) => Ok(Value::Int(0)),    // no such key
            Err(e) => Err(e),
        }
    }
    // ANCHOR_END: limit_for

    // ANCHOR: limits_total
    fn limits_total(rec: &Record) -> Result<Value, Error> {
        let mut sum = 0;

        for entry in rec.bin("limits")?.map()?.entries() {
            let (_kind, limit) = entry?;
            sum += limit.int()?;
        }

        Ok(Value::Int(sum))
    }
    // ANCHOR_END: limits_total

    // ANCHOR: log_payment
    fn log_payment(rec: &mut Record, amount: i64) -> Result<Value, Error> {
        let history = rec.bin("history")?.list()?;
        let count = history.len() + 1;

        let out = as_udf::list(count)?;
        out.extend(history.iter())?.push(amount)?;      // the stored elements, then the new one

        rec.set("history", out)?;
        rec.save()?;

        Ok(Value::Int(count as i64))
    }
    // ANCHOR_END: log_payment

    // ANCHOR: summary
    fn summary(rec: &Record) -> Result<Value, Error> {
        let out = as_udf::map(5)?;

        out.insert("owner", rec.bin("owner")?)?         // a stored value goes in as it is
            .insert("balance", rec.bin("balance")?)?
            .insert("generation", rec.generation())?
            .insert("bins", rec.bin_count())?
            .insert("ttl", rec.ttl().map_or(-1, i64::from))?;

        Ok(out.into_value())
    }
    // ANCHOR_END: summary

    // ANCHOR: renew
    fn renew(rec: &mut Record, seconds: i64) -> Result<Value, Error> {
        let renewals = rec.int("renewals")?.unwrap_or(0) + 1;

        rec.set("renewals", renewals)?;
        rec.set_ttl(seconds as u32)?;                   // takes effect with the save
        rec.save()?;

        Ok(Value::Int(renewals))
    }
    // ANCHOR_END: renew

    // ANCHOR: clear_note
    fn clear_note(rec: &mut Record) -> Result<Value, Error> {
        rec.remove("note")?;                            // staged, like a set
        rec.save()?;

        Ok(Value::Nil)
    }
    // ANCHOR_END: clear_note

    // ANCHOR: close
    fn close(rec: &mut Record) -> Result<Value, Error> {
        rec.delete()?;                                  // no save: the delete is the write

        Ok(Value::Nil)
    }
    // ANCHOR_END: close
}

/// A `no_std` cdylib needs a panic handler; a trap is what the host reports best.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

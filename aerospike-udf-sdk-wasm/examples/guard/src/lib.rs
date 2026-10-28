//! Guards: the write-path events on an account set - a constraint, a stamp, a delete lock, and a fence
//! around truncation.
//!
//! These are the handlers the other databases' triggers are mostly written for: refuse a write that
//! breaks a rule, add a bin every write should carry, keep a protected record from being deleted, and keep
//! a set from being emptied by a slip. Each is bound with `event-set`:
//!
//! ```text
//! event-set:event=before-write;namespace=test;set=accounts;module=guard;function=guard_account
//! event-set:event=before-write;namespace=test;set=accounts;module=guard;function=stamp_region;order=1
//! event-set:event=before-delete;namespace=test;set=accounts;module=guard;function=guard_delete
//! event-set:event=before-truncate;namespace=test;module=guard;function=guard_truncate
//! event-set:event=after-truncate;namespace=test;module=guard;function=note_truncate
//! event-set:event=before-expire;namespace=test;set=accounts;module=guard;function=keep_marked
//! event-set:event=before-evict;namespace=test;set=accounts;module=guard;function=protect_marked
//! event-set:event=after-read;namespace=test;set=accounts;module=guard;function=hide_secret
//! event-set:event=node-lifecycle;module=guard;function=note_lifecycle
//! event-set:event=cluster-change;module=guard;function=note_cluster
//! event-set:event=partition-transfer;namespace=test;module=guard;function=note_transfer
//! event-set:event=after-write;namespace=test;set=accounts;module=guard;function=note_write
//! event-set:event=after-delete;namespace=test;set=accounts;module=guard;function=note_delete
//! event-set:event=timer;namespace=test;set=accounts;module=guard;function=tick
//! event-set:event=before-ship;namespace=test;set=accounts;module=guard;function=strip_secret
//! event-timer-set:namespace=test;set=accounts;key=alice;name=tick;in=1000
//! ```
//!
//! A `before-write` binding may carry a filter expression (`exp=` or `ael=`), evaluated against the record
//! the write would leave, so the function runs only on the records it is about.

#![no_std]

// `Error`, `Record`, `Write`, `Delete` and `Truncate` are not imported: the types in an `event!` arm are
// part of the macro's grammar, not names resolved here.
use aerospike_udf_sdk_wasm::{
    self as as_udf, event, Decision, Eviction, Expiry, Gate, ReadResult, ShipDecision, Value,
};

event! {
    /// The constraint and the stamp: a balance may not go negative, and every write that gets through is
    /// marked with where it came from and how many writes the record has seen.
    ///
    /// `before` is the record as it was (empty on a create), `after` the record the write would leave.
    /// Returning `Record` replaces `after`; `after.with_bins(out)` keeps every bin the write produced and
    /// adds or overrides the ones in `out`.
    on before_write
    fn guard_account(before: Record, after: Record, write: Write) -> Result<Decision, Error> {
        if let Some(balance) = after.int("balance")? {
            if balance < 0 {
                return Ok(Decision::Reject { code: 22, message: "balance may not go negative" });
            }
        }

        let out = as_udf::map(2)?;

        out.put("stamped", write.source().as_str())?;
        out.put("writes", before.int("writes")?.unwrap_or(0) + 1)?;

        Ok(Decision::Record { bins: after.with_bins(out)?, ttl: None })
    }

    /// A second handler on the same set, run before `guard_account` when bound with a lower `order`: it
    /// adds a bin the first one then sees in `after`.
    on before_write
    fn stamp_region(_before: Record, after: Record, _write: Write) -> Result<Decision, Error> {
        let out = as_udf::map(1)?;

        out.put("region", "eu")?;

        Ok(Decision::Record { bins: after.with_bins(out)?, ttl: None })
    }

    /// A record with `locked: true` cannot be deleted.
    on before_delete
    fn guard_delete(record: Record, _delete: Delete) -> Result<Gate, Error> {
        if matches!(record.bin("locked"), Ok(Value::Bool(true))) {
            return Ok(Gate::Refuse { code: 22, message: "record is locked" });
        }

        Ok(Gate::Allow)
    }

    /// Sets whose name starts with `keep` cannot be truncated.
    on before_truncate
    fn guard_truncate(truncate: Truncate) -> Result<Gate, Error> {
        let mut buf = [0u8; 64];

        if truncate.set(&mut buf)?.starts_with("keep") {
            return Ok(Gate::Refuse { code: 22, message: "this set is protected from truncation" });
        }

        Ok(Gate::Allow)
    }

    /// After a truncate took effect on this node: nothing to do here but be counted; a real handler would
    /// write the outbox.
    on after_truncate
    fn note_truncate(_truncate: Truncate) -> Result<(), Error> {
        Ok(())
    }

    /// Records marked `keep: true` outlive their TTL: each time the supervisor comes for one, it is kept
    /// for another hour. Everything else expires as it would have.
    on before_expire
    fn keep_marked(record: Record, _expire: Expire) -> Result<Expiry, Error> {
        if matches!(record.bin("keep"), Ok(Value::Bool(true))) {
            return Ok(Expiry::Keep { ttl: Some(3600) });
        }

        Ok(Expiry::Expire)
    }

    /// The same mark protects a record from eviction.
    on before_evict
    fn protect_marked(record: Record, _evict: Evict) -> Result<Eviction, Error> {
        if matches!(record.bin("keep"), Ok(Value::Bool(true))) {
            return Ok(Eviction::Protect);
        }

        Ok(Eviction::Evict)
    }

    /// Reads do not see `secret`, and carry `read_by` so a client can tell a rewritten record.
    on after_read
    fn hide_secret(record: Record, read: Read) -> Result<ReadResult, Error> {
        let out = as_udf::map(2)?;

        out.put("secret", Value::Nil)?;
        out.put("read_by", read.source().as_str())?;

        Ok(ReadResult::Record { bins: record.with_bins(out)? })
    }

    /// The once-per-node events: nothing to do here but be counted. A real handler would warm a cache,
    /// tell an external system which node holds what, or seed state at `binding-deployed`.
    on node_lifecycle
    fn note_lifecycle(_phase: Lifecycle) -> Result<(), Error> {
        Ok(())
    }

    on cluster_change
    fn note_cluster(_cluster: Cluster) -> Result<(), Error> {
        Ok(())
    }

    on partition_transfer
    fn note_transfer(_transfer: Transfer) -> Result<(), Error> {
        Ok(())
    }

    /// The after-events run off the write path, from a queue, with the record as it was and as it is. A
    /// handler here cannot change anything; it observes, and will feed the outbox once there is one.
    on after_write
    fn note_write(_before: Record, _after: Record, _write: Write) -> Result<(), Error> {
        Ok(())
    }

    on after_delete
    fn note_delete(_record: Record, _delete: Delete) -> Result<(), Error> {
        Ok(())
    }

    /// A timer handler runs as a record UDF on the record the timer was set on: it counts the tick and,
    /// up to three, re-arms itself. The first timer comes from `event-timer-set` or `timer_set` in a UDF.
    on timer
    fn tick(timer: Timer) -> Result<(), Error> {
        let ticks = match as_udf::bin("ticks") {
            Ok(v) => v.int()?,
            Err(_) => 0,
        } + 1;

        as_udf::set("ticks", ticks)?;
        as_udf::update()?;

        if timer.is("tick") && ticks < 3 {
            as_udf::timer_set("tick", 200)?;
        }

        Ok(())
    }

    /// Records ship to other DCs without their `secret` bin.
    on before_ship
    fn strip_secret(record: Record, _ship: Ship) -> Result<ShipDecision, Error> {
        if record.bin("secret").is_err() {
            return Ok(ShipDecision::Ship);
        }

        let out = as_udf::map(1)?;

        out.put("secret", Value::Nil)?;

        Ok(ShipDecision::Record { bins: record.with_bins(out)? })
    }
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

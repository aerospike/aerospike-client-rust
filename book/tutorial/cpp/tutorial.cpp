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

// The C++ side of the book's tutorials. The pages record-udfs/tutorial.md and events/tutorial.md include
// the code between the ANCHOR comments, so what the book shows is what compiles here.

// ANCHOR: setup
#include "aerospike_udf.hpp"
// ANCHOR_END: setup

// ANCHOR: visit
UDF_FUNCTION(visit, udf::Result<int64_t>, udf::Record& rec, int64_t by)
{
	int64_t n = rec.int_or("visits", 0) + by;   // 0 when the bin is absent

	UDF_TRY(rec.set("visits", n));              // staged, not yet written
	UDF_TRY(rec.save());                        // creates the record when there is none

	return n;
}
// ANCHOR_END: visit

// ANCHOR: withdraw
UDF_FUNCTION(withdraw, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
{
	// An error when the bin is absent, and another when it is not an integer.
	int64_t balance = UDF_TRY(UDF_TRY(rec.bin("balance")).as_int());

	if (amount > balance) return udf::error(AS_UDF_REFUSED, "insufficient funds");

	UDF_TRY(rec.set("balance", balance - amount));
	UDF_TRY(rec.save());

	return balance - amount;
}
// ANCHOR_END: withdraw

// ANCHOR: rename
UDF_FUNCTION(rename, udf::Result<bool>, udf::Record& rec, udf::Text<64> name)
{
	udf::Text<64> owner = UDF_TRY(UDF_TRY(rec.bin("owner")).as_text<64>());   // copied out of the record

	if (owner == name.view()) return false;

	UDF_TRY(rec.set("owner", name.view()));
	UDF_TRY(rec.save());

	return true;
}
// ANCHOR_END: rename

// ANCHOR: total
UDF_FUNCTION(total, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t sum = 0;

	for (udf::Value payment : rec.list("history")) {
		sum += UDF_TRY(payment.as_int());
	}

	return sum;
}
// ANCHOR_END: total

// ANCHOR: limit_for
UDF_FUNCTION(limit_for, udf::Result<int64_t>, const udf::Record& rec, udf::Text<64> kind)
{
	udf::Value limits = UDF_TRY(rec.bin("limits"));
	udf::Optional<udf::Value> limit = limits.get(kind.view());

	if (!limit) return 0;   // no such key

	return limit->as_int();
}
// ANCHOR_END: limit_for

// ANCHOR: limits_total
UDF_FUNCTION(limits_total, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t sum = 0;

	for (udf::Entry e : rec.map("limits")) {
		sum += UDF_TRY(e.value.as_int());
	}

	return sum;
}
// ANCHOR_END: limits_total

// ANCHOR: log_payment
UDF_FUNCTION(log_payment, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
{
	udf::Value stored = UDF_TRY(rec.bin("history"));

	int64_t count = (int64_t)stored.size() + 1;
	udf::List out((int32_t)count);

	out.add_all(stored).add(amount);   // the stored elements, then the new one

	UDF_TRY(rec.set("history", out));
	UDF_TRY(rec.save());

	return count;
}
// ANCHOR_END: log_payment

// ANCHOR: summary
UDF_FUNCTION(summary, udf::Result<udf::Map>, const udf::Record& rec)
{
	udf::Map out(5);

	out.put("owner", rec.get("owner").value_or(udf::Value()))   // a stored value goes in as it is
	    .put("balance", rec.int_or("balance", 0))
	    .put("generation", rec.generation())
	    .put("bins", rec.bin_count())
	    .put("ttl", rec.ttl());

	UDF_TRY(out.result());

	return out;
}
// ANCHOR_END: summary

// ANCHOR: renew
UDF_FUNCTION(renew, udf::Result<int64_t>, udf::Record& rec, int64_t seconds)
{
	int64_t n = rec.int_or("renewals", 0) + 1;

	UDF_TRY(rec.set("renewals", n));
	UDF_TRY(rec.set_ttl((int32_t)seconds));   // takes effect with the save
	UDF_TRY(rec.save());

	return n;
}
// ANCHOR_END: renew

// ANCHOR: clear_note
UDF_FUNCTION(clear_note, udf::Result<void>, udf::Record& rec)
{
	UDF_TRY(rec.remove("note"));   // staged, like a set
	UDF_TRY(rec.save());

	return {};
}
// ANCHOR_END: clear_note

// ANCHOR: close
UDF_FUNCTION(close, udf::Result<void>, udf::Record& rec)
{
	UDF_TRY(rec.delete_record());   // no save: the delete is the write

	return {};
}
// ANCHOR_END: close

// ANCHOR: no_overdraft
UDF_ON_BEFORE_WRITE(no_overdraft)
{
	(void)before; (void)write;

	udf::Optional<int64_t> balance = after.as_int("balance");   // empty when absent or nil

	if (balance && *balance < 0) {
		return udf::Decision::reject(22, "balance may not go negative");
	}

	return udf::Decision::accept();
}
// ANCHOR_END: no_overdraft

// ANCHOR: stamp
UDF_ON_BEFORE_WRITE(stamp)
{
	udf::Map out(2);

	out.put("writes", before.as_int("writes").value_or(0) + 1)
	    .put("source", write.source_view());

	// Every bin the write produced, with the two above on top.
	return udf::Decision::record(after.with_bins(out));
}
// ANCHOR_END: stamp

// ANCHOR: keep_locked
UDF_ON_BEFORE_DELETE(keep_locked)
{
	(void)del;

	if (record.as_bool("locked").value_or(false)) {
		return udf::Gate::refuse(22, "record is locked");
	}

	return udf::Gate::allow();
}
// ANCHOR_END: keep_locked

// ANCHOR: hide_secret
UDF_ON_AFTER_READ(hide_secret)
{
	(void)read;

	if (!record.has("secret")) return udf::ReadResult::unchanged();

	udf::Map out(1);

	out.put_null("secret");   // nil in an answer: no such bin

	return udf::ReadResult::record(record.with_bins(out));
}
// ANCHOR_END: hide_secret

// ANCHOR: keep_marked
UDF_ON_BEFORE_EXPIRE(keep_marked)
{
	(void)expire;

	if (record.as_bool("keep").value_or(false)) {
		return udf::Expiry::keep(3600);
	}

	return udf::Expiry::expire();
}
// ANCHOR_END: keep_marked

// ANCHOR: tick
UDF_ON_TIMER(tick)
{
	(void)timer;

	int64_t n = record.int_or("ticks", 0) + 1;   // a timer handler is handed the record, writable

	UDF_TRY(record.set("ticks", n));
	UDF_TRY(record.save());

	if (n < 3) {
		udf::timer_set("tick", 200);   // run again in 200 ms
	}

	return {};
}
// ANCHOR_END: tick

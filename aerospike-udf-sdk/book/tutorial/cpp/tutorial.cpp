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

// The C++ side of the book's tutorials. The page record-udfs/tutorial.md includes the code between the
// ANCHOR comments, so what the book shows is what compiles here.

// ANCHOR: setup
#include "aerospike_udf.hpp"
// ANCHOR_END: setup

// ANCHOR: visit
UDF_FUNCTION(visit, udf::Result<int64_t>, udf::Record& rec, int64_t by)
{
	int64_t n = UDF_TRY(rec.int_or("visits", 0)) + by;   // 0 when the bin is absent, an error when it is not an int

	UDF_TRY(rec.set("visits", n));              // staged, not yet written
	UDF_TRY(rec.save());                        // creates the record when there is none

	return n;
}
// ANCHOR_END: visit

// ANCHOR: withdraw
UDF_FUNCTION(withdraw, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
{
	// An error when the bin is absent, another when it is not an integer, and one when it is nil.
	int64_t balance = UDF_TRY(udf::required(UDF_TRY(rec.bin("balance")).as_int(), "withdraw: balance is nil"));

	if (amount > balance) return udf::error(AS_UDF_REFUSED, "insufficient funds");

	UDF_TRY(rec.set("balance", balance - amount));
	UDF_TRY(rec.save());

	return balance - amount;
}
// ANCHOR_END: withdraw

// ANCHOR: rename
UDF_FUNCTION(rename, udf::Result<bool>, udf::Record& rec, udf::Text<64> name)
{
	udf::Text<64> owner = UDF_TRY(udf::required(UDF_TRY(rec.bin("owner")).as_string<64>(), "rename: owner is nil"));   // copied out of the record

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

	// Nothing when the bin is absent, an error when it is not a list.
	if (udf::Optional<udf::ListRange> history = UDF_TRY(rec.list("history"))) {
		for (udf::Value payment : *history) {
			sum += UDF_TRY(udf::required(payment.as_int(), "total: a nil payment"));
		}
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

	return udf::required(limit->as_int(), "limit_for: the limit is nil");
}
// ANCHOR_END: limit_for

// ANCHOR: limits_total
UDF_FUNCTION(limits_total, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t sum = 0;

	if (udf::Optional<udf::MapRange> limits = UDF_TRY(rec.map("limits"))) {
		for (udf::Entry e : *limits) {
			sum += UDF_TRY(udf::required(e.value.as_int(), "limits_total: a nil limit"));
		}
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
	    .put("balance", UDF_TRY(rec.int_or("balance", 0)))
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
	int64_t n = UDF_TRY(rec.int_or("renewals", 0)) + 1;

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

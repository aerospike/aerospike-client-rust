/*
 * Copyright 2015-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Guards in C++: the same handlers as aerospike-udf-sdk-wasm/examples/guard - a before-write that refuses a
// negative balance and stamps every write that gets through, a before-delete lock, a truncate fence, and
// a timer that counts ticks and re-arms itself up to three.
//
// The UDF_ON_* macros are the exports: each defines the handler with the kind's parameters, the export
// that decodes the event and delivers the answer, and the signature event-set checks.

#include "aerospike_udf.hpp"

static constexpr udf::Bin TICKS{"ticks"};

// Refuses a negative balance; otherwise stamps the write's source and counts it, keeping every other bin.
UDF_ON_BEFORE_WRITE(guard_account)
{
	if (udf::Optional<int64_t> balance = after.as_int("balance"); balance && *balance < 0) {
		return udf::Decision::reject(22, "balance may not go negative");
	}

	udf::Map out(4);

	out.put("stamped", write.source_view()).put("writes", before.as_int("writes").value_or(0) + 1);

	return udf::Decision::record(after.with_bins(out));
}

UDF_ON_BEFORE_DELETE(guard_delete)
{
	if (record.as_bool("locked").value_or(false)) {
		return udf::Gate::refuse(22, "record is locked");
	}

	return udf::Gate::allow();
}

UDF_ON_BEFORE_TRUNCATE(guard_truncate)
{
	if (truncate.set_view().starts_with("keep")) {
		return udf::Gate::refuse(22, "this set is protected from truncation");
	}

	return udf::Gate::allow();
}

// The timer handler runs as a record UDF: the record functions work.
UDF_ON_TIMER(tick)
{
	int64_t n = TICKS.int_or(0) + 1;

	UDF_TRY(TICKS.set(n));
	UDF_TRY(udf::commit());

	if (n < 3) {
		udf::timer_set("tick", 200);
	}

	return {};
}

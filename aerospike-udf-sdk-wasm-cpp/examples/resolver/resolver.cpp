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

// An XDR merge, the same as the Rust, C, Go and Java resolvers: visits take the larger count, tags unite
// in LUT order, the profile is whichever side wrote it last, a bin deleted by the later writer stays
// deleted, and everything else is the newer side's. See aerospike-udf-sdk-wasm/examples/resolver for why
// each rule is the way it is; this is that module line for line, over udf::Pair and udf::Both.

#include "aerospike_udf.hpp"

// A bin one side deleted stays deleted when the delete is the later write for that bin: a nil in the
// returned record is "no such bin". Without this, the other side's copy would come back through
// with_newer below - the resurrection every merge has to guard against.
static void apply_deletes(const udf::Pair& pair, udf::Map& out, const udf::EventRecord& side, bool by_remote)
{
	if (!side.deleted_any()) {
		return;
	}

	for (udf::Value name : udf::Value(side.deleted).list()) {
		udf::Result<udf::Text<64>> bin = name.as_text<64>();

		if (bin && pair.remote_newer(bin->view()) == by_remote) {
			out.put_null(bin->view());
		}
	}
}

UDF_ON_XDR_MERGE(merge_account)
{
	udf::Pair pair = conflict.pair();
	udf::Map out(8);

	// A count each DC advanced on its own: both sides' increments are real, so the merge is the larger
	// of the two, not the sum (the sum would double-count the visits both already knew of).
	if (udf::Optional<int64_t> visits = pair.as_int("visits").max()) {
		out.put("visits", *visits);
	}

	// A set each DC added to: the union. Its order is part of the result, so it is the order both DCs
	// agree on - the older side's tags, then the newer side's new ones - never "mine first".
	udf::Both<udf::Value> sides = pair.list("tags");
	udf::Optional<udf::Value> older = sides.remote_newer() ? sides.local : sides.remote;
	udf::Optional<udf::Value> newer = sides.remote_newer() ? sides.remote : sides.local;
	udf::List tags(16);

	if (older) {
		tags.add_all(*older);
	}

	if (newer) {
		for (udf::Value tag : newer->list()) {
			udf::Text<64> text = UDF_TRY(tag.as_text<64>());

			if (!older || !udf::contains(*older, text.view())) {
				tags.add(tag);
			}
		}
	}

	out.put("tags", tags);

	// A document one side rewrote whole: last writer wins, by LUT, which both DCs agree on.
	if (udf::Optional<udf::Value> profile = pair.bin("profile").newer()) {
		out.put("profile", *profile);
	}

	apply_deletes(pair, out, current, false);
	apply_deletes(pair, out, incoming, true);

	// Everything else is the newer side's - the policy's answer for a bin this handler has no opinion on.
	// `current.with_bins(out)` would read more naturally and be wrong: it prefers a side, and the other
	// DC prefers the other side.
	return udf::Resolution::record(pair.with_newer(out));
}

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

// A record of registers, in C++: every bin is a last-writer-wins register, stamped by the server. The same
// module as aerospike-udf-sdk/wasm-rust/examples/registers.
//
// examples/lwwset keeps a timestamp per element inside the record, written by its own UDFs. The server
// already keeps one per bin: when the bin was last written, and by which cluster. An xdr-merge handler
// reads both from either side, with
// bin_last_update_time(name) and bin_source_id(name), and that pair is a register's stamp: the later
// write wins, and of two writes in the same millisecond, the one from the cluster with the larger id.
//
// So a register has no write UDF. A client writes theme, lang or email with a plain put and deletes one
// by writing nil to it; the server stamps the bin, and merge_settings picks the later side bin by bin. A
// deleted bin keeps its stamp and is listed in deleted_bins(), so a delete that is the later write stays
// a delete, and one that is the earlier write does not undo what came after it.
//
// A set bound to a handler never uses the server's bin convergence, so the handler does that part itself.
// It is here for the one bin that is not a register: seen, the highest message number this user has read,
// on any device. Its merge is the larger number, whichever side wrote last, which no policy that picks a
// side can express. It has the module's one write UDF, saw(n), which never lowers it: a merge can only keep
// a maximum that the writes kept too.
//
//   event-set:event=xdr-merge;namespace=test;set=settings;module=registers;function=merge_settings;on-failure=abandon

#include "aerospike_udf.hpp"

static constexpr udf::StrView SEEN{"seen"};
static constexpr int NAME_MAX = 32;

using Name = udf::Text<NAME_MAX>;

// A register's stamp on one side: when the bin was last written there, and by which cluster. A side that
// has nothing to say about the bin has no stamp, which is less than any stamp.
struct Stamp {
	bool known = false;
	int64_t time = 0;
	int64_t source = 0;

	// Time first, then source id; no stamp is below every stamp and not above itself.
	bool operator>(const Stamp& o) const
	{
		if (!known || !o.known) return known && !o.known;
		return time > o.time || (time == o.time && source > o.source);
	}
};

// Whether this side deleted the bin and still remembers that it did.
static udf::Result<bool> deleted(const udf::EventRecord& side, udf::StrView name)
{
	for (udf::Value bin : side.deleted_bins()) {
		Name n = UDF_TRY(udf::required(bin.as_string<NAME_MAX>(), "registers: a nil bin name"));

		if (n == name) return true;
	}

	return false;
}

// The side's stamp for a bin it has or has deleted; no stamp otherwise.
static udf::Result<Stamp> stamp(const udf::EventRecord& side, udf::StrView name)
{
	if (!side.has(name) && !UDF_TRY(deleted(side, name))) return Stamp{};

	return Stamp{true, side.bin_last_update_time(name), side.bin_source_id(name)};
}

// The merge: seen is the larger number; every other bin is the side that wrote it last.
UDF_ON_XDR_MERGE(merge_settings)
{
	udf::Map out(16);
	udf::Optional<int64_t> mine = UDF_TRY(current.as_int(SEEN));
	udf::Optional<int64_t> theirs = UDF_TRY(incoming.as_int(SEEN));

	if (mine || theirs) {
		out.put(SEEN, mine && (!theirs || *mine > *theirs) ? *mine : *theirs);
	}

	const udf::EventRecord* sides[] = {&current, &incoming};

	for (const udf::EventRecord* side : sides) {
		const udf::ListRange lists[] = {side->bin_names(), side->deleted_bins()};

		for (const udf::ListRange& names : lists) {
			for (udf::Value bin : names) {
				Name name = UDF_TRY(udf::required(bin.as_string<NAME_MAX>(), "registers: a nil bin name"));
				Stamp here = UDF_TRY(stamp(current, name.view()));

				// The first pass decided every bin the current side knows of.
				if (name == SEEN || (side == &incoming && here.known)) continue;

				Stamp there = UDF_TRY(stamp(incoming, name.view()));
				const udf::EventRecord& winner = there > here ? incoming : current;

				// A nil in the answer is "no such bin": the winner's delete, kept.
				if (udf::Optional<udf::Value> v = winner.get(name.view())) {
					out.put(name.view(), *v);
				}
				else {
					out.put_nil(name.view());
				}
			}
		}
	}

	return udf::Resolution::record(out);
}

// Records that message n was read. Returns the highest number read so far.
UDF_FUNCTION(saw, udf::Result<int64_t>, udf::Record& rec, int64_t n)
{
	int64_t seen = UDF_TRY(rec.int_or(SEEN, 0));

	if (n > seen) {
		UDF_TRY(rec.set(SEEN, n));
		UDF_TRY(rec.save());
	}

	return n > seen ? n : seen;
}

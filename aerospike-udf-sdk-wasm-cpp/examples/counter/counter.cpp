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

// The smallest CRDT in C++: a grow-only counter, merged across data centres by the xdr-merge event. The
// same module as aerospike-udf-sdk-wasm/examples/counter.
//
// One bin, counts, holds a map of site -> count. Each data centre adds only to its own entry
// (increment("dc1", 3)), and the counter's value is the sum of every entry (value()). When a write from one
// site arrives at the other, merge_counter keeps, for every site, the larger of the two counts. A site's
// count only ever grows, so the larger one is always the later one: the merge needs no clock, no LUT and
// no writer id, and it is commutative, idempotent and associative by construction, which the server's dry
// run checks on real bytes.
//
//   event-set:event=xdr-merge;namespace=test;set=counters;module=counter;function=merge_counter;on-failure=abandon

#include "aerospike_udf.hpp"

static constexpr udf::StrView COUNTS{"counts"};
static constexpr int MAX_SITES = 64;
static constexpr int SITE_MAX = 32;

using Site = udf::Text<SITE_MAX>;

// The per-site counts gathered from one or both sides: for every site, the larger count seen. Fixed
// storage, since a module has no allocator.
struct Table {
	Site sites[MAX_SITES];
	int64_t counts[MAX_SITES];
	int n = 0;

	int find(udf::StrView site) const
	{
		for (int i = 0; i < n; i++) {
			if (sites[i] == site) return i;
		}
		return -1;
	}

	udf::Result<void> offer(udf::StrView site, int64_t count)
	{
		int i = find(site);

		if (i >= 0) {
			if (count > counts[i]) counts[i] = count;
			return {};
		}

		if (n == MAX_SITES) return udf::error(AS_UDF_ERR, "too many sites");

		sites[n].assign(site);
		counts[n] = count;
		n++;

		return {};
	}

	// Every entry of a stored counts map.
	udf::Result<void> offer_all(const udf::Value& stored)
	{
		for (udf::Entry e : stored.map()) {
			Site site = UDF_TRY(udf::required(e.key.as_string<SITE_MAX>(), "counter: a nil site"));
			int64_t count = UDF_TRY(udf::required(e.value.as_int(), "counter: a nil count"));

			UDF_TRY(offer(site.view(), count));
		}

		return {};
	}

	int64_t total() const
	{
		int64_t sum = 0;
		for (int i = 0; i < n; i++) sum += counts[i];
		return sum;
	}

	// The counts as a built map, for a bin or a resolution.
	udf::Map to_map() const
	{
		udf::Map m(n > 0 ? n : 1);
		for (int i = 0; i < n; i++) m.put(sites[i].view(), counts[i]);
		return m;
	}
};

// The counts of a record, whichever record it is: the one a UDF was called on, or one side of a merge. Both
// are read through udf::ReadRecord, so one function serves the UDFs and the handler.
template <class R>
static udf::Result<void> load(const udf::ReadRecord<R>& rec, Table& t)
{
	if (udf::Optional<udf::Value> v = rec.get(COUNTS)) {
		return t.offer_all(*v);
	}

	return {};
}

// The merge: for every site, the larger of the two counts. Every other bin is the incoming one's.
UDF_ON_XDR_MERGE(merge_counter)
{
	Table t;

	const udf::EventRecord* sides[] = {&current, &incoming};

	for (const udf::EventRecord* side : sides) {
		UDF_TRY(load(*side, t));
	}

	udf::Map out(2);

	out.put("counts", t.to_map());

	return udf::Resolution::record(incoming.with_bins(out));
}

// Adds n to this site's count. Returns the counter's value. The site is a Text<32>, so a longer name is
// rejected before the body runs, as the Rust module's Str<32> rejects it.
UDF_FUNCTION(increment, udf::Result<int64_t>, udf::Record& rec, Site site, int64_t n)
{
	Table t;

	UDF_TRY(load(rec, t));

	int i = t.find(site.view());

	if (i >= 0) {
		t.counts[i] += n;
	}
	else {
		UDF_TRY(t.offer(site.view(), n));
	}

	UDF_TRY(rec.set(COUNTS, t.to_map()));
	UDF_TRY(rec.save());

	return t.total();
}

// The counter's value: the sum of every site's count.
UDF_FUNCTION(value, udf::Result<int64_t>, const udf::Record& rec)
{
	Table t;

	UDF_TRY(load(rec, t));

	return t.total();
}

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

// A counter that can go down, in C++: the PN-Counter, two grow-only counters glued together, merged across
// data centres by the xdr-merge event. The same module as aerospike-udf-sdk-wasm/examples/pncounter.
//
// The grow-only counter cannot subtract: a decrement would shrink a site's entry, and the per-site maximum
// would throw the shrink away. So this module never subtracts. The record keeps two maps of site -> amount,
// inc and dec, and both only ever grow: increment("dc1", 3) adds to inc["dc1"], decrement("dc1", 1) adds to
// dec["dc1"], and the counter's value is the sum of inc minus the sum of dec. The merge is the grow-only
// counter's, run over each map on its own: for every site, the larger amount.
//
// What it cannot do is hold a floor: two sites that each see a stock of one and each take one merge to
// minus one. A bound is not a property a merge can keep.
//
//   event-set:event=xdr-merge;namespace=test;set=stock;module=pncounter;function=merge_pncounter;on-failure=abandon

#include "aerospike_udf.hpp"

static constexpr udf::Bin INC{"inc"};
static constexpr udf::Bin DEC{"dec"};
static constexpr int MAX_SITES = 64;
static constexpr int SITE_MAX = 32;

using Site = udf::Text<SITE_MAX>;

// The per-site amounts of one map, gathered from one or both sides: for every site, the larger amount
// seen. Fixed storage, since a module has no allocator.
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

	// Keeps the larger amount for a site.
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

	// Every entry of a stored map.
	udf::Result<void> offer_all(const udf::Value& stored)
	{
		for (udf::Entry e : stored.map()) {
			Site site = UDF_TRY(e.key.as_text<SITE_MAX>());
			int64_t count = UDF_TRY(e.value.as_int());

			UDF_TRY(offer(site.view(), count));
		}

		return {};
	}

	// Adds n to a site's amount, creating the site if it is new.
	udf::Result<void> add(udf::StrView site, int64_t n)
	{
		int i = find(site);

		if (i >= 0) {
			counts[i] += n;
			return {};
		}

		return offer(site, n);
	}

	int64_t total() const
	{
		int64_t sum = 0;
		for (int i = 0; i < n; i++) sum += counts[i];
		return sum;
	}

	// The amounts as a built map, for a bin or a resolution.
	udf::Map to_map() const
	{
		udf::Map m(n > 0 ? n : 1);
		for (int i = 0; i < n; i++) m.put(sites[i].view(), counts[i]);
		return m;
	}
};

// One of the record's maps.
static udf::Result<void> load(const udf::Bin& bin, Table& t)
{
	t.n = 0;

	if (udf::Optional<udf::Value> v = bin.get()) {
		return t.offer_all(*v);
	}

	return {};
}

// The merge: for every site of each map, the larger amount. Every other bin is the incoming one's.
UDF_ON_XDR_MERGE(merge_pncounter)
{
	static Table t;
	const char* bins[] = {"inc", "dec"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		t.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(t.offer_all(*stored));
			}
		}

		out.put(bin, t.to_map());
	}

	return udf::Resolution::record(incoming.with_bins(out));
}

// The counter's value: everything added minus everything taken.
static udf::Result<int64_t> value_of()
{
	static Table t;

	UDF_TRY(load(INC, t));
	int64_t added = t.total();
	UDF_TRY(load(DEC, t));

	return added - t.total();
}

// Adds n to this site's entry of one map and commits; answers the counter's value.
static udf::Result<int64_t> grow(const udf::Bin& bin, const Site& site, int64_t n)
{
	if (n < 0) return udf::error(AS_UDF_ERR, "the amount must not be negative: an entry only grows");

	static Table t;

	UDF_TRY(load(bin, t));
	UDF_TRY(t.add(site.view(), n));
	UDF_TRY(bin.set(t.to_map()));
	UDF_TRY(udf::commit());

	return value_of();
}

// Adds n to this site's increments. Returns the counter's value. The site is a Text<32>, so a longer name
// is rejected before the body runs, as the Rust module's Str<32> rejects it.
UDF_FUNCTION(increment, udf::Result<int64_t>, Site site, int64_t n)
{
	return grow(INC, site, n);
}

// Adds n to this site's decrements. Returns the counter's value.
UDF_FUNCTION(decrement, udf::Result<int64_t>, Site site, int64_t n)
{
	return grow(DEC, site, n);
}

// The counter's value: the sum of every site's increments minus the sum of every site's decrements.
UDF_FUNCTION(value, udf::Result<int64_t>)
{
	return value_of();
}

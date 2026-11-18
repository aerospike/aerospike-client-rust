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

// A set with re-adds, in C++: the LWW-Element-Set, merged across data centres by the xdr-merge event. The
// same module as aerospike-udf-sdk-wasm/examples/lwwset.
//
// Two map bins, addedAt and removedAt, hold a timestamp per tag, the server's clock in milliseconds at the
// write. A tag is a member when its add is later than its removal; at equal stamps the removal wins.
// Adding a tag back after a removal is just a later stamp. The merge is the PN-Counter's: for every tag of
// each map, the larger stamp. The merge never reads the clock (the server refuses it inside a handler);
// only the write UDFs stamp, bumping past the tag's last stamp when the clock has not moved.
//
//   event-set:event=xdr-merge;namespace=test;set=tags;module=lwwset;function=merge_lwwset;on-failure=abandon

#include "aerospike_udf.hpp"

static constexpr udf::StrView ADDED_AT{"addedAt"};
static constexpr udf::StrView REMOVED_AT{"removedAt"};
static constexpr int MAX_TAGS = 64;
static constexpr int TAG_MAX = 32;

using Tag = udf::Text<TAG_MAX>;

// The stamps of one map, gathered from one or both sides: for every tag, the later stamp seen.
struct Stamps {
	Tag tags[MAX_TAGS];
	int64_t at[MAX_TAGS];
	int n = 0;

	int find(udf::StrView tag) const
	{
		for (int i = 0; i < n; i++) {
			if (tags[i] == tag) return i;
		}
		return -1;
	}

	// The tag's stamp, or 0 for none.
	int64_t get(udf::StrView tag) const
	{
		int i = find(tag);
		return i >= 0 ? at[i] : 0;
	}

	// Keeps the later stamp for a tag.
	udf::Result<void> offer(udf::StrView tag, int64_t stamp)
	{
		int i = find(tag);

		if (i >= 0) {
			if (stamp > at[i]) at[i] = stamp;
			return {};
		}

		if (n == MAX_TAGS) return udf::error(AS_UDF_ERR, "too many tags");

		tags[n].assign(tag);
		at[n] = stamp;
		n++;

		return {};
	}

	// Every entry of a stored map.
	udf::Result<void> offer_all(const udf::Value& stored)
	{
		for (udf::Entry e : stored.map()) {
			Tag tag = UDF_TRY(e.key.as_text<TAG_MAX>());
			int64_t stamp = UDF_TRY(e.value.as_int());

			UDF_TRY(offer(tag.view(), stamp));
		}

		return {};
	}

	udf::Map to_map() const
	{
		udf::Map m(n > 0 ? n : 1);
		for (int i = 0; i < n; i++) m.put(tags[i].view(), at[i]);
		return m;
	}
};

// The merge: for every tag of each map, the later stamp. Every other bin is the incoming one's.
UDF_ON_XDR_MERGE(merge_lwwset)
{
	static Stamps s;
	const char* bins[] = {"addedAt", "removedAt"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		s.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(s.offer_all(*stored));
			}
		}

		out.put(bin, s.to_map());
	}

	return udf::Resolution::record(incoming.with_bins(out));
}

static Stamps added;
static Stamps removed;

// Both of the record's maps.
static udf::Result<void> load(const udf::Record& rec)
{
	added.n = 0;
	removed.n = 0;

	if (udf::Optional<udf::Value> v = rec.get(ADDED_AT)) UDF_TRY(added.offer_all(*v));
	if (udf::Optional<udf::Value> v = rec.get(REMOVED_AT)) UDF_TRY(removed.offer_all(*v));

	return {};
}

// Whether a tag is a member: added, and later than removed. A tie goes to the removal.
static bool is_member(udf::StrView tag)
{
	int64_t a = added.get(tag);
	return a > 0 && a > removed.get(tag);
}

// The members, in order, as a built list.
static udf::Result<udf::List> members_of(const udf::Record& rec)
{
	UDF_TRY(load(rec));

	// The stored map comes out key-ordered, but a tag added on this site sits at the end: pick the
	// smallest remaining each time, so the answer is sorted.
	bool done[MAX_TAGS] = {};
	udf::List l(added.n > 0 ? added.n : 1);

	for (int k = 0; k < added.n; k++) {
		int next = -1;

		for (int i = 0; i < added.n; i++) {
			if (!done[i] && (next < 0 || added.tags[i].view() < added.tags[next].view())) next = i;
		}

		done[next] = true;

		if (is_member(added.tags[next].view())) l.add(added.tags[next].view());
	}

	return l;
}

// Stamps the tag in one of the maps and saves: the clock, or one past the tag's last stamp when the
// clock has not moved since.
static udf::Result<void> stamp(udf::Record& rec, udf::StrView bin, Stamps& into, udf::StrView tag)
{
	UDF_TRY(load(rec));

	int64_t last = added.get(tag) > removed.get(tag) ? added.get(tag) : removed.get(tag);
	int64_t now = udf::now();
	int64_t at = now > last + 1 ? now : last + 1;

	UDF_TRY(into.offer(tag, at));
	UDF_TRY(rec.set(bin, into.to_map()));

	return rec.save();
}

// Adds a tag, or adds it back. Returns the members. The tag is a Text<32>, so a longer one is rejected
// before the body runs, as the Rust module's Str<32> rejects it.
UDF_FUNCTION(add, udf::Result<udf::List>, udf::Record& rec, Tag tag)
{
	UDF_TRY(stamp(rec, ADDED_AT, added, tag.view()));

	return members_of(rec);
}

// Removes a tag that is a member. Returns the members.
UDF_FUNCTION(remove, udf::Result<udf::List>, udf::Record& rec, Tag tag)
{
	UDF_TRY(load(rec));

	if (!is_member(tag.view())) return udf::error(AS_UDF_ERR, "not a member");

	UDF_TRY(stamp(rec, REMOVED_AT, removed, tag.view()));

	return members_of(rec);
}

// The members, in order.
UDF_FUNCTION(members, udf::Result<udf::List>, const udf::Record& rec)
{
	return members_of(rec);
}

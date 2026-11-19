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

// A set with removals, in C++: the 2P-Set, merged across data centres by the xdr-merge event. The same
// module as aerospike-udf-sdk-wasm/examples/twopset.
//
// Two list bins, added and removed, both only ever grow; a tag is a member when it is in added and not in
// removed; the merge takes the union of each. A tag in removed is a tombstone: it beats a concurrent add
// on the other side, and it stays forever, which is why a removed tag can never come back. Both lists are
// kept sorted and without duplicates, so the same set is always the same bytes.
//
//   event-set:event=xdr-merge;namespace=test;set=tags;module=twopset;function=merge_twopset;on-failure=abandon

#include "aerospike_udf.hpp"

static constexpr udf::StrView ADDED{"added"};
static constexpr udf::StrView REMOVED{"removed"};
static constexpr int MAX_TAGS = 64;
static constexpr int TAG_MAX = 32;

using Tag = udf::Text<TAG_MAX>;

// A sorted set of tags with no duplicates, gathered from one or both sides. Fixed storage, since a module
// has no allocator.
struct Tags {
	Tag tags[MAX_TAGS];
	int n = 0;

	// Where the tag is, or where it would go.
	int position(udf::StrView tag, bool& found) const
	{
		for (int i = 0; i < n; i++) {
			if (tags[i].view() == tag) {
				found = true;
				return i;
			}
			if (tag < tags[i].view()) {
				found = false;
				return i;
			}
		}
		found = false;
		return n;
	}

	bool contains(udf::StrView tag) const
	{
		bool found;
		position(tag, found);
		return found;
	}

	// Adds a tag in order; nothing when it is there already.
	udf::Result<void> insert(udf::StrView tag)
	{
		bool found;
		int at = position(tag, found);

		if (found) return {};
		if (n == MAX_TAGS) return udf::error(AS_UDF_ERR, "too many tags");

		for (int i = n; i > at; i--) tags[i] = tags[i - 1];
		tags[at].assign(tag);
		n++;

		return {};
	}

	// Every item of a stored list.
	udf::Result<void> offer_all(const udf::Value& stored)
	{
		for (udf::Value item : stored.list()) {
			Tag tag = UDF_TRY(udf::required(item.as_string<TAG_MAX>(), "twopset: a nil tag"));

			UDF_TRY(insert(tag.view()));
		}

		return {};
	}

	// The tags not in except, as a built list, in order.
	udf::List to_list(const Tags* except) const
	{
		udf::List l(n > 0 ? n : 1);
		for (int i = 0; i < n; i++) {
			if (except == nullptr || !except->contains(tags[i].view())) l.add(tags[i].view());
		}
		return l;
	}
};

// One of the record's lists into a table.
static udf::Result<void> load(const udf::Record& rec, udf::StrView bin, Tags& t)
{
	t.n = 0;

	if (udf::Optional<udf::Value> v = rec.get(bin)) {
		return t.offer_all(*v);
	}

	return {};
}

// The merge: the union of each list. Every other bin is the incoming one's.
UDF_ON_XDR_MERGE(merge_twopset)
{
	static Tags t;
	const char* bins[] = {"added", "removed"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		t.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(t.offer_all(*stored));
			}
		}

		out.put(bin, t.to_list(nullptr));
	}

	return udf::Resolution::record(incoming.with_bins(out));
}

static Tags added_tags;
static Tags removed_tags;

// The members: added and not removed.
static udf::Result<udf::List> members_of(const udf::Record& rec)
{
	UDF_TRY(load(rec, ADDED, added_tags));
	UDF_TRY(load(rec, REMOVED, removed_tags));

	return added_tags.to_list(&removed_tags);
}

// Adds a tag to one of the lists and saves.
static udf::Result<void> grow(udf::Record& rec, udf::StrView bin, Tags& t, udf::StrView tag)
{
	UDF_TRY(load(rec, bin, t));

	if (t.contains(tag)) return {};

	UDF_TRY(t.insert(tag));
	UDF_TRY(rec.set(bin, t.to_list(nullptr)));

	return rec.save();
}

// Adds a tag. Returns the members. A tag removed before stays removed. The tag is a Text<32>, so a longer
// one is rejected before the body runs, as the Rust module's Str<32> rejects it.
UDF_FUNCTION(add, udf::Result<udf::List>, udf::Record& rec, Tag tag)
{
	UDF_TRY(grow(rec, ADDED, added_tags, tag.view()));

	return members_of(rec);
}

// Removes a tag that is a member, forever. Returns the members.
UDF_FUNCTION(remove, udf::Result<udf::List>, udf::Record& rec, Tag tag)
{
	UDF_TRY(load(rec, ADDED, added_tags));
	UDF_TRY(load(rec, REMOVED, removed_tags));

	if (!added_tags.contains(tag.view()) || removed_tags.contains(tag.view())) {
		return udf::error(AS_UDF_ERR, "not a member");
	}

	UDF_TRY(grow(rec, REMOVED, removed_tags, tag.view()));

	return members_of(rec);
}

// The members, in order.
UDF_FUNCTION(members, udf::Result<udf::List>, const udf::Record& rec)
{
	return members_of(rec);
}

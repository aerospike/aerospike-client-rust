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

// The C++ counterpart of the Rust SDK's test fixture (aerospike-udf-sdk-wasm/examples/udf): one function
// per corner of the record API that the Rust SDK has and the client tests check, so that
// tests/src/udf_cpp.rs can hold the C++ SDK to the same surface. Nothing here is an example to copy; it
// is the SDK's own coverage.

#include "aerospike_udf.hpp"

static constexpr const char* GEO = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";

// The sum of the integers in list bin l, ranged over; any other element fails the call.
UDF_FUNCTION(sum_list, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t total = 0;

	for (udf::Value v : rec.list("l")) {
		total += UDF_TRY(v.as_int());
	}

	return total;
}

// Everything the record knows about itself, as a map.
UDF_FUNCTION(describe, udf::Result<udf::Map>, const udf::Record& rec)
{
	udf::Map out(8);

	out.put("generation", rec.generation())
	    .put("bins", rec.bin_count())
	    .put("size", rec.size())
	    .put("lut", rec.last_update_time())
	    .put("ttl", rec.ttl());

	udf::Text<64> set = rec.set_name();

	if (set.len > 0) {
		out.put("set", set.view());
	}
	else {
		out.put_null("set");
	}

	UDF_TRY(out.result());

	return out;
}

// The typed readers over scalar bins - the bin that must be there and of its kind, the bin that may be
// missing, and the bin with a default - and a map ranged over entry by entry.
UDF_FUNCTION(typed, udf::Result<udf::Map>, const udf::Record& rec)
{
	int64_t i = UDF_TRY(UDF_TRY(rec.bin("i")).as_int());
	double d = UDF_TRY(UDF_TRY(rec.bin("d")).as_double());
	udf::Optional<udf::Text<64>> s = rec.as_text<64>("s");
	bool b = rec.bool_or("b", false);

	if (!s) return udf::error(AS_UDF_NOT_FOUND, "typed: bin s is not a string");
	if (rec.as_int("nope") || rec.int_or("nope", -1) != -1 || rec.has("nope") || rec.bin("nope")) {
		return udf::error(AS_UDF_ERR, "typed: a bin that is not there was read");
	}

	int64_t sum = 0;
	udf::List keys(4);

	for (udf::Entry e : rec.map("m")) {
		if (e.value.is_int()) sum += e.value.payload;
		keys.add(e.key);
	}

	udf::Map out(8);

	out.put("i", i).put("d", d).put("s", s->view()).put("b", b).put("msum", sum).put("keys", keys);
	UDF_TRY(out.result());

	return out;
}

// The writes the typed natives do not cover: packed lists, a bool, a bin removed and a GeoJSON.
UDF_FUNCTION(write_lists, udf::Result<int64_t>, udf::Record& rec)
{
	static const int64_t w[] = {5, -1, 1LL << 40};
	static const double wd[] = {0.5, -4.0};

	UDF_TRY(rec.set_ints("w", w, 3));
	UDF_TRY(rec.set_doubles("wd", wd, 2));
	UDF_TRY(rec.set("flag", true));
	UDF_TRY(rec.remove("gone"));
	UDF_TRY(rec.set_geojson("geo", GEO));
	UDF_TRY(rec.save());

	return 1;
}

// [doc[index], <geojson>, {1: "one", "first": doc[0]}]: stored values into the builders by position, a
// GeoJSON that stays a GeoJSON, and a map with an integer key.
UDF_FUNCTION(relist, udf::Result<udf::List>, const udf::Record& rec, int64_t index)
{
	udf::Value first;
	udf::Value picked;
	bool found = false;
	int64_t i = 0;

	for (udf::Value v : rec.list("doc")) {
		if (i == 0) first = v;

		if (i == index) {
			picked = v;
			found = true;
			break;
		}

		i++;
	}

	if (!found) return udf::error(AS_UDF_NOT_FOUND, "relist: doc has no such element");

	udf::Map inner(2);

	inner.put((int64_t)1, "one").put("first", first);

	udf::List out(3);

	out.add(picked).add_geojson(GEO).add(inner);
	UDF_TRY(out.result());

	return out;
}

// Writes a GeoJSON bin back as the kind it was read as, and reports the document's length.
UDF_FUNCTION(geo_roundtrip, udf::Result<int64_t>, udf::Record& rec, udf::Text<32> name)
{
	udf::Optional<udf::Value> doc = rec.get(name);

	if (!doc || doc->kind() != udf::Kind::GeoJson) {
		return udf::error(AS_UDF_TYPE_MISMATCH, "that bin does not hold geojson");
	}

	UDF_TRY(rec.set(name, *doc));
	UDF_TRY(rec.save());

	return (int64_t)doc->len;
}

// A GeoJSON of the guest's as the result.
UDF_FUNCTION(geo_result, udf::Value)
{
	return udf::Value::geojson(GEO);
}

// Sets the record's TTL and saves.
UDF_FUNCTION(touch_ttl, udf::Result<int64_t>, udf::Record& rec, int64_t seconds)
{
	UDF_TRY(rec.set_ttl((int32_t)seconds));
	UDF_TRY(rec.set("touched", 1));
	UDF_TRY(rec.save());

	return 1;
}

// Deletes the record.
UDF_FUNCTION(remove_me, udf::Result<int64_t>, udf::Record& rec)
{
	UDF_TRY(rec.delete_record());

	return 1;
}

// Saves with nothing staged. Answers whether the record exists afterwards.
UDF_FUNCTION(save_nothing, udf::Result<bool>, udf::Record& rec)
{
	UDF_TRY(rec.save());

	return rec.exists();
}

// Deletes the record, staged writes and all. Answers what exists() says straight afterwards.
UDF_FUNCTION(delete_it, udf::Result<bool>, udf::Record& rec)
{
	UDF_TRY(rec.set("n", 99));
	UDF_TRY(rec.delete_record());

	return rec.exists();
}

// Removes a bin by name and saves. Answers how many bins are left.
UDF_FUNCTION(drop_bin, udf::Result<int64_t>, udf::Record& rec, udf::Text<32> name)
{
	UDF_TRY(rec.remove(name));
	UDF_TRY(rec.save());

	return rec.bin_count();
}

// Sets a bin to nil and saves, which removes it: a bin cannot hold nil.
UDF_FUNCTION(nil_bin, udf::Result<int64_t>, udf::Record& rec, udf::Text<32> name)
{
	UDF_TRY(rec.set(name, udf::Value::nil()));
	UDF_TRY(rec.save());

	return rec.bin_count();
}

// One element of a list bin by its index, and whether a map the call builds knows its keys.
UDF_FUNCTION(index_probe, udf::Result<udf::Map>, const udf::Record& rec, udf::Text<32> name, int64_t index)
{
	udf::Value list = UDF_TRY(rec.bin(name));

	if (!list.is_list()) return udf::error(AS_UDF_TYPE_MISMATCH, "index_probe: that bin is not a list");

	udf::Map out(4);

	out.put("len", (int64_t)list.size());

	if (udf::Optional<udf::Value> at = list.at(index)) {
		out.put("at", *at);
	}
	else {
		out.put("at", "absent");
	}

	out.put((int64_t)7, "seven");

	bool knows = out.contains("len") && out.contains((int64_t)7);
	bool invents = out.contains("nope") || out.contains((int64_t)8);

	out.put("knows", knows).put("invents", invents);
	UDF_TRY(out.result());

	return out;
}

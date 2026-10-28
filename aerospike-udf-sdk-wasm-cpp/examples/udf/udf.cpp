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

static constexpr udf::Bin L{"l"};
static constexpr udf::Bin M{"m"};
static constexpr udf::Bin DOC{"doc"};

// The sum of the integers in list bin l, ranged over; any other element fails the call.
UDF_FUNCTION(sum_list, udf::Result<int64_t>)
{
	int64_t total = 0;

	for (udf::Value v : L.list()) {
		total += UDF_TRY(v.as_int());
	}

	return total;
}

// Everything the record knows about itself, as a map.
UDF_FUNCTION(describe, udf::Result<udf::Map>)
{
	udf::Map out(8);

	out.put("generation", udf::generation())
	    .put("bins", udf::bin_count())
	    .put("size", udf::record_size())
	    .put("lut", udf::last_update_time())
	    .put("ttl", udf::ttl());

	udf::Text<64> set = udf::set_name();

	if (set.len > 0) {
		out.put("set", set.view());
	}
	else {
		out.put_null("set");
	}

	UDF_TRY(out.result());

	return out;
}

// The typed readers over scalar bins, and a map ranged over entry by entry.
UDF_FUNCTION(typed, udf::Result<udf::Map>)
{
	int64_t i = UDF_TRY(udf::Bin("i").as_int());
	double d = UDF_TRY(udf::Bin("d").as_double());
	udf::Text<64> s = UDF_TRY(udf::Bin("s").as_text<64>());
	bool b = UDF_TRY(udf::Bin("b").as_bool());

	int64_t sum = 0;
	udf::List keys(4);

	for (udf::Entry e : M.map()) {
		if (e.value.is_int()) sum += e.value.payload;
		keys.add(e.key);
	}

	udf::Map out(8);

	out.put("i", i).put("d", d).put("s", s.view()).put("b", b).put("msum", sum).put("keys", keys);
	UDF_TRY(out.result());

	return out;
}

// The writes the typed natives do not cover: packed lists, a bool, a nil and a GeoJSON.
UDF_FUNCTION(write_lists, udf::Result<int64_t>)
{
	static const int64_t w[] = {5, -1, 1LL << 40};
	static const double wd[] = {0.5, -4.0};

	UDF_TRY(udf::Bin("w").set_ints(w, 3));
	UDF_TRY(udf::Bin("wd").set_doubles(wd, 2));
	UDF_TRY(udf::Bin("flag").set(true));
	UDF_TRY(udf::Bin("gone").set_null());
	UDF_TRY(udf::Bin("geo").set_geojson(GEO));
	UDF_TRY(udf::commit());

	return 1;
}

// [doc[index], <geojson>, {1: "one", "first": doc[0]}]: stored values into the builders by position, a
// GeoJSON that stays a GeoJSON, and a map with an integer key.
UDF_FUNCTION(relist, udf::Result<udf::List>, int64_t index)
{
	udf::Value first;
	udf::Value picked;
	bool found = false;
	int64_t i = 0;

	for (udf::Value v : DOC.list()) {
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
UDF_FUNCTION(geo_roundtrip, udf::Result<int64_t>, udf::Text<32> name)
{
	udf::Bin bin(name.c_str());
	udf::Optional<udf::Value> doc = bin.get();

	if (!doc || doc->kind() != udf::Kind::GeoJson) {
		return udf::error(AS_UDF_TYPE_MISMATCH, "that bin does not hold geojson");
	}

	UDF_TRY(bin.set(*doc));
	UDF_TRY(udf::commit());

	return (int64_t)doc->len;
}

// A GeoJSON of the guest's as the result.
UDF_FUNCTION(geo_result, udf::Value)
{
	return udf::Value::geojson(GEO);
}

// Sets the record's TTL and commits.
UDF_FUNCTION(touch_ttl, udf::Result<int64_t>, int64_t seconds)
{
	UDF_TRY(udf::set_ttl((int32_t)seconds));
	UDF_TRY(udf::Bin("touched").set(1));
	UDF_TRY(udf::commit());

	return 1;
}

// Deletes the record.
UDF_FUNCTION(remove_me, udf::Result<int64_t>)
{
	UDF_TRY(udf::remove_record());

	return 1;
}

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

// The C++ counterpart of the Rust SDK's test fixture (aerospike-udf-sdk/wasm-rust/examples/udf): one function
// per corner of the record API that the Rust SDK has and the client tests check, so that
// tests/src/udf_cpp.rs can hold the C++ SDK to the same surface. Nothing here is an example to copy; it
// is the SDK's own coverage.

#include "aerospike_udf.hpp"

static constexpr const char* GEO = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";

// The sum of the integers in list bin l, ranged over; any other element fails the call.
UDF_FUNCTION(sum_list, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t total = 0;

	for (udf::Value v : UDF_TRY(rec.list("l")).value_or(udf::ListRange())) {
		total += UDF_TRY(udf::required(v.as_int(), "sum_list: a nil element"));
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
		out.put_nil("set");
	}

	UDF_TRY(out.result());

	return out;
}

// The typed readers over scalar bins - the bin that must be there and of its kind, the bin that may be
// missing, and the bin with a default - and a map ranged over entry by entry.
UDF_FUNCTION(typed, udf::Result<udf::Map>, const udf::Record& rec)
{
	int64_t i = UDF_TRY(udf::required(UDF_TRY(rec.bin("i")).as_int(), "typed: bin i is nil"));
	double d = UDF_TRY(udf::required(UDF_TRY(rec.bin("d")).as_double(), "typed: bin d is nil"));
	udf::Optional<udf::Text<64>> s = UDF_TRY(rec.as_string<64>("s"));
	bool b = UDF_TRY(rec.bool_or("b", false));

	if (!s) return udf::error(AS_UDF_NOT_FOUND, "typed: bin s is not a string");
	if (UDF_TRY(rec.as_int("nope")) || UDF_TRY(rec.int_or("nope", -1)) != -1 || rec.has("nope") || rec.bin("nope")) {
		return udf::error(AS_UDF_ERR, "typed: a bin that is not there was read");
	}

	int64_t sum = 0;
	udf::List keys(4);

	for (udf::Entry e : UDF_TRY(rec.map("m")).value_or(udf::MapRange())) {
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

	for (udf::Value v : UDF_TRY(rec.list("doc")).value_or(udf::ListRange())) {
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

// Takes what the call has left, by building empty lists through the C API until the host refuses one: when the
// call's memory on the server is spent. Nothing gives them back before the call ends.
static void take_every_handle()
{
	while (as_udf_list_new(0) > 0) {
	}
}

// The reads that cannot answer when the host cannot read a stored list or map, after the record's list bin l
// and map bin m have been read and a bin x saved. With `what` a walk ("walk", "map_walk") the call's handles
// are first all taken, so the walk cannot open its cursor; with a lookup ("contains", "get") a save that the
// server refuses (GeoJSON it does not accept) first ends the values read from the stored record, since a
// lookup takes no handle. Either way the call must stop with the SDK's message rather than answer: there is
// no answer here that is not a lie. "reread" takes every handle and then reads l again, which reuses the
// handle of the first read and must succeed. The saved x is undone with the rest of a call that fails.
UDF_FUNCTION(stop_probe, udf::Result<int64_t>, udf::Record& rec, udf::Text<16> what)
{
	udf::ListRange l = UDF_TRY(udf::required(rec.list("l"), "stop_probe: no list bin l"));
	udf::MapRange m = UDF_TRY(udf::required(rec.map("m"), "stop_probe: no map bin m"));

	UDF_TRY(rec.set("x", 1));
	UDF_TRY(rec.save());

	if (what == "walk" || what == "map_walk" || what == "reread") {
		take_every_handle();
	}
	else {
		UDF_TRY(rec.set_geojson("g", "not geojson"));
		if (rec.save()) return udf::error(AS_UDF_ERR, "stop_probe: the server took a save it should refuse");
	}

	if (what == "walk") {
		int64_t total = 0;
		for (udf::Value v : l) total += v.is_int() ? v.payload : 0;
		return total;
	}
	if (what == "map_walk") {
		int64_t entries = 0;
		for (udf::Entry e : m) entries += e.value.is_null() ? 0 : 1;
		return entries;
	}
	if (what == "contains") return m.contains("a") ? 1 : 0;
	if (what == "get") return m.value().get("a").has_value() ? 1 : 0;
	if (what == "reread") return UDF_TRY(udf::required(rec.list("l"), "stop_probe: l unreadable again")).size();

	return udf::error(AS_UDF_ERR, "stop_probe: no such probe");
}

// Builds `count` maps, each holding a list, nested in one outer list: each is given back when it goes out of
// scope, the outer list keeping it. Then walks every list of list bin `nested` with a range-for of its own,
// whose cursor closes when the loop ends. Answers [elements, sum]. The call holds a few handles at a time
// however large `count` is: what bounds it is its memory, not a number of handles.
UDF_FUNCTION(churn, udf::Result<udf::List>, const udf::Record& rec, int64_t count)
{
	udf::List out(0);

	for (int64_t i = 0; i < count; i++) {
		udf::List inner(2);
		udf::Map entry(1);

		inner.add(i).add(1);
		entry.put("l", inner);
		out.add(entry);
	}
	UDF_TRY(out.result());

	udf::ListRange nested = UDF_TRY(udf::required(rec.list("nested"), "churn: no list bin nested"));
	int64_t sum = 0;

	for (udf::Value row : nested) {
		for (udf::Value cell : row.list()) sum += UDF_TRY(cell.as_int()).value_or(0);
	}

	udf::List answer(2);

	answer.add((int64_t)out.size()).add(sum);
	return answer;
}

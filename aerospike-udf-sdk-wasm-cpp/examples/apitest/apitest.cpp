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

// The conformance module of the C++ SDK: the same `apitest` every guest SDK has, written to one
// specification so that a harness can call each export of all of them with the same record and the same
// arguments and compare the answers. Nothing here is an example to copy; it is the header's public API used
// the way a UDF would use it, one export per corner.
//
// Almost every export answers a list whose first element is a tag: "some", "none", "wrong", or a status
// named after the host's code ("ok", "err", "absent", "refused", "small").
//
// A string parameter is a `udf::Text<65536>`, the 64 KiB the specification allows a string argument, taken by
// const reference. A Text lives inline, so the generated export and the readers keep several of them on the
// stack at once: the Makefile builds this module with a 4 MiB linear-memory stack in place of clang's 64 KB
// (see "The stack" in the header).
//
// The arity of every export is checked by the generated code: more arguments than an export declares, or more
// than 128 in all for `ar`, fail the call.

#include "aerospike_udf.hpp"

using Str = udf::Text<65536>;

// The largest string or blob the module reads from an argument or builds for `mk`, and room for the user key.
static unsigned char BIG[1 << 20];
static int64_t INTS[256];
// The largest `n` that `mk` builds for; anything over, or negative, is refused before a builder is called.
static constexpr int64_t MK_MAX = 1000000;
static double DOUBLES[256];

static constexpr const char* GEO = "{\"type\":\"Point\",\"coordinates\":[1,2]}";

//==========================================================
// Small things.
//

// A sum that wraps: signed overflow is undefined in C++, and the stored integers are anything.
static int64_t wrap_add(int64_t a, int64_t b)
{
	return (int64_t)((uint64_t)a + (uint64_t)b);
}

// The tag of a status, named after the host's code.
static const char* tag(udf::Status rc)
{
	if (rc == AS_UDF_OK) return "ok";
	if (rc == AS_UDF_NOT_FOUND) return "absent";
	if (rc == AS_UDF_TYPE_MISMATCH) return "wrong";
	if (rc == AS_UDF_REFUSED) return "refused";
	if (rc <= AS_UDF_NEEDED_BASE) return "small";
	return "err";
}

// The tag of a call that yields no value: "ok", or the tag of the status its error carries.
static const char* tag(const udf::Result<void>& r)
{
	return r ? "ok" : tag(r.error().status);
}

// The tag of a typed reader's answer that holds no value: "none" for nothing there, the status's tag otherwise.
template <class T>
static const char* none_or_tag(const udf::Result<udf::Optional<T>>& r)
{
	return r ? "none" : tag(r.error().status);
}

// The kind of a value, as `val` and `ar` answer it.
static const char* kind_name(const udf::Value& v)
{
	switch (v.kind()) {
	case udf::Kind::Nil: return "nil";
	case udf::Kind::Bool: return "bool";
	case udf::Kind::Int: return "int";
	case udf::Kind::Double: return "double";
	case udf::Kind::Str: return "string";
	case udf::Kind::Blob: return "blob";
	case udf::Kind::List: return "list";
	case udf::Kind::Map: return "map";
	case udf::Kind::GeoJson: return "geojson";
	case udf::Kind::Hll: return "hll";
	default: return "other";
	}
}

// `prefix` followed by the decimal text of `i`: "s12", "k0".
static udf::Text<24> label(char prefix, int64_t i)
{
	udf::Text<24> t;
	char tmp[20];
	int32_t n = 0;
	uint64_t u = i < 0 ? 0 - (uint64_t)i : (uint64_t)i;

	t.buf[t.len++] = prefix;
	if (i < 0) t.buf[t.len++] = '-';
	do {
		tmp[n++] = (char)('0' + u % 10);
		u /= 10;
	} while (u != 0);
	while (n > 0) t.buf[t.len++] = tmp[--n];
	t.buf[t.len] = '\0';

	return t;
}

//==========================================================
// rd(name, how): one reader of the record.
//

UDF_FUNCTION(rd, udf::Result<udf::List>, const udf::Record& rec, const Str& name, const Str& how)
{
	udf::List out(2);

	if (how == "has") {
		out.add("some").add(rec.has(name));
	}
	else if (how == "get") {
		if (udf::Optional<udf::Value> v = rec.get(name)) {
			out.add("some").add(*v);
		}
		else {
			out.add("none");
		}
	}
	else if (how == "bin") {
		udf::Result<udf::Value> v = rec.bin(name);

		if (v) {
			out.add("some").add(*v);
		}
		else {
			out.add(tag(v.error().status));
		}
	}
	else if (how == "int") {
		udf::Result<udf::Optional<int64_t>> v = rec.as_int(name);

		if (v && *v) {
			out.add("some").add(**v);
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "double") {
		udf::Result<udf::Optional<double>> v = rec.as_double(name);

		if (v && *v) {
			out.add("some").add(**v);
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "bool") {
		udf::Result<udf::Optional<bool>> v = rec.as_bool(name);

		if (v && *v) {
			out.add("some").add(**v);
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "string") {
		udf::Result<udf::Optional<Str>> v = rec.as_string<65536>(name);

		if (v && *v) {
			out.add("some").add((*v)->view());
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "blob") {
		udf::Result<udf::Optional<udf::Bytes<65536>>> v = rec.as_blob<65536>(name);

		if (v && *v) {
			out.add("some").add_blob((*v)->buf, (*v)->len);
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "list") {
		udf::Result<udf::Optional<udf::ListRange>> v = rec.list(name);

		if (v && *v) {
			out.add("some").add((*v)->value());
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "map") {
		udf::Result<udf::Optional<udf::MapRange>> v = rec.map(name);

		if (v && *v) {
			out.add("some").add((*v)->value());
		}
		else {
			out.add(none_or_tag(v));
		}
	}
	else if (how == "int_or") {
		udf::Result<int64_t> v = rec.int_or(name, 7);

		if (v) {
			out.add("some").add(*v);
		}
		else {
			out.add(tag(v.error().status));
		}
	}
	else if (how == "double_or") {
		udf::Result<double> v = rec.double_or(name, 7.5);

		if (v) {
			out.add("some").add(*v);
		}
		else {
			out.add(tag(v.error().status));
		}
	}
	else if (how == "bool_or") {
		udf::Result<bool> v = rec.bool_or(name, true);

		if (v) {
			out.add("some").add(*v);
		}
		else {
			out.add(tag(v.error().status));
		}
	}
	else {
		return udf::error(AS_UDF_ERR, "rd: unknown how");
	}

	return out;
}

//==========================================================
// wr(name, how, v?): one write, staged and saved.
//

UDF_FUNCTION(wr, udf::Result<udf::List>, udf::Record& rec, const Str& name, const Str& how, udf::Args v)
{
	// The value is taken as the rest of the arguments, so that one left out (none) is told from one
	// passed as nil (a nil): an Optional parameter is empty for both.
	if (v.size() > 1) {
		return udf::error(AS_UDF_ERR, "wr takes at most 3 arguments");
	}

	bool missing = v.size() == 0;
	udf::Value value = missing ? udf::Value::nil() : v[0];
	udf::Result<void> stage;

	if (how == "set") {
		stage = missing ? udf::Result<void>(udf::error(AS_UDF_TYPE_MISMATCH, "wr: no value")) : rec.set(name, value);
	}
	else if (how == "blob") {
		// A string or a blob, nothing else: bytes_into() would take GeoJSON and an HLL too.
		udf::Result<int32_t> n = value.is_string() || value.is_blob() ? value.bytes_into(BIG, (int32_t)sizeof(BIG)) :
		                                                                 udf::Result<int32_t>(udf::error(AS_UDF_TYPE_MISMATCH, "wr: not a string or blob"));

		if (n) {
			stage = rec.set_blob(name, BIG, *n);
		}
		else {
			stage = n.error();
		}
	}
	else if (how == "geojson") {
		udf::Result<udf::Optional<Str>> doc = value.as_string<65536>();

		if (doc && *doc) {
			stage = rec.set_geojson(name, (*doc)->view());
		}
		else {
			stage = doc ? udf::error(AS_UDF_TYPE_MISMATCH, "wr: no GeoJSON text") : doc.error();
		}
	}
	else if (how == "ints" || how == "doubles") {
		// A list whose every element is of the kind, at most 256 of them; anything else is "wrong".
		bool ints = how == "ints";
		int32_t count = 0;
		bool ok = value.is_list() && value.size() <= 256;

		for (udf::Value e : value.list()) {
			if (!ok) break;
			if (ints) {
				udf::Result<udf::Optional<int64_t>> x = e.as_int();
				ok = x && *x;
				if (ok) INTS[count++] = **x;
			}
			else {
				udf::Result<udf::Optional<double>> x = e.as_double();
				ok = x && *x;
				if (ok) DOUBLES[count++] = **x;
			}
		}

		if (!ok) {
			stage = udf::error(AS_UDF_TYPE_MISMATCH, "wr: not a list of 256 numbers of the kind at most");
		}
		else {
			stage = ints ? rec.set_ints(name, INTS, count) : rec.set_doubles(name, DOUBLES, count);
		}
	}
	else if (how == "remove") {
		stage = rec.remove(name);
	}
	else {
		return udf::error(AS_UDF_ERR, "wr: unknown how");
	}

	udf::List out(3);

	out.add(tag(stage));

	if (stage) {
		out.add(tag(rec.save()));
	}
	else {
		out.add("skipped");
	}

	out.add(rec.bin_count());

	return out;
}

//==========================================================
// meta(): what the record says of itself.
//

UDF_FUNCTION(meta, udf::Result<udf::Map>, const udf::Record& rec)
{
	unsigned char digest[20];
	udf::Map out(8);
	int64_t ttl = rec.ttl();

	out.put("exists", rec.exists())
	    .put("generation", rec.generation())
	    .put("ttl", ttl == 0 ? (int64_t)-1 : ttl)
	    .put("bins", rec.bin_count())
	    .put("sized", rec.size() > 0)
	    .put("set", rec.set_name().view());

	udf::Optional<udf::Value> key = UDF_TRY(rec.key(BIG, (int32_t)sizeof(BIG)));

	if (key) {
		out.put("key", *key);
	}
	else {
		out.put_nil("key");
	}

	out.put("digest", (int64_t)rec.digest(digest, (int32_t)sizeof(digest)));

	return out;
}

//==========================================================
// val(v): the kind of a value, and the value handed back.
//

UDF_FUNCTION(val, udf::Result<udf::List>, udf::Value v)
{
	udf::List out(3);

	out.add(kind_name(v)).add(v);

	if (v.is_list() || v.is_map()) {
		out.add((int64_t)v.size());
	}

	return out;
}

//==========================================================
// mk(what, n): a value built with the builders, one call an element.
//

// The failure of a builder, as the error `mk` fails with: "mk: " and the tag of its status.
static udf::Error mk_error(udf::Status rc)
{
	if (rc == AS_UDF_NOT_FOUND) return udf::error(rc, "mk: absent");
	if (rc == AS_UDF_TYPE_MISMATCH) return udf::error(rc, "mk: wrong");
	if (rc == AS_UDF_REFUSED) return udf::error(rc, "mk: refused");
	if (rc <= AS_UDF_NEEDED_BASE) return udf::error(rc, "mk: small");
	return udf::error(rc, "mk: err");
}

// Element `i` of the mixed list, appended.
static void add_mixed(udf::List& list, int64_t i)
{
	switch (i % 6) {
	case 0:
		list.add(i);
		break;
	case 1:
		list.add((double)i + 0.5);
		break;
	case 2:
		list.add(i % 4 == 2);
		break;
	case 3:
		list.add(label('s', i).view());
		break;
	case 4: {
		unsigned char b = (unsigned char)(i & 0xff);

		list.add_blob(&b, 1);
		break;
	}
	default:
		list.add_nil();
		break;
	}
}

// Element `i` of the mixed list, put under a string key.
static void put_mixed(udf::Map& map, udf::StrView key, int64_t i)
{
	switch (i % 6) {
	case 0:
		map.put(key, i);
		break;
	case 1:
		map.put(key, (double)i + 0.5);
		break;
	case 2:
		map.put(key, i % 4 == 2);
		break;
	case 3:
		map.put(key, label('s', i).view());
		break;
	case 4: {
		unsigned char b = (unsigned char)(i & 0xff);

		map.put_blob(key, &b, 1);
		break;
	}
	default:
		map.put_nil(key);
		break;
	}
}

UDF_FUNCTION(mk, udf::Result<udf::Value>, const Str& what, int64_t n)
{
	if (n < 0 || n > MK_MAX) return mk_error(AS_UDF_REFUSED);

	if (what == "list_int") {
		udf::List out(8);

		for (int64_t i = 0; i < n && !out.error().failed(); i++) {
			out.add(i);
		}

		if (out.error().failed()) return mk_error(out.error().status);

		return out.value();
	}

	if (what == "list_mixed") {
		udf::List out(8);

		for (int64_t i = 0; i < n && !out.error().failed(); i++) {
			add_mixed(out, i);
		}

		if (out.error().failed()) return mk_error(out.error().status);

		return out.value();
	}

	if (what == "map_str") {
		udf::Map out(8);

		for (int64_t i = 0; i < n && !out.error().failed(); i++) {
			out.put(label('k', i).view(), i);
		}

		if (out.error().failed()) return mk_error(out.error().status);

		return out.value();
	}

	if (what == "map_int") {
		udf::Map out(8);

		for (int64_t i = 0; i < n && !out.error().failed(); i++) {
			out.put(i, label('v', i).view());
		}

		if (out.error().failed()) return mk_error(out.error().status);

		return out.value();
	}

	if (what == "map_mixed") {
		udf::Map out(8);

		for (int64_t i = 0; i < n && !out.error().failed(); i++) {
			put_mixed(out, label('k', i).view(), i);
		}

		if (out.error().failed()) return mk_error(out.error().status);

		return out.value();
	}

	if (what == "nested") {
		udf::List inner(1);

		inner.add(n);

		for (int64_t i = 1; i < n && !inner.error().failed(); i++) {
			udf::List outer(1);

			outer.add(inner);
			inner = outer;
		}

		if (inner.error().failed()) return mk_error(inner.error().status);

		return inner.value();
	}

	if (what == "string") {
		for (int64_t i = 0; i < n; i++) {
			BIG[i] = 'x';
		}

		return udf::Value::of(udf::StrView{(const char*)BIG, (int32_t)n});
	}

	if (what == "blob") {
		for (int64_t i = 0; i < n; i++) {
			BIG[i] = (unsigned char)(i & 0xff);
		}

		return udf::Value::blob(BIG, (int32_t)n);
	}

	return udf::error(AS_UDF_ERR, "mk: unknown what");
}

//==========================================================
// walk(name): a stored list or map, ranged over and looked up in.
//

UDF_FUNCTION(walk, udf::Result<udf::List>, const udf::Record& rec, const Str& name)
{
	udf::List out(8);
	udf::Optional<udf::Value> bin = rec.get(name);
	udf::Value nil = udf::Value::nil();

	if (!bin) {
		out.add("none");
	}
	else if (bin->is_list()) {
		int64_t len = bin->size();
		int64_t sum = 0;
		int64_t strings = 0;

		for (udf::Value e : bin->list()) {
			if (e.is_int()) sum = wrap_add(sum, e.payload);
			if (e.is_string()) strings++;
		}

		out.add("list")
		    .add(len)
		    .add(sum)
		    .add(strings)
		    .add(bin->at(0).value_or(nil))
		    .add(bin->at(len - 1).value_or(nil))
		    .add(bin->at(len) ? "some" : "none");
	}
	else if (bin->is_map()) {
		int64_t sum = 0;

		for (udf::Entry e : bin->map()) {
			if (e.value.is_int()) sum = wrap_add(sum, e.value.payload);
		}

		out.add("map")
		    .add((int64_t)bin->size())
		    .add(sum)
		    .add(bin->contains("k1"))
		    .add(bin->contains((int64_t)1))
		    .add(bin->get("k1").value_or(nil))
		    .add(bin->get((int64_t)1).value_or(nil));
	}
	else {
		out.add("wrong");
	}

	return out;
}

//==========================================================
// ar(a, b?, rest...): the arguments as they arrive.
//

UDF_FUNCTION(ar, udf::Result<udf::List>, int64_t a, const udf::Optional<Str>& b, udf::Args rest)
{
	udf::List out(8);

	out.add(a);

	if (b) {
		out.add(b->view());
	}
	else {
		out.add_nil();
	}

	out.add((int64_t)rest.size());

	for (udf::Value v : rest) {
		out.add(kind_name(v));
	}

	return out;
}

//==========================================================
// life(what): staging, saving, deleting, the TTL, and failing after a save.
//

UDF_FUNCTION(life, udf::Result<udf::List>, udf::Record& rec, const Str& what)
{
	udf::List out(3);

	// A staging call that fails is the whole answer: its tag, and nothing after it.
	if (what == "staged") {
		udf::Result<void> staged = rec.set("t", 1);
		udf::Result<int64_t> t = staged ? rec.int_or("t", -1) : udf::Result<int64_t>(staged.error());

		if (!t) {
			out.add(tag(t.error().status));
		}
		else {
			out.add("some").add(*t).add(rec.bin_count());
		}
	}
	else if (what == "save_twice") {
		udf::Result<void> staged = rec.set("t", 1);

		if (!staged) {
			out.add(tag(staged));
		}
		else {
			udf::Result<void> first = rec.save();

			staged = rec.set("t", 2);

			if (!staged) {
				out.add(tag(staged));
			}
			else {
				udf::Result<void> second = rec.save();

				out.add(tag(first)).add(tag(second));
			}
		}
	}
	else if (what == "delete") {
		udf::Result<void> deleted = rec.delete_record();

		out.add(tag(deleted)).add(rec.exists());
	}
	else if (what == "ttl") {
		udf::Result<void> ttl = rec.set_ttl(100);

		// A refused set_ttl is a failed staging call: its status is the answer.
		out.add(tag(ttl));

		if (ttl) {
			udf::Result<void> saved = rec.save();

			out.add(tag(saved));
		}
	}
	else if (what == "fail_after_save") {
		udf::Result<void> staged = rec.set("t", 9);

		if (!staged) {
			out.add(tag(staged));
		}
		else {
			rec.save();

			return udf::error(AS_UDF_ERR, "life: failing");
		}
	}
	else {
		return udf::error(AS_UDF_ERR, "life: unknown what");
	}

	return out;
}

//==========================================================
// res(kind): a result of each kind, returned.
//

UDF_FUNCTION(res, udf::Result<udf::Value>, const Str& kind)
{
	if (kind == "nil") return udf::Value::nil();
	if (kind == "int") return udf::Value::of((int64_t)INT64_MIN);
	if (kind == "double") return udf::Value::of(1e308);
	if (kind == "bool") return udf::Value::of(true);
	if (kind == "string") return udf::Value::of(udf::StrView(""));
	if (kind == "blob") return udf::Value::blob("", 0);
	if (kind == "geojson") return udf::Value::geojson(GEO);

	if (kind == "list") {
		udf::List out(0);

		UDF_TRY(out.result());

		return out.value();
	}

	if (kind == "map") {
		udf::Map out(0);

		UDF_TRY(out.result());

		return out.value();
	}

	if (kind == "fail_long") {
		static char message[601];

		for (int32_t i = 0; i < 600; i++) {
			message[i] = 'x';
		}

		message[600] = '\0';

		return udf::error(AS_UDF_ERR, message);
	}

	return udf::error(AS_UDF_ERR, "res: unknown kind");
}

//==========================================================
// txt(s): a string argument, and the string handed back.
//

UDF_FUNCTION(txt, udf::Result<udf::List>, const Str& s)
{
	udf::List out(3);

	out.add((int64_t)s.len).add(s.view()).add(s.len > 0 ? (int64_t)(unsigned char)s.buf[0] : (int64_t)-1);

	return out;
}

//==========================================================
// cmp(a, b): the stored-value comparison of two bins.
//

UDF_FUNCTION(cmp, udf::Result<udf::List>, const udf::Record& rec, const Str& a, const Str& b)
{
	udf::List out(2);
	udf::Optional<udf::Value> va = rec.get(a);
	udf::Optional<udf::Value> vb = rec.get(b);

	if (!va || !vb) {
		out.add("none");
	}
	else {
		// cmp_stored() checks the kinds itself: a scalar is an error of status AS_UDF_TYPE_MISMATCH, never a sign.
		udf::Result<int32_t> sign = va->cmp_stored(*vb);

		if (sign) {
			out.add("some").add(*sign);
		}
		else {
			out.add(tag(sign.error().status));
		}
	}

	return out;
}

//==========================================================
// agg: a stream UDF. One instance sees every record, so the state is two statics.
//

static int64_t agg_count;
static int64_t agg_sum;

UDF_STREAM_RECORD(agg, void, const udf::Record& rec)
{
	udf::Result<udf::Optional<int64_t>> n = rec.as_int("n");

	if (n && *n) {
		agg_count++;
		agg_sum = wrap_add(agg_sum, **n);
	}
}

UDF_STREAM_END(agg, udf::Result<udf::List>)
{
	udf::List out(2);

	out.add(agg_count).add(agg_sum);

	return out;
}

//==========================================================
// merge_api: an xdr-merge handler that answers what the two records, the Pair and a Both say.
//

// A list of bin names, as the record gives them: a range, built into a list.
static udf::List names(udf::ListRange range)
{
	udf::List out(8);

	for (udf::Value name : range) {
		out.add(name);
	}

	return out;
}

UDF_ON_XDR_MERGE(merge_api)
{
	udf::Pair pair = udf::Pair::of(current, incoming);
	udf::Map out(20);

	// A side whose n is not an integer, or whose s is not a string, fails the handler: the readers say so.
	udf::Both<int64_t> n = UDF_TRY(pair.as_int("n"));

	if (udf::Optional<int64_t> max = n.max()) out.put("max", *max);
	if (udf::Optional<int64_t> min = n.min()) out.put("min", *min);
	if (udf::Optional<int64_t> sum = n.sum()) out.put("sum", *sum);

	udf::Both<Str> s = UDF_TRY(pair.as_string<65536>("s"));

	if (udf::Optional<Str> newer = s.newer()) out.put("newer_s", newer->view());
	if (udf::Optional<Str> older = s.older()) out.put("older_s", older->view());
	if (udf::Optional<Str> c_or_i = s.current_or_incoming()) out.put("c_or_i", c_or_i->view());
	if (udf::Optional<Str> i_or_c = s.incoming_or_current()) out.put("i_or_c", i_or_c->view());

	out.put("i_newer", pair.incoming_is_newer("s"));

	out.put("c_names", names(current.bin_names()));
	out.put("i_names", names(incoming.bin_names()));
	out.put("c_del", names(current.deleted_bins()));
	out.put("i_del", names(incoming.deleted_bins()));
	out.put("lost", names(incoming.lost_bins()));

	udf::List c_meta(6);

	c_meta.add(current.generation())
	    .add(current.last_update_time())
	    .add(current.ttl())
	    .add(current.source_id())
	    .add(current.is_tombstone())
	    .add(current.uses_bin_convergence());
	out.put("c_meta", c_meta);

	udf::List i_meta(6);

	i_meta.add(incoming.generation())
	    .add(incoming.last_update_time())
	    .add(incoming.ttl())
	    .add(incoming.source_id())
	    .add(incoming.is_delete())
	    .add(incoming.is_partial());
	out.put("i_meta", i_meta);

	udf::List blut(2);

	blut.add(current.bin_last_update_time("s")).add(incoming.bin_last_update_time("s"));
	out.put("blut", blut);

	udf::List bsrc(2);

	bsrc.add(current.bin_source_id("s")).add(incoming.bin_source_id("s"));
	out.put("bsrc", bsrc);

	return udf::Resolution::record(out);
}

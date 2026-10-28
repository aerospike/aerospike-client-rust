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
#pragma once

// Writing an Aerospike WASM UDF in C++, for the server's mod-wamr engine.
//
// Header-only, over the C header (aerospike_udf.h): the natives and the contract's constants come from
// there, and this adds the typed tier - Bin, Value, Map, List, range-for over stored collections, Result
// and Error instead of status codes, the event records and answers, Pair and Both for a merge, and the
// UDF_FUNCTION / UDF_STREAM / UDF_ON_* macros that derive the exports and their signatures from a
// function's parameter types, the way the Rust SDK's macros do. A module is a freestanding C++20 file
// compiled with clang for wasm32, no standard library, no exceptions, no RTTI:
//
//   clang -x c++ -std=c++20 --target=wasm32 -O3 -nostdlib -nostdinc++ -ffreestanding -fno-exceptions \
//       -fno-rtti -fvisibility=hidden -Wl,--no-entry -I<c sdk>/include -I<this>/include -o my.wasm my.cpp
//
// Nothing here allocates: strings live in fixed buffers (Text<N>), collections are built on the host, and
// a module needs no operator new. The one clang extension used is the statement expression behind UDF_TRY.

extern "C" {
#include "aerospike_udf.h"
}

namespace udf {

//==========================================================
// Small things the standard library would have given us.
//

using size_t = decltype(sizeof(0));

namespace detail {

template <class T> struct RemoveRef { using type = T; };
template <class T> struct RemoveRef<T&> { using type = T; };
template <class T> struct RemoveRef<T&&> { using type = T; };
template <class T> struct RemoveCV { using type = T; };
template <class T> struct RemoveCV<const T> { using type = T; };
template <class T> using Bare = typename RemoveCV<typename RemoveRef<T>::type>::type;

template <class A, class B> struct IsSame { static constexpr bool value = false; };
template <class A> struct IsSame<A, A> { static constexpr bool value = true; };
template <class A, class B> constexpr bool is_same = IsSame<A, B>::value;

template <size_t... I> struct IndexSeq {};
template <size_t N, size_t... I> struct MakeSeq : MakeSeq<N - 1, N - 1, I...> {};
template <size_t... I> struct MakeSeq<0, I...> { using type = IndexSeq<I...>; };
template <size_t N> using MakeIndexSeq = typename MakeSeq<N>::type;

template <class... T> struct TypeList { static constexpr size_t size = sizeof...(T); };

template <class T> T&& declval();

constexpr int32_t cstrlen(const char* s)
{
	int32_t n = 0;
	while (s[n] != '\0') n++;
	return n;
}

constexpr bool str_eq(const char* a, int32_t alen, const char* b, int32_t blen)
{
	if (alen != blen) return false;
	for (int32_t i = 0; i < alen; i++) if (a[i] != b[i]) return false;
	return true;
}

constexpr bool is_ident(char c)
{
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

// A message built without allocation: one static buffer, valid until the next message. Every error the
// SDK raises is delivered to the host before another is built, so one is enough.
struct Message {
	char buf[256];
	int32_t len = 0;

	Message& add(const char* s)
	{
		for (int32_t i = 0; s[i] != '\0' && len < (int32_t)sizeof(buf) - 1; i++) buf[len++] = s[i];
		buf[len] = '\0';
		return *this;
	}

	Message& add(const char* s, int32_t n)
	{
		for (int32_t i = 0; i < n && len < (int32_t)sizeof(buf) - 1; i++) buf[len++] = s[i];
		buf[len] = '\0';
		return *this;
	}

	Message& add(int64_t n)
	{
		char tmp[21];
		int32_t i = 0;
		bool negative = n < 0;
		if (n == 0) tmp[i++] = '0';
		while (n != 0) {
			int64_t d = n % 10;
			tmp[i++] = (char)('0' + (d < 0 ? -d : d));
			n /= 10;
		}
		if (negative && len < (int32_t)sizeof(buf) - 1) buf[len++] = '-';
		while (i > 0 && len < (int32_t)sizeof(buf) - 1) buf[len++] = tmp[--i];
		buf[len] = '\0';
		return *this;
	}
};

inline Message& message()
{
	static Message m;
	m.len = 0;
	m.buf[0] = '\0';
	return m;
}

} // namespace detail

//==========================================================
// Errors and results.
//

using Status = int32_t;

// What a failed call reports: the native's status and a message naming what failed, which the generated
// export delivers to the client. The message is a literal or the SDK's message buffer.
struct Error {
	Status status = AS_UDF_OK;
	const char* message = nullptr;

	constexpr bool failed() const { return message != nullptr; }
};

inline Error error(Status status, const char* message) { return Error{status, message}; }

// A value or an error, the Rust SDK's Result: `if (!r) return r.error();` or UDF_TRY(r).
template <class T>
class Result {
public:
	Result(T value) : ok_(true), value_(static_cast<T&&>(value)) {}
	Result(Error e) : ok_(false), value_(), err_(e) {}

	bool ok() const { return ok_; }
	explicit operator bool() const { return ok_; }
	const T& operator*() const { return value_; }
	T& operator*() { return value_; }
	const T* operator->() const { return &value_; }
	T* operator->() { return &value_; }
	T take() { return static_cast<T&&>(value_); }
	Error error() const { return err_; }

private:
	bool ok_;
	T value_;
	Error err_;
};

template <>
class Result<void> {
public:
	Result() : ok_(true) {}
	Result(Error e) : ok_(false), err_(e) {}

	bool ok() const { return ok_; }
	explicit operator bool() const { return ok_; }
	void take() const {}
	Error error() const { return err_; }

private:
	bool ok_;
	Error err_;
};

using Ok = Result<void>;

// A value that may be missing, for a bin that is absent or nil and a side of a merge that has no such bin.
template <class T>
class Optional {
public:
	Optional() : has_(false), value_() {}
	Optional(T value) : has_(true), value_(static_cast<T&&>(value)) {}

	bool has_value() const { return has_; }
	explicit operator bool() const { return has_; }
	const T& operator*() const { return value_; }
	T& operator*() { return value_; }
	const T* operator->() const { return &value_; }
	T* operator->() { return &value_; }
	T value_or(T dflt) const { return has_ ? value_ : dflt; }

private:
	bool has_;
	T value_;
};

// Propagates an error out of a function returning a Result, and yields the value otherwise: the Rust `?`.
// A statement expression, which clang and gcc provide.
#define UDF_TRY(expr)                                                     \
	({                                                                    \
		auto&& udf_try_r = (expr);                                        \
		if (!udf_try_r.ok()) return udf_try_r.error();                    \
		udf_try_r.take();                                                 \
	})

namespace detail {

inline Error status_error(Status rc, const char* what)
{
	return Error{rc, message().add(what).add(" (").add((int64_t)rc).add(")").buf};
}

} // namespace detail

//==========================================================
// Strings and bytes without an allocator.
//

// A view of bytes the caller keeps alive: a literal, a Text, a buffer.
struct StrView {
	const char* data = "";
	int32_t len = 0;

	constexpr StrView() = default;
	constexpr StrView(const char* s) : data(s), len(detail::cstrlen(s)) {}
	constexpr StrView(const char* s, int32_t n) : data(s), len(n) {}

	constexpr bool operator==(StrView o) const { return detail::str_eq(data, len, o.data, o.len); }
	constexpr bool operator!=(StrView o) const { return !(*this == o); }
	constexpr bool operator<(StrView o) const
	{
		int32_t n = len < o.len ? len : o.len;
		for (int32_t i = 0; i < n; i++) {
			if ((unsigned char)data[i] != (unsigned char)o.data[i]) return (unsigned char)data[i] < (unsigned char)o.data[i];
		}
		return len < o.len;
	}
	constexpr bool starts_with(StrView p) const { return len >= p.len && detail::str_eq(data, p.len, p.data, p.len); }
};

// A string of up to N bytes, inline: the Rust SDK's Str<N>. A UDF parameter of this type takes a string
// argument of at most N bytes (`str[N]` in the signature).
template <size_t N>
struct Text {
	char buf[N + 1] = {};
	int32_t len = 0;

	constexpr Text() = default;
	constexpr Text(StrView s) { assign(s); }

	constexpr bool assign(StrView s)
	{
		if (s.len > (int32_t)N) return false;
		for (int32_t i = 0; i < s.len; i++) buf[i] = s.data[i];
		buf[s.len] = '\0';
		len = s.len;
		return true;
	}

	constexpr StrView view() const { return StrView{buf, len}; }
	constexpr operator StrView() const { return view(); }
	constexpr const char* c_str() const { return buf; }
	constexpr bool operator==(StrView o) const { return view() == o; }
	constexpr bool operator!=(StrView o) const { return !(view() == o); }
	constexpr bool operator<(const Text& o) const { return view() < o.view(); }
};

// Bytes of up to N, inline: the Rust SDK's Bytes<N>; a parameter of this type takes a string or a blob.
template <size_t N>
struct Bytes {
	unsigned char buf[N] = {};
	int32_t len = 0;
};

//==========================================================
// Values.
//

enum class Kind : int32_t {
	Absent = AS_UDF_ABSENT,
	Nil = AS_UDF_NIL,
	Bool = AS_UDF_BOOL,
	Int = AS_UDF_INT,
	Double = AS_UDF_DOUBLE,
	Str = AS_UDF_STR,
	Blob = AS_UDF_BLOB,
	List = AS_UDF_LIST,
	Map = AS_UDF_MAP,
	Other = AS_UDF_OTHER,
	GeoJson = AS_UDF_GEOJSON,
	Hll = AS_UDF_HLL,
	GuestStr = AS_UDF_GUEST_STR,
	GuestBlob = AS_UDF_GUEST_BLOB,
	GuestGeoJson = AS_UDF_GUEST_GEOJSON,
	Handle = AS_UDF_HANDLE,
};

class ListRange;
class MapRange;

// The 16-byte value every read is: the C struct with the accessors. A stored string or collection is
// held by position, never copied until asked.
struct Value : as_udf_value {
	constexpr Value() : as_udf_value{AS_UDF_ABSENT, 0, 0} {}
	constexpr Value(const as_udf_value& v) : as_udf_value(v) {}

	constexpr Kind kind() const { return (Kind)this->as_udf_value::kind; }
	constexpr bool is_absent() const { return kind() == Kind::Absent; }
	constexpr bool is_nil() const { return kind() == Kind::Nil; }
	constexpr bool is_null() const { return is_nil() || is_absent(); }
	constexpr bool is_int() const { return kind() == Kind::Int; }
	constexpr bool is_double() const { return kind() == Kind::Double; }
	constexpr bool is_bool() const { return kind() == Kind::Bool; }
	constexpr bool is_str() const { return kind() == Kind::Str; }
	constexpr bool is_blob() const { return kind() == Kind::Blob; }
	constexpr bool is_list() const { return kind() == Kind::List; }
	constexpr bool is_map() const { return kind() == Kind::Map; }
	constexpr bool is_stored() const
	{
		Kind k = kind();
		return k == Kind::Str || k == Kind::Blob || k == Kind::GeoJson || k == Kind::Hll || k == Kind::List || k == Kind::Map;
	}

	int32_t src() const { return as_udf_value_src(this); }
	int32_t at() const { return as_udf_value_at(this); }
	double as_double_bits() const { return as_udf_value_double(this); }

	// The scalar kinds as C++ values, or an error naming the kind found.
	Result<int64_t> as_int() const
	{
		if (!is_int()) return kind_error("an int");
		return payload;
	}

	// A double, or an int widened to one.
	Result<double> as_double() const
	{
		if (is_double()) return as_double_bits();
		if (is_int()) return (double)payload;
		return kind_error("a double");
	}

	Result<bool> as_bool() const
	{
		if (!is_bool()) return kind_error("a bool");
		return payload != 0;
	}

	// The text of a stored string into an inline Text<N>; an error when it does not fit.
	template <size_t N>
	Result<Text<N>> as_text() const
	{
		if (kind() == Kind::GuestStr) return Text<N>(StrView{(const char*)(uintptr_t)payload, (int32_t)len});
		if (!is_str()) return kind_error("a string");
		Text<N> t;
		if (len > N) return error(AS_UDF_NEEDED_BASE, "the string does not fit");
		int32_t n = as_udf_bytes(this, t.buf, (int32_t)N);
		if (n < 0) return detail::status_error(n, "could not read the string");
		t.len = n;
		t.buf[n] = '\0';
		return t;
	}

	// The bytes of a stored string, blob, GeoJSON or HLL into a buffer. Returns how many.
	Result<int32_t> bytes_into(void* out, int32_t cap) const
	{
		if (!is_stored() || is_list() || is_map()) return kind_error("a string or blob");
		int32_t n = as_udf_bytes(this, out, cap);
		if (n < 0) return detail::status_error(n, "could not read the bytes");
		return n;
	}

	template <size_t N>
	Result<Bytes<N>> as_bytes() const
	{
		Bytes<N> b;
		Result<int32_t> n = bytes_into(b.buf, (int32_t)N);
		if (!n) return n.error();
		b.len = *n;
		return b;
	}

	// A blob of 32-bit floats straight into an array, element for element. Returns how many.
	Result<int32_t> floats_into(float* out, int32_t count) const
	{
		if (!is_blob()) return kind_error("a blob");
		int32_t n = as_udf_bytes(this, out, count * 4);
		if (n < 0) return detail::status_error(n, "the blob does not fit the array");
		return n / 4;
	}

	// Whether a stored string reads as text.
	bool is(StrView text) const { return is_str() && (int32_t)len == text.len && str_eq_stored(text); }

	// A stored value's bytes as the server stores them, msgpack header included: the encoding a convergent
	// merge compares when nothing else tells two candidates apart.
	Result<int32_t> stored_bytes_into(void* out, int32_t cap) const
	{
		int32_t n = as_udf_stored_bytes(this, out, cap);
		if (n < 0) return detail::status_error(n, "could not read the stored bytes");
		return n;
	}

	// Compares two stored values byte by byte without copying either: negative, zero or positive.
	int32_t cmp_stored(const Value& other) const { return as_udf_cmp_stored(this, &other); }

	// The entry under a string or integer key of a stored map; nothing when there is none.
	Optional<Value> get(StrView key) const
	{
		Value out;
		if (!is_map() || as_udf_coll_get(src(), at(), key.data, key.len, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	Optional<Value> get(int64_t key) const
	{
		Value out;
		if (!is_map() || as_udf_coll_get_i(src(), at(), key, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	// A walk over a stored list or map: `for (Value v : value.list())`, `for (auto [k, v] : value.map())`.
	inline ListRange list() const;
	inline MapRange map() const;

	// Values of the guest's, for a batch, a result or a collection entry.
	static constexpr Value of(int64_t i) { Value v; v.as_udf_value::kind = AS_UDF_INT; v.payload = i; return v; }
	static constexpr Value of(int i) { return of((int64_t)i); }
	static Value of(const char* s) { return of(StrView(s)); }
	static Value of(double d) { Value v; v.as_udf_value::kind = AS_UDF_DOUBLE; __builtin_memcpy(&v.payload, &d, 8); return v; }
	static constexpr Value of(bool b) { Value v; v.as_udf_value::kind = AS_UDF_BOOL; v.payload = b ? 1 : 0; return v; }
	static Value of(StrView s) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_STR; v.len = (uint32_t)s.len; v.payload = (int64_t)(uintptr_t)s.data; return v; }
	static Value blob(const void* b, int32_t n) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_BLOB; v.len = (uint32_t)n; v.payload = (int64_t)(uintptr_t)b; return v; }
	static Value geojson(StrView doc) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_GEOJSON; v.len = (uint32_t)doc.len; v.payload = (int64_t)(uintptr_t)doc.data; return v; }
	static constexpr Value nil() { Value v; v.as_udf_value::kind = AS_UDF_NIL; return v; }
	static constexpr Value handle(int32_t h) { Value v; v.as_udf_value::kind = AS_UDF_HANDLE; v.payload = h; return v; }

private:
	Error kind_error(const char* want) const
	{
		return Error{AS_UDF_TYPE_MISMATCH, detail::message().add("the value is not ").add(want).add(" (kind ").add((int64_t)this->as_udf_value::kind).add(")").buf};
	}

	bool str_eq_stored(StrView text) const
	{
		char buf[128];
		if (text.len > (int32_t)sizeof(buf)) return false;
		int32_t n = as_udf_bytes(this, buf, (int32_t)sizeof(buf));
		return n == text.len && detail::str_eq(buf, n, text.data, text.len);
	}
};

//==========================================================
// Ranging over stored collections: the Rust SDK's Coll::iter and entries. An iterator owns a cursor and a
// window of 32 values; the cursor is released when the loop ends or breaks (the iterator's destructor).
// Collections nest, as long as the inner walk ends before the outer one steps on.
//

struct Entry {
	Value key;
	Value value;
};

namespace detail {

struct CursorIter {
	int32_t cursor = 0;
	Value window[32];
	int32_t got = 0;
	int32_t at = 0;
	bool last = false;
	bool map = false;

	CursorIter() = default;
	CursorIter(const CursorIter&) = delete;
	CursorIter& operator=(const CursorIter&) = delete;
	CursorIter(CursorIter&& o) : cursor(o.cursor), got(o.got), at(o.at), last(o.last), map(o.map)
	{
		for (int32_t i = 0; i < got; i++) window[i] = o.window[i];
		o.cursor = 0;
	}
	~CursorIter() { close(); }

	void close()
	{
		if (cursor > 0) as_udf_cursor_close(cursor);
		cursor = 0;
	}

	bool done() const { return cursor <= 0; }

	// Fills the window and positions on its first element; closes when it is empty.
	void prime()
	{
		if (cursor <= 0) return;
		got = as_udf_cursor_values(cursor, window, (int32_t)sizeof(window));
		at = 0;
		last = got < 32;
		if (got < (map ? 2 : 1)) close();
	}
};

} // namespace detail

struct RangeEnd {};

class ListRange {
public:
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll)
		{
			map = false;
			cursor = coll.is_list() ? as_udf_cursor_enter_at(coll.src(), coll.at()) : 0;
			prime();
		}
		Iterator(Iterator&&) = default;
		Value operator*() const { return window[at]; }
		Iterator& operator++()
		{
			if (at + 1 < got) {
				at++;
			}
			else if (last) {
				close();
			}
			else {
				prime();
			}
			return *this;
		}
		bool operator!=(RangeEnd) const { return !done(); }
	};

	explicit ListRange(const Value& coll) : coll_(coll) {}
	Iterator begin() const { return Iterator(coll_); }
	RangeEnd end() const { return {}; }

private:
	Value coll_;
};

class MapRange {
public:
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll)
		{
			map = true;
			cursor = coll.is_map() ? as_udf_cursor_enter_at(coll.src(), coll.at()) : 0;
			prime();
		}
		Iterator(Iterator&&) = default;
		Entry operator*() const { return Entry{window[at], window[at + 1]}; }
		Iterator& operator++()
		{
			if (at + 3 < got) {
				at += 2;
			}
			else if (last) {
				close();
			}
			else {
				prime();
			}
			return *this;
		}
		bool operator!=(RangeEnd) const { return !done(); }
	};

	explicit MapRange(const Value& coll) : coll_(coll) {}
	Iterator begin() const { return Iterator(coll_); }
	RangeEnd end() const { return {}; }

private:
	Value coll_;
};

inline ListRange Value::list() const { return ListRange(*this); }
inline MapRange Value::map() const { return MapRange(*this); }

// Whether a stored list of strings holds text.
inline bool contains(const Value& list, StrView text)
{
	for (Value v : list.list()) if (v.is(text)) return true;
	return false;
}

//==========================================================
// Collections built on the host: the Rust SDK's List and Map. Every put is one import and chains; the
// first refusal is kept and answered by error(), and a UDF returning a Map or List fails with it.
//

class List;

class Map {
public:
	// A null map, for a Result<Map> that failed; a real one takes a capacity.
	Map() : handle_(0) {}
	explicit Map(int32_t capacity) : handle_(as_udf_map_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a map");
	}
	static Map wrap(int32_t handle) { Map m(Wrapped{}, handle); return m; }

	int32_t handle() const { return handle_; }
	Error error() const { return err_; }
	Result<void> result() const { return err_.failed() ? Result<void>(err_) : Result<void>(); }
	int32_t size() const { return as_udf_coll_size(handle_); }
	Value value() const { return Value::handle(handle_); }

	// Whether the map holds key: it packs the map, so for the occasional check, not a loop.
	bool has(StrView key) const
	{
		int32_t src = as_udf_pack_handle(handle_);
		if (src <= 0) return false;
		Value v;
		return as_udf_coll_get(src, 0, key.data, key.len, &v, (int32_t)sizeof(v)) == AS_UDF_OK;
	}

	Map& put(StrView key, int64_t v) { return check(as_udf_map_put_str_int(handle_, key.data, key.len, v), key); }
	Map& put(StrView key, int v) { return put(key, (int64_t)v); }
	Map& put(StrView key, double v) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, as_udf_val_double(v)), key); }
	Map& put(StrView key, bool v) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, as_udf_val_bool(v ? 1 : 0)), key); }
	Map& put(StrView key, StrView v) { return check(as_udf_map_put_str_str(handle_, key.data, key.len, v.data, v.len), key); }
	Map& put(StrView key, const char* v) { return put(key, StrView(v)); }
	Map& put(StrView key, const Value& v) { return check(as_udf_map_put_value(handle_, c_str(key), &v), key); }
	Map& put(StrView key, const Map& v) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, v.handle_), key).carry(v.err_); }
	inline Map& put(StrView key, const List& v);
	Map& put_blob(StrView key, const void* b, int32_t n) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, as_udf_val_blob(b, n)), key); }
	Map& put_geojson(StrView key, StrView doc) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, as_udf_val_geojson(doc.data, doc.len)), key); }
	// A nil entry: in a record answer, "no such bin".
	Map& put_null(StrView key) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, as_udf_val_nil()), key); }

	// Integer keys.
	Map& put(int64_t key, int64_t v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_int(v)), "an integer key"); }
	Map& put(int64_t key, int v) { return put(key, (int64_t)v); }
	Map& put(int64_t key, StrView v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_str(v.data, v.len)), "an integer key"); }
	Map& put(int64_t key, const char* v) { return put(key, StrView(v)); }
	Map& put(int64_t key, const Value& v) { return check(put_i_value(key, v), "an integer key"); }
	Map& put(int64_t key, const Map& v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), v.handle_), "an integer key").carry(v.err_); }
	inline Map& put(int64_t key, const List& v);

	// Every entry of a stored map, by position.
	Map& put_all(const Value& stored)
	{
		for (Entry e : stored.map()) {
			if (e.key.is_int()) {
				put(e.key.payload, e.value);
			}
			else {
				Result<Text<64>> k = e.key.as_text<64>();
				if (!k) { carry(k.error()); break; }
				put(k->view(), e.value);
			}
		}
		return *this;
	}

	// Pairs (pairs[0], pairs[1] is the first) in one import.
	Map& put_entries(const Value* pairs, int32_t count)
	{
		int32_t put = as_udf_map_put_entries(handle_, pairs, count * (int32_t)sizeof(Value));
		if (put != count / 2 && !err_.failed()) err_ = udf::error(AS_UDF_ERR, "could not put an entry");
		return *this;
	}

private:
	struct Wrapped {};
	Map(Wrapped, int32_t handle) : handle_(handle) {}

	static const char* c_str(StrView key)
	{
		static char buf[64];
		int32_t n = key.len < 63 ? key.len : 63;
		for (int32_t i = 0; i < n; i++) buf[i] = key.data[i];
		buf[n] = '\0';
		return buf;
	}

	int32_t put_i_value(int64_t key, const Value& v)
	{
		switch (v.kind()) {
		case Kind::Int: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_int(v.payload));
		case Kind::Double: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_double(v.as_double_bits()));
		case Kind::Bool: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_bool((int32_t)v.payload));
		case Kind::Nil: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_nil());
		case Kind::GuestStr: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_str((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestBlob: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return as_udf_map_put(handle_, as_udf_val_int(key), (int32_t)v.payload);
		case Kind::Absent: return AS_UDF_NOT_FOUND;
		default: return as_udf_map_put_i_at(handle_, key, v.src(), v.at());
		}
	}

	Map& check(int32_t rc, StrView key)
	{
		if (rc != AS_UDF_OK && !err_.failed()) {
			err_ = Error{rc, detail::message().add("could not put ").add(key.data, key.len).add(" (").add((int64_t)rc).add(")").buf};
		}
		return *this;
	}

	Map& carry(Error e)
	{
		if (e.failed() && !err_.failed()) err_ = e;
		return *this;
	}

	int32_t handle_;
	Error err_;
	friend class List;
};

class List {
public:
	List() : handle_(0) {}
	explicit List(int32_t capacity) : handle_(as_udf_list_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a list");
	}
	static List wrap(int32_t handle) { List l(Wrapped{}, handle); return l; }

	int32_t handle() const { return handle_; }
	Error error() const { return err_; }
	Result<void> result() const { return err_.failed() ? Result<void>(err_) : Result<void>(); }
	int32_t size() const { return as_udf_coll_size(handle_); }
	Value value() const { return Value::handle(handle_); }

	List& add(int64_t v) { return check(as_udf_list_push_int(handle_, v)); }
	List& add(int v) { return add((int64_t)v); }
	List& add(double v) { return check(as_udf_list_push(handle_, as_udf_val_double(v))); }
	List& add(bool v) { return check(as_udf_list_push(handle_, as_udf_val_bool(v ? 1 : 0))); }
	List& add(StrView v) { return check(as_udf_list_push_str(handle_, v.data, v.len)); }
	List& add(const char* v) { return add(StrView(v)); }
	List& add(const Value& v) { return check(push_value(v)); }
	List& add(const Map& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	List& add(const List& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	List& add_blob(const void* b, int32_t n) { return check(as_udf_list_push(handle_, as_udf_val_blob(b, n))); }
	List& add_geojson(StrView doc) { return check(as_udf_list_push(handle_, as_udf_val_geojson(doc.data, doc.len))); }
	List& add_null() { return check(as_udf_list_push(handle_, as_udf_val_nil())); }

	// Every element of a stored list, by position.
	List& add_all(const Value& stored)
	{
		for (Value v : stored.list()) add(v);
		return *this;
	}

	// Values in one import.
	List& add_values(const Value* vals, int32_t count)
	{
		int32_t added = as_udf_list_push_values(handle_, vals, count * (int32_t)sizeof(Value));
		if (added != count && !err_.failed()) err_ = udf::error(AS_UDF_ERR, "could not add a value");
		return *this;
	}

private:
	struct Wrapped {};
	List(Wrapped, int32_t handle) : handle_(handle) {}

	int32_t push_value(const Value& v)
	{
		switch (v.kind()) {
		case Kind::Int: return as_udf_list_push_int(handle_, v.payload);
		case Kind::Double: return as_udf_list_push(handle_, as_udf_val_double(v.as_double_bits()));
		case Kind::Bool: return as_udf_list_push(handle_, as_udf_val_bool((int32_t)v.payload));
		case Kind::Nil: return as_udf_list_push(handle_, as_udf_val_nil());
		case Kind::GuestStr: return as_udf_list_push_str(handle_, (const char*)(uintptr_t)v.payload, (int32_t)v.len);
		case Kind::GuestBlob: return as_udf_list_push(handle_, as_udf_val_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return as_udf_list_push(handle_, as_udf_val_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return as_udf_list_push(handle_, (int32_t)v.payload);
		case Kind::Absent: return AS_UDF_NOT_FOUND;
		default: return as_udf_list_push_at(handle_, v.src(), v.at());
		}
	}

	List& check(int32_t rc)
	{
		if (rc != AS_UDF_OK && !err_.failed()) err_ = detail::status_error(rc, "could not add to the list");
		return *this;
	}

	List& carry(Error e)
	{
		if (e.failed() && !err_.failed()) err_ = e;
		return *this;
	}

	int32_t handle_;
	Error err_;
	friend class Map;
};

inline Map& Map::put(StrView key, const List& v) { return check(as_udf_map_put_str_handle(handle_, key.data, key.len, v.handle_), key).carry(v.err_); }
inline Map& Map::put(int64_t key, const List& v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), v.handle_), "an integer key").carry(v.err_); }

//==========================================================
// Bins of the record in hand.
//

// A bin by name, made once (`static constexpr udf::Bin COUNTS{"counts"}`): typed reads with or without a
// default, writes of every kind, staged until commit(), and the walk of a collection bin.
class Bin {
public:
	constexpr Bin(const char* name) : name_(name), len_(detail::cstrlen(name)) {}

	constexpr StrView name() const { return StrView{name_, len_}; }

	// The bin as a value; nothing when the record has no such bin.
	Optional<Value> get() const
	{
		Value v;
		if (as_udf_bin_value(name_, len_, &v, (int32_t)sizeof(v)) != AS_UDF_OK) return {};
		return v;
	}

	bool exists() const
	{
		Value v;
		return as_udf_bin_value(name_, len_, &v, (int32_t)sizeof(v)) == AS_UDF_OK;
	}

	Result<int64_t> as_int() const { Result<Value> v = typed(Kind::Int, "an int"); if (!v) return v.error(); return v->as_int(); }
	Result<double> as_double() const { Result<Value> v = read(); if (!v) return v.error(); return v->as_double(); }
	Result<bool> as_bool() const { Result<Value> v = typed(Kind::Bool, "a bool"); if (!v) return v.error(); return v->as_bool(); }
	template <size_t N> Result<Text<N>> as_text() const { Result<Value> v = read(); if (!v) return v.error(); return v->as_text<N>(); }
	template <size_t N> Result<Bytes<N>> as_bytes() const { Result<Value> v = read(); if (!v) return v.error(); return v->as_bytes<N>(); }
	Result<int32_t> floats_into(float* out, int32_t count) const { Result<Value> v = read(); if (!v) return v.error(); return v->floats_into(out, count); }

	// The Or forms: the default when the bin is absent or nil (or of another kind).
	int64_t int_or(int64_t dflt) const { Optional<Value> v = get(); return v && v->is_int() ? v->payload : dflt; }
	double double_or(double dflt) const { Optional<Value> v = get(); return v && v->is_double() ? v->as_double_bits() : dflt; }
	bool bool_or(bool dflt) const { Optional<Value> v = get(); return v && v->is_bool() ? v->payload != 0 : dflt; }

	// A walk over a collection bin; nothing when absent or of another kind.
	ListRange list() const { Optional<Value> v = get(); return ListRange(v ? *v : Value()); }
	MapRange map() const { Optional<Value> v = get(); return MapRange(v ? *v : Value()); }

	// Writes, staged until commit().
	Result<void> set(int64_t v) const { return check(as_udf_set_int(name_, len_, v)); }
	Result<void> set(int v) const { return set((int64_t)v); }
	Result<void> set(double v) const { return check(as_udf_set_double(name_, len_, v)); }
	Result<void> set(bool v) const { return check(as_udf_set_bool(name_, len_, v ? 1 : 0)); }
	Result<void> set(StrView v) const { return check(as_udf_set_str(name_, len_, v.data, v.len)); }
	Result<void> set(const char* v) const { return set(StrView(v)); }
	Result<void> set(const Map& m) const { if (m.error().failed()) return m.error(); return check(as_udf_set_handle(name_, len_, m.handle())); }
	Result<void> set(const List& l) const { if (l.error().failed()) return l.error(); return check(as_udf_set_handle(name_, len_, l.handle())); }
	Result<void> set_blob(const void* b, int32_t n) const { return check(as_udf_set_blob(name_, len_, b, n)); }
	// A GeoJSON document, kept as the geospatially indexable kind rather than a string.
	Result<void> set_geojson(StrView doc) const { return check(as_udf_set_geojson(name_, len_, doc.data, doc.len)); }
	Result<void> set_null() const { return check(as_udf_set_nil(name_, len_)); }
	// A list of integers or doubles from an array, in one import, no value built per element.
	Result<void> set_ints(const int64_t* vals, int32_t count) const { return check(as_udf_set_list_ints(name_, len_, vals, count * 8)); }
	Result<void> set_doubles(const double* vals, int32_t count) const { return check(as_udf_set_list_doubles(name_, len_, vals, count * 8)); }

	// A value: one read from the record by position, keeping its kind; or a scalar, guest bytes, a handle.
	Result<void> set(const Value& v) const
	{
		switch (v.kind()) {
		case Kind::Int: return set(v.payload);
		case Kind::Double: return set(v.as_double_bits());
		case Kind::Bool: return set(v.payload != 0);
		case Kind::Nil: return set_null();
		case Kind::GuestStr: return check(as_udf_set_str(name_, len_, (const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestBlob: return check(as_udf_set_blob(name_, len_, (const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return check(as_udf_set_geojson(name_, len_, (const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return check(as_udf_set_handle(name_, len_, (int32_t)v.payload));
		case Kind::Absent: return Error{AS_UDF_NOT_FOUND, "could not write an absent value"};
		default: return check(as_udf_set_at(name_, len_, v.src(), v.at()));
		}
	}

	Result<void> remove() const { return check(as_udf_bin_remove(name_, len_)); }

private:
	Result<Value> read() const
	{
		Value v;
		int32_t rc = as_udf_bin_value(name_, len_, &v, (int32_t)sizeof(v));
		if (rc == AS_UDF_NOT_FOUND) return Error{rc, detail::message().add("bin ").add(name_).add(" is absent").buf};
		if (rc != AS_UDF_OK) return Error{rc, detail::message().add("bin ").add(name_).add(" could not be read").buf};
		return v;
	}

	Result<Value> typed(Kind kind, const char* what) const
	{
		Result<Value> v = read();
		if (!v) return v;
		if (v->kind() != kind) return Error{AS_UDF_TYPE_MISMATCH, detail::message().add("bin ").add(name_).add(" is not ").add(what).buf};
		return v;
	}

	Result<void> check(int32_t rc) const
	{
		if (rc == AS_UDF_OK) return {};
		return Error{rc, detail::message().add("could not write bin ").add(name_).add(" (").add((int64_t)rc).add(")").buf};
	}

	const char* name_;
	int32_t len_;
};

//==========================================================
// The record: commit, delete, metadata, results.
//

// Writes the staged bins, creating the record if it does not exist.
inline Result<void> commit()
{
	int32_t rc = as_udf_rec_exists() ? as_udf_rec_update() : as_udf_rec_create();
	if (rc != AS_UDF_OK) return detail::status_error(rc, "could not commit the record");
	return {};
}

inline Result<void> remove_record()
{
	int32_t rc = as_udf_rec_remove();
	if (rc != AS_UDF_OK) return detail::status_error(rc, "could not delete the record");
	return {};
}

inline bool exists() { return as_udf_rec_exists() != 0; }
inline int64_t generation() { return as_udf_gen(); }
inline int64_t ttl() { return as_udf_ttl(); }
inline int64_t last_update_time() { return as_udf_last_update(); }
inline int64_t record_size() { return as_udf_record_size(); }
inline int64_t bin_count() { return as_udf_numbins(); }
// The server's clock in ms since the epoch, or -1 inside an event handler, where it is refused.
inline int64_t now() { return as_udf_current_time(); }

// The record's set name, into a Text; empty when it is in no set.
inline Text<64> set_name()
{
	Text<64> t;
	int32_t n = as_udf_set_name(t.buf, 64);
	t.len = n < 0 ? 0 : n;
	t.buf[t.len] = '\0';
	return t;
}

// Sets the time to live the next commit carries, in seconds: 0 for the namespace default, -1 for never.
inline Result<void> set_ttl(int32_t seconds)
{
	int32_t rc = as_udf_set_ttl(seconds);
	if (rc != AS_UDF_OK) return detail::status_error(rc, "could not set the TTL");
	return {};
}

inline Kind key_type() { return (Kind)as_udf_key_type(); }
inline int64_t key_int() { return as_udf_key_int(); }
inline int32_t key_bytes(void* out, int32_t cap) { return as_udf_key_bytes(out, cap); }
inline int32_t digest(void* out, int32_t cap) { return as_udf_digest(out, cap); }

// Fails the call with a message and returns the status an export returns.
inline int32_t fail(StrView message)
{
	as_udf_fail(message.data, message.len);
	return 1;
}

inline int32_t fail(Error e) { return fail(StrView(e.message != nullptr ? e.message : "failed")); }

inline void log(int32_t level, StrView message) { as_udf_log(level, message.data, message.len); }

// Sets a value of any kind as the result.
inline Result<void> result_of(const Value& v)
{
	switch (v.kind()) {
	case Kind::Int: as_udf_result_int(v.payload); break;
	case Kind::Double: as_udf_result_double(v.as_double_bits()); break;
	case Kind::Bool: as_udf_result_bool((int32_t)v.payload); break;
	case Kind::Nil: case Kind::Absent: as_udf_result_nil(); break;
	case Kind::GuestStr: as_udf_result_str((const char*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::GuestBlob: as_udf_result_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::GuestGeoJson: as_udf_result_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::Handle: {
		int32_t rc = as_udf_result_handle((int32_t)v.payload);
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not return the collection");
		break;
	}
	default: {
		int32_t rc = as_udf_result_at(v.src(), v.at());
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not return the value");
		break;
	}
	}
	return {};
}

// Timers, from a record UDF or a timer handler.
inline Result<void> timer_set(StrView name, int64_t delay_ms)
{
	int32_t rc = as_udf_timer_set(name.data, name.len, delay_ms);
	if (rc != AS_UDF_OK) return detail::status_error(rc, "could not set the timer");
	return {};
}

inline Result<void> timer_cancel(StrView name)
{
	int32_t rc = as_udf_timer_cancel(name.data, name.len);
	if (rc != AS_UDF_OK) return detail::status_error(rc, "could not cancel the timer");
	return {};
}

//==========================================================
// Events, the typed tier.
//

// One record an event hands over, read-only, with its metadata and typed reads; the C struct underneath.
struct EventRecord : as_udf_event_record {
	// The bin as a value; nothing when the side has no such bin or it is nil.
	Optional<Value> get(StrView name) const
	{
		Value v;
		if (as_udf_event_bin_value(which, name.data, name.len, &v, (int32_t)sizeof(v)) != AS_UDF_OK || v.is_nil()) return {};
		return v;
	}

	// Whether the side has the bin, nil included.
	bool has(StrView name) const
	{
		Value v;
		return as_udf_event_bin_value(which, name.data, name.len, &v, (int32_t)sizeof(v)) == AS_UDF_OK;
	}

	Optional<int64_t> as_int(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_int()) return {}; return v->payload; }
	Optional<double> as_double(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_double()) return {}; return v->as_double_bits(); }
	Optional<bool> as_bool(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_bool()) return {}; return v->payload != 0; }
	template <size_t N> Optional<Text<N>> as_text(StrView name) const
	{
		Optional<Value> v = get(name);
		if (!v) return {};
		Result<Text<N>> t = v->as_text<N>();
		if (!t) return {};
		return *t;
	}

	ListRange list(StrView name) const { Optional<Value> v = get(name); return ListRange(v && v->is_list() ? *v : Value()); }
	MapRange map(StrView name) const { Optional<Value> v = get(name); return MapRange(v && v->is_map() ? *v : Value()); }

	// The record's bin names.
	ListRange names() const { return ListRange(Value(bins)); }

	int64_t bin_lut(StrView name) const { return as_udf_map_int(&bin_luts, Map_c_str(name), lut); }
	int64_t bin_src(StrView name) const { return as_udf_map_int(&bin_srcs, Map_c_str(name), src_id); }

	bool deleted_any() const { return deleted.kind == AS_UDF_LIST && deleted.len > 0; }

	// `overrides` with every bin of this record it lacks put on top, by position: the Rust SDK's
	// with_bins. It prefers one side, so an xdr-merge handler uses Pair::with_newer instead.
	Map& with_bins(Map& overrides) const
	{
		for (Value name : names()) {
			Result<Text<64>> n = name.as_text<64>();
			if (!n) continue;
			if (overrides.has(n->view())) continue;
			Value v;
			if (as_udf_event_bin_value(which, n->buf, n->len, &v, (int32_t)sizeof(v)) == AS_UDF_OK) overrides.put(n->view(), v);
		}
		return overrides;
	}

private:
	static const char* Map_c_str(StrView key)
	{
		static char buf[64];
		int32_t n = key.len < 63 ? key.len : 63;
		for (int32_t i = 0; i < n; i++) buf[i] = key.data[i];
		buf[n] = '\0';
		return buf;
	}
};

static_assert(sizeof(EventRecord) == sizeof(as_udf_event_record), "EventRecord adds no state");

// A bin on both sides of a conflict, either of which may be missing: the Rust SDK's Both<T>.
template <class T>
struct Both {
	Optional<T> local;
	Optional<T> remote;
	int64_t local_lut = 0;
	int64_t remote_lut = 0;
	int64_t local_src = 0;
	int64_t remote_src = 0;

	// Whether the remote side wrote this bin later: by the bin's LUT, then source id - the order the server's
	// own bin convergence uses.
	bool remote_newer() const { return remote_lut > local_lut || (remote_lut == local_lut && remote_src > local_src); }

	Optional<T> newer() const { return remote_newer() ? remote_or_local() : local_or_remote(); }
	Optional<T> older() const { return remote_newer() ? local_or_remote() : remote_or_local(); }
	Optional<T> local_or_remote() const { return local ? local : remote; }
	Optional<T> remote_or_local() const { return remote ? remote : local; }

	template <class F>
	Optional<T> merge(F f) const
	{
		if (local && remote) return f(*local, *remote);
		return local_or_remote();
	}

	Optional<T> max() const { return merge([](const T& l, const T& r) { return r > l ? r : l; }); }
	Optional<T> min() const { return merge([](const T& l, const T& r) { return r < l ? r : l; }); }
	T sum() const { return local.value_or(T()) + remote.value_or(T()); }
};

// The two sides of a conflict, read bin by bin into a Both.
struct Pair {
	const EventRecord& local;
	const EventRecord& remote;

	Both<int64_t> as_int(StrView name) const { return both(name, local.as_int(name), remote.as_int(name)); }
	Both<double> as_double(StrView name) const { return both(name, local.as_double(name), remote.as_double(name)); }
	Both<bool> as_bool(StrView name) const { return both(name, local.as_bool(name), remote.as_bool(name)); }
	Both<Value> bin(StrView name) const { return both(name, local.get(name), remote.get(name)); }
	Both<Value> list(StrView name) const
	{
		Optional<Value> l = local.get(name), r = remote.get(name);
		return both(name, l && l->is_list() ? l : Optional<Value>(), r && r->is_list() ? r : Optional<Value>());
	}

	bool remote_newer(StrView name) const
	{
		int64_t ll = local.bin_lut(name), rl = remote.bin_lut(name);
		return rl > ll || (rl == ll && remote.bin_src(name) > local.bin_src(name));
	}

	// Every bin either side has that `overrides` lacks, from the side that wrote it later: symmetric.
	Map& with_newer(Map& overrides) const
	{
		const EventRecord* sides[2] = {&local, &remote};
		for (const EventRecord* side : sides) {
			for (Value name : side->names()) {
				Result<Text<64>> n = name.as_text<64>();
				if (!n || overrides.has(n->view())) continue;
				Optional<Value> v = bin(n->view()).newer();
				if (v) overrides.put(n->view(), *v);
			}
		}
		return overrides;
	}

private:
	template <class T>
	Both<T> both(StrView name, Optional<T> l, Optional<T> r) const
	{
		Both<T> b;
		b.local = l;
		b.remote = r;
		b.local_lut = local.bin_lut(name);
		b.remote_lut = remote.bin_lut(name);
		b.local_src = local.bin_src(name);
		b.remote_src = remote.bin_src(name);
		return b;
	}
};

// What an xdr-merge is about; the C struct with the typed records and the pair.
struct Conflict : as_udf_conflict {
	const EventRecord& current() const { return static_cast<const EventRecord&>(local); }
	const EventRecord& incoming() const { return static_cast<const EventRecord&>(remote); }
	Pair pair() const { return Pair{current(), incoming()}; }
	bool involves_delete() const { return current().deleted_any() || incoming().deleted_any(); }
	Value lost_bins() const { return Value(lost); }
};

static_assert(sizeof(Conflict) == sizeof(as_udf_conflict), "Conflict adds no state");

// The kinds' own inputs: the C structs, with the strings as views.
struct WriteInfo : as_udf_write_info { StrView source_view() const { return StrView(source); } };
struct DeleteInfo : as_udf_delete_info { StrView source_view() const { return StrView(source); } };
struct TruncateInfo : as_udf_truncate_info { StrView ns_view() const { return StrView(ns); } StrView set_view() const { return StrView(set); } };
struct ExpireInfo : as_udf_expire_info {};
struct ReadInfo : as_udf_read_info { StrView source_view() const { return StrView(source); } };
struct ShipInfo : as_udf_ship_info { StrView dc_view() const { return StrView(dc); } };
struct LifecycleInfo : as_udf_lifecycle_info { StrView phase_view() const { return StrView(phase); } StrView node_view() const { return StrView(node); } };
struct ClusterInfo : as_udf_cluster_info {};
struct TransferInfo : as_udf_transfer_info { StrView ns_view() const { return StrView(ns); } StrView peer_view() const { return StrView(peer); } };
struct TimerInfo : as_udf_timer_info { StrView name_view() const { return StrView(name); } };

// The answers. Each kind's type is built by its functions and delivered by the generated export.
struct Answer {
	const char* verdict;
	int32_t code;
	const char* message;
	int32_t bins;
	int64_t ttl;
	Error err;

	// An empty answer, for a Result that failed; never delivered.
	Answer() : verdict(""), code(0), message(nullptr), bins(0), ttl(-1), err() {}
	Answer(const char* verdict_, int32_t code_ = 0, const char* message_ = nullptr, int32_t bins_ = 0, int64_t ttl_ = -1, Error err_ = {})
	    : verdict(verdict_), code(code_), message(message_), bins(bins_), ttl(ttl_), err(err_) {}

	int32_t deliver() const
	{
		if (err.failed()) return fail(err);
		return as_udf_answer(verdict, code, message, bins, ttl);
	}
};

struct Decision : Answer {
	static Decision accept() { return Decision{{"accept"}}; }
	static Decision reject(int32_t code, const char* message) { return Decision{{"reject", code, message}}; }
	// Writes these bins instead; a bin absent from the map, or nil, is deleted.
	static Decision record(const Map& bins, int64_t ttl = -1) { return Decision{{"record", 0, nullptr, bins.handle(), ttl, bins.error()}}; }
};

struct Gate : Answer {
	static Gate allow() { return Gate{{"allow"}}; }
	static Gate refuse(int32_t code, const char* message) { return Gate{{"refuse", code, message}}; }
};

struct Expiry : Answer {
	static Expiry expire() { return Expiry{{"expire"}}; }
	static Expiry keep(int64_t ttl) { return Expiry{{"keep", 0, nullptr, 0, ttl}}; }
};

struct Eviction : Answer {
	static Eviction evict() { return Eviction{{"evict"}}; }
	static Eviction protect() { return Eviction{{"protect"}}; }
};

struct ReadResult : Answer {
	static ReadResult unchanged() { return ReadResult{{"unchanged"}}; }
	static ReadResult record(const Map& bins) { return ReadResult{{"record", 0, nullptr, bins.handle(), -1, bins.error()}}; }
};

struct ShipDecision : Answer {
	static ShipDecision ship() { return ShipDecision{{"ship"}}; }
	static ShipDecision skip() { return ShipDecision{{"skip"}}; }
	static ShipDecision record(const Map& bins) { return ShipDecision{{"record", 0, nullptr, bins.handle(), -1, bins.error()}}; }
};

// What an xdr-merge handler returns.
struct Resolution : Answer {
	static Resolution keep_local() { return Resolution{{"keep-local"}}; }
	static Resolution take_remote() { return Resolution{{"take-remote"}}; }
	static Resolution policy() { return Resolution{{"policy"}}; }
	// The final record: these bins, whole; a bin absent or nil is deleted. ttl -1 keeps the incoming write's.
	static Resolution record(const Map& bins, int64_t ttl = -1) { return Resolution{{"record", 0, nullptr, bins.handle(), ttl, bins.error()}}; }

	int32_t deliver() const
	{
		if (err.failed()) return fail(err);
		if (bins == 0) {
			as_udf_result_str(verdict, detail::cstrlen(verdict));
			return 0;
		}
		return as_udf_resolve_record(bins, ttl);
	}
};

//==========================================================
// Typed exports: the parameter decoders, the result delivery and the signature text the UDF_* macros use.
//

// Every remaining argument, for a last parameter of this type (`any...` in the signature).
struct Args {
	const Value* values = nullptr;
	int32_t count = 0;
	const Value* begin() const { return values; }
	const Value* end() const { return values + count; }
	Value operator[](int32_t i) const { return values[i]; }
};

namespace detail {

inline Error missing(const char* fn, int32_t i, const char* p)
{
	return Error{AS_UDF_NOT_FOUND, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is missing").buf};
}

inline Error mismatch(const char* fn, int32_t i, const char* p, const char* want)
{
	return Error{AS_UDF_TYPE_MISMATCH, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is not ").add(want).buf};
}

inline bool none(const Value* a, int32_t n, int32_t i) { return i >= n || a[i].is_absent() || a[i].is_nil(); }

// How a parameter of type T is decoded and named in the signature.
template <class T> struct Param;

template <> struct Param<int64_t> {
	static constexpr const char* name = "int";
	static constexpr bool optional = false, rest = false;
	static Result<int64_t> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_int()) return mismatch(fn, i, p, "an int");
		return a[i].payload;
	}
};

template <> struct Param<int32_t> {
	static constexpr const char* name = "int";
	static constexpr bool optional = false, rest = false;
	static Result<int32_t> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		Result<int64_t> v = Param<int64_t>::decode(a, n, i, fn, p);
		if (!v) return v.error();
		if ((int64_t)(int32_t)*v != *v) return mismatch(fn, i, p, "a 32-bit int");
		return (int32_t)*v;
	}
};

template <> struct Param<double> {
	static constexpr const char* name = "double";
	static constexpr bool optional = false, rest = false;
	static Result<double> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (a[i].is_double()) return a[i].as_double_bits();
		if (a[i].is_int()) return (double)a[i].payload;
		return mismatch(fn, i, p, "a double");
	}
};

template <> struct Param<bool> {
	static constexpr const char* name = "bool";
	static constexpr bool optional = false, rest = false;
	static Result<bool> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_bool()) return mismatch(fn, i, p, "a bool");
		return a[i].payload != 0;
	}
};

template <size_t N> struct Param<Text<N>> {
	static constexpr const char* name = "str";
	static constexpr size_t capacity = N;
	static constexpr bool optional = false, rest = false;
	static Result<Text<N>> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_str()) return mismatch(fn, i, p, "a string");
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		return a[i].as_text<N>();
	}
};

template <size_t N> struct Param<Bytes<N>> {
	static constexpr const char* name = "bytes";
	static constexpr size_t capacity = N;
	static constexpr bool optional = false, rest = false;
	static Result<Bytes<N>> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_str() && !a[i].is_blob()) return mismatch(fn, i, p, "a string or a blob");
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		return a[i].as_bytes<N>();
	}
};

template <> struct Param<Value> {
	static constexpr const char* name = "any";
	static constexpr bool optional = false, rest = false;
	static Result<Value> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		return a[i];
	}
};

template <class T> struct Param<Optional<T>> {
	static constexpr const char* name = Param<T>::name;
	static constexpr bool optional = true, rest = false;
	static Result<Optional<T>> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (none(a, n, i)) return Optional<T>();
		Result<T> v = Param<T>::decode(a, n, i, fn, p);
		if (!v) return v.error();
		return Optional<T>(v.take());
	}
};

template <> struct Param<Args> {
	static constexpr const char* name = "any";
	static constexpr bool optional = false, rest = true;
	static Result<Args> decode(const Value* a, int32_t n, int32_t i, const char*, const char*)
	{
		Args r;
		r.values = a + (i < n ? i : n);
		r.count = i < n ? n - i : 0;
		return r;
	}
};

template <class T> struct Capacity { static constexpr size_t value = 0; };
template <size_t N> struct Capacity<Text<N>> { static constexpr size_t value = N; };
template <size_t N> struct Capacity<Bytes<N>> { static constexpr size_t value = N; };
template <class T> struct Capacity<Optional<T>> { static constexpr size_t value = Capacity<T>::value; };

// How a return type is delivered as the result.
template <class T> struct Deliver;
template <> struct Deliver<void> { static Result<void> go() { as_udf_result_nil(); return {}; } };
template <> struct Deliver<int64_t> { static Result<void> go(int64_t v) { as_udf_result_int(v); return {}; } };
template <> struct Deliver<int32_t> { static Result<void> go(int32_t v) { as_udf_result_int(v); return {}; } };
template <> struct Deliver<double> { static Result<void> go(double v) { as_udf_result_double(v); return {}; } };
template <> struct Deliver<bool> { static Result<void> go(bool v) { as_udf_result_bool(v ? 1 : 0); return {}; } };
template <> struct Deliver<StrView> { static Result<void> go(StrView v) { as_udf_result_str(v.data, v.len); return {}; } };
template <size_t N> struct Deliver<Text<N>> { static Result<void> go(const Text<N>& v) { as_udf_result_str(v.buf, v.len); return {}; } };
template <> struct Deliver<Value> { static Result<void> go(const Value& v) { return result_of(v); } };
template <> struct Deliver<Map> {
	static Result<void> go(const Map& m)
	{
		if (m.error().failed()) return m.error();
		int32_t rc = as_udf_result_handle(m.handle());
		if (rc != AS_UDF_OK) return status_error(rc, "could not return the map");
		return {};
	}
};
template <> struct Deliver<List> {
	static Result<void> go(const List& l)
	{
		if (l.error().failed()) return l.error();
		int32_t rc = as_udf_result_handle(l.handle());
		if (rc != AS_UDF_OK) return status_error(rc, "could not return the list");
		return {};
	}
};

// A function's return, whether a Result<T> or a plain T, as a Result<void> once delivered.
template <class R> struct Outcome {
	static Result<void> deliver(const R& r) { return Deliver<R>::go(r); }
};
template <class T> struct Outcome<Result<T>> {
	static Result<void> deliver(const Result<T>& r)
	{
		if (!r) return r.error();
		return Deliver<T>::go(*r);
	}
};
template <> struct Outcome<Result<void>> {
	static Result<void> deliver(const Result<void>& r)
	{
		if (!r) return r.error();
		return Deliver<void>::go();
	}
};

template <class F> struct FnTraits;
template <class R, class... A> struct FnTraits<R (*)(A...)> {
	using Ret = R;
	using Params = TypeList<A...>;
	static constexpr size_t arity = sizeof...(A);
	static constexpr bool raw = sizeof...(A) == 2 && is_same<TypeList<A...>, TypeList<int32_t, int32_t>> && is_same<R, int32_t>;
	static constexpr bool rest = (false || ... || Param<Bare<A>>::rest);
};

// The parameter names, parsed at compile time from the stringised parameter list.
struct ParamNames {
	static constexpr size_t max = 16;
	char names[max][32] = {};
	size_t count = 0;

	constexpr ParamNames(const char* text)
	{
		// Split at top-level commas; a parameter's name is its trailing identifier.
		int32_t depth = 0;
		int32_t start = 0;
		int32_t i = 0;
		for (;; i++) {
			char c = text[i];
			if (c == '<' || c == '(' || c == '[') depth++;
			if (c == '>' || c == ')' || c == ']') depth--;
			if ((c == ',' && depth == 0) || c == '\0') {
				take(text, start, i);
				start = i + 1;
				if (c == '\0') break;
			}
		}
	}

	constexpr void take(const char* text, int32_t from, int32_t to)
	{
		int32_t end = to;
		while (end > from && (text[end - 1] == ' ' || text[end - 1] == '\t')) end--;
		int32_t begin = end;
		while (begin > from && is_ident(text[begin - 1])) begin--;
		if (begin == end || count == max) return;
		int32_t n = 0;
		for (int32_t k = begin; k < end && n < 31; k++) names[count][n++] = text[k];
		names[count][n] = '\0';
		count++;
	}
};

// The signature text, built at compile time: `fn name(site: str[32], n: int) -> value`.
struct Signature {
	char text[512] = {};
	size_t len = 0;

	constexpr void add(const char* s)
	{
		for (int32_t i = 0; s[i] != '\0' && len < sizeof(text) - 1; i++) text[len++] = s[i];
	}

	constexpr void add(size_t n)
	{
		char tmp[20] = {};
		int32_t i = 0;
		if (n == 0) tmp[i++] = '0';
		while (n != 0) {
			tmp[i++] = (char)('0' + n % 10);
			n /= 10;
		}
		while (i > 0 && len < sizeof(text) - 1) text[len++] = tmp[--i];
	}

	const char* data() const { return text; }
};

template <class... A>
constexpr void add_params(Signature& s, const ParamNames& names, TypeList<A...>)
{
	size_t i = 0;
	s.add("(");
	((s.add(i > 0 ? ", " : ""),
	  s.add(i < names.count ? names.names[i] : "arg"),
	  s.add(Param<Bare<A>>::optional ? "?: " : ": "),
	  s.add(Param<Bare<A>>::name),
	  Capacity<Bare<A>>::value > 0 ? (s.add("["), s.add(Capacity<Bare<A>>::value), s.add("]")) : (void)0,
	  s.add(Param<Bare<A>>::rest ? "..." : ""),
	  i++), ...);
	s.add(")");
}

template <auto Fn>
constexpr Signature function_signature(const char* name, const char* params)
{
	using T = FnTraits<decltype(Fn)>;
	static_assert(!T::raw, "a function taking (int32_t args, int32_t argc) is the raw form: use UDF_FUNCTION_RAW");
	Signature s;
	s.add("fn ");
	s.add(name);
	ParamNames names(params);
	add_params(s, names, typename T::Params{});
	s.add(" -> value");
	return s;
}

template <auto Fn>
constexpr Signature params_text(const char* params)
{
	using T = FnTraits<decltype(Fn)>;
	static_assert(!T::raw, "a stream half taking (int32_t args, int32_t argc) is the raw form: use UDF_STREAM_RAW");
	Signature s;
	ParamNames names(params);
	add_params(s, names, typename T::Params{});
	return s;
}

constexpr Signature stream_signature(const char* name, const Signature& record, const Signature& end)
{
	Signature s;
	s.add("stream ");
	s.add(name);
	s.add(" { record");
	s.add(record.text);
	s.add(" end");
	s.add(end.text);
	s.add(" }");
	return s;
}

constexpr Signature literal_signature(const char* text)
{
	Signature s;
	s.add(text);
	return s;
}

// The call: decode every parameter, keep the first error, call the function, deliver its return.
template <auto Fn, class... P>
int32_t call_impl(const char* fn, const Error& err, bool record_half, P&&... p)
{
	(void)fn;
	if (err.failed()) return fail(err);
	using R = typename FnTraits<decltype(Fn)>::Ret;
	if constexpr (is_same<R, void>) {
		Fn(static_cast<P&&>(p)...);
		if (!record_half) as_udf_result_nil();
		return 0;
	}
	else {
		R r = Fn(static_cast<P&&>(p)...);
		if (record_half) {
			if constexpr (is_same<R, Result<void>>) {
				if (!r) return fail(r.error());
			}
			return 0;
		}
		Result<void> out = Outcome<R>::deliver(r);
		if (!out) return fail(out.error());
		return 0;
	}
}

// Decodes every parameter (the first error wins), calls the function and delivers its return.
template <auto Fn, class... A, size_t... I>
int32_t call(const char* fn, const ParamNames& names, const Value* a, int32_t n, bool record_half, TypeList<A...>, IndexSeq<I...>)
{
	Error err;
	auto keep = [&](auto r) {
		if (!r.ok() && !err.failed()) err = r.error();
		return r.ok() ? r.take() : decltype(r.take())();
	};
	return call_impl<Fn>(fn, err, record_half, keep(Param<Bare<A>>::decode(a, n, (int32_t)I, fn, names.names[I]))...);
}

// The export's body for a record UDF, a stream half or anything typed: raw functions pass through.
template <auto Fn>
int32_t invoke(const char* fn, const ParamNames& names, int32_t args, int32_t argc, bool record_half)
{
	using T = FnTraits<decltype(Fn)>;
	if constexpr (T::raw) {
		return Fn(args, argc);
	}
	else {
		constexpr size_t cap = T::rest ? 128 : (T::arity == 0 ? 1 : T::arity);
		Value a[cap];
		int32_t n = as_udf_args(args, argc, a, (int32_t)cap);
		if (!T::rest && argc > (int32_t)T::arity) {
			return fail(Error{AS_UDF_ERR, message().add(fn).add(" takes at most ").add((int64_t)T::arity).add(" arguments, got ").add((int64_t)argc).buf});
		}
		return call<Fn>(fn, names, a, n, record_half, typename T::Params{}, MakeIndexSeq<T::arity>{});
	}
}

} // namespace detail
} // namespace udf

//==========================================================
// The macros. Each defines the implementation function with the given parameter list, the export that
// decodes the arguments and delivers the return, and the signature export derived from the types.
//

#define UDF_SIG_EXPORT(name, expr)                                                                       \
	static constexpr ::udf::detail::Signature udf_sig_##name = expr;                                     \
	extern "C" __attribute__((export_name("__udf_sig_" #name))) int32_t udf_sig_fn_##name(void)           \
	{                                                                                                    \
		return (int32_t)(uintptr_t)udf_sig_##name.data();                                                \
	}

// A record UDF: `UDF_FUNCTION(name, ReturnType, params...) { body }`. The return is a Result<T> or a plain
// T (int64_t, double, bool, StrView, Text<N>, Value, Map, List, void); the parameters are int64_t,
// int32_t, double, bool, Text<N>, Bytes<N>, Value, Optional<T> of those, and Args last. A function declared
// (int32_t args, int32_t argc) returning int32_t is the raw form; see UDF_FUNCTION_RAW.
#define UDF_FUNCTION(name, ret, ...)                                                                     \
	static ret name##_udf_impl(__VA_ARGS__);                                                             \
	static constexpr ::udf::detail::ParamNames udf_names_##name{#__VA_ARGS__};                           \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_udf_impl>(#name, udf_names_##name, args, argc, false);      \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::function_signature<&name##_udf_impl>(#name, #__VA_ARGS__))        \
	static ret name##_udf_impl(__VA_ARGS__)

// The raw form of a record UDF: `UDF_FUNCTION_RAW(name, "(bin: str, k?: int)") { ... }` with args and argc
// in scope, returning the status.
#define UDF_FUNCTION_RAW(name, params)                                                                   \
	static int32_t name##_udf_impl(int32_t args, int32_t argc);                                           \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		return name##_udf_impl(args, argc);                                                              \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::literal_signature("fn " #name params " -> value"))                 \
	static int32_t name##_udf_impl(int32_t args, int32_t argc)

// A stream UDF: `UDF_STREAM_RECORD(name, ret, params...) { ... }` then `UDF_STREAM_END(name, ret, params...)
// { ... }`, in that order; the record half returns void or Result<void>, the end half the partial. State is
// static. UDF_STREAM_RAW(name, "(record params)", "(end params)") declares both halves raw.
#define UDF_STREAM_RECORD(name, ret, ...)                                                                \
	static ret name##_record_udf_impl(__VA_ARGS__);                                                      \
	static constexpr ::udf::detail::ParamNames udf_names_##name##_record{#__VA_ARGS__};                  \
	static constexpr ::udf::detail::Signature udf_params_##name##_record =                               \
	    ::udf::detail::params_text<&name##_record_udf_impl>(#__VA_ARGS__);                               \
	extern "C" __attribute__((export_name(#name "_record"))) int32_t udf_export_##name##_record(int32_t args, int32_t argc) \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_record_udf_impl>(#name, udf_names_##name##_record, args, argc, true); \
	}                                                                                                    \
	static ret name##_record_udf_impl(__VA_ARGS__)

#define UDF_STREAM_END(name, ret, ...)                                                                   \
	static ret name##_end_udf_impl(__VA_ARGS__);                                                         \
	static constexpr ::udf::detail::ParamNames udf_names_##name##_end{#__VA_ARGS__};                     \
	static constexpr ::udf::detail::Signature udf_params_##name##_end =                                  \
	    ::udf::detail::params_text<&name##_end_udf_impl>(#__VA_ARGS__);                                  \
	extern "C" __attribute__((export_name(#name "_end"))) int32_t udf_export_##name##_end(int32_t args, int32_t argc) \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_end_udf_impl>(#name, udf_names_##name##_end, args, argc, false); \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::stream_signature(#name, udf_params_##name##_record, udf_params_##name##_end)) \
	static ret name##_end_udf_impl(__VA_ARGS__)

#define UDF_STREAM_RAW(name, record_params, end_params)                                                  \
	static int32_t name##_record_udf_impl(int32_t args, int32_t argc);                                    \
	static int32_t name##_end_udf_impl(int32_t args, int32_t argc);                                       \
	extern "C" __attribute__((export_name(#name "_record"))) int32_t udf_export_##name##_record(int32_t args, int32_t argc) \
	{                                                                                                    \
		return name##_record_udf_impl(args, argc);                                                       \
	}                                                                                                    \
	extern "C" __attribute__((export_name(#name "_end"))) int32_t udf_export_##name##_end(int32_t args, int32_t argc) \
	{                                                                                                    \
		return name##_end_udf_impl(args, argc);                                                          \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::literal_signature("stream " #name " { record" record_params " end" end_params " }"))

// Event handlers: `UDF_ON_BEFORE_WRITE(name) { ... }` defines
// `Result<Decision> name(const EventRecord& before, const EventRecord& after, const WriteInfo& write)` and
// so on, one macro per kind, each with the parameters and answer the kind takes; the kinds that answer
// nothing return Result<void>. The export decodes the event and delivers the answer.
#define UDF_HANDLER_SIG(name, kind, params) UDF_SIG_EXPORT(name, ::udf::detail::literal_signature("event " kind "/1 " #name params))

#define UDF_EVENT_TWO_RECORDS(name, kind, params, Info, decode_info, Ret, p0, p1, pinfo)                \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::EventRecord&, const ::udf::EventRecord&, const ::udf::Info&); \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::Value desc;                                                                               \
		::udf::EventRecord r0, r1;                                                                       \
		::udf::Info info;                                                                                \
		if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK || as_udf_event_get_record(&desc, #p0, 0, &r0) != AS_UDF_OK \
		    || as_udf_event_get_record(&desc, #p1, 1, &r1) != AS_UDF_OK || decode_info(&desc, &info) != AS_UDF_OK) { \
			return ::udf::fail(#name ": not a " kind " event");                                           \
		}                                                                                                \
		::udf::Result<Ret> r = name##_udf_impl(r0, r1, info);                                            \
		if (!r) return ::udf::fail(r.error());                                                           \
		return ::udf::detail::deliver_answer(r);                                                         \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, kind, params)                                                                  \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::EventRecord& p0, const ::udf::EventRecord& p1, const ::udf::Info& pinfo)

#define UDF_EVENT_ONE_RECORD(name, kind, params, Info, decode_info, Ret, pinfo)                           \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::EventRecord&, const ::udf::Info&);            \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::Value desc;                                                                               \
		::udf::EventRecord r0;                                                                           \
		::udf::Info info;                                                                                \
		if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK || as_udf_event_get_record(&desc, "record", 0, &r0) != AS_UDF_OK \
		    || decode_info(&desc, &info) != AS_UDF_OK) {                                                 \
			return ::udf::fail(#name ": not a " kind " event");                                           \
		}                                                                                                \
		::udf::Result<Ret> r = name##_udf_impl(r0, info);                                                \
		if (!r) return ::udf::fail(r.error());                                                           \
		return ::udf::detail::deliver_answer(r);                                                         \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, kind, params)                                                                  \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::EventRecord& record, const ::udf::Info& pinfo)

#define UDF_EVENT_INFO_ONLY(name, kind, params, Info, decode_info, Ret, pinfo)                            \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::Info&);                                       \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::Value desc;                                                                               \
		::udf::Info info;                                                                                \
		if (as_udf_event_desc(args, argc, &desc) != AS_UDF_OK || decode_info(&desc, &info) != AS_UDF_OK) { \
			return ::udf::fail(#name ": not a " kind " event");                                           \
		}                                                                                                \
		::udf::Result<Ret> r = name##_udf_impl(info);                                                    \
		if (!r) return ::udf::fail(r.error());                                                           \
		return ::udf::detail::deliver_answer(r);                                                         \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, kind, params)                                                                  \
	static ::udf::Result<Ret> name##_udf_impl(const ::udf::Info& pinfo)

namespace udf::detail {

template <class T> int32_t deliver_answer(const Result<T>& r) { return r->deliver(); }
inline int32_t deliver_answer(const Result<void>&) { return as_udf_answer_done(); }

} // namespace udf::detail

#define UDF_ON_BEFORE_WRITE(name) UDF_EVENT_TWO_RECORDS(name, "before-write", "(before: record, after: record, write: write) -> decision", WriteInfo, as_udf_event_write_info, ::udf::Decision, before, after, write)
#define UDF_ON_AFTER_WRITE(name) UDF_EVENT_TWO_RECORDS(name, "after-write", "(before: record, after: record, write: write) -> ()", WriteInfo, as_udf_event_write_info, void, before, after, write)
#define UDF_ON_BEFORE_DELETE(name) UDF_EVENT_ONE_RECORD(name, "before-delete", "(record: record, delete: delete) -> gate", DeleteInfo, as_udf_event_delete_info, ::udf::Gate, del)
#define UDF_ON_AFTER_DELETE(name) UDF_EVENT_ONE_RECORD(name, "after-delete", "(record: record, delete: delete) -> ()", DeleteInfo, as_udf_event_delete_info, void, del)
#define UDF_ON_BEFORE_EXPIRE(name) UDF_EVENT_ONE_RECORD(name, "before-expire", "(record: record, expire: expire) -> expiry", ExpireInfo, as_udf_event_expire_info, ::udf::Expiry, expire)
#define UDF_ON_BEFORE_EVICT(name) UDF_EVENT_ONE_RECORD(name, "before-evict", "(record: record, evict: evict) -> eviction", ExpireInfo, as_udf_event_expire_info, ::udf::Eviction, evict)
#define UDF_ON_AFTER_READ(name) UDF_EVENT_ONE_RECORD(name, "after-read", "(record: record, read: read) -> read-result", ReadInfo, as_udf_event_read_info, ::udf::ReadResult, read)
#define UDF_ON_BEFORE_TRUNCATE(name) UDF_EVENT_INFO_ONLY(name, "before-truncate", "(truncate: truncate) -> gate", TruncateInfo, as_udf_event_truncate_info, ::udf::Gate, truncate)
#define UDF_ON_AFTER_TRUNCATE(name) UDF_EVENT_INFO_ONLY(name, "after-truncate", "(truncate: truncate) -> ()", TruncateInfo, as_udf_event_truncate_info, void, truncate)
#define UDF_ON_BEFORE_SHIP(name) UDF_EVENT_ONE_RECORD(name, "before-ship", "(record: record, ship: ship) -> ship-decision", ShipInfo, as_udf_event_ship_info, ::udf::ShipDecision, ship)
#define UDF_ON_PARTITION_TRANSFER(name) UDF_EVENT_INFO_ONLY(name, "partition-transfer", "(transfer: transfer) -> ()", TransferInfo, as_udf_event_transfer_info, void, transfer)
#define UDF_ON_CLUSTER_CHANGE(name) UDF_EVENT_INFO_ONLY(name, "cluster-change", "(cluster: cluster) -> ()", ClusterInfo, as_udf_event_cluster_info, void, cluster)
#define UDF_ON_NODE_LIFECYCLE(name) UDF_EVENT_INFO_ONLY(name, "node-lifecycle", "(lifecycle: lifecycle) -> ()", LifecycleInfo, as_udf_event_lifecycle_info, void, lifecycle)

// A timer: `UDF_ON_TIMER(name) { ... }` defines `Result<void> name(const TimerInfo& timer)`, run as a record UDF.
#define UDF_ON_TIMER(name)                                                                               \
	static ::udf::Result<void> name##_udf_impl(const ::udf::TimerInfo&);                                 \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::TimerInfo info;                                                                           \
		if (as_udf_event_timer_info(args, argc, &info) != AS_UDF_OK) return ::udf::fail(#name ": not a timer event"); \
		::udf::Result<void> r = name##_udf_impl(info);                                                   \
		if (!r) return ::udf::fail(r.error());                                                           \
		return as_udf_answer_done();                                                                     \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, "timer", "(timer: timer) -> ()")                                                \
	static ::udf::Result<void> name##_udf_impl(const ::udf::TimerInfo& timer)

// An xdr-merge: `UDF_ON_XDR_MERGE(name) { ... }` defines
// `Result<Resolution> name(const EventRecord& current, const EventRecord& incoming, const Conflict& conflict)`.
#define UDF_ON_XDR_MERGE(name)                                                                           \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord&, const ::udf::EventRecord&, const ::udf::Conflict&); \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::Conflict c;                                                                               \
		if (as_udf_event_decode(args, argc, &c) != AS_UDF_OK) return ::udf::fail(#name ": not an xdr-merge event"); \
		::udf::Result<::udf::Resolution> r = name##_udf_impl(c.current(), c.incoming(), c);               \
		if (!r) return ::udf::fail(r.error());                                                           \
		return r->deliver();                                                                             \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, "xdr-merge", "(current: record, incoming: record, conflict: conflict) -> resolution") \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord& current, const ::udf::EventRecord& incoming, const ::udf::Conflict& conflict)

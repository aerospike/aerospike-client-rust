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
// there, and this adds the typed tier - Record, Value, Map, List, range-for over stored collections, Result
// and Error instead of status codes, the records and the answer of an xdr-merge, Pair and Both for a merge,
// and the UDF_FUNCTION / UDF_STREAM / UDF_ON_XDR_MERGE macros that derive the exports and their signatures from a
// function's parameter types, the way the Rust SDK's macros do. A module is a freestanding C++20 file
// compiled with clang for wasm32, no standard library, no exceptions, no RTTI:
//
//   clang -x c++ -std=c++20 --target=wasm32 -O3 -nostdlib -nostdinc++ -ffreestanding -fno-exceptions \
//       -fno-rtti -fvisibility=hidden -Wl,--no-entry -I<c sdk>/include -I<this>/include -o my.wasm my.cpp
//
// Nothing here allocates: strings live in fixed buffers (Text<N>), collections are built on the host, and
// a module needs no operator new. The one clang extension used is the statement expression behind UDF_TRY.
//
// The record a UDF is called on is an object, udf::Record, that the function takes as its first parameter:
// `udf::Record& rec` to write it, `const udf::Record& rec` to read it, and no such parameter when it does
// not touch the record. Everything about the record is a method on it; what stays in the namespace is what
// is not about the record: now(), log(), fail(), result() and the Map and List builders.

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

	// Whether this is an error. Decided by the message alone: a status without a message does not count.
	constexpr bool failed() const { return message != nullptr; }
};

// Builds an Error from a status and a message. The message is not copied, so it must outlive the error:
// a literal, or the SDK's message buffer.
inline Error error(Status status, const char* message) { return Error{status, message}; }

// A value or an error, the Rust SDK's Result: `if (!r) return r.error();` or UDF_TRY(r).
template <class T>
class Result {
public:
	Result(T value) : ok_(true), value_(static_cast<T&&>(value)) {}
	Result(Error e) : ok_(false), value_(), err_(e) {}

	// Whether the call succeeded and a value is held.
	bool ok() const { return ok_; }
	explicit operator bool() const { return ok_; }
	const T& operator*() const { return value_; }
	T& operator*() { return value_; }
	const T* operator->() const { return &value_; }
	T* operator->() { return &value_; }
	// Moves the value out. Meaningful only when ok(): a failed Result holds a value-initialised T.
	T take() { return static_cast<T&&>(value_); }
	// The error of a failed Result; on success, an Error whose failed() is false.
	Error error() const { return err_; }

private:
	bool ok_;
	T value_;
	Error err_;
};

// The Result of a call that yields no value: success, or an error. `Ok` is its alias.
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

	// Whether a value is present.
	bool has_value() const { return has_; }
	explicit operator bool() const { return has_; }
	const T& operator*() const { return value_; }
	T& operator*() { return value_; }
	const T* operator->() const { return &value_; }
	T* operator->() { return &value_; }
	// The value, or `dflt` when there is none.
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
	// Whether the view begins with the bytes of `p`. An empty `p` always matches.
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

	// Copies `s` in and NUL-terminates it. Returns false, leaving the text unchanged, when `s` is longer
	// than N bytes.
	constexpr bool assign(StrView s)
	{
		if (s.len > (int32_t)N) return false;
		for (int32_t i = 0; i < s.len; i++) buf[i] = s.data[i];
		buf[s.len] = '\0';
		len = s.len;
		return true;
	}

	// The text as a view into this object's buffer, valid while the Text is alive and unchanged.
	constexpr StrView view() const { return StrView{buf, len}; }
	constexpr operator StrView() const { return view(); }
	// The text as a NUL-terminated C string, valid while the Text is alive.
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

	// The kind of the value: what the host tagged a read with, or what the guest built.
	constexpr Kind kind() const { return (Kind)this->as_udf_value::kind; }
	// Whether there is no value: the kind of a default-constructed Value.
	constexpr bool is_absent() const { return kind() == Kind::Absent; }
	// Whether the value is an explicit nil.
	constexpr bool is_nil() const { return kind() == Kind::Nil; }
	// Whether there is nothing to read: the value is nil or it is absent. Not another name for is_nil(),
	// which is false of an absent value.
	constexpr bool is_null() const { return is_nil() || is_absent(); }
	// Whether the value is an integer.
	constexpr bool is_int() const { return kind() == Kind::Int; }
	// Whether the value is a double. An integer is not one, though as_double() widens it.
	constexpr bool is_double() const { return kind() == Kind::Double; }
	// Whether the value is a boolean.
	constexpr bool is_bool() const { return kind() == Kind::Bool; }
	// Whether the value is a stored string. A string built by Value::of is of kind GuestStr and is not one.
	constexpr bool is_string() const { return kind() == Kind::Str; }
	// Whether the value is a stored blob. A blob built by Value::blob is of kind GuestBlob and is not one.
	constexpr bool is_blob() const { return kind() == Kind::Blob; }
	// Whether the value is a stored list.
	constexpr bool is_list() const { return kind() == Kind::List; }
	// Whether the value is a stored map.
	constexpr bool is_map() const { return kind() == Kind::Map; }
	// Whether the value is held by position in a source on the host, not in the value itself: a string,
	// blob, GeoJSON, HLL, list or map.
	constexpr bool is_stored() const
	{
		Kind k = kind();
		return k == Kind::Str || k == Kind::Blob || k == Kind::GeoJson || k == Kind::Hll || k == Kind::List || k == Kind::Map;
	}

	// The handle of the source a stored value sits in. Meaningful only when is_stored().
	int32_t src() const { return as_udf_value_src(this); }
	// The offset of a stored value within its source. Meaningful only when is_stored().
	int32_t offset() const { return as_udf_value_at(this); }
	// How much a stored value holds: the elements of a list, the entries of a map, the bytes of a string,
	// blob, GeoJSON or HLL. 0 for anything else.
	int32_t size() const { return is_stored() ? (int32_t)len : 0; }
	// Whether size() is 0.
	bool empty() const { return size() == 0; }
	// The payload reinterpreted as a double, with no check of the kind. Use as_double() unless the kind is
	// already known to be Double.
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

	// The boolean, or a type-mismatch error when the value is not a bool.
	Result<bool> as_bool() const
	{
		if (!is_bool()) return kind_error("a bool");
		return payload != 0;
	}

	// The text of a stored string into an inline Text<N>; an error when it does not fit.
	template <size_t N>
	Result<Text<N>> as_string() const
	{
		if (kind() == Kind::GuestStr) return Text<N>(StrView{(const char*)(uintptr_t)payload, (int32_t)len});
		if (!is_string()) return kind_error("a string");
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

	// The bytes of a stored string, blob, GeoJSON or HLL into an inline Bytes<N>. An error when the value is
	// of another kind or does not fit in N bytes.
	template <size_t N>
	Result<Bytes<N>> as_blob() const
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
	bool is(StrView text) const { return is_string() && (int32_t)len == text.len && str_eq_stored(text); }

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
		if (!is_map() || as_udf_coll_get(src(), offset(), key.data, key.len, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	// The entry under an integer key of a stored map; nothing when there is none or the value is not a map.
	Optional<Value> get(int64_t key) const
	{
		Value out;
		if (!is_map() || as_udf_coll_get_int(src(), offset(), key, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	// Whether a stored map has an entry under a string key. False when the value is not a map.
	bool contains(StrView key) const { return get(key).has_value(); }
	// Whether a stored map has an entry under an integer key. False when the value is not a map.
	bool contains(int64_t key) const { return get(key).has_value(); }

	// The element at `index` of a stored list, which the host finds without the guest walking the list;
	// nothing past the end, for a negative index or when the value is not a list.
	Optional<Value> at(int64_t index) const
	{
		Value out;
		if (!is_list() || index < 0 || index >= (int64_t)len) return {};
		if (as_udf_list_get(src(), offset(), (int32_t)index, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	// A walk over a stored list or map: `for (Value v : value.list())`, `for (auto [k, v] : value.map())`.
	inline ListRange list() const;
	// A walk over the entries of a stored map, each an Entry of key and value. Empty when the value is not
	// a map.
	inline MapRange map() const;

	// Values of the guest's, for a batch, a result or a collection entry.
	static constexpr Value of(int64_t i) { Value v; v.as_udf_value::kind = AS_UDF_INT; v.payload = i; return v; }
	// An integer value from an int, widened to 64 bits.
	static constexpr Value of(int i) { return of((int64_t)i); }
	// A string value viewing a NUL-terminated string. The bytes are not copied: they must stay alive until
	// the value has been handed to the host.
	static Value of(const char* s) { return of(StrView(s)); }
	// A double value.
	static Value of(double d) { Value v; v.as_udf_value::kind = AS_UDF_DOUBLE; __builtin_memcpy(&v.payload, &d, 8); return v; }
	// A boolean value.
	static constexpr Value of(bool b) { Value v; v.as_udf_value::kind = AS_UDF_BOOL; v.payload = b ? 1 : 0; return v; }
	// A string value viewing the guest's bytes (kind GuestStr). The bytes are not copied: they must stay
	// alive until the value has been handed to the host.
	static Value of(StrView s) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_STR; v.len = (uint32_t)s.len; v.payload = (int64_t)(uintptr_t)s.data; return v; }
	// A blob value viewing `n` bytes at `b` (kind GuestBlob). Not copied, as for a string.
	static Value blob(const void* b, int32_t n) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_BLOB; v.len = (uint32_t)n; v.payload = (int64_t)(uintptr_t)b; return v; }
	// A GeoJSON value viewing the document's text (kind GuestGeoJson). Not copied, as for a string.
	static Value geojson(StrView doc) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_GEOJSON; v.len = (uint32_t)doc.len; v.payload = (int64_t)(uintptr_t)doc.data; return v; }
	// A nil value.
	static constexpr Value nil() { Value v; v.as_udf_value::kind = AS_UDF_NIL; return v; }
	// A value standing for a collection built on the host, by its handle: what Map::value() and
	// List::value() return.
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

// One entry of a stored map, as MapRange yields it: `for (auto [k, v] : value.map())`.
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

// The end of a ListRange or MapRange walk: an iterator compares unequal to it until the walk is done.
struct RangeEnd {};

// The elements of a stored list, for a range-for. Made by Value::list(), Record::list() and
// EventRecord::list(); empty when the value it was made from is not a list.
class ListRange {
public:
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll)
		{
			map = false;
			cursor = coll.is_list() ? as_udf_cursor_enter_at(coll.src(), coll.offset()) : 0;
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
	// The number of elements the walk yields: 0 when it was made from something that is not a list.
	int32_t size() const { return coll_.is_list() ? coll_.size() : 0; }
	// Whether the walk yields nothing.
	bool empty() const { return size() == 0; }
	// The element at `index`, without walking to it; nothing past the end.
	Optional<Value> at(int64_t index) const { return coll_.at(index); }
	// Opens a host cursor on the list and returns an iterator on its first element. Every call opens its
	// own cursor, released when the iterator is destroyed.
	Iterator begin() const { return Iterator(coll_); }
	// The end sentinel the iterator is compared with.
	RangeEnd end() const { return {}; }

private:
	Value coll_;
};

// The entries of a stored map, for a range-for. Made by Value::map(), Record::map() and
// EventRecord::map(); empty when the value it was made from is not a map.
class MapRange {
public:
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll)
		{
			map = true;
			cursor = coll.is_map() ? as_udf_cursor_enter_at(coll.src(), coll.offset()) : 0;
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
	// The number of entries the walk yields: 0 when it was made from something that is not a map.
	int32_t size() const { return coll_.is_map() ? coll_.size() : 0; }
	// Whether the walk yields nothing.
	bool empty() const { return size() == 0; }
	// Whether the map has an entry under a string key.
	bool contains(StrView key) const { return coll_.contains(key); }
	// Whether the map has an entry under an integer key.
	bool contains(int64_t key) const { return coll_.contains(key); }
	// Opens a host cursor on the map and returns an iterator on its first entry. Every call opens its own
	// cursor, released when the iterator is destroyed.
	Iterator begin() const { return Iterator(coll_); }
	// The end sentinel the iterator is compared with.
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

// A map being built on the host, held by handle. A put the host refuses does not stop the chain: the
// first failure is kept and reported by error() and result().
class Map {
public:
	// A null map, for a Result<Map> that failed; a real one takes a capacity.
	Map() : handle_(0) {}
	explicit Map(int32_t capacity) : handle_(as_udf_map_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a map");
	}
	// wrap() adopts a handle the host already issued, without building a new map.
	static Map wrap(int32_t handle) { Map m(Wrapped{}, handle); return m; }

	// The host's handle of the map: 0 for a null map, not positive when the map could not be built.
	int32_t handle() const { return handle_; }
	// The first failure of building the map or of a put on it; failed() is false when there was none.
	Error error() const { return err_; }
	// error() as a Result<void>, for UDF_TRY or a return.
	Result<void> result() const { return err_.failed() ? Result<void>(err_) : Result<void>(); }
	// The number of entries the map holds, asked of the host; negative when the handle is not a valid one.
	int32_t size() const { return as_udf_coll_size(handle_); }
	// The map as a Value of kind Handle, to nest it in a batch (List::add_values, Map::put_entries) or to
	// pass it to udf::result().
	Value value() const { return Value::handle(handle_); }

	// Whether the map holds no entry, which is also what a map that could not be built answers.
	bool empty() const { return size() <= 0; }

	// Whether the map holds an entry under a string key. The host answers for the map it is building, so
	// nothing is packed and the check is fit for a loop.
	bool contains(StrView key) const { return as_udf_map_has(handle_, key.data, key.len) > 0; }
	// Whether the map holds an entry under an integer key.
	bool contains(int64_t key) const { return as_udf_map_has_int(handle_, key) > 0; }

	// Puts an integer under a string key. Like every put, returns the map for chaining and keeps the first
	// failure for error().
	Map& put(StrView key, int64_t v) { return check(as_udf_map_put_string_int(handle_, key.data, key.len, v), key); }
	// Puts an int under a string key, widened to 64 bits.
	Map& put(StrView key, int v) { return put(key, (int64_t)v); }
	// Puts a double under a string key.
	Map& put(StrView key, double v) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, as_udf_val_double(v)), key); }
	// Puts a boolean under a string key.
	Map& put(StrView key, bool v) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, as_udf_val_bool(v ? 1 : 0)), key); }
	// Puts a string under a string key.
	Map& put(StrView key, StrView v) { return check(as_udf_map_put_string_string(handle_, key.data, key.len, v.data, v.len), key); }
	// Puts a NUL-terminated string under a string key.
	Map& put(StrView key, const char* v) { return put(key, StrView(v)); }
	// Puts a value of any kind under a string key: a scalar, guest bytes, a handle, or a stored value taken
	// by position and keeping its kind. An absent value is refused. The key is cut to 63 bytes.
	Map& put(StrView key, const Value& v) { return check(as_udf_map_put_value(handle_, c_str(key), &v), key); }
	// Nests a built map under a string key. A failure already kept by `v` carries over to this map.
	Map& put(StrView key, const Map& v) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, v.handle_), key).carry(v.err_); }
	// Nests a built list under a string key. A failure already kept by `v` carries over to this map.
	inline Map& put(StrView key, const List& v);
	// Puts `n` bytes at `b` as a blob under a string key.
	Map& put_blob(StrView key, const void* b, int32_t n) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, as_udf_val_blob(b, n)), key); }
	// Puts a GeoJSON document under a string key, as the GeoJSON kind, not as a string.
	Map& put_geojson(StrView key, StrView doc) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, as_udf_val_geojson(doc.data, doc.len)), key); }
	// A nil entry: in a record answer, "no such bin".
	Map& put_nil(StrView key) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, as_udf_val_nil()), key); }

	// Integer keys.
	Map& put(int64_t key, int64_t v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_int(v)), "an integer key"); }
	// Puts an int under an integer key, widened to 64 bits.
	Map& put(int64_t key, int v) { return put(key, (int64_t)v); }
	// Puts a string under an integer key.
	Map& put(int64_t key, StrView v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_string(v.data, v.len)), "an integer key"); }
	// Puts a NUL-terminated string under an integer key.
	Map& put(int64_t key, const char* v) { return put(key, StrView(v)); }
	// Puts a value of any kind under an integer key: a scalar, guest bytes, a handle, or a stored value
	// taken by position and keeping its kind. An absent value is refused.
	Map& put(int64_t key, const Value& v) { return check(put_i_value(key, v), "an integer key"); }
	// Nests a built map under an integer key. A failure already kept by `v` carries over to this map.
	Map& put(int64_t key, const Map& v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), v.handle_), "an integer key").carry(v.err_); }
	// Nests a built list under an integer key. A failure already kept by `v` carries over to this map.
	inline Map& put(int64_t key, const List& v);

	// Every entry of a stored map, by position.
	Map& put_all(const Value& stored)
	{
		for (Entry e : stored.map()) {
			if (e.key.is_int()) {
				put(e.key.payload, e.value);
			}
			else {
				Result<Text<64>> k = e.key.as_string<64>();
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
		case Kind::GuestStr: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_string((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestBlob: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return as_udf_map_put(handle_, as_udf_val_int(key), as_udf_val_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return as_udf_map_put(handle_, as_udf_val_int(key), (int32_t)v.payload);
		case Kind::Absent: return AS_UDF_NOT_FOUND;
		default: return as_udf_map_put_int_at(handle_, key, v.src(), v.offset());
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

// A list being built on the host, held by handle. An add the host refuses does not stop the chain: the
// first failure is kept and reported by error() and result().
class List {
public:
	// A null list, for a Result<List> that failed; a real one takes a capacity.
	List() : handle_(0) {}
	explicit List(int32_t capacity) : handle_(as_udf_list_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a list");
	}
	// wrap() adopts a handle the host already issued, without building a new list.
	static List wrap(int32_t handle) { List l(Wrapped{}, handle); return l; }

	// The host's handle of the list: 0 for a null list, not positive when the list could not be built.
	int32_t handle() const { return handle_; }
	// The first failure of building the list or of an add to it; failed() is false when there was none.
	Error error() const { return err_; }
	// error() as a Result<void>, for UDF_TRY or a return.
	Result<void> result() const { return err_.failed() ? Result<void>(err_) : Result<void>(); }
	// The number of elements the list holds, asked of the host; negative when the handle is not a valid one.
	int32_t size() const { return as_udf_coll_size(handle_); }
	// Whether the list holds no element, which is also what a list that could not be built answers.
	bool empty() const { return size() <= 0; }
	// The list as a Value of kind Handle, to nest it in a batch (List::add_values, Map::put_entries) or to
	// pass it to udf::result().
	Value value() const { return Value::handle(handle_); }

	// Appends an integer. Like every add, returns the list for chaining and keeps the first failure for
	// error().
	List& add(int64_t v) { return check(as_udf_list_push_int(handle_, v)); }
	// Appends an int, widened to 64 bits.
	List& add(int v) { return add((int64_t)v); }
	// Appends a double.
	List& add(double v) { return check(as_udf_list_push(handle_, as_udf_val_double(v))); }
	// Appends a boolean.
	List& add(bool v) { return check(as_udf_list_push(handle_, as_udf_val_bool(v ? 1 : 0))); }
	// Appends a string.
	List& add(StrView v) { return check(as_udf_list_push_string(handle_, v.data, v.len)); }
	// Appends a NUL-terminated string.
	List& add(const char* v) { return add(StrView(v)); }
	// Appends a value of any kind: a scalar, guest bytes, a handle, or a stored value taken by position and
	// keeping its kind. An absent value is refused.
	List& add(const Value& v) { return check(push_value(v)); }
	// Appends a built map as one element. A failure already kept by `v` carries over to this list.
	List& add(const Map& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	// Appends a built list as one element, nested. A failure already kept by `v` carries over to this list.
	List& add(const List& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	// Appends `n` bytes at `b` as a blob.
	List& add_blob(const void* b, int32_t n) { return check(as_udf_list_push(handle_, as_udf_val_blob(b, n))); }
	// Appends a GeoJSON document, as the GeoJSON kind, not as a string.
	List& add_geojson(StrView doc) { return check(as_udf_list_push(handle_, as_udf_val_geojson(doc.data, doc.len))); }
	// Appends a nil element.
	List& add_nil() { return check(as_udf_list_push(handle_, as_udf_val_nil())); }

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
		case Kind::GuestStr: return as_udf_list_push_string(handle_, (const char*)(uintptr_t)v.payload, (int32_t)v.len);
		case Kind::GuestBlob: return as_udf_list_push(handle_, as_udf_val_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return as_udf_list_push(handle_, as_udf_val_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return as_udf_list_push(handle_, (int32_t)v.payload);
		case Kind::Absent: return AS_UDF_NOT_FOUND;
		default: return as_udf_list_push_at(handle_, v.src(), v.offset());
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

inline Map& Map::put(StrView key, const List& v) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, v.handle_), key).carry(v.err_); }
inline Map& Map::put(int64_t key, const List& v) { return check(as_udf_map_put(handle_, as_udf_val_int(key), v.handle_), "an integer key").carry(v.err_); }

//==========================================================
// Records: what reading one offers, and the record a UDF is called on.
//

// Reading a record, whichever record it is. udf::Record, the record a UDF was called on, and
// udf::EventRecord, a record an event hands over, both derive from it, so a helper written as
// `template <class R> int64_t total(const udf::ReadRecord<R>& rec)` reads from either. It is a mixin over
// the one thing the two do differently, `read(name, out)`: there is no virtual call and no state, so an
// EventRecord stays the C struct it wraps.
template <class Self>
class ReadRecord {
public:
	// The bin as a value; nothing when the record has no such bin or it is nil.
	Optional<Value> get(StrView name) const
	{
		Value v;
		if (!self().read(name, v) || v.is_nil()) return {};
		return v;
	}

	// Whether the record has the bin, nil included.
	bool has(StrView name) const
	{
		Value v;
		return self().read(name, v);
	}

	// The bin that must be there, as a value: an error naming the bin when the record has none. Chain the
	// value's own typed reader for a bin that must also be of a kind: `UDF_TRY(UDF_TRY(rec.bin("n")).as_int())`.
	Result<Value> bin(StrView name) const
	{
		Value v;
		if (!self().read(name, v)) return Error{AS_UDF_NOT_FOUND, detail::message().add("bin ").add(name.data, name.len).add(" is absent").buf};
		return v;
	}

	// The bin as an integer; nothing when the record has no such bin, it is nil or it is of another kind.
	Optional<int64_t> as_int(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_int()) return {}; return v->payload; }
	// The bin as a double; nothing when the record has no such bin, it is nil or it is of another kind. An
	// integer bin is not widened.
	Optional<double> as_double(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_double()) return {}; return v->as_double_bits(); }
	// The bin as a boolean; nothing when the record has no such bin, it is nil or it is of another kind.
	Optional<bool> as_bool(StrView name) const { Optional<Value> v = get(name); if (!v || !v->is_bool()) return {}; return v->payload != 0; }
	// The bin's string as an inline Text<N>; nothing when the record has no such bin, it is nil, it is not a
	// string or it does not fit in N bytes.
	template <size_t N> Optional<Text<N>> as_string(StrView name) const
	{
		Optional<Value> v = get(name);
		if (!v) return {};
		Result<Text<N>> t = v->template as_string<N>();
		if (!t) return {};
		return *t;
	}
	// The bytes of a string, blob, GeoJSON or HLL bin as an inline Bytes<N>; nothing when the record has no
	// such bin, it is nil, it is of another kind or it does not fit in N bytes.
	template <size_t N> Optional<Bytes<N>> as_blob(StrView name) const
	{
		Optional<Value> v = get(name);
		if (!v) return {};
		Result<Bytes<N>> b = v->template as_blob<N>();
		if (!b) return {};
		return *b;
	}

	// The Or forms: the bin's integer, or `dflt` when the bin is absent, nil or of another kind.
	int64_t int_or(StrView name, int64_t dflt) const { return as_int(name).value_or(dflt); }
	// The bin's double, or `dflt` when the bin is absent, nil or of another kind. An integer bin is not
	// widened here: it gives the default.
	double double_or(StrView name, double dflt) const { return as_double(name).value_or(dflt); }
	// The bin's boolean, or `dflt` when the bin is absent, nil or of another kind.
	bool bool_or(StrView name, bool dflt) const { return as_bool(name).value_or(dflt); }

	// A blob bin of 32-bit floats into an array of `count` elements. Returns how many were read; an error
	// when the bin is absent, is not a blob or does not fit the array.
	Result<int32_t> floats_into(StrView name, float* out, int32_t count) const
	{
		Result<Value> v = bin(name);
		if (!v) return v.error();
		return v->floats_into(out, count);
	}

	// A walk over a list bin; empty when the bin is missing or not a list.
	ListRange list(StrView name) const { Optional<Value> v = get(name); return ListRange(v && v->is_list() ? *v : Value()); }
	// A walk over the entries of a map bin; empty when the bin is missing or not a map.
	MapRange map(StrView name) const { Optional<Value> v = get(name); return MapRange(v && v->is_map() ? *v : Value()); }

private:
	const Self& self() const { return static_cast<const Self&>(*this); }
};

namespace detail {
struct RecordAccess;
}

// The record a UDF was called on. A record UDF and the per-record half of a stream UDF take it as
// their first parameter, before the call's arguments. It is not one of them: a client does not
// pass it, the arity check does not count it and the function's signature export does not list it. A
// function that does not touch the record leaves the parameter out.
//
//   UDF_FUNCTION(withdraw, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
//   {
//       int64_t balance = UDF_TRY(UDF_TRY(rec.bin("balance")).as_int());
//
//       UDF_TRY(rec.set("balance", balance - amount));
//       UDF_TRY(rec.save());
//
//       return balance - amount;
//   }
//
// `const udf::Record&` is the read-only record: the reading methods are const and the writing ones are
// not, so a function that takes it that way cannot change the record, and the per-record half of a stream
// UDF, whose record takes no write, can take it no other way. Only the SDK makes one, and it cannot be
// copied, so a read-only record stays read-only. The reading methods are ReadRecord's.
class Record : public ReadRecord<Record> {
public:
	Record(const Record&) = delete;
	Record& operator=(const Record&) = delete;

	// What ReadRecord reads through: the bin as the host hands it out. False when the record has no such
	// bin.
	bool read(StrView name, Value& out) const
	{
		return as_udf_rec_bin_value(name.data, name.len, &out, (int32_t)sizeof(out)) == AS_UDF_OK;
	}

	// Writes, staged until save().
	Result<void> set(StrView name, int64_t v) { return check(name, as_udf_rec_set_int(name.data, name.len, v)); }
	// Stages an int, widened to 64 bits.
	Result<void> set(StrView name, int v) { return set(name, (int64_t)v); }
	// Stages a double.
	Result<void> set(StrView name, double v) { return check(name, as_udf_rec_set_double(name.data, name.len, v)); }
	// Stages a boolean.
	Result<void> set(StrView name, bool v) { return check(name, as_udf_rec_set_bool(name.data, name.len, v ? 1 : 0)); }
	// Stages a string.
	Result<void> set(StrView name, StrView v) { return check(name, as_udf_rec_set_string(name.data, name.len, v.data, v.len)); }
	// Stages a NUL-terminated string.
	Result<void> set(StrView name, const char* v) { return set(name, StrView(v)); }
	// Stages a built map as the bin's value. Fails with the map's own error when one of its puts failed.
	Result<void> set(StrView name, const Map& m) { if (m.error().failed()) return m.error(); return check(name, as_udf_rec_set_handle(name.data, name.len, m.handle())); }
	// Stages a built list as the bin's value. Fails with the list's own error when one of its adds failed.
	Result<void> set(StrView name, const List& l) { if (l.error().failed()) return l.error(); return check(name, as_udf_rec_set_handle(name.data, name.len, l.handle())); }
	// Stages `n` bytes at `b` as a blob.
	Result<void> set_blob(StrView name, const void* b, int32_t n) { return check(name, as_udf_rec_set_blob(name.data, name.len, b, n)); }
	// A GeoJSON document, kept as the geospatially indexable kind rather than a string.
	Result<void> set_geojson(StrView name, StrView doc) { return check(name, as_udf_rec_set_geojson(name.data, name.len, doc.data, doc.len)); }
	// A list of integers from an array of `count` elements, in one import, no value built per element.
	// Written at once, not staged: it needs no save().
	Result<void> set_ints(StrView name, const int64_t* vals, int32_t count) { return check(name, as_udf_rec_set_ints(name.data, name.len, vals, count * 8)); }
	// A list of doubles from an array of `count` elements, in one import. Written at once, like set_ints.
	Result<void> set_doubles(StrView name, const double* vals, int32_t count) { return check(name, as_udf_rec_set_doubles(name.data, name.len, vals, count * 8)); }

	// A value: one read from a record by position, keeping its kind; or a scalar, guest bytes, a handle. A
	// nil removes the bin, as remove() does: a bin cannot hold nil.
	Result<void> set(StrView name, const Value& v)
	{
		switch (v.kind()) {
		case Kind::Int: return set(name, v.payload);
		case Kind::Double: return set(name, v.as_double_bits());
		case Kind::Bool: return set(name, v.payload != 0);
		case Kind::Nil: return remove(name);
		case Kind::GuestStr: return check(name, as_udf_rec_set_string(name.data, name.len, (const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestBlob: return check(name, as_udf_rec_set_blob(name.data, name.len, (const void*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::GuestGeoJson: return check(name, as_udf_rec_set_geojson(name.data, name.len, (const char*)(uintptr_t)v.payload, (int32_t)v.len));
		case Kind::Handle: return check(name, as_udf_rec_set_handle(name.data, name.len, (int32_t)v.payload));
		case Kind::Absent: return Error{AS_UDF_NOT_FOUND, "could not write an absent value"};
		default: return check(name, as_udf_rec_set_at(name.data, name.len, v.src(), v.offset()));
		}
	}

	// Removes a bin: staged like a set, and applied by save(). Deleting the whole record is delete_record().
	Result<void> remove(StrView name)
	{
		int32_t rc = as_udf_rec_remove(name.data, name.len);
		if (rc == AS_UDF_OK) return {};
		return Error{rc, detail::message().add("could not remove bin ").add(name.data, name.len).add(" (").add((int64_t)rc).add(")").buf};
	}

	// Whether the record exists on the server.
	bool exists() const { return as_udf_rec_exists() != 0; }
	// The record's generation, its modification count.
	int64_t generation() const { return as_udf_rec_generation(); }
	// The record's remaining time to live in seconds; 0 when it does not expire.
	int64_t ttl() const { return as_udf_rec_ttl(); }
	// When the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC), which is how the
	// server stores it. Not on the same scale as now(), which counts from the Unix epoch.
	int64_t last_update_time() const { return as_udf_rec_last_update_time(); }
	// The record's stored size in bytes.
	int64_t size() const { return as_udf_rec_size(); }
	// The number of bins the record has.
	int64_t bin_count() const { return as_udf_rec_bin_count(); }

	// The record's set name, into a Text; empty when it is in no set.
	Text<64> set_name() const
	{
		Text<64> t;
		int32_t n = as_udf_rec_set_name(t.buf, 64);
		t.len = n < 0 ? 0 : n;
		t.buf[t.len] = '\0';
		return t;
	}

	// Sets the time to live the next save carries, in seconds: 0 for the namespace default, -1 for never.
	Result<void> set_ttl(int32_t seconds)
	{
		int32_t rc = as_udf_rec_set_ttl(seconds);
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not set the TTL");
		return {};
	}

	// The kind of the record's user key; Kind::Absent when the record stores none, which is the case unless
	// it was written with send_key.
	Kind key_type() const { return (Kind)as_udf_rec_key_type(); }
	// The user key when it is an integer; 0 when the record stores no key or it is of another kind, so
	// check key_type() first.
	int64_t key_int() const { return as_udf_rec_key_int(); }
	// The bytes of a string or blob user key into `out`. Returns the length; AS_UDF_NOT_FOUND when the
	// record stores no key, a status at or below AS_UDF_NEEDED_BASE when `cap` is too small.
	int32_t key_bytes(void* out, int32_t cap) const { return as_udf_rec_key_bytes(out, cap); }
	// The record's user key as a value: an integer in the value itself, a string or blob copied into `out`
	// and viewed there, so `out` has to outlive the value. Absent when the record stores no key or the key
	// does not fit in `cap` bytes.
	Value key(void* out, int32_t cap) const { return Value(as_udf_rec_key(out, cap)); }
	// The record's digest, 20 bytes, into `out`. Returns the length, or a negative status: at or below
	// AS_UDF_NEEDED_BASE when `cap` is too small.
	int32_t digest(void* out, int32_t cap) const { return as_udf_rec_digest(out, cap); }

	// Writes the record: the staged bins and removals go to it, and it is created when it does not exist.
	// With nothing staged it is still a write to a record that exists (the server treats it as a touch: the
	// generation and the last-update time move); on a key with no record it creates nothing.
	Result<void> save()
	{
		int32_t rc = as_udf_rec_save();
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not save the record");
		return {};
	}

	// Deletes the record; nothing has to be saved first, and what was staged goes with it. The server
	// carries the delete out when the call returns, so exists() keeps answering what it did until then.
	// Named so because `delete` is a keyword, and so that it cannot be read as remove(name), which removes
	// one bin.
	Result<void> delete_record()
	{
		int32_t rc = as_udf_rec_delete();
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not delete the record");
		return {};
	}

private:
	Record() = default;
	friend struct detail::RecordAccess;

	static Result<void> check(StrView name, int32_t rc)
	{
		if (rc == AS_UDF_OK) return {};
		return Error{rc, detail::message().add("could not write bin ").add(name.data, name.len).add(" (").add((int64_t)rc).add(")").buf};
	}
};

namespace detail {

// Not API: how the generated exports make the record they hand to a function.
struct RecordAccess {
	static Record make() { return Record(); }
};

} // namespace detail

//==========================================================
// What is not about the record: the clock, failure, the log, results.
//

// The server's clock in ms since the Unix epoch, or -1 inside an event handler, where it is refused.
inline int64_t now() { return as_udf_now(); }

// Fails the call with a message and returns the status an export returns.
inline int32_t fail(StrView message)
{
	as_udf_fail(message.data, message.len);
	return 1;
}

// Fails the call with the error's message, or "failed" when it has none, and returns the status an
// export returns.
inline int32_t fail(Error e) { return fail(StrView(e.message != nullptr ? e.message : "failed")); }

// Writes a message to the server's log at AS_UDF_LOG_WARN, AS_UDF_LOG_INFO or AS_UDF_LOG_DEBUG; any
// other level logs as debug. The server keeps the first 511 bytes.
inline void log(int32_t level, StrView message) { as_udf_log(level, message.data, message.len); }

// Sets a value of any kind as the result.
inline Result<void> result(const Value& v)
{
	switch (v.kind()) {
	case Kind::Int: as_udf_result_int(v.payload); break;
	case Kind::Double: as_udf_result_double(v.as_double_bits()); break;
	case Kind::Bool: as_udf_result_bool((int32_t)v.payload); break;
	case Kind::Nil: case Kind::Absent: as_udf_result_nil(); break;
	case Kind::GuestStr: as_udf_result_string((const char*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::GuestBlob: as_udf_result_blob((const void*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::GuestGeoJson: as_udf_result_geojson((const char*)(uintptr_t)v.payload, (int32_t)v.len); break;
	case Kind::Handle: {
		int32_t rc = as_udf_result_handle((int32_t)v.payload);
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not return the collection");
		break;
	}
	default: {
		int32_t rc = as_udf_result_at(v.src(), v.offset());
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not return the value");
		break;
	}
	}
	return {};
}

//==========================================================
// Events, the typed tier: xdr-merge.
//

// One record the event hands over, read-only, with its metadata; the C struct underneath. Its bins are
// read with ReadRecord's methods, the ones udf::Record has: get(), has(), bin(), the typed readers with and
// without a default, list() and map(). An xdr-merge hands over two, `current` and `incoming`, and what the
// server says of the merge is asked of the record it is about: is_delete(), is_partial() and lost_bins() of
// the incoming one, is_tombstone() of the current one, uses_bin_convergence() of either. Everything is
// asked through a method: each one hides the C field of the same name.
struct EventRecord : as_udf_event_record, ReadRecord<EventRecord> {
	// What ReadRecord reads through: the bin of this side as the host hands it out. False when the side
	// has no such bin.
	bool read(StrView name, Value& out) const
	{
		return as_udf_event_bin_value(which, name.data, name.len, &out, (int32_t)sizeof(out)) == AS_UDF_OK;
	}

	// The record's generation. Zero on both sides of an xdr-merge: each cluster sets its own, and a merge
	// must not read it.
	int64_t generation() const { return as_udf_event_record::generation; }
	// When the record was last written. Zero on both sides of an xdr-merge, for the same reason as
	// generation(); bin_last_update_time() is what orders the two sides.
	int64_t last_update_time() const { return as_udf_event_record::last_update_time; }
	// The record's time to live in seconds; 0 when it does not expire.
	int64_t ttl() const { return as_udf_event_record::ttl; }
	// The XDR source id of the cluster that wrote this version of the record.
	int64_t source_id() const { return as_udf_event_record::source_id; }

	// The record's bin names.
	ListRange bin_names() const { return ListRange(Value(as_udf_event_record::bin_names)); }
	// The names of the bins this side deleted. A deleted bin reads as absent, so this is the one way to
	// tell a bin that was removed from one that was never there. Empty when it deleted none.
	ListRange deleted_bins() const { return ListRange(Value(as_udf_event_record::deleted_bins)); }
	// Whether this side deleted any bin: deleted_bins() is not empty.
	bool deleted_any() const { return !deleted_bins().empty(); }

	// When this side last wrote the bin: the bin's own last-update time where the server keeps one, the
	// record's otherwise. The name is cut to 63 bytes.
	int64_t bin_last_update_time(StrView name) const { return as_udf_map_int(&bin_last_update_times, Map_c_str(name), as_udf_event_record::last_update_time); }
	// The source id of this side's last write of the bin: the bin's own where the server keeps one, the
	// record's otherwise. The name is cut to 63 bytes.
	int64_t bin_source_id(StrView name) const { return as_udf_map_int(&bin_source_ids, Map_c_str(name), as_udf_event_record::source_id); }

	// Whether the incoming write is a record delete, in which case this record has no bins. False on the
	// current record.
	bool is_delete() const { return as_udf_event_record::is_delete; }
	// Whether the incoming write carries only the bins that changed, so that a bin this record lacks may
	// well exist on the other cluster. False on the current record.
	bool is_partial() const { return as_udf_event_record::is_partial; }
	// The names of the incoming bins that would lose under bin convergence. Empty on the current record.
	ListRange lost_bins() const { return ListRange(Value(as_udf_event_record::lost_bins)); }
	// Whether the current record is a tombstone: it exists but has no bins. False on the incoming record.
	bool is_tombstone() const { return as_udf_event_record::is_tombstone; }
	// Whether per-bin timestamps are meaningful, for a time-based merge to check before it trusts
	// bin_last_update_time(). The same answer on both records.
	bool uses_bin_convergence() const { return as_udf_event_record::uses_bin_convergence; }

	// `overrides` with every bin of this record it lacks put on top, by position: the Rust SDK's
	// with_bins. It prefers one side, so an xdr-merge handler uses Pair::with_newer instead.
	Map& with_bins(Map& overrides) const
	{
		for (Value name : bin_names()) {
			Result<Text<64>> n = name.as_string<64>();
			if (!n) continue;
			if (overrides.contains(n->view())) continue;
			Value v;
			if (read(n->view(), v)) overrides.put(n->view(), v);
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

// The two sides of a bin in the order both clusters agree on, as Both::ordered() gives them: the side that
// wrote the bin earlier, then the side that wrote it later. `auto [older, newer] = both.ordered();`
template <class T>
struct Ordered {
	Optional<T> older;
	Optional<T> newer;
};

struct Pair;

// A bin on both sides of a merge, either of which may be missing: the Rust SDK's Both<T>. Made by a Pair,
// which also gives it what orders the two sides.
template <class T>
struct Both {
	Optional<T> current;
	Optional<T> incoming;

	// Whether the incoming side wrote this bin later: by the bin's last-update time, then source id - the
	// order the server's own bin convergence uses.
	bool incoming_is_newer() const
	{
		return incoming_last_update_time_ > current_last_update_time_ ||
		       (incoming_last_update_time_ == current_last_update_time_ && incoming_source_id_ > current_source_id_);
	}

	// The value of the side that wrote the bin later, or the other side's when that side has none; nothing
	// when neither has one.
	Optional<T> newer() const { return incoming_is_newer() ? incoming_or_current() : current_or_incoming(); }
	// The value of the side that wrote the bin earlier, or the other side's when that side has none;
	// nothing when neither has one.
	Optional<T> older() const { return incoming_is_newer() ? current_or_incoming() : incoming_or_current(); }
	// Both sides, the one that wrote the bin earlier first: the order both clusters agree on, for a merge
	// whose result depends on the order its inputs are taken in. A side with no value stays empty.
	Ordered<T> ordered() const { return incoming_is_newer() ? Ordered<T>{current, incoming} : Ordered<T>{incoming, current}; }
	// The current value when there is one, the incoming otherwise. It prefers a side, so it is not symmetric.
	Optional<T> current_or_incoming() const { return current ? current : incoming; }
	// The incoming value when there is one, the current otherwise. It prefers a side, so it is not symmetric.
	Optional<T> incoming_or_current() const { return incoming ? incoming : current; }

	// `f(current, incoming)` when both sides have a value; otherwise the one value there is, or nothing.
	// The result is symmetric only when `f` is commutative.
	template <class F>
	Optional<T> merge(F f) const
	{
		if (current && incoming) return f(*current, *incoming);
		return current_or_incoming();
	}

	// The larger of the two values, or the one there is; nothing when neither side has one.
	Optional<T> max() const { return merge([](const T& c, const T& i) { return i > c ? i : c; }); }
	// The smaller of the two values, or the one there is; nothing when neither side has one.
	Optional<T> min() const { return merge([](const T& c, const T& i) { return i < c ? i : c; }); }
	// The sum of both sides, a missing side counting as a value-initialised T (zero for a number).
	T sum() const { return current.value_or(T()) + incoming.value_or(T()); }

private:
	friend struct Pair;

	int64_t current_last_update_time_ = 0;
	int64_t incoming_last_update_time_ = 0;
	int64_t current_source_id_ = 0;
	int64_t incoming_source_id_ = 0;
};

// The two records of an xdr-merge side by side, read bin by bin into a Both. Made from the two records the
// handler is given: `udf::Pair pair = udf::Pair::of(current, incoming);`. It refers to them, so it lives no
// longer than they do.
struct Pair {
	const EventRecord& current;
	const EventRecord& incoming;

	// The pair of the two records of a merge. Each record knows which one it is, so the two may be given in
	// either order: of(current, incoming) and of(incoming, current) are the same pair.
	static Pair of(const EventRecord& a, const EventRecord& b) { return a.which <= b.which ? Pair{a, b} : Pair{b, a}; }

	// An integer bin on both sides, with what orders the two sides for it. A side is empty where the bin
	// is missing, nil or of another kind.
	Both<int64_t> as_int(StrView name) const { return both(name, current.as_int(name), incoming.as_int(name)); }
	// A double bin on both sides, likewise. An integer bin is not widened and reads as empty.
	Both<double> as_double(StrView name) const { return both(name, current.as_double(name), incoming.as_double(name)); }
	// A boolean bin on both sides, likewise.
	Both<bool> as_bool(StrView name) const { return both(name, current.as_bool(name), incoming.as_bool(name)); }
	// A string bin on both sides, each as an inline Text<N>. A side is empty where the bin is missing, nil,
	// not a string or longer than N bytes.
	template <size_t N> Both<Text<N>> as_string(StrView name) const { return both(name, current.as_string<N>(name), incoming.as_string<N>(name)); }
	// The bytes of a string, blob, GeoJSON or HLL bin on both sides, each as an inline Bytes<N>. A side is
	// empty where the bin is missing, nil, of another kind or longer than N bytes.
	template <size_t N> Both<Bytes<N>> as_blob(StrView name) const { return both(name, current.as_blob<N>(name), incoming.as_blob<N>(name)); }
	// A bin of any kind on both sides, as values. A side is empty where the bin is missing or nil.
	Both<Value> bin(StrView name) const { return both(name, current.get(name), incoming.get(name)); }
	// A list bin on both sides, as values to walk. A side is empty where the bin is missing or not a list.
	Both<Value> list(StrView name) const
	{
		Optional<Value> c = current.get(name), i = incoming.get(name);
		return both(name, c && c->is_list() ? c : Optional<Value>(), i && i->is_list() ? i : Optional<Value>());
	}
	// A map bin on both sides, as values to walk or look up in. A side is empty where the bin is missing or
	// not a map.
	Both<Value> map(StrView name) const
	{
		Optional<Value> c = current.get(name), i = incoming.get(name);
		return both(name, c && c->is_map() ? c : Optional<Value>(), i && i->is_map() ? i : Optional<Value>());
	}

	// Whether the incoming side wrote the bin later: by the bin's last-update time, then by source id.
	bool incoming_is_newer(StrView name) const
	{
		int64_t ct = current.bin_last_update_time(name), it = incoming.bin_last_update_time(name);
		return it > ct || (it == ct && incoming.bin_source_id(name) > current.bin_source_id(name));
	}

	// Every bin either side has that `overrides` lacks, from the side that wrote it later: symmetric.
	Map& with_newer(Map& overrides) const
	{
		const EventRecord* sides[2] = {&current, &incoming};
		for (const EventRecord* side : sides) {
			for (Value name : side->bin_names()) {
				Result<Text<64>> n = name.as_string<64>();
				if (!n || overrides.contains(n->view())) continue;
				Optional<Value> v = bin(n->view()).newer();
				if (v) overrides.put(n->view(), *v);
			}
		}
		return overrides;
	}

private:
	template <class T>
	Both<T> both(StrView name, Optional<T> c, Optional<T> i) const
	{
		Both<T> b;
		b.current = c;
		b.incoming = i;
		b.current_last_update_time_ = current.bin_last_update_time(name);
		b.incoming_last_update_time_ = incoming.bin_last_update_time(name);
		b.current_source_id_ = current.bin_source_id(name);
		b.incoming_source_id_ = incoming.bin_source_id(name);
		return b;
	}
};

// What an xdr-merge handler returns: one of the verdicts, or the final record. It is built by its
// functions and delivered by the generated export.
struct Resolution {
	const char* verdict;
	int32_t bins;
	int64_t ttl;
	Error err;

	// An empty resolution, for a Result that failed; never delivered.
	Resolution() : verdict(""), bins(0), ttl(-1), err() {}
	Resolution(const char* verdict_, int32_t bins_ = 0, int64_t ttl_ = -1, Error err_ = {})
	    : verdict(verdict_), bins(bins_), ttl(ttl_), err(err_) {}

	// Keeps the current record; the incoming write is not applied.
	static Resolution keep_local() { return Resolution{"keep-local"}; }
	// Takes the incoming write.
	static Resolution take_remote() { return Resolution{"take-remote"}; }
	// Leaves the outcome to the policy the handler stands in for.
	static Resolution policy() { return Resolution{"policy"}; }
	// Removes the record on this cluster. The other cluster decides for itself, so a merge that deletes
	// has to delete on both sides for the two to agree. (`delete` is a keyword, as for Record.)
	static Resolution delete_record() { return Resolution{"delete"}; }
	// The final record: these bins, whole; a bin absent or nil is deleted. ttl -1 keeps the incoming write's.
	// At least one bin must be left: a record of no bins is the handler's failure, not a deletion.
	static Resolution record(const Map& bins, int64_t ttl = -1) { return Resolution{"record", bins.handle(), ttl, bins.error()}; }

	// Delivers the resolution and returns the status the export returns: the verdict word, or for record()
	// the final bins and TTL. Fails the call instead when the bins map kept a failure. The generated export
	// calls it.
	int32_t deliver() const
	{
		if (err.failed()) return fail(err);
		if (bins == 0) {
			as_udf_result_string(verdict, detail::cstrlen(verdict));
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
	// The first argument, for a range-for over the arguments.
	const Value* begin() const { return values; }
	// One past the last argument.
	const Value* end() const { return values + count; }
	// How many arguments there are.
	int32_t size() const { return count; }
	// Whether there is none.
	bool empty() const { return count == 0; }
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
		if (!a[i].is_string()) return mismatch(fn, i, p, "a string");
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		return a[i].as_string<N>();
	}
};

template <size_t N> struct Param<Bytes<N>> {
	static constexpr const char* name = "bytes";
	static constexpr size_t capacity = N;
	static constexpr bool optional = false, rest = false;
	static Result<Bytes<N>> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_string() && !a[i].is_blob()) return mismatch(fn, i, p, "a string or a blob");
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		return a[i].as_blob<N>();
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
template <> struct Deliver<StrView> { static Result<void> go(StrView v) { as_udf_result_string(v.data, v.len); return {}; } };
template <size_t N> struct Deliver<Text<N>> { static Result<void> go(const Text<N>& v) { as_udf_result_string(v.buf, v.len); return {}; } };
template <> struct Deliver<Value> { static Result<void> go(const Value& v) { return result(v); } };
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

// What an exported function looks like. `Params` and `arity` are the client's arguments only: a leading
// udf::Record is the record the export hands in, so it is not decoded, not counted and not in the
// signature. `record` says whether the function takes one, `writes` whether it takes it writable.
template <class F> struct FnTraits;
template <class R, class... A> struct FnTraits<R (*)(A...)> {
	static_assert(!(false || ... || is_same<Bare<A>, Record>),
	    "the record is a function's first parameter, taken as udf::Record& or const udf::Record&");
	using Ret = R;
	using Params = TypeList<A...>;
	static constexpr size_t arity = sizeof...(A);
	static constexpr bool raw = sizeof...(A) == 2 && is_same<TypeList<A...>, TypeList<int32_t, int32_t>> && is_same<R, int32_t>;
	static constexpr bool rest = (false || ... || Param<Bare<A>>::rest);
	static constexpr bool record = false, writes = false;
};
template <class R, class... A> struct FnTraits<R (*)(Record&, A...)> {
	static_assert(!(false || ... || is_same<Bare<A>, Record>), "a function takes the record once, first");
	using Ret = R;
	using Params = TypeList<A...>;
	static constexpr size_t arity = sizeof...(A);
	static constexpr bool raw = false;
	static constexpr bool rest = (false || ... || Param<Bare<A>>::rest);
	static constexpr bool record = true, writes = true;
};
template <class R, class... A> struct FnTraits<R (*)(const Record&, A...)> {
	static_assert(!(false || ... || is_same<Bare<A>, Record>), "a function takes the record once, first");
	using Ret = R;
	using Params = TypeList<A...>;
	static constexpr size_t arity = sizeof...(A);
	static constexpr bool raw = false;
	static constexpr bool rest = (false || ... || Param<Bare<A>>::rest);
	static constexpr bool record = true, writes = false;
};

// How many leading parameters of Fn are not the client's: 1 when it takes the record, else 0.
template <auto Fn> constexpr size_t leading = FnTraits<decltype(Fn)>::record ? 1 : 0;

// The parameter names, parsed at compile time from the stringised parameter list. The first `skip` of them
// are left out: the record parameter, which is in the list the macro was given but is not an argument.
struct ParamNames {
	static constexpr size_t max = 16;
	char names[max][32] = {};
	size_t count = 0;
	size_t skip = 0;

	constexpr ParamNames(const char* text, size_t skip_ = 0) : skip(skip_)
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
		if (skip > 0) {
			skip--;
			return;
		}
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
	ParamNames names(params, leading<Fn>);
	add_params(s, names, typename T::Params{});
	s.add(" -> value");
	return s;
}

// The parameter list of one half of a stream UDF. `RecordHalf` says which: the per-record half may take
// the record, read-only, because a stream's record takes no write; the end half runs after the last
// record and has none.
template <auto Fn, bool RecordHalf>
constexpr Signature params_text(const char* params)
{
	using T = FnTraits<decltype(Fn)>;
	static_assert(!T::raw, "a stream half taking (int32_t args, int32_t argc) is the raw form: use UDF_STREAM_RAW");
	static_assert(!RecordHalf || !T::writes, "a stream's per-record half cannot write the record: take it as const udf::Record&");
	static_assert(RecordHalf || !T::record, "a stream's end half has no record: it runs after the last one");
	Signature s;
	ParamNames names(params, leading<Fn>);
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
	if constexpr (FnTraits<decltype(Fn)>::record) {
		Record rec = RecordAccess::make();
		return call_impl<Fn>(fn, err, record_half, rec, keep(Param<Bare<A>>::decode(a, n, (int32_t)I, fn, names.names[I]))...);
	}
	else {
		return call_impl<Fn>(fn, err, record_half, keep(Param<Bare<A>>::decode(a, n, (int32_t)I, fn, names.names[I]))...);
	}
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
// int32_t, double, bool, Text<N>, Bytes<N>, Value, Optional<T> of those, and Args last. Before them the
// function may take the record it was called on, `udf::Record& rec` to write it or `const udf::Record& rec`
// to read it: the export hands it in, and it is neither an argument of the call nor part of the signature.
// A function declared (int32_t args, int32_t argc) returning int32_t is the raw form; see UDF_FUNCTION_RAW.
#define UDF_FUNCTION(name, ret, ...)                                                                     \
	static ret name##_udf_impl(__VA_ARGS__);                                                             \
	static constexpr ::udf::detail::ParamNames udf_names_##name{#__VA_ARGS__, ::udf::detail::leading<&name##_udf_impl>}; \
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
// { ... }`, in that order; the record half returns void or Result<void>, the end half the partial. The
// record half may take the record first, as `const udf::Record& rec` only: a stream's record takes no
// write. The end half has no record. State is static. UDF_STREAM_RAW(name, "(record params)",
// "(end params)") declares both halves raw.
#define UDF_STREAM_RECORD(name, ret, ...)                                                                \
	static ret name##_record_udf_impl(__VA_ARGS__);                                                      \
	static constexpr ::udf::detail::ParamNames udf_names_##name##_record{#__VA_ARGS__, ::udf::detail::leading<&name##_record_udf_impl>}; \
	static constexpr ::udf::detail::Signature udf_params_##name##_record =                               \
	    ::udf::detail::params_text<&name##_record_udf_impl, true>(#__VA_ARGS__);                         \
	extern "C" __attribute__((export_name(#name "_record"))) int32_t udf_export_##name##_record(int32_t args, int32_t argc) \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_record_udf_impl>(#name, udf_names_##name##_record, args, argc, true); \
	}                                                                                                    \
	static ret name##_record_udf_impl(__VA_ARGS__)

// The end half of a stream UDF, declared after its UDF_STREAM_RECORD and exported as `<name>_end`: its
// return is delivered as the result, the partial. It also emits the stream's signature, built from the
// parameter lists of both halves, which is why it comes second.
#define UDF_STREAM_END(name, ret, ...)                                                                   \
	static ret name##_end_udf_impl(__VA_ARGS__);                                                         \
	static constexpr ::udf::detail::ParamNames udf_names_##name##_end{#__VA_ARGS__};                     \
	static constexpr ::udf::detail::Signature udf_params_##name##_end =                                  \
	    ::udf::detail::params_text<&name##_end_udf_impl, false>(#__VA_ARGS__);                           \
	extern "C" __attribute__((export_name(#name "_end"))) int32_t udf_export_##name##_end(int32_t args, int32_t argc) \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_end_udf_impl>(#name, udf_names_##name##_end, args, argc, false); \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::stream_signature(#name, udf_params_##name##_record, udf_params_##name##_end)) \
	static ret name##_end_udf_impl(__VA_ARGS__)

// Both halves of a stream UDF in the raw form. Declares `int32_t name_record_udf_impl(int32_t args,
// int32_t argc)` and `name_end_udf_impl` likewise, exports them as `<name>_record` and `<name>_end`, and
// emits the signature from the two parameter strings. No body follows the macro: the module defines the
// two functions itself, each returning the status.
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

// The signature of an event handler, `event <kind>/1 name(...)`, which `event-set` checks before binding.
#define UDF_HANDLER_SIG(name, kind, params) UDF_SIG_EXPORT(name, ::udf::detail::literal_signature("event " kind "/1 " #name params))

// An xdr-merge handler: `UDF_ON_XDR_MERGE(name) { ... }` defines
// `Result<Resolution> name(const EventRecord& current, const EventRecord& incoming)`: the record as it is on
// this cluster and the one that arrived over XDR. The export decodes the event into the two and delivers
// the resolution.
#define UDF_ON_XDR_MERGE(name)                                                                           \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord&, const ::udf::EventRecord&); \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::EventRecord current, incoming;                                                            \
		if (as_udf_event_decode(args, argc, &current, &incoming) != AS_UDF_OK) return ::udf::fail(#name ": not an xdr-merge event"); \
		::udf::Result<::udf::Resolution> r = name##_udf_impl(current, incoming);                         \
		if (!r) return ::udf::fail(r.error());                                                           \
		return r->deliver();                                                                             \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, "xdr-merge", "(current: record, incoming: record) -> resolution")               \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord& current, const ::udf::EventRecord& incoming)

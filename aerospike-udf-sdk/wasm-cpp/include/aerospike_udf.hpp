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
//
// The stack. Text<N> and Bytes<N> hold their bytes inline, so a parameter, a local or a returned value of one is
// N bytes of the wasm stack, and a generated export holds every parameter at once, plus a copy on the way into
// the function. clang gives a wasm32 module 64 KB of stack by default, which a single Text<65536> parameter
// already overflows; an overflow is not reported at build time and traps the call (or, with no guard page,
// overwrites the module's statics). So add up the capacities of the largest function's parameters and locals,
// double it for the copies, leave some 16 KB for the SDK, and link with `-Wl,-z,stack-size=N` for that much:
// a module whose function takes two Text<65536> parameters wants about 512 KB, and examples/apitest, which takes
// several, links with 2 MiB. Text<N> above a few kilobytes is better kept in a static where the function allows
// it. The stack comes out of the module's linear memory, so a larger one makes every instance larger.

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

// A message built without allocation, in one of a small ring of static buffers. An error the SDK makes points
// into the buffer it was built in and the next one is built in the next buffer, so one error's message is not
// overwritten by the next: it stays readable until MESSAGE_SLOTS further messages have been built.
struct Message {
	// What the host keeps of a call's failure, 511 bytes, and the terminator. A message longer than that
	// ends in "...", so a reader can see it was cut; a cut never splits a UTF-8 character.
	char buf[512];
	int32_t len = 0;
	bool cut = false;

	void put(char c)
	{
		if (cut) return;
		if (len < (int32_t)sizeof(buf) - 1) {
			buf[len++] = c;
			buf[len] = '\0';
			return;
		}
		// Full: the last three bytes become the mark, backed off to the start of a character.
		len = (int32_t)sizeof(buf) - 4;
		while (len > 0 && ((unsigned char)buf[len] & 0xC0) == 0x80) len--;
		buf[len++] = '.';
		buf[len++] = '.';
		buf[len++] = '.';
		buf[len] = '\0';
		cut = true;
	}

	Message& add(const char* s)
	{
		for (int32_t i = 0; s[i] != '\0' && !cut; i++) put(s[i]);
		return *this;
	}

	Message& add(const char* s, int32_t n)
	{
		for (int32_t i = 0; i < n && !cut; i++) put(s[i]);
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
		if (negative) put('-');
		while (i > 0) put(tmp[--i]);
		return *this;
	}
};

// How many messages the SDK keeps before it reuses the oldest one's buffer.
constexpr int32_t MESSAGE_SLOTS = 8;

inline Message& message()
{
	static Message ring[MESSAGE_SLOTS];
	static int32_t next = 0;
	Message& m = ring[next];
	next = (next + 1) % MESSAGE_SLOTS;
	m.len = 0;
	m.cut = false;
	m.buf[0] = '\0';
	return m;
}

// A sum that wraps: signed overflow is undefined in C++, and a merge adds whatever two clusters stored.
constexpr int64_t wrapping_add(int64_t a, int64_t b) { return (int64_t)((uint64_t)a + (uint64_t)b); }
constexpr double wrapping_add(double a, double b) { return a + b; }
template <class T> constexpr T wrapping_add(const T& a, const T& b) { return a + b; }

} // namespace detail

//==========================================================
// Errors and results.
//

using Status = int32_t;

// What a failed call reports: the native's status (an AS_UDF_* code) and a message naming what failed, which the
// generated export delivers to the client as the UDF's error. A function returning a Result fails by returning
// one: `return udf::error(AS_UDF_ERR, "withdraw: insufficient funds");`.
//
// `status` is the error's own and is what a caller in the module tests: AS_UDF_NOT_FOUND, AS_UDF_TYPE_MISMATCH,
// AS_UDF_REFUSED, AS_UDF_ERR, or a needed-length status (at or below AS_UDF_NEEDED_BASE) for a buffer that was too
// small. It is a plain value and nothing changes it. Every error the SDK makes carries the status of the host call
// or the check that failed.
//
// The message is not owned. It is a string literal, which lasts for ever, or text the SDK built in one of
// eight static buffers that it uses in turn: such a message is not overwritten by the next error, and stays
// readable until eight more messages have been built. So an error may be kept across further SDK calls, and
// across a few that fail, and still be returned or passed to fail() with its own text; a module that collects
// errors over a long loop should copy the text of the one it keeps (`udf::Text<511>` holds any of them) or
// report by status.
struct Error {
	Status status = AS_UDF_OK;
	const char* message = nullptr;

	// Whether this is an error. Decided by the message alone: a default-constructed Error has none and is not
	// one, and a status without a message does not count.
	constexpr bool failed() const { return message != nullptr; }
};

// Builds an Error from a status and a message, to return from a function whose return type is a Result. `status`
// is one of the AS_UDF_* codes and is for the caller in the module; the client sees the message. The message is
// not copied, so it must outlive the error: pass a string literal.
inline Error error(Status status, const char* message) { return Error{status, message}; }

// A value or an error, the Rust SDK's Result: what every call that can fail returns, and what a UDF returns to
// have its value delivered or its error reported. It converts to bool, true on success; `*r` and `r->` reach the
// value, which is only meaningful then. A function returns either a T or an Error and the Result is built from
// it. `Result<void>`, also named `Ok`, is the same with no value: return `{}` for success.
//
// ```cpp
// udf::Result<int64_t> balance_of(const udf::Record& rec)
// {
// 	udf::Optional<int64_t> balance = UDF_TRY(rec.as_int("balance"));
// 	if (!balance) return udf::error(AS_UDF_NOT_FOUND, "no bin balance");
// 	return *balance;
// }
// ```
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
	// Moves the value out, leaving the Result's own copy moved-from. Meaningful only when ok(): a failed Result
	// holds a value-initialised T. What UDF_TRY yields.
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

// A value that may be missing: what a typed reader yields for a bin, an element or an entry that is absent or
// nil, a map lookup that found nothing, a side of a merge that has no such bin, and what a UDF takes for an
// argument the client may leave out. It converts to bool, true when there is a value; `*o` and `o->` reach it and
// are only meaningful then (an empty one holds a value-initialised T: 0, an empty Text, an absent Value).
//
// Missing is not the same as wrong. A typed reader returns a `Result<Optional<T>>`: an error (of status
// AS_UDF_TYPE_MISMATCH) when what is there is of another kind, an empty Optional when nothing is there, the
// value otherwise. UDF_TRY takes the error out and leaves the Optional.
//
// ```cpp
// if (udf::Optional<int64_t> n = UDF_TRY(rec.as_int("n"))) {
// 	total += *n;
// }
// int64_t limit = UDF_TRY(rec.int_or("limit", 100));
// ```
template <class T>
class Optional {
public:
	Optional() : has_(false), value_() {}
	Optional(T value) : has_(true), value_(static_cast<T&&>(value)) {}

	// Whether a value is present. The conversion to bool says the same, so `if (o)` and `if (!o)` are the usual
	// tests.
	bool has_value() const { return has_; }
	explicit operator bool() const { return has_; }
	const T& operator*() const { return value_; }
	T& operator*() { return value_; }
	const T* operator->() const { return &value_; }
	T* operator->() { return &value_; }
	// The value, or `dflt` when there is none. Returns a copy.
	T value_or(T dflt) const { return has_ ? value_ : dflt; }
	// Moves the value out. Meaningful only when there is one.
	T take_value() { return static_cast<T&&>(value_); }
	// The value as a Result, or the error `e` when there is none: for a caller to whom a missing value is a
	// failure. `int64_t n = UDF_TRY(UDF_TRY(v.as_int()).ok_or(udf::error(AS_UDF_NOT_FOUND, "sum: a nil element")));`
	Result<T> ok_or(Error e) const
	{
		if (!has_) return e;
		return value_;
	}

private:
	bool has_;
	T value_;
};

// Propagates an error out of the enclosing function and yields the value otherwise: the Rust `?`. `expr` is a
// Result; when it failed, the enclosing function returns its error at once, so that function must itself return a
// Result (or an Error). With a `Result<void>` it is used as a statement. A statement expression, which clang and
// gcc provide.
//
// ```cpp
// int64_t balance = UDF_TRY(rec.int_or("balance", 0));
// UDF_TRY(rec.set("balance", balance - amount));
// UDF_TRY(rec.save());
// ```
#define UDF_TRY(expr)                                                     \
	({                                                                    \
		auto&& udf_try_r = (expr);                                        \
		if (!udf_try_r.ok()) return udf_try_r.error();                    \
		udf_try_r.take();                                                 \
	})

// The value a typed reader found, for a caller to whom nothing there is a failure too: the reader's own error
// when it gave one (AS_UDF_TYPE_MISMATCH for another kind), and an error of status AS_UDF_NOT_FOUND carrying
// `what` when it found nothing. `int64_t n = UDF_TRY(udf::required(v.as_int(), "count: a nil element"));`
template <class T>
Result<T> required(Result<Optional<T>> r, const char* what)
{
	if (!r) return r.error();
	if (!*r) return Error{AS_UDF_NOT_FOUND, what};
	return r->take_value();
}

namespace detail {

inline Error status_error(Status rc, const char* what)
{
	return Error{rc, message().add(what).add(" (").add((int64_t)rc).add(")").buf};
}

} // namespace detail

//==========================================================
// Strings and bytes without an allocator.
//

// A view of bytes the caller keeps alive: a pointer and a length, nothing owned and nothing copied. Every bin
// name, map key and string argument of the SDK is taken as one, and it converts from a string literal (measured
// up to its NUL) and from a Text, so `rec.set("name", "value")` needs no ceremony. The bytes need no terminator
// and `data` is not guaranteed to have one. Compared with ==, != and <, byte by byte. A bin name is 1 to 15
// bytes.
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

// A string of up to N bytes held inline, with a terminating NUL after them: the Rust SDK's Str<N>, and how the
// SDK hands a string out when there is no allocator. `len` is its length in bytes and `buf` the bytes. A UDF
// parameter of this type takes a string argument of at most N bytes (`str[N]` in the signature) and the call
// fails for a longer one; `as_string<N>()` on a Value or a record reads a stored string into one, and reports a
// longer one as an error. Converts to StrView.
//
// A Text is N + 1 bytes wherever it is, and a parameter or a local is on the stack: see "The stack" at the head
// of this file before giving a parameter a capacity in the tens of kilobytes.
//
// ```cpp
// udf::Optional<udf::Text<32>> owner = UDF_TRY(rec.as_string<32>("owner"));
// if (owner && *owner == "admin") udf::log(AS_UDF_LOG_INFO, owner->view());
// ```
template <size_t N>
struct Text {
	char buf[N + 1] = {};
	int32_t len = 0;

	constexpr Text() = default;
	// From a view known to fit: a literal, another Text of no greater capacity. Nothing is cut: a view longer
	// than N bytes is a mistake in the module, and it stops the call with a trap (and does not compile in a
	// constant expression) rather than leave a shortened or empty text behind. Where the length is not known,
	// assign() is the checked form.
	constexpr Text(StrView s)
	{
		if (!assign(s)) __builtin_trap();
	}

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

// Up to N bytes held inline: the Rust SDK's Bytes<N>. `len` is how many of `buf` are in use; there is no
// terminator. A UDF parameter of this type takes a string or a blob argument of at most N bytes (`bytes[N]` in
// the signature). `as_blob<N>()` on a Value or a record reads a blob into one, and only a blob; `as_bytes<N>()`
// reads the raw bytes of a string, blob, GeoJSON or HLL. On the stack it is N bytes: see "The stack" at the head
// of this file.
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

namespace detail {

// A stored value the host could not read where an answer of "nothing" would be taken for "not there": a
// lookup or a walk in a value a failed save has ended, or a walk the call's memory has no room for. There is no
// error to return from those, and a wrong "nothing" is worse than none, so the call is failed with a
// message that says why and stopped.
[[noreturn]] inline void unreadable()
{
	as_udf_fail_str("could not read a bin or a stored list or map: the call has spent the memory it may hold on "
	                "the server, or the value is no longer readable after a failed save");
	__builtin_trap();
}

} // namespace detail

// The 16-byte value every read is: the C struct with accessors. It is what a bin, an argument, a list element and
// a map key or entry come back as. A scalar is in the value itself. A stored string, blob, list or map is held by
// position: the bytes stay on the host until asked for, so copying a Value copies a reference, not the data, and
// the value is valid until the call ends. Test the kind with the `is_` methods, or read with a typed reader:
// as_int(), as_double(), as_bool(), `as_string<N>()` and `as_blob<N>()`; list() and map() range over a
// collection, get() and at() look one element up.
//
// A typed reader has three outcomes, and a caller tells them apart without reading a message: nothing (an empty
// Optional) when the value is absent or nil, the value when it is of that kind, and an error of status
// AS_UDF_TYPE_MISMATCH when it is of another kind. Nothing is converted on the way: an integer is not a double,
// a string is not a blob, GeoJSON is not a string.
//
// A list, map or blob value read from a bin before a save that failed can no longer be read: read the bin again.
//
// ```cpp
// udf::Value v = UDF_TRY(rec.bin("profile"));
// if (v.is_map()) {
// 	udf::Optional<udf::Value> age = v.get("age");
// 	if (age) return UDF_TRY(age->as_int()).value_or(0);
// }
// return UDF_TRY(v.as_int()).value_or(0);
// ```
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
	// which is false of an absent value. What the typed readers answer an empty Optional for.
	constexpr bool is_null() const { return is_nil() || is_absent(); }
	// Whether the value is an integer.
	constexpr bool is_int() const { return kind() == Kind::Int; }
	// Whether the value is a double. An integer is not one, and as_double() does not make it one.
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
	// Whether the value is stored GeoJSON: text, but not a string, and read with bytes_into() or `as_bytes<N>()`.
	constexpr bool is_geojson() const { return kind() == Kind::GeoJson; }
	// Whether the value is a stored HyperLogLog: bytes, but not a blob, and read with bytes_into() or
	// `as_bytes<N>()`.
	constexpr bool is_hll() const { return kind() == Kind::Hll; }
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
	// Whether size() is 0: an empty string, blob, list or map, and also every scalar, nil and absent value, which
	// have no size.
	bool empty() const { return size() == 0; }
	// The payload reinterpreted as a double, with no check of the kind. Use as_double() unless the kind is
	// already known to be Double.
	double as_double_bits() const { return as_udf_value_double(this); }

	// The integer. Nothing when the value is absent or nil; an error of status AS_UDF_TYPE_MISMATCH, naming the
	// kind found, when it is of another kind. A double is not truncated to one. When the kind is already known,
	// `payload` is the integer.
	Result<Optional<int64_t>> as_int() const
	{
		if (is_null()) return Optional<int64_t>();
		if (!is_int()) return kind_error("an int");
		return Optional<int64_t>(payload);
	}

	// The double. Nothing when the value is absent or nil; an error of status AS_UDF_TYPE_MISMATCH when it is of
	// another kind. An integer is another kind: nothing is widened, here or on a record.
	Result<Optional<double>> as_double() const
	{
		if (is_null()) return Optional<double>();
		if (!is_double()) return kind_error("a double");
		return Optional<double>(as_double_bits());
	}

	// The boolean. Nothing when the value is absent or nil; a type-mismatch error when it is of another kind.
	Result<Optional<bool>> as_bool() const
	{
		if (is_null()) return Optional<bool>();
		if (!is_bool()) return kind_error("a bool");
		return Optional<bool>(payload != 0);
	}

	// The text of a string into an inline `Text<N>`: `v.as_string<32>()`. One host call, which copies the bytes.
	// Nothing when the value is absent or nil; an error of status AS_UDF_TYPE_MISMATCH when it is not a string
	// (GeoJSON is not one); and an error with a needed-length status when the string is longer than N bytes: its
	// length is size(), to choose N by, and `-(status) - 16` says it too. A string of the guest's own (Value::of)
	// reads the same way, with the same error when it does not fit. Nothing is ever cut short.
	template <size_t N>
	Result<Optional<Text<N>>> as_string() const
	{
		if (is_null()) return Optional<Text<N>>();
		if (kind() != Kind::Str && kind() != Kind::GuestStr) return kind_error("a string");
		if (len > N) return too_long("the string", (int64_t)N);
		Optional<Text<N>> out{Text<N>()};
		Text<N>& t = *out;
		if (kind() == Kind::GuestStr) {
			__builtin_memcpy(t.buf, (const void*)(uintptr_t)payload, len);
			t.len = (int32_t)len;
		}
		else {
			int32_t n = as_udf_bytes(this, t.buf, (int32_t)N);
			if (n < 0) return detail::status_error(n, "could not read the string");
			t.len = n;
		}
		t.buf[t.len] = '\0';
		return out;
	}

	// Copies the raw bytes of a string, blob, GeoJSON or HLL into `out`, which holds `cap` bytes, and returns how
	// many; no terminator is written. The one reader that takes all four kinds, and bytes of the guest's own
	// (Value::of a string, Value::blob, Value::geojson) with them: for a caller that wants the bytes whatever
	// they are labelled. An error of status AS_UDF_NOT_FOUND for an absent or nil value, AS_UDF_TYPE_MISMATCH for
	// any other kind (a scalar, a list, a map), and a needed-length status when the value is longer than `cap`
	// bytes, in which case nothing is written; size() is the length needed.
	Result<int32_t> bytes_into(void* out, int32_t cap) const
	{
		Kind k = kind();
		if (is_null()) return Error{AS_UDF_NOT_FOUND, "there is no value to read the bytes of"};
		if (k == Kind::GuestStr || k == Kind::GuestBlob || k == Kind::GuestGeoJson) {
			if ((int64_t)len > (int64_t)cap) return too_long("the value", cap);
			__builtin_memcpy(out, (const void*)(uintptr_t)payload, len);
			return (int32_t)len;
		}
		if (k != Kind::Str && k != Kind::Blob && k != Kind::GeoJson && k != Kind::Hll) return kind_error("a string, blob, GeoJSON or HLL");
		if ((int64_t)len > (int64_t)cap) return too_long("the value", cap);
		int32_t n = as_udf_bytes(this, out, cap);
		if (n < 0) return detail::status_error(n, "could not read the bytes");
		return n;
	}

	// The raw bytes of a string, blob, GeoJSON or HLL into an inline Bytes<N>: bytes_into() with the buffer
	// supplied. Nothing when the value is absent or nil; an error of status AS_UDF_TYPE_MISMATCH for any other
	// kind, and a needed-length status when the value does not fit in N bytes.
	template <size_t N>
	Result<Optional<Bytes<N>>> as_bytes() const
	{
		if (is_null()) return Optional<Bytes<N>>();
		Optional<Bytes<N>> out{Bytes<N>()};
		Result<int32_t> n = bytes_into(out->buf, (int32_t)N);
		if (!n) return n.error();
		out->len = *n;
		return out;
	}

	// The bytes of a blob into an inline Bytes<N>. Strict: only a blob (a stored one, or one made by
	// Value::blob). Nothing when the value is absent or nil; an error of status AS_UDF_TYPE_MISMATCH when it is of
	// another kind, a string, GeoJSON and an HLL included, whose raw bytes `as_bytes<N>()` reads; and a
	// needed-length status when the blob does not fit in N bytes.
	template <size_t N>
	Result<Optional<Bytes<N>>> as_blob() const
	{
		if (is_null()) return Optional<Bytes<N>>();
		if (kind() != Kind::Blob && kind() != Kind::GuestBlob) return kind_error("a blob");
		return as_bytes<N>();
	}

	// Copies a blob of 32-bit floats straight into an array, element for element, with no decoding: the blob's
	// bytes are taken as the floats' own little-endian bytes. `count` is the capacity of `out` in floats. Returns
	// how many were read, the blob's length divided by 4. An error of status AS_UDF_TYPE_MISMATCH when the value
	// is not a blob (AS_UDF_NOT_FOUND when it is absent or nil), and a needed-length status when it holds more
	// than `count` floats, in which case nothing is written.
	Result<int32_t> floats_into(float* out, int32_t count) const
	{
		if (is_null()) return Error{AS_UDF_NOT_FOUND, "there is no blob to read floats from"};
		if (!is_blob()) return kind_error("a blob");
		int32_t n = as_udf_bytes(this, out, count * 4);
		if (n < 0) return detail::status_error(n, "the blob does not fit the array");
		return n / 4;
	}

	// Whether a stored string reads as `text`, of any length. False for a value that is not a stored string. One
	// host call when the lengths agree, none otherwise; the string is then copied out into as many bytes of stack
	// as `text` is long, so a text of many kilobytes needs a stack that size.
	bool is(StrView text) const
	{
		if (!is_string() || (int64_t)len != (int64_t)text.len) return false;
		char small[64];
		char* buf = text.len <= (int32_t)sizeof(small) ? small : (char*)__builtin_alloca((size_t)text.len);
		return as_udf_string_eq(this, text.data, text.len, buf);
	}

	// Copies a stored value's bytes as the server stores them, msgpack header included, into `out`, which holds
	// `cap` bytes, and returns how many: the encoding a convergent merge compares when nothing else tells two
	// candidates apart. For a value held by position (is_stored()); an error of status AS_UDF_TYPE_MISMATCH for
	// one that is not, AS_UDF_NOT_FOUND for an absent or nil one, and a needed-length status when `cap` is too
	// small. To compare two values without copying either, use cmp_stored().
	Result<int32_t> stored_bytes_into(void* out, int32_t cap) const
	{
		int32_t n = as_udf_stored_bytes(this, out, cap);
		if (n < 0) return detail::status_error(n, "could not read the stored bytes");
		return n;
	}

	// Compares two stored values byte by byte on their stored encodings, without copying either: exactly -1, 0 or
	// 1, a shorter encoding that is a prefix of the other first. Both must be held by position (is_stored()): when
	// either is a scalar, nil, absent or a value of the guest's, or the host cannot compare the two, the answer is
	// an error of status AS_UDF_TYPE_MISMATCH, never a number that could be read as an order. The order is of
	// encodings, not of meaning, and is the same on every cluster: a tie-break, not a sort order for numbers.
	Result<int32_t> cmp_stored(const Value& other) const
	{
		int32_t order = 0;
		int32_t rc = as_udf_cmp_stored(this, &other, &order);
		if (rc != AS_UDF_OK) return Error{rc, "cmp_stored: not two stored values"};
		return order;
	}

	// The entry under a string key of a stored map; nothing when the map has no such key or the value is not a
	// map: a key that is not there is nothing, not an error. An entry that holds nil is found, as a nil Value.
	// The key is of any length. The host searches the map's entries in order, so the cost grows with the map; to
	// read many entries, range over map() once.
	Optional<Value> get(StrView key) const
	{
		Value out;
		if (!is_map()) return {};

		int32_t rc = as_udf_coll_get(src(), offset(), key.data, key.len, &out, (int32_t)sizeof(out));

		if (rc == AS_UDF_NOT_FOUND) return {};
		if (rc != AS_UDF_OK) detail::unreadable();
		return out;
	}

	// The entry under an integer key of a stored map; nothing when there is none or the value is not a map.
	Optional<Value> get(int64_t key) const
	{
		Value out;
		if (!is_map()) return {};

		int32_t rc = as_udf_coll_get_int(src(), offset(), key, &out, (int32_t)sizeof(out));

		if (rc == AS_UDF_NOT_FOUND) return {};
		if (rc != AS_UDF_OK) detail::unreadable();
		return out;
	}

	// Whether a stored map has an entry under a string key. False when the value is not a map.
	bool contains(StrView key) const { return get(key).has_value(); }
	// Whether a stored map has an entry under an integer key. False when the value is not a map.
	bool contains(int64_t key) const { return get(key).has_value(); }

	// The element at `index` of a stored list, which the host finds without the guest walking the list;
	// nothing past the end, for a negative index or when the value is not a list: an index that is not there is
	// nothing, not an error.
	Optional<Value> at(int64_t index) const
	{
		Value out;
		if (!is_list() || index < 0 || index >= (int64_t)len) return {};
		if (as_udf_list_get(src(), offset(), (int32_t)index, &out, (int32_t)sizeof(out)) != AS_UDF_OK) return {};
		return out;
	}

	// A walk over the elements of a stored list: `for (udf::Value v : value.list())`. Empty when the value is not
	// a list, an absent Value included; to tell a list from a value that is not one, test is_list() or read the
	// bin with Record::list(). Walks nest, a list inside a list, as long as the inner walk ends before the outer
	// one steps on.
	inline ListRange list() const;
	// A walk over the entries of a stored map, each an Entry of key and value: `for (auto [k, v] : value.map())`.
	// Empty when the value is not a map.
	inline MapRange map() const;

	// An integer value of the guest's. The `of` functions, blob(), geojson(), nil() and handle() make values to
	// hand to the host: an element for List::add or a batch (List::add_values, Map::put_entries), an entry for
	// Map::put, a bin for Record::set, a result for udf::result().
	static constexpr Value of(int64_t i) { Value v; v.as_udf_value::kind = AS_UDF_INT; v.payload = i; return v; }
	// An integer value from an int, widened to 64 bits.
	static constexpr Value of(int i) { return of((int64_t)i); }
	// A string value viewing a NUL-terminated string. The bytes are not copied: they must stay alive until
	// the value has been handed to the host.
	static Value of(const char* s) { return of(StrView(s)); }
	// A double value of the guest's, to hand to the host. See of(int64_t).
	static Value of(double d) { Value v; v.as_udf_value::kind = AS_UDF_DOUBLE; __builtin_memcpy(&v.payload, &d, 8); return v; }
	// A boolean value of the guest's, to hand to the host. See of(int64_t).
	static constexpr Value of(bool b) { Value v; v.as_udf_value::kind = AS_UDF_BOOL; v.payload = b ? 1 : 0; return v; }
	// A string value viewing the guest's bytes (kind GuestStr). The bytes are not copied: they must stay
	// alive until the value has been handed to the host.
	static Value of(StrView s) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_STR; v.len = (uint32_t)s.len; v.payload = (int64_t)(uintptr_t)s.data; return v; }
	// A blob value viewing `n` bytes at `b` (kind GuestBlob). Not copied, as for a string.
	static Value blob(const void* b, int32_t n) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_BLOB; v.len = (uint32_t)n; v.payload = (int64_t)(uintptr_t)b; return v; }
	// A GeoJSON value viewing the document's text (kind GuestGeoJson). Not copied, as for a string.
	static Value geojson(StrView doc) { Value v; v.as_udf_value::kind = AS_UDF_GUEST_GEOJSON; v.len = (uint32_t)doc.len; v.payload = (int64_t)(uintptr_t)doc.data; return v; }
	// A nil value, to hand to the host: a nil list element or map entry, or the nil result. Set as a bin it
	// removes the bin, and as a bin of an xdr-merge record answer it deletes that bin.
	static constexpr Value nil() { Value v; v.as_udf_value::kind = AS_UDF_NIL; return v; }
	// A value standing for a collection built on the host, by its handle: what Map::value() and
	// List::value() return.
	static constexpr Value handle(int32_t h) { Value v; v.as_udf_value::kind = AS_UDF_HANDLE; v.payload = h; return v; }

private:
	Error kind_error(const char* want) const
	{
		return Error{AS_UDF_TYPE_MISMATCH, detail::message().add("the value is not ").add(want).add(" (kind ").add((int64_t)this->as_udf_value::kind).add(")").buf};
	}

	// A value of `len` bytes that the caller's `cap` bytes do not hold: the status is the needed-length one,
	// `-(status) - 16` being the size to retry with.
	Error too_long(const char* what, int64_t cap) const
	{
		return Error{AS_UDF_NEEDED_BASE - (int32_t)len, detail::message().add(what).add(" is ").add((int64_t)len).add(" bytes, at most ").add(cap).add(" fit").buf};
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

	// A walk the host could not open or continue. A range-for has no way to answer an error, and ending
	// the loop early would hand the body a list shorter than the stored one as if it were the whole of it,
	// so the call is stopped instead: failed with a message that says why, then trapped.
	[[noreturn]] static void broken() { unreadable(); }

	// Enters a stored collection of the expected kind. Of another kind, it is an empty walk; of that kind
	// and not enterable, the call stops.
	void open(const Value& coll, bool want_map)
	{
		map = want_map;
		cursor = 0;

		if (want_map ? coll.is_map() : coll.is_list()) {
			cursor = as_udf_cursor_enter_at(coll.src(), coll.offset());

			if (cursor <= 0) broken();
		}

		prime();
	}

	// Fills the window and positions on its first element; closes when it is empty.
	void prime()
	{
		if (cursor <= 0) return;
		got = as_udf_cursor_values(cursor, window, (int32_t)sizeof(window));
		if (got < 0) broken();
		at = 0;
		last = got < 32;
		if (got < (map ? 2 : 1)) close();
	}
};

} // namespace detail

// The end of a ListRange or MapRange walk: an iterator compares unequal to it until the walk is done.
struct RangeEnd {};

// The elements of a stored list, for a range-for. Made by Value::list(), and handed out by Record::list() and
// EventRecord::list() when the bin is a list; empty when the value it was made from is not a list. The loop reads the list 32 elements a host call and holds
// a cursor on the host while it runs, given back when it ends, breaks or returns. Each element is a Value: a
// scalar in itself, a string or nested collection by position. If the host cannot open or continue the walk (the
// call's memory on the server has no room for the cursor, or the value ended by a failed save), the call is
// failed and stopped there: a loop is never handed a shorter list than the stored one.
//
// ```cpp
// int64_t total = 0;
// if (udf::Optional<udf::ListRange> scores = UDF_TRY(rec.list("scores"))) {
// 	for (udf::Value v : *scores) total += UDF_TRY(v.as_int()).value_or(0);
// }
// ```
class ListRange {
public:
	// An empty walk, made from nothing.
	ListRange() = default;
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll) { open(coll, false); }
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
	// The value the walk was made from: the stored list, to keep, hand back or store.
	Value value() const { return coll_; }
	// The number of elements the walk yields: 0 when it was made from something that is not a list.
	int32_t size() const { return coll_.is_list() ? coll_.size() : 0; }
	// Whether the walk yields nothing: the list has no elements, or the range was made from something that is not
	// a list.
	bool empty() const { return size() == 0; }
	// The element at `index`, counted from 0, in one host call and without walking to it; nothing past the end or
	// for a negative index. The host steps over the elements before it, so use the range-for to read them all.
	Optional<Value> at(int64_t index) const { return coll_.at(index); }
	// Opens a host cursor on the list and returns an iterator on its first element. Every call opens its
	// own cursor, released when the iterator is destroyed.
	Iterator begin() const { return Iterator(coll_); }
	// The end sentinel the iterator is compared with.
	RangeEnd end() const { return {}; }

private:
	Value coll_;
};

// The entries of a stored map, for a range-for. Made by Value::map(), and handed out by Record::map() and
// EventRecord::map() when the bin is a map; empty when the value it was made from is not a map. Each step yields an Entry, which a structured binding takes
// apart. The loop reads 16 entries a host call and holds one of the call's handles while it runs.
// A walk the host cannot open or continue stops the call, as for ListRange.
//
// ```cpp
// udf::Map out(8);
// udf::MapRange stock = UDF_TRY(rec.map("stock")).value_or(udf::MapRange());
// for (auto [key, value] : stock) {
// 	if (value.is_int() && value.payload > 0) out.put(key, value);
// }
// ```
class MapRange {
public:
	// An empty walk, made from nothing.
	MapRange() = default;
	class Iterator : detail::CursorIter {
	public:
		Iterator(const Value& coll) { open(coll, true); }
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
	// The value the walk was made from: the stored map, to keep, hand back or store.
	Value value() const { return coll_; }
	// The number of entries the walk yields: 0 when it was made from something that is not a map.
	int32_t size() const { return coll_.is_map() ? coll_.size() : 0; }
	// Whether the walk yields nothing: the map has no entries, or the range was made from something that is not a
	// map.
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

// Whether a stored list holds the string `text` among its elements; elements that are not strings never match.
// False when `list` is not a list. Walks the list, one string comparison an element, so it is for short lists
// such as an event record's bin names. `text` is of any length (see Value::is for the stack it takes).
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

// A map being built on the host, held by handle: the way to make a map bin, a map result, or the bins of an
// xdr-merge answer. `udf::Map m(capacity)` starts one; the capacity is a hint and the map grows past it. The puts
// chain, and a put the host refuses does not stop the chain: the first failure is kept and reported by error()
// and result(), and Record::set, a returned Map and Resolution::record all fail with it, so one check at the end
// is enough. A put under a key that is already there replaces its value. The map is ordered by key, whatever
// order the entries were put in. It is built once and not read back entry by entry: contains() and size() are
// what it answers.
//
// A Map owns its handle and gives it back to the host when it is destroyed. It moves and does not copy; a
// moved-from Map is null and gives nothing back. What it was put into, added to, set as a bin or returned as keeps
// the map, since the host holds its own reference to what it is handed, so a loop that builds a map per iteration
// and nests it holds one at a time: a call is bounded by its memory on the server, not by a count of handles.
//
// ```cpp
// udf::Map profile(4);
// profile.put("name", "Ada").put("visits", 3).put("score", 0.5).put("active", true);
// UDF_TRY(rec.set("profile", profile));
// UDF_TRY(rec.save());
// ```
class Map {
public:
	// `udf::Map()` is a null map, holding no handle: what a `Result<Map>` that failed carries, and every put on
	// it fails. `udf::Map(capacity)` starts a real one: the capacity is a hint of how many entries it will hold,
	// and the map grows past it.
	Map() : handle_(0) {}
	explicit Map(int32_t capacity) : handle_(as_udf_map_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a map");
	}
	Map(const Map&) = delete;
	Map& operator=(const Map&) = delete;
	Map(Map&& o) : handle_(o.handle_), err_(o.err_) { o.handle_ = 0; }
	Map& operator=(Map&& o)
	{
		if (this != &o) {
			close();
			handle_ = o.handle_;
			err_ = o.err_;
			o.handle_ = 0;
		}
		return *this;
	}
	~Map() { close(); }
	// wrap() adopts a handle the host already issued (one from the C header's as_udf_map_new), without building a
	// new map, and owns it from then on: it is given back when the Map is destroyed. The handle is not checked: a
	// put on one that is not a built map fails.
	static Map wrap(int32_t handle) { Map m(Wrapped{}, handle); return m; }

	// Gives the handle back to the host now rather than at destruction, leaving a null map. What the map was
	// handed to keeps it. Closing twice, or a null map, does nothing.
	void close()
	{
		if (handle_ > 0) as_udf_handle_close(handle_);
		handle_ = 0;
	}
	// Gives up the handle without closing it, leaving a null map: for a caller that hands it to the C API and
	// closes it there, or lets it go at the end of the call.
	int32_t release()
	{
		int32_t h = handle_;
		handle_ = 0;
		return h;
	}

	// The host's handle of the map: 0 for a null map, not positive when the map could not be built.
	int32_t handle() const { return handle_; }
	// The first failure of building the map or of a put on it; failed() is false when there was none.
	Error error() const { return err_; }
	// error() as a Result<void>, for UDF_TRY or a return.
	Result<void> result() const { return err_.failed() ? Result<void>(err_) : Result<void>(); }
	// The number of entries the map holds, asked of the host; negative when the handle is not a valid one.
	int32_t size() const { return as_udf_coll_size(handle_); }
	// The map as a Value of kind Handle, to nest it in a batch (List::add_values, Map::put_entries) or to
	// pass it to udf::result(). The Value names the map's handle, so it is good while the Map lives; a Value
	// that has to outlive the Map, such as one a function returns, is `udf::Value::handle(m.release())`.
	Value value() const { return Value::handle(handle_); }

	// Whether the map holds no entry, which is also what a map that could not be built answers.
	bool empty() const { return size() <= 0; }

	// Whether the map holds an entry under a string key. The host answers for the map it is building, so
	// nothing is packed and the check is fit for a loop.
	bool contains(StrView key) const { return as_udf_map_has(handle_, key.data, key.len) > 0; }
	// Whether the map holds an entry under an integer key.
	bool contains(int64_t key) const { return as_udf_map_has_int(handle_, key) > 0; }

	// Puts an integer under a string key, replacing the entry when the key is already there. Like every put,
	// returns the map for chaining and keeps the first failure for error(). The key and a string value are copied
	// by the host, so neither has to outlive the call.
	Map& put(StrView key, int64_t v) { return check(as_udf_map_put_string_int(handle_, key.data, key.len, v), key); }
	// Puts an int under a string key, widened to 64 bits.
	Map& put(StrView key, int v) { return put(key, (int64_t)v); }
	// Puts a double under a string key. Like every put, it takes no handle, so a loop puts as many entries as the
	// call's memory allows.
	Map& put(StrView key, double v) { return put(key, Value::of(v)); }
	// Puts a boolean under a string key.
	Map& put(StrView key, bool v) { return put(key, Value::of(v)); }
	// Puts a string under a string key.
	Map& put(StrView key, StrView v) { return check(as_udf_map_put_string_string(handle_, key.data, key.len, v.data, v.len), key); }
	// Puts a NUL-terminated string under a string key.
	Map& put(StrView key, const char* v) { return put(key, StrView(v)); }
	// Puts a value of any kind under a string key: a scalar, guest bytes, a handle, or a stored value taken
	// by position and keeping its kind. An absent value is refused (AS_UDF_NOT_FOUND). The key is of any length
	// and is put whole.
	Map& put(StrView key, const Value& v) { return check(as_udf_map_put_string_value(handle_, key.data, key.len, &v), key); }
	// Nests a built map under a string key. A failure already kept by `v` carries over to this map.
	Map& put(StrView key, const Map& v) { return check(as_udf_map_put_string_handle(handle_, key.data, key.len, v.handle_), key).carry(v.err_); }
	// Nests a built list under a string key. A failure already kept by `v` carries over to this map.
	inline Map& put(StrView key, const List& v);
	// Puts `n` bytes at `b` as a blob under a string key, copied by the host.
	Map& put_blob(StrView key, const void* b, int32_t n) { return put(key, Value::blob(b, n)); }
	// Puts a GeoJSON document under a string key, as the GeoJSON kind, not as a string; the text is copied by the
	// host and not validated here.
	Map& put_geojson(StrView key, StrView doc) { return put(key, Value::geojson(doc)); }
	// Puts a nil entry under a string key. In the bins of an xdr-merge record answer a nil entry means "this
	// record has no such bin": it deletes the bin.
	Map& put_nil(StrView key) { return put(key, Value::nil()); }

	// Puts an integer under an integer key, replacing the entry when the key is already there. A put under an
	// integer key takes no handle either, for the key or the value.
	Map& put(int64_t key, int64_t v) { return put(key, Value::of(v)); }
	// Puts an int under an integer key, widened to 64 bits.
	Map& put(int64_t key, int v) { return put(key, (int64_t)v); }
	// Puts a string under an integer key.
	Map& put(int64_t key, StrView v) { return put(key, Value::of(v)); }
	// Puts a NUL-terminated string under an integer key.
	Map& put(int64_t key, const char* v) { return put(key, StrView(v)); }
	// Puts a value of any kind under an integer key: a scalar, guest bytes, a handle, or a stored value
	// taken by position and keeping its kind. An absent value is refused.
	Map& put(int64_t key, const Value& v) { return check(as_udf_map_put_int_value(handle_, key, &v), "an integer key"); }
	// Nests a built map under an integer key. A failure already kept by `v` carries over to this map.
	Map& put(int64_t key, const Map& v) { return put(key, v.value()).carry(v.err_); }
	// Nests a built list under an integer key. A failure already kept by `v` carries over to this map.
	inline Map& put(int64_t key, const List& v);

	// Puts every entry of a stored map into this one, by position and with each key's and value's kind kept: the
	// way to start from a map bin and change a few entries. Does nothing when `stored` is not a map. Keys of any
	// kind and length are carried over whole; an entry the host does not take is kept as the map's failure. One
	// host call an entry, and no handle.
	Map& put_all(const Value& stored)
	{
		for (Entry e : stored.map()) {
			Value pair[2] = {e.key, e.value};
			int32_t n = as_udf_map_put_entries(handle_, pair, (int32_t)sizeof(pair));
			if (n != 1) {
				check(n < 0 ? n : AS_UDF_REFUSED, "an entry of a stored map");
				break;
			}
		}
		return *this;
	}

	// Puts a batch of entries in one host call and without taking handles. `pairs` is an array of Values, a key
	// then its value: pairs[0] and pairs[1] are the first entry. `count` is the number of Values, two for each
	// entry, so it is even. Keys are strings or integers made with Value::of; values are made with Value::of,
	// blob(), geojson(), nil() or a built collection's value(), or are values that were read. Guest bytes are
	// copied during the call. When the host does not take them all, the entries before the one it refused are in
	// the map and the failure is kept.
	//
	// ```cpp
	// udf::Value pairs[] = {udf::Value::of("x"), udf::Value::of(1.5), udf::Value::of("y"), udf::Value::of(2.5)};
	// udf::Map point(2);
	// point.put_entries(pairs, 4);
	// ```
	Map& put_entries(const Value* pairs, int32_t count)
	{
		int32_t put = as_udf_map_put_entries(handle_, pairs, count * (int32_t)sizeof(Value));
		if (put != count / 2) check(put < 0 ? put : AS_UDF_REFUSED, "an entry of the batch");
		return *this;
	}

private:
	struct Wrapped {};
	Map(Wrapped, int32_t handle) : handle_(handle) {}

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
	friend struct EventRecord;
	friend struct Pair;
};

// A list being built on the host, held by handle: the way to make a list bin or a list result. `udf::List
// l(capacity)` starts one; the capacity is a hint and the list grows past it. The adds chain, and an add the host
// refuses does not stop the chain: the first failure is kept and reported by error() and result(), and
// Record::set and a returned List fail with it. It is built once and not read back element by element. Like a
// Map, a List owns its handle, gives it back when destroyed, and moves rather than copies.
//
// ```cpp
// UDF_FUNCTION(evens, udf::Result<udf::List>, const udf::Record& rec)
// {
// 	udf::List out(8);
// 	for (udf::Value v : rec.list("numbers")) {
// 		if (v.is_int() && v.payload % 2 == 0) out.add(v);
// 	}
// 	return out;
// }
// ```
class List {
public:
	// `udf::List()` is a null list, holding no handle: what a `Result<List>` that failed carries, and every add
	// to it fails. `udf::List(capacity)` starts a real one: the capacity is a hint of how many elements it will
	// hold, and the list grows past it.
	List() : handle_(0) {}
	explicit List(int32_t capacity) : handle_(as_udf_list_new(capacity))
	{
		if (handle_ <= 0) err_ = udf::error(AS_UDF_ERR, "could not build a list");
	}
	List(const List&) = delete;
	List& operator=(const List&) = delete;
	List(List&& o) : handle_(o.handle_), err_(o.err_) { o.handle_ = 0; }
	List& operator=(List&& o)
	{
		if (this != &o) {
			close();
			handle_ = o.handle_;
			err_ = o.err_;
			o.handle_ = 0;
		}
		return *this;
	}
	~List() { close(); }
	// wrap() adopts a handle the host already issued (one from the C header's as_udf_list_new), without building
	// a new list, and owns it from then on. The handle is not checked: an add to one that is not a built list
	// fails.
	static List wrap(int32_t handle) { List l(Wrapped{}, handle); return l; }

	// Gives the handle back to the host now, leaving a null list; what the list was handed to keeps it. Closing
	// twice, or a null list, does nothing.
	void close()
	{
		if (handle_ > 0) as_udf_handle_close(handle_);
		handle_ = 0;
	}
	// Gives up the handle without closing it, leaving a null list.
	int32_t release()
	{
		int32_t h = handle_;
		handle_ = 0;
		return h;
	}

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
	// pass it to udf::result(). Good while the List lives, as Map::value() says.
	Value value() const { return Value::handle(handle_); }

	// Appends an integer. Like every add, returns the list for chaining and keeps the first failure for
	// error().
	List& add(int64_t v) { return check(as_udf_list_push_int(handle_, v)); }
	// Appends an int, widened to 64 bits.
	List& add(int v) { return add((int64_t)v); }
	// Appends a double. Like every add, it takes no handle, so a loop adds as many elements as the call's memory
	// allows.
	List& add(double v) { return check(as_udf_list_push_double(handle_, v)); }
	// Appends a boolean.
	List& add(bool v) { return check(as_udf_list_push_bool(handle_, v)); }
	// Appends a string: the bytes of the view, copied by the host. Takes no handle.
	List& add(StrView v) { return check(as_udf_list_push_string(handle_, v.data, v.len)); }
	// Appends a NUL-terminated string, copied by the host. What a string literal picks.
	List& add(const char* v) { return add(StrView(v)); }
	// Appends a value of any kind: a scalar, guest bytes, a handle, or a stored value taken by position and
	// keeping its kind. An absent value is refused.
	List& add(const Value& v) { return check(as_udf_list_push_value(handle_, &v)); }
	// Appends a built map as one element. A failure already kept by `v` carries over to this list.
	List& add(const Map& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	// Appends a built list as one element, nested. A failure already kept by `v` carries over to this list.
	List& add(const List& v) { return check(as_udf_list_push(handle_, v.handle_)).carry(v.err_); }
	// Appends `n` bytes at `b` as a blob, copied by the host.
	List& add_blob(const void* b, int32_t n) { return check(as_udf_list_push_blob(handle_, b, n)); }
	// Appends a GeoJSON document, as the GeoJSON kind, not as a string; the text is copied by the host and not
	// validated here. Takes a handle, as the double form of add does.
	List& add_geojson(StrView doc) { return check(as_udf_list_push_geojson(handle_, doc.data, doc.len)); }
	// Appends a nil element.
	List& add_nil() { return check(as_udf_list_push_nil(handle_)); }

	// Appends every element of a stored list, by position and with each element's kind kept: the way to start
	// from a list bin and add to it. Does nothing when `stored` is not a list. One host call an element.
	List& add_all(const Value& stored)
	{
		for (Value v : stored.list()) add(v);
		return *this;
	}

	// Appends `count` values from an array in one host call and without taking handles: the form for a long list.
	// The values are made with Value::of, blob(), geojson(), nil() or a built collection's value(), or are values
	// that were read. Guest bytes are copied during the call. When the host does not take them all, the values
	// before the one it refused are in the list and the failure is kept.
	//
	// ```cpp
	// udf::Value row[] = {udf::Value::of(42), udf::Value::of(0.5), udf::Value::of("ok")};
	// udf::List out(3);
	// out.add_values(row, 3);
	// ```
	List& add_values(const Value* vals, int32_t count)
	{
		int32_t added = as_udf_list_push_values(handle_, vals, count * (int32_t)sizeof(Value));
		if (added != count) check(added < 0 ? added : AS_UDF_REFUSED);
		return *this;
	}

private:
	struct Wrapped {};
	List(Wrapped, int32_t handle) : handle_(handle) {}

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
inline Map& Map::put(int64_t key, const List& v) { return put(key, v.value()).carry(v.err_); }

//==========================================================
// Records: what reading one offers, and the record a UDF is called on.
//

// Reading a record, whichever record it is. udf::Record, the record a UDF was called on, and udf::EventRecord, a
// record an event hands over, both derive from it, so a helper written as `template <class R> int64_t total(const
// udf::ReadRecord<R>& rec)` reads from either. It is a mixin over the one thing the two do differently,
// `read(name, out)`: there is no virtual call and no state, so an EventRecord stays the C struct it wraps.
//
// There are three ways to read a bin, by how much the function insists on. bin(name) is the bin that must be
// there: a Result, an error of status AS_UDF_NOT_FOUND when it is not. as_int(name) and the other typed readers
// (as_double, as_bool, as_string, as_blob, as_bytes, list, map) are the bin that may be missing, and they have
// three outcomes a caller tells apart without reading a message: an empty Optional when the bin is absent or nil,
// the value when it is of that kind, and an error of status AS_UDF_TYPE_MISMATCH when it is of another kind.
// int_or(name, dflt) and its kin take a default, which stands in for an absent bin only: a bin of another kind is
// the same error. Nothing is converted on the way: an integer bin is not a double. A bin name is 1 to 15 bytes;
// any other name reads as absent.
//
// ```cpp
// int64_t balance = UDF_TRY(UDF_TRY(rec.bin("balance")).as_int()).value_or(0);  // must be there
// udf::Optional<udf::Text<32>> owner = UDF_TRY(rec.as_string<32>("owner"));     // may be missing
// int64_t limit = UDF_TRY(rec.int_or("limit", 1000));                           // has a default
// udf::Optional<udf::ListRange> history = UDF_TRY(rec.list("history"));         // a list bin, as a range
// bool busy = history && history->size() > 100;
// ```
template <class Self>
class ReadRecord {
public:
	// The bin as a value of any kind; nothing when the record has no such bin or it holds nil. A read that fails
	// for another reason (no handle free) would look like an absent bin here, so it stops the call instead, as
	// C's as_udf_rec_get does; bin() and the typed readers report it as an error. Reading the same string, blob,
	// list or map bin again reuses the handle the first read took.
	Optional<Value> get(StrView name) const
	{
		Value v;
		Status rc = self().read(name, v);
		if (rc != AS_UDF_OK && rc != AS_UDF_NOT_FOUND) detail::unreadable();
		if (rc != AS_UDF_OK || v.is_nil()) return {};
		return v;
	}

	// Whether the record has the bin. On the record of a UDF, a bin the call has set counts and one it has
	// removed does not; on an event record, a bin that side deleted, or that a partial incoming write does not
	// carry, does not. A read that fails for another reason stops the call, as get() does.
	bool has(StrView name) const
	{
		Value v;
		Status rc = self().read(name, v);
		if (rc != AS_UDF_OK && rc != AS_UDF_NOT_FOUND) detail::unreadable();
		return rc == AS_UDF_OK;
	}

	// The bin that must be there, as a value: an error of status AS_UDF_NOT_FOUND naming the bin when the record
	// has none, and of the host's status when it could not be read. Chain the value's own typed reader for a bin
	// that must also be of a kind: `UDF_TRY(UDF_TRY(rec.bin("n")).as_int())`.
	Result<Value> bin(StrView name) const
	{
		Value v;
		Status rc = self().read(name, v);
		if (rc == AS_UDF_NOT_FOUND) return Error{AS_UDF_NOT_FOUND, detail::message().add("bin ").add(name.data, name.len).add(" is absent").buf};
		if (rc != AS_UDF_OK) return Error{rc, detail::message().add("could not read bin ").add(name.data, name.len).add(" (").add((int64_t)rc).add(")").buf};
		return v;
	}

	// The bin as an integer: nothing when the bin is absent or nil, an error of status AS_UDF_TYPE_MISMATCH when
	// it is of another kind (a double is not truncated).
	Result<Optional<int64_t>> as_int(StrView name) const { return UDF_TRY(fetch(name)).as_int(); }
	// The bin as a double: nothing when absent or nil, a type-mismatch error for another kind. An integer bin is
	// another kind: it is not widened.
	Result<Optional<double>> as_double(StrView name) const { return UDF_TRY(fetch(name)).as_double(); }
	// The bin as a boolean: nothing when absent or nil, a type-mismatch error for another kind. The Optional
	// says whether the bin was there and `*o` what it holds, so test the two apart.
	Result<Optional<bool>> as_bool(StrView name) const { return UDF_TRY(fetch(name)).as_bool(); }
	// The bin's string as an inline `Text<N>`: `rec.as_string<32>("owner")`. Nothing when absent or nil; a
	// type-mismatch error when the bin is not a string (a GeoJSON bin is not one); an error with a needed-length
	// status when it is longer than N bytes.
	template <size_t N> Result<Optional<Text<N>>> as_string(StrView name) const { return UDF_TRY(fetch(name)).template as_string<N>(); }
	// The bytes of a blob bin as an inline `Bytes<N>`. Strict: only a blob. Nothing when absent or nil; a
	// type-mismatch error for any other kind, a string, GeoJSON or HLL bin included, whose raw bytes as_bytes()
	// reads; an error with a needed-length status when the blob is longer than N bytes.
	template <size_t N> Result<Optional<Bytes<N>>> as_blob(StrView name) const { return UDF_TRY(fetch(name)).template as_blob<N>(); }
	// The raw bytes of a string, blob, GeoJSON or HLL bin as an inline `Bytes<N>`, whatever the bin is labelled.
	// Nothing when absent or nil; a type-mismatch error for any other kind; an error with a needed-length status
	// when the bytes are longer than N.
	template <size_t N> Result<Optional<Bytes<N>>> as_bytes(StrView name) const { return UDF_TRY(fetch(name)).template as_bytes<N>(); }

	// The bin's integer, or `dflt` when the bin is absent or nil: the form for a counter that starts at zero on a
	// record that does not have it yet. The default stands in for nothing else: a bin of another kind is an
	// error of status AS_UDF_TYPE_MISMATCH.
	Result<int64_t> int_or(StrView name, int64_t dflt) const { return UDF_TRY(as_int(name)).value_or(dflt); }
	// The bin's double, or `dflt` when the bin is absent or nil; a type-mismatch error for another kind, an
	// integer bin included.
	Result<double> double_or(StrView name, double dflt) const { return UDF_TRY(as_double(name)).value_or(dflt); }
	// The bin's boolean, or `dflt` when the bin is absent or nil; a type-mismatch error for another kind.
	Result<bool> bool_or(StrView name, bool dflt) const { return UDF_TRY(as_bool(name)).value_or(dflt); }

	// Copies a blob bin of 32-bit floats into an array with room for `count` of them, element for element.
	// Returns how many were read; an error when the bin is absent (AS_UDF_NOT_FOUND), is not a blob
	// (AS_UDF_TYPE_MISMATCH) or holds more than `count` floats (a needed-length status).
	Result<int32_t> floats_into(StrView name, float* out, int32_t count) const
	{
		Result<Value> v = bin(name);
		if (!v) return v.error();
		return v->floats_into(out, count);
	}

	// A list bin, as a range to walk and the value it was made from (`->value()`). Nothing when the bin is
	// absent or nil; an error of status AS_UDF_TYPE_MISMATCH when it is of another kind; otherwise the range,
	// whose size() is 0 for an empty list. So absent, wrong and empty are three answers:
	// `if (auto l = UDF_TRY(rec.list("tags"))) for (udf::Value v : *l) ...`.
	Result<Optional<ListRange>> list(StrView name) const
	{
		Value v = UDF_TRY(fetch(name));
		if (v.is_null()) return Optional<ListRange>();
		if (!v.is_list()) return kind_error(name, "a list");
		return Optional<ListRange>(ListRange(v));
	}
	// A map bin, as a range over its entries and the value it was made from (`->value()`). Nothing when the bin
	// is absent or nil; an error of status AS_UDF_TYPE_MISMATCH when it is of another kind; otherwise the range,
	// whose size() is 0 for an empty map.
	Result<Optional<MapRange>> map(StrView name) const
	{
		Value v = UDF_TRY(fetch(name));
		if (v.is_null()) return Optional<MapRange>();
		if (!v.is_map()) return kind_error(name, "a map");
		return Optional<MapRange>(MapRange(v));
	}

private:
	const Self& self() const { return static_cast<const Self&>(*this); }

	// The bin for a typed reader: an absent Value when the record has none, an error when it could not be read.
	Result<Value> fetch(StrView name) const
	{
		Value v;
		Status rc = self().read(name, v);
		if (rc == AS_UDF_NOT_FOUND) return Value();
		if (rc != AS_UDF_OK) return Error{rc, detail::message().add("could not read bin ").add(name.data, name.len).add(" (").add((int64_t)rc).add(")").buf};
		return v;
	}

	static Error kind_error(StrView name, const char* want)
	{
		return Error{AS_UDF_TYPE_MISMATCH, detail::message().add("bin ").add(name.data, name.len).add(" is not ").add(want).buf};
	}
};

namespace detail {
struct RecordAccess;
}

// The record a UDF was called on. A record UDF and the per-record half of a stream UDF take it as their first
// parameter, before the call's arguments. It is not one of them: a client does not pass it, the arity check does
// not count it and the function's signature export does not list it. A function that does not touch the record
// leaves the parameter out.
//
// ```cpp
// UDF_FUNCTION(withdraw, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
// {
// 	int64_t balance = UDF_TRY(UDF_TRY(rec.bin("balance")).as_int());
//
// 	UDF_TRY(rec.set("balance", balance - amount));
// 	UDF_TRY(rec.save());
//
// 	return balance - amount;
// }
// ```
//
// The writes are staged: set() and remove() change nothing on the server until save(), and a function that
// returns without saving has changed nothing. A call that fails writes nothing either, even what it had already
// saved: a save() that succeeded, and a set_ints() or set_doubles(), are undone when the function then returns an
// Error. The record may not exist yet, which is how a UDF creates one: exists() says, and save() creates it.
//
// On a key with no record exists() is false, generation(), last_update_time(), size() and bin_count() are 0,
// there is no TTL, every bin reads as absent and reading creates nothing. set_ints(), set_doubles() and
// delete_record() are refused there (AS_UDF_REFUSED) and create nothing. A save() with a bin staged creates the
// record; a save() with no bin staged (only a remove() or a set_ttl()) creates nothing and the call fails with
// "key not found".
//
// `const udf::Record&` is the read-only record: the reading methods are const and the writing ones are not, so a
// function that takes it that way cannot change the record, and the per-record half of a stream UDF, whose record
// takes no write, can take it no other way. Only the SDK makes one, and it cannot be copied, so a read-only
// record stays read-only. The reading methods are ReadRecord's.
class Record : public ReadRecord<Record> {
public:
	Record(const Record&) = delete;
	Record& operator=(const Record&) = delete;

	// What ReadRecord reads through: the bin as the host hands it out. False when the record has no such bin, in
	// which case `out` is absent. A function reads with get(), bin() and the typed readers, which are built on
	// it.
	Status read(StrView name, Value& out) const
	{
		return as_udf_rec_bin_value(name.data, name.len, &out, (int32_t)sizeof(out));
	}

	// Stages an integer into a bin. Like every set, it changes nothing on the server until save(), and a read of
	// the bin later in the call sees the staged value. The name is 1 to 15 bytes. An error when the name is not a
	// bin name, or when the record takes no write, as in a stream UDF; its message names the bin and the status.
	//
	// ```cpp
	// UDF_TRY(rec.set("visits", UDF_TRY(rec.int_or("visits", 0)) + 1));
	// UDF_TRY(rec.set("site", "home"));
	// UDF_TRY(rec.remove("draft"));
	// UDF_TRY(rec.save());
	// ```
	Result<void> set(StrView name, int64_t v) { return check(name, as_udf_rec_set_int(name.data, name.len, v)); }
	// Stages an int, widened to 64 bits.
	Result<void> set(StrView name, int v) { return set(name, (int64_t)v); }
	// Stages a double into a bin. See set(name, int64_t).
	Result<void> set(StrView name, double v) { return check(name, as_udf_rec_set_double(name.data, name.len, v)); }
	// Stages a boolean into a bin, stored as a boolean, not as 0 or 1. See set(name, int64_t).
	Result<void> set(StrView name, bool v) { return check(name, as_udf_rec_set_bool(name.data, name.len, v ? 1 : 0)); }
	// Stages a string into a bin: the bytes of the view, copied by the host, so the view need not outlive the
	// call. An empty string is a value, not a removal.
	Result<void> set(StrView name, StrView v) { return check(name, as_udf_rec_set_string(name.data, name.len, v.data, v.len)); }
	// Stages a NUL-terminated string into a bin. What a string literal picks.
	Result<void> set(StrView name, const char* v) { return set(name, StrView(v)); }
	// Stages a built map as the bin's value. Fails with the map's own error when one of its puts failed, so the
	// puts need no check of their own. The bin and the Map share the one value.
	Result<void> set(StrView name, const Map& m) { if (m.error().failed()) return m.error(); return check(name, as_udf_rec_set_handle(name.data, name.len, m.handle())); }
	// Stages a built list as the bin's value. Fails with the list's own error when one of its adds failed, so the
	// adds need no check of their own.
	Result<void> set(StrView name, const List& l) { if (l.error().failed()) return l.error(); return check(name, as_udf_rec_set_handle(name.data, name.len, l.handle())); }
	// Stages `n` bytes at `b` as a blob, copied by the host. An error, of status AS_UDF_TYPE_MISMATCH, when the
	// bin holds bytes of another subtype, such as an HLL, which a plain blob would silently relabel: write a
	// value read from such a bin back with set(name, value), which keeps its kind.
	Result<void> set_blob(StrView name, const void* b, int32_t n) { return check(name, as_udf_rec_set_blob(name.data, name.len, b, n)); }
	// Stages a GeoJSON document, kept as the geospatially indexable kind rather than a string. The text is copied
	// by the host and not validated here: it is save() that fails for text the server would not accept as
	// GeoJSON.
	Result<void> set_geojson(StrView name, StrView doc) { return check(name, as_udf_rec_set_geojson(name.data, name.len, doc.data, doc.len)); }
	// A list of integers from an array of `count` elements, in one import, no value built per element.
	// Written at once, not staged: it needs no save(). It is undone when the call then fails, and it is refused
	// (AS_UDF_REFUSED) on a key with no record, which it does not create.
	Result<void> set_ints(StrView name, const int64_t* vals, int32_t count) { return check(name, as_udf_rec_set_ints(name.data, name.len, vals, count * 8)); }
	// A list of doubles from an array of `count` elements, in one import. Written at once, like set_ints: undone
	// when the call then fails, and refused on a key with no record.
	Result<void> set_doubles(StrView name, const double* vals, int32_t count) { return check(name, as_udf_rec_set_doubles(name.data, name.len, vals, count * 8)); }

	// A value: one read from a record by position, keeping its kind; or a scalar, guest bytes, a handle. A
	// nil removes the bin, as remove() does: a bin cannot hold nil.
	Result<void> set(StrView name, const Value& v)
	{
		if (v.is_absent()) return Error{AS_UDF_NOT_FOUND, "could not write an absent value"};
		return check(name, as_udf_rec_set_value(name.data, name.len, &v));
	}

	// Removes a bin: staged like a set, and applied by save(). Removing a bin that is not there is not an error.
	// Deleting the whole record is delete_record().
	Result<void> remove(StrView name)
	{
		int32_t rc = as_udf_rec_remove(name.data, name.len);
		if (rc == AS_UDF_OK) return {};
		return Error{rc, detail::message().add("could not remove bin ").add(name.data, name.len).add(" (").add((int64_t)rc).add(")").buf};
	}

	// Whether the record exists on the server. A record UDF may be called on a key that has no record, which is
	// how it creates one; a function that must not create, or must not overwrite, checks this first. The record
	// is locked for the whole call, so nothing comes between the check and the save. The answer does not change
	// after delete_record(), which is carried out when the call returns. False on a key with no record, where
	// reading creates nothing.
	bool exists() const { return as_udf_rec_exists() != 0; }
	// The record's generation: the count of writes it has taken, which the server raises on each one. 0 on a key
	// with no record.
	int64_t generation() const { return as_udf_rec_generation(); }
	// The record's remaining time to live in seconds; 0 when it does not expire. A key with no record has no TTL.
	int64_t ttl() const { return as_udf_rec_ttl(); }
	// When the record was last written, as the server stores it: milliseconds since the Citrusleaf epoch,
	// 2010-01-01T00:00:00Z, which is Unix milliseconds minus 1262304000000. Not on the same scale as now(), which
	// counts from the Unix epoch. 0 on a key with no record.
	int64_t last_update_time() const { return as_udf_rec_last_update_time(); }
	// The record's stored size in bytes; 0 on a key with no record.
	int64_t size() const { return as_udf_rec_size(); }
	// The number of bins the record has, as it is stored: a bin the call has staged is not counted until save()
	// has written it. 0 on a key with no record.
	int64_t bin_count() const { return as_udf_rec_bin_count(); }

	// The record's set name, into a Text; empty when it is in no set. A set name is at most 63 bytes, so it always
	// fits.
	Text<64> set_name() const
	{
		Text<64> t;
		int32_t n = as_udf_rec_set_name(t.buf, 64);
		t.len = n < 0 ? 0 : n;
		t.buf[t.len] = '\0';
		return t;
	}

	// Sets the time to live the next save carries, in seconds: 0 for the namespace default, -1 (all 32 bits set)
	// for a record that never expires. It takes effect with the write, so follow it with save(). On a key with no
	// record it creates nothing by itself: a save() with a TTL set and no bin staged fails the call with "key not
	// found". An error where there is no record that can take one: a stream UDF's record.
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
	// and viewed there, so `out` has to outlive the value. Nothing when the record stores no key; an error with a
	// needed-length status when the key does not fit in `cap` bytes, which is not the same answer.
	Result<Optional<Value>> key(void* out, int32_t cap) const
	{
		Value v;
		int32_t rc = as_udf_rec_key(out, cap, &v);
		if (rc == AS_UDF_NOT_FOUND) return Optional<Value>();
		if (rc != AS_UDF_OK) return detail::status_error(rc, "the user key does not fit");
		return Optional<Value>(v);
	}
	// The record's digest, 20 bytes, into `out`. Returns the length, or a negative status: at or below
	// AS_UDF_NEEDED_BASE when `cap` is too small.
	int32_t digest(void* out, int32_t cap) const { return as_udf_rec_digest(out, cap); }

	// Writes the record: the staged bins and removals go to it, and it is created when it does not exist. Nothing
	// staged reaches the server without it. With nothing staged it is still a write to a record that exists (the
	// server treats it as a touch: the generation and the last-update time move). On a key with no record, a save
	// with a bin staged creates the record; a save with no bin staged (nothing, or only a remove() or a set_ttl())
	// creates nothing and the call fails with "key not found". What a save wrote is not final until the call
	// succeeds: a call that fails after a save that succeeded writes nothing.
	//
	// An error when the server does not take the write, as in a stream UDF, or rejects it, as for a GeoJSON bin
	// whose text is not GeoJSON. After a save that failed, a list, map or blob value read from a bin earlier in
	// the call can no longer be read: read the bin again.
	Result<void> save()
	{
		int32_t rc = as_udf_rec_save();
		if (rc != AS_UDF_OK) return detail::status_error(rc, "could not save the record");
		return {};
	}

	// Deletes the record; nothing has to be saved first, and what was staged goes with it. The server carries the
	// delete out when the call returns, so exists() keeps answering what it did until then. An error when the
	// server does not delete it: in a stream UDF, and on a key with no record (AS_UDF_REFUSED), where nothing is
	// created either. Named so because `delete` is a keyword, and so that it cannot be read as remove(name), which
	// removes one bin.
	//
	// ```cpp
	// UDF_FUNCTION(close_account, udf::Result<void>, udf::Record& rec)
	// {
	// 	if (rec.int_or("balance", 0) != 0) return udf::error(AS_UDF_ERR, "close_account: balance is not zero");
	// 	return rec.delete_record();
	// }
	// ```
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

// The server's wall clock in milliseconds since the Unix epoch, or -1 inside an event handler, where the clock is
// refused: a merge that reads it cannot converge. Not on the scale of Record::last_update_time(), which counts
// from 2010.
inline int64_t now() { return as_udf_now(); }

// Fails the call with a message, which the client receives as the UDF's error (the first 511 bytes of it), and
// returns the non-zero status an export returns. For the raw form, where the function is the export: `return
// udf::fail("sum: no list bin");`. A function declared with UDF_FUNCTION fails by returning an Error instead, and
// the generated export calls this. A call that fails writes nothing, even what it had already saved: a
// Record::save() that succeeded, and a set_ints() or set_doubles(), are undone.
inline int32_t fail(StrView message)
{
	as_udf_fail(message.data, message.len);
	return 1;
}

// Fails the call with the error's message, or "failed" when it has none, and returns the status an
// export returns.
inline int32_t fail(Error e) { return fail(StrView(e.message != nullptr ? e.message : "failed")); }

// Writes a line to the server's log at AS_UDF_LOG_WARN, AS_UDF_LOG_INFO or AS_UDF_LOG_DEBUG; any other level logs
// as debug. The server keeps the first 511 bytes and escapes control characters. A call may write 16 lines and a
// module 100 a second; what is over is dropped. A line from an event handler is dropped.
inline void log(int32_t level, StrView message) { as_udf_log(level, message.data, message.len); }

// Sets a value of any kind as the call's result: a scalar, guest bytes, a built collection by its value(), or a
// value that was read, handed back by position and with its kind kept. An absent value sets nil. Of several
// results the last one wins. For the raw form; a function declared with UDF_FUNCTION returns its result instead.
// An error when the host does not take a collection or a stored value.
//
// ```cpp
// UDF_FUNCTION_RAW(first_tag, "()")
// {
// 	udf::Value tags;
// 	if (as_udf_rec_bin("tags", &tags) != AS_UDF_OK) return udf::fail("first_tag: no bin tags");
// 	udf::Result<void> r = udf::result(tags.at(0).value_or(udf::Value::nil()));
// 	return r ? 0 : udf::fail(r.error());
// }
// ```
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

// One record the event hands over, read-only, with its metadata; the C struct underneath. Its bins are read with
// ReadRecord's methods, the ones udf::Record has: get(), has(), bin(), the typed readers with and without a
// default, list() and map(). It has none of the writing methods: what a handler returns is what changes the
// record. An xdr-merge hands over two, `current` and `incoming`, and what the server says of the merge is asked
// of the record it is about: is_delete() and is_partial() of the incoming one, is_tombstone() of the current
// one. Everything is asked through a method. The C struct underneath is
// a private base: its fields are not reachable, so a handler cannot read a raw field where a method says what it
// means (bin_last_update_time(name) rather than the map of times), nor change one the SDK relies on.
//
// ```cpp
// UDF_ON_XDR_MERGE(keep_larger)
// {
// 	if (incoming.is_delete()) return udf::Resolution::policy();
// 	int64_t mine = current.int_or("visits", 0), theirs = incoming.int_or("visits", 0);
// 	return theirs > mine ? udf::Resolution::take_remote() : udf::Resolution::keep_local();
// }
// ```
namespace detail {
struct EventAccess;
}

struct EventRecord : private as_udf_event_record, public ReadRecord<EventRecord> {
	// What ReadRecord reads through: the bin of this record as the host hands it out, and the host's status:
	// AS_UDF_NOT_FOUND when the record has no such bin, in which case `out` is left as it was. A handler reads
	// with get(), bin() and the typed readers, which are built on it.
	Status read(StrView name, Value& out) const
	{
		return as_udf_event_bin_value(which, name.data, name.len, &out, (int32_t)sizeof(out));
	}

	// The record's generation. Zero on both sides of an xdr-merge: each cluster sets its own, and a merge
	// must not read it.
	int64_t generation() const { return as_udf_event_record::generation; }
	// When the record was last written, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z. Zero on
	// both sides of an xdr-merge, for the same reason as generation(); bin_last_update_time() is what orders the
	// two sides.
	int64_t last_update_time() const { return as_udf_event_record::last_update_time; }
	// The XDR source id of the cluster that wrote this version of the record.
	int64_t source_id() const { return as_udf_event_record::source_id; }

	// Whether this is the incoming record, the one that arrived over XDR; false for the current one.
	bool is_incoming() const { return which != 0; }

	// The record's bin names.
	ListRange bin_names() const { return ListRange(Value(as_udf_event_record::bin_names)); }
	// The names of the bins this side deleted. A deleted bin reads as absent, so this is the one way to
	// tell a bin that was removed from one that was never there. Empty when it deleted none: the server then
	// sends no list at all rather than an empty one, and the range made from nothing is empty, so size() is the
	// count either way.
	ListRange deleted_bins() const { return ListRange(Value(as_udf_event_record::deleted_bins)); }
	// Whether this side deleted any bin: deleted_bins() is not empty.
	bool deleted_any() const { return !deleted_bins().empty(); }

	// When this side last wrote the bin, as the server's timestamp for it, in milliseconds since the Citrusleaf
	// epoch, 2010-01-01T00:00:00Z (Unix milliseconds minus 1262304000000). The server gives a time for every bin of
	// both sides, a bin the side deleted included: for a write that came from another cluster, the time that
	// cluster shipped, or, where it shipped none, the time the write arrived here. A bin the record neither has
	// nor deleted gets the record's time, which is 0 in an xdr-merge. A number to order the two sides by, not to
	// compare with a clock. The name is cut to 63 bytes.
	int64_t bin_last_update_time(StrView name) const { return per_bin(bin_last_update_times, name, as_udf_event_record::last_update_time); }
	// The source id of this side's last write of the bin. The server gives one for every bin of both sides; a bin
	// with none of its own gets the record's. The name is cut to 63 bytes.
	int64_t bin_source_id(StrView name) const { return per_bin(bin_source_ids, name, as_udf_event_record::source_id); }

	// Whether the incoming write is a record delete, in which case this record has no bins. False on the
	// current record.
	bool is_delete() const { return as_udf_event_record::is_delete; }
	// Whether the incoming write carries only the bins that changed, so that a bin this record lacks may
	// well exist on the other cluster. False on the current record.
	bool is_partial() const { return as_udf_event_record::is_partial; }
	// Whether the current record is a tombstone: it exists but has no bins. False on the incoming record.
	bool is_tombstone() const { return as_udf_event_record::is_tombstone; }

	// `overrides` with every bin of this record it lacks put on top, by position: the Rust SDK's
	// with_bins. It prefers one side, so an xdr-merge handler uses Pair::with_newer instead.
	Map& with_bins(Map& overrides) const
	{
		for (Value name : bin_names()) {
			Result<Optional<Text<64>>> n = name.as_string<64>();
			if (!n || !*n) {
				overrides.carry(n ? Error{AS_UDF_TYPE_MISMATCH, "with_bins: a bin name is not a string"} : n.error());
				break;
			}
			StrView bin = (*n)->view();
			if (overrides.contains(bin)) continue;
			Value v;
			Status rc = read(bin, v);
			if (rc == AS_UDF_NOT_FOUND) continue;
			if (rc != AS_UDF_OK) {
				overrides.carry(detail::status_error(rc, "with_bins: could not read a bin"));
				break;
			}
			overrides.put(bin, v);
		}
		return overrides;
	}

private:
	friend struct detail::EventAccess;

	// The integer under `name` in one of the per-bin maps, or `dflt` when the map has none (or is absent).
	static int64_t per_bin(const as_udf_value& map, StrView name, int64_t dflt)
	{
		Optional<Value> v = Value(map).get(name);
		return v && v->is_int() ? v->payload : dflt;
	}
};

namespace detail {

// What the generated xdr-merge export decodes the event through: the C decoder fills the struct a handler
// cannot reach.
struct EventAccess {
	static int32_t decode(int32_t args, int32_t argc, EventRecord& current, EventRecord& incoming)
	{
		return as_udf_event_decode(args, argc, &current, &incoming);
	}
};

} // namespace detail

static_assert(sizeof(EventRecord) == sizeof(as_udf_event_record), "EventRecord adds no state");

// The two sides of a bin in the order both clusters agree on, as Both::ordered() gives them: the side that
// wrote the bin earlier, then the side that wrote it later. `auto [older, newer] = both.ordered();`
template <class T>
struct Ordered {
	Optional<T> older;
	Optional<T> newer;
};

struct Pair;

// A bin on both sides of a merge, either of which may be missing: the Rust SDK's `Both<T>`. Made by a Pair, which
// also gives it what orders the two sides. `current` and `incoming` are the two values. The merges come in two
// kinds. max(), min(), sum() and merge(f) with a commutative `f` depend on the values alone. newer(), older() and
// ordered() go by which side wrote the bin later, an order both clusters compute the same. Either kind gives the
// same answer on both clusters; current_or_incoming() and incoming_or_current() do not, and are for a handler
// that has already established which side to prefer.
//
// ```cpp
// udf::Both<int64_t> visits = pair.as_int("visits");
// if (udf::Optional<int64_t> most = visits.max()) out.put("visits", *most);
// if (udf::Optional<udf::Value> profile = pair.bin("profile").newer()) out.put("profile", *profile);
// ```
template <class T>
struct Both {
	Optional<T> current;
	Optional<T> incoming;

	// Whether the incoming side wrote this bin later: by the bin's last-update time, then source id.
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
	// The sum of both sides, or the one value there is (a missing side counts as zero); nothing when neither side
	// has the bin, as for max() and min(), so a handler leaves out a bin neither side has rather than writing a 0
	// nobody wrote. An integer sum wraps on overflow (two's complement), as it would in Rust's release builds,
	// rather than being undefined.
	Optional<T> sum() const { return merge([](const T& c, const T& i) { return detail::wrapping_add(c, i); }); }

private:
	friend struct Pair;

	int64_t current_last_update_time_ = 0;
	int64_t incoming_last_update_time_ = 0;
	int64_t current_source_id_ = 0;
	int64_t incoming_source_id_ = 0;
};

// The two records of an xdr-merge side by side, read bin by bin into a Both. Made from the two records the
// handler is given. It refers to them, so it lives no longer than they do. Each reader returns the bin on both
// sides with what orders them, and with_newer() fills in every bin the handler had no opinion on.
//
// ```cpp
// UDF_ON_XDR_MERGE(merge_account)
// {
// 	udf::Pair pair = udf::Pair::of(current, incoming);
// 	udf::Map out(4);
//
// 	if (udf::Optional<int64_t> visits = pair.as_int("visits").max()) out.put("visits", *visits);
//
// 	return udf::Resolution::record(pair.with_newer(out));
// }
// ```
struct Pair {
	const EventRecord& current;
	const EventRecord& incoming;

	// The pair of the two records of a merge. Each record knows which one it is, so the two may be given in
	// either order: of(current, incoming) and of(incoming, current) are the same pair.
	static Pair of(const EventRecord& a, const EventRecord& b) { return !a.is_incoming() || b.is_incoming() ? Pair{a, b} : Pair{b, a}; }

	// An integer bin on both sides, with what orders the two sides for it. A side is empty where the bin is
	// absent or nil. A side that holds another kind is an error of status AS_UDF_TYPE_MISMATCH, as it is for the
	// record's own reader: a merge is not left to treat a mistyped bin as a missing one.
	Result<Both<int64_t>> as_int(StrView name) const { return both(name, UDF_TRY(current.as_int(name)), UDF_TRY(incoming.as_int(name))); }
	// A double bin on both sides, likewise. An integer bin is another kind, not widened.
	Result<Both<double>> as_double(StrView name) const { return both(name, UDF_TRY(current.as_double(name)), UDF_TRY(incoming.as_double(name))); }
	// A boolean bin on both sides, likewise.
	Result<Both<bool>> as_bool(StrView name) const { return both(name, UDF_TRY(current.as_bool(name)), UDF_TRY(incoming.as_bool(name))); }
	// A string bin on both sides, each as an inline Text<N>, likewise; a string longer than N bytes is an error
	// with a needed-length status.
	template <size_t N> Result<Both<Text<N>>> as_string(StrView name) const { return both(name, UDF_TRY(current.template as_string<N>(name)), UDF_TRY(incoming.template as_string<N>(name))); }
	// A blob bin on both sides, each as an inline Bytes<N>, likewise. Strict, as the record's as_blob is.
	template <size_t N> Result<Both<Bytes<N>>> as_blob(StrView name) const { return both(name, UDF_TRY(current.template as_blob<N>(name)), UDF_TRY(incoming.template as_blob<N>(name))); }
	// A bin of any kind on both sides, as values. A side is empty where the bin is missing or nil.
	Both<Value> bin(StrView name) const { return both(name, current.get(name), incoming.get(name)); }
	// A list bin on both sides, as values to walk. A side is empty where the bin is absent or nil; a side of
	// another kind is a type-mismatch error.
	Result<Both<Value>> list(StrView name) const
	{
		Optional<ListRange> c = UDF_TRY(current.list(name)), i = UDF_TRY(incoming.list(name));
		return both(name, c ? Optional<Value>(c->value()) : Optional<Value>(), i ? Optional<Value>(i->value()) : Optional<Value>());
	}
	// A map bin on both sides, as values to walk or look up in. A side is empty where the bin is absent or nil;
	// a side of another kind is a type-mismatch error.
	Result<Both<Value>> map(StrView name) const
	{
		Optional<MapRange> c = UDF_TRY(current.map(name)), i = UDF_TRY(incoming.map(name));
		return both(name, c ? Optional<Value>(c->value()) : Optional<Value>(), i ? Optional<Value>(i->value()) : Optional<Value>());
	}

	// Whether the incoming side wrote the bin later: by the bin's last-update time, then by source id.
	bool incoming_is_newer(StrView name) const
	{
		int64_t ct = current.bin_last_update_time(name), it = incoming.bin_last_update_time(name);
		return it > ct || (it == ct && incoming.bin_source_id(name) > current.bin_source_id(name));
	}

	// Puts every bin either side has that `overrides` lacks into it, each from the side that wrote it later, and
	// returns `overrides`: the rest of the record as last-writer-wins per bin would leave it, which is symmetric.
	// The bins the handler decided are put first and are not touched. A bin the newer side deleted (it is in that
	// side's deleted_bins()) stays deleted: it is put as nil, which leaves it out of the record, and the older
	// side's copy is not brought back. A bin the newer side simply does not carry (a partial write) is taken from
	// the other side. A name that cannot be read, or a bin that cannot, is kept as the map's failure.
	Map& with_newer(Map& overrides) const
	{
		const EventRecord* sides[2] = {&current, &incoming};
		for (const EventRecord* side : sides) {
			for (Value name : side->bin_names()) {
				Result<Optional<Text<64>>> n = name.as_string<64>();
				if (!n || !*n) {
					overrides.carry(n ? Error{AS_UDF_TYPE_MISMATCH, "with_newer: a bin name is not a string"} : n.error());
					return overrides;
				}
				StrView bin_name = (*n)->view();
				if (overrides.contains(bin_name)) continue;
				const EventRecord& newer = incoming_is_newer(bin_name) ? incoming : current;
				const EventRecord& older = &newer == &incoming ? current : incoming;
				if (contains(newer.deleted_bins().value(), bin_name)) {
					overrides.put_nil(bin_name);
					continue;
				}
				Optional<Value> v = newer.get(bin_name);
				if (!v) v = older.get(bin_name);
				if (v) overrides.put(bin_name, *v);
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

// What an xdr-merge handler returns: one of the verdicts, or the final record. It is built by its static
// functions and delivered by the generated export; the fields are for that export, not for a handler to set.
//
// ```cpp
// if (incoming.is_delete()) return udf::Resolution::policy();
// if (!pair.incoming_is_newer("profile")) return udf::Resolution::keep_local();
// return udf::Resolution::record(pair.with_newer(out));
// ```
struct Resolution {
	const char* verdict;
	// The bins of record(), owned until the resolution is delivered and then given back with it.
	Map bins;
	Error err;

	// An empty resolution, for a Result that failed; never delivered.
	Resolution() : verdict(""), bins(), err() {}
	Resolution(const char* verdict_) : verdict(verdict_), bins(), err() {}
	Resolution(const char* verdict_, Map&& bins_, Error err_)
	    : verdict(verdict_), bins(static_cast<Map&&>(bins_)), err(err_) {}

	// Keeps the current record as it is on this cluster; the incoming write is not applied.
	static Resolution keep_local() { return Resolution{"keep-local"}; }
	// Takes the incoming write: its bins are applied over the current record.
	static Resolution take_remote() { return Resolution{"take-remote"}; }
	// Does what the server would have done without a handler. A set bound to a handler never uses bin
	// convergence, so this applies the incoming write, as take_remote() does. The answer for the cases a
	// handler has no opinion on.
	static Resolution policy() { return Resolution{"policy"}; }
	// Removes the record on this cluster and leaves a durable tombstone, so a late write cannot bring it back.
	// The delete is not shipped: the other cluster decides for itself, so a merge that deletes has to delete on
	// both sides for the two to agree. (`delete` is a keyword, as for Record.)
	static Resolution delete_record() { return Resolution{"delete"}; }
	// Removes the record on this cluster outright, with no tombstone, so a later write creates it again. Like
	// delete_record(), it is not shipped to the other cluster.
	static Resolution expunge() { return Resolution{"expunge"}; }
	// The final record: these bins, whole. The record is left with exactly the bins of the map; a bin absent from
	// it or nil in it is deleted. At least one bin must be left: a record of no bins is the handler's failure,
	// not a deletion, which is delete_record(). No answer carries a TTL: the server gives the merged record the
	// later of the two records' expirations. A failure the map kept from one of its puts fails the call when the
	// resolution is delivered. The resolution takes the map over, so it outlives the handler's own Map, which is
	// left null: `record(pair.with_newer(out))` is the usual form.
	static Resolution record(Map& bins)
	{
		Error e = bins.error();
		return Resolution{"record", static_cast<Map&&>(bins), e};
	}
	// The same, for a map made in place.
	static Resolution record(Map&& bins) { return record(bins); }

	// Delivers the resolution and returns the status the export returns: the verdict word, or for record()
	// the final bins. Fails the call instead when the bins map kept a failure. The generated export calls it.
	int32_t deliver() const
	{
		if (err.failed()) return fail(err);
		if (bins.handle() == 0) {
			as_udf_result_string(verdict, detail::cstrlen(verdict));
			return 0;
		}
		return as_udf_resolve_record(bins.handle());
	}
};

//==========================================================
// Typed exports: the parameter decoders, the result delivery and the signature text the UDF_* macros use.
//

// Every remaining argument of the call, for a last parameter of this type (`any...` in the signature): a function
// that takes any number of arguments. A UDF takes at most 128 arguments in all (AS_UDF_MAX_ARGS): a call with more
// fails, with a message naming the limit and the count, and none is dropped. It ranges like an array (`for (udf::Value v : rest)`)
// and indexes with [], which does not check the index. The values are valid for the call.
//
// ```cpp
// UDF_FUNCTION(sum, udf::Result<int64_t>, udf::Args rest)
// {
// 	int64_t total = 0;
// 	for (udf::Value v : rest) total += UDF_TRY(v.as_int()).value_or(0);
// 	return total;
// }
// ```
struct Args {
	const Value* values = nullptr;
	int32_t count = 0;
	// The first argument, for a range-for over the arguments.
	const Value* begin() const { return values; }
	// One past the last argument, for a range-for over the arguments.
	const Value* end() const { return values + count; }
	// How many arguments the client passed after the ones the function names.
	int32_t size() const { return count; }
	// Whether the client passed no argument after the ones the function names.
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
		if ((int64_t)(int32_t)*v != *v) {
			return Error{AS_UDF_TYPE_MISMATCH, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is out of range for a 32-bit int").buf};
		}
		return (int32_t)*v;
	}
};

template <> struct Param<double> {
	static constexpr const char* name = "double";
	static constexpr bool optional = false, rest = false;
	static Result<double> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		// An integer is not widened, as no reader widens one: a client that means 3.0 sends 3.0.
		if (a[i].is_double()) return a[i].as_double_bits();
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
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE - (int32_t)a[i].len, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		Result<Optional<Text<N>>> t = a[i].template as_string<N>();
		if (!t) return t.error();
		return **t;
	}
};

template <size_t N> struct Param<Bytes<N>> {
	static constexpr const char* name = "bytes";
	static constexpr size_t capacity = N;
	static constexpr bool optional = false, rest = false;
	static Result<Bytes<N>> decode(const Value* a, int32_t n, int32_t i, const char* fn, const char* p)
	{
		if (i >= n || a[i].is_absent()) return missing(fn, i, p);
		if (!a[i].is_string() && !a[i].is_blob()) return mismatch(fn, i, p, "a blob");
		if (a[i].len > N) return Error{AS_UDF_NEEDED_BASE - (int32_t)a[i].len, message().add(fn).add(": argument ").add((int64_t)i + 1).add(" (").add(p).add(") is ").add((int64_t)a[i].len).add(" bytes, at most ").add((int64_t)N).buf};
		Result<Optional<Bytes<N>>> b = a[i].template as_bytes<N>();
		if (!b) return b.error();
		return **b;
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
		constexpr size_t cap = T::rest ? AS_UDF_MAX_ARGS : (T::arity == 0 ? 1 : T::arity);
		constexpr int32_t most = T::rest ? AS_UDF_MAX_ARGS : (int32_t)T::arity;
		if (argc > most) {
			return fail(Error{AS_UDF_ERR, message().add(fn).add(" takes at most ").add((int64_t)most).add(" arguments, got ").add((int64_t)argc).buf});
		}
		Value a[cap];
		int32_t n = as_udf_args(args, argc, a, (int32_t)cap);
		if (n < 0) return fail(detail::status_error(n, "could not read the arguments"));
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

// A record UDF: `UDF_FUNCTION(name, ReturnType, params...) { body }`. The module exports it as `name`, with a
// signature a client can read. The return is a `Result<T>` or a plain T (int64_t, int32_t, double, bool, StrView,
// `Text<N>`, Value, Map, List, void): the value becomes the call's result, nil for void, and an Error returned
// through a Result fails the call with its message.
//
// The parameters are the call's arguments, decoded and checked before the body runs: int64_t, int32_t
// (range-checked), double (an integer argument is not widened), bool, `Text<N>` (a string of at most N bytes),
// `Bytes<N>` (a string or blob of at most N bytes), Value (anything), `Optional<T>` of those for an argument that
// may be left out or nil, and Args last for the rest. A missing argument, one of the wrong kind, one too long, or
// more arguments than parameters fails the call with a message naming the argument, and the body does not run.
//
// Before them the function may take the record it was called on, `udf::Record& rec` to write it or `const
// udf::Record& rec` to read it: the export hands it in, and it is neither an argument of the call nor part of the
// signature. A function declared (int32_t args, int32_t argc) returning int32_t is the raw form; see
// UDF_FUNCTION_RAW.
//
// ```cpp
// UDF_FUNCTION(add, udf::Result<int64_t>, udf::Record& rec, udf::Text<15> bin, udf::Optional<int64_t> by)
// {
// 	int64_t n = rec.int_or(bin, 0) + by.value_or(1);
//
// 	UDF_TRY(rec.set(bin, n));
// 	UDF_TRY(rec.save());
//
// 	return n;
// }
// ```
#define UDF_FUNCTION(name, ret, ...)                                                                     \
	static ret name##_udf_impl(__VA_ARGS__);                                                             \
	static constexpr ::udf::detail::ParamNames udf_names_##name{#__VA_ARGS__, ::udf::detail::leading<&name##_udf_impl>}; \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		return ::udf::detail::invoke<&name##_udf_impl>(#name, udf_names_##name, args, argc, false);      \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::function_signature<&name##_udf_impl>(#name, #__VA_ARGS__))        \
	static ret name##_udf_impl(__VA_ARGS__)

// The raw form of a record UDF: `UDF_FUNCTION_RAW(name, "(bin: str, k?: int)") { ... }`. The body is the export
// itself, with `int32_t args` and `int32_t argc` in scope, and returns the status: 0, or the non-zero udf::fail()
// returns. Nothing is decoded or delivered for it: it reads its arguments with as_udf_args, reads the record with
// the C header's `as_udf_rec_` functions and sets its result with udf::result(). `params` is the parameter list
// of the signature, as text, and is not checked against the body. For a function that wants to avoid the typed
// form's decoding.
#define UDF_FUNCTION_RAW(name, params)                                                                   \
	static int32_t name##_udf_impl(int32_t args, int32_t argc);                                           \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		return name##_udf_impl(args, argc);                                                              \
	}                                                                                                    \
	UDF_SIG_EXPORT(name, ::udf::detail::literal_signature("fn " #name params " -> value"))                 \
	static int32_t name##_udf_impl(int32_t args, int32_t argc)

// The per-record half of a stream (aggregation) UDF: `UDF_STREAM_RECORD(name, ret, params...) { ... }`, followed
// by `UDF_STREAM_END(name, ret, params...) { ... }`, in that order. It is exported as `<name>_record` and called
// once for every record the query reads. It returns void or `Result<void>`: an error ends the stream with its
// message, and any other return is ignored. It may take the record first, as `const udf::Record& rec` only: a
// stream's record takes no write, and `udf::Record&` does not compile. Its parameters are decoded as
// UDF_FUNCTION's are, on every record.
//
// State is static: one instance of the module sees every record a query thread reads, and nothing resets a static
// between records. UDF_STREAM_RAW declares both halves raw.
//
// ```cpp
// static int64_t total;
//
// UDF_STREAM_RECORD(sum_n, void, const udf::Record& rec)
// {
// 	total += rec.int_or("n", 0);
// }
//
// UDF_STREAM_END(sum_n, int64_t)
// {
// 	return total;
// }
// ```
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

// The end half of a stream UDF, declared after its UDF_STREAM_RECORD and exported as `<name>_end`. It runs once
// after the last record a query thread reads, and its return is delivered as the result, the partial: any return
// type UDF_FUNCTION takes. It has no record, and a record parameter here does not compile. It also emits the
// stream's signature, built from the parameter lists of both halves, which is why it comes second.
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

// Both halves of a stream UDF in the raw form: `UDF_STREAM_RAW(name, "(record params)", "(end params)")`.
// Declares `int32_t name_record_udf_impl(int32_t args, int32_t argc)` and `name_end_udf_impl` likewise, exports
// them as `<name>_record` and `<name>_end`, and emits the signature from the two parameter strings. No body
// follows the macro: the module defines the two functions itself, each returning the status, 0 or non-zero. A raw
// half is not handed a udf::Record: it reads the record with the C header's `as_udf_rec_` functions. The form for
// a stream that reads its arguments once, on the first record, and keeps them in statics.
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

// An xdr-merge handler: `UDF_ON_XDR_MERGE(name) { ... }` defines `Result<Resolution> name(const EventRecord&
// current, const EventRecord& incoming)`: the record as it is on this cluster and the one that arrived over XDR.
// The two parameter names are fixed, so the body reads them by those names. The export decodes the event into the
// two, delivers the resolution the body returns, and fails the call with the message of an Error it returns; the
// macro also emits the signature `event-set` checks before binding the handler.
//
// A handler cannot write a record, read the clock (now() is -1) or log (a line it writes is dropped), and must be
// symmetric: both clusters resolve the same pair with the sides swapped and have to land on the same record. The
// record-level generation() and last_update_time() are 0 on both sides.
//
// ```cpp
// UDF_ON_XDR_MERGE(merge_account)
// {
// 	udf::Pair pair = udf::Pair::of(current, incoming);
// 	udf::Map out(4);
//
// 	if (udf::Optional<int64_t> visits = pair.as_int("visits").max()) out.put("visits", *visits);
//
// 	return udf::Resolution::record(pair.with_newer(out));
// }
// ```
#define UDF_ON_XDR_MERGE(name)                                                                           \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord&, const ::udf::EventRecord&); \
	extern "C" __attribute__((export_name(#name))) int32_t udf_export_##name(int32_t args, int32_t argc)  \
	{                                                                                                    \
		::udf::EventRecord current, incoming;                                                            \
		if (::udf::detail::EventAccess::decode(args, argc, current, incoming) != AS_UDF_OK) return ::udf::fail(#name ": not an xdr-merge event"); \
		::udf::Result<::udf::Resolution> r = name##_udf_impl(current, incoming);                         \
		if (!r) return ::udf::fail(r.error());                                                           \
		return r->deliver();                                                                             \
	}                                                                                                    \
	UDF_HANDLER_SIG(name, "xdr-merge", "(current: record, incoming: record) -> resolution")               \
	static ::udf::Result<::udf::Resolution> name##_udf_impl(const ::udf::EventRecord& current, const ::udf::EventRecord& incoming)

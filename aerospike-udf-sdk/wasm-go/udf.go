// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// Package udf writes Aerospike WASM UDFs in Go, for the server's mod-wamr engine, built with TinyGo:
//
//	tinygo build -o my_udf.wasm -target=wasm-unknown -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking .
//
// The contract is the one aerospike-udf-sdk-wasm (Rust) documents: every read is a 16-byte Value, a string or
// collection is read by position from a source the host holds, nothing is msgpack. A UDF is an exported
// function `func name(args, argc int32) int32` returning 0 on success, which sets its result with Result*
// and fails with Fail. A stream (aggregation) UDF exports `<name>_record` and `<name>_end`. cmd/udfgen
// writes those exports from `udf:` directives, so a UDF is an ordinary Go function.
//
// The record a UDF is called on is a *Record, the function's first parameter when it wants one; every
// read, write and piece of metadata of the record is a method on it, and Save writes what was staged.
// What is left on the package is what is not about the record: the clock, the log, results, the list and
// map builders, values, arguments and the xdr-merge event.
//
// TinyGo brings a runtime and an allocator, so unlike the Rust SDK's default build nothing here is
// allocation-free by construction. Package-level arrays keep a hot path allocation-free when that matters.
package udf

import (
	"math"
	"unsafe"
)

//==========================================================
// The imports.
//

//go:wasmimport aerospike bin_value
func binValue(name unsafe.Pointer, nameLen int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike set_int
func setInt(name unsafe.Pointer, nameLen int32, value int64) int32

//go:wasmimport aerospike set_double
func setDouble(name unsafe.Pointer, nameLen int32, value float64) int32

//go:wasmimport aerospike set_str
func setStr(name unsafe.Pointer, nameLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike set_blob
func setBlob(name unsafe.Pointer, nameLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike set_handle
func setHandle(name unsafe.Pointer, nameLen int32, handle int32) int32

//go:wasmimport aerospike set_at
func setAt(name unsafe.Pointer, nameLen int32, src int32, at int32) int32

//go:wasmimport aerospike bin_remove
func binRemove(name unsafe.Pointer, nameLen int32) int32

//go:wasmimport aerospike cursor_values
func cursorValues(cursor int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike cursor_bytes_at
func cursorBytesAt(src int32, at int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike coll_bytes_at
func collBytesAt(src int32, at int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike coll_cmp_at
func collCmpAt(src1 int32, at1 int32, src2 int32, at2 int32) int32

//go:wasmimport aerospike cursor_enter_at
func cursorEnterAt(src int32, at int32) int32

//go:wasmimport aerospike coll_get
func collGet(src int32, at int32, key unsafe.Pointer, keyLen int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike coll_get_i
func collGetI(src int32, at int32, key int64, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike cursor_close
func cursorClose(cursor int32)

//go:wasmimport aerospike args_values
func argsValues(handle int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike list_get
func listGet(src int32, at int32, index int32, out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike map_has
func mapHas(handle int32, key unsafe.Pointer, keyLen int32) int32

//go:wasmimport aerospike map_has_i
func mapHasI(handle int32, key int64) int32

//go:wasmimport aerospike coll_size
func collSize(handle int32) int32

//go:wasmimport aerospike list_new
func listNew(capacity int32) int32

//go:wasmimport aerospike map_new
func mapNew(capacity int32) int32

//go:wasmimport aerospike map_put_str_int
func mapPutStrInt(handle int32, key unsafe.Pointer, keyLen int32, value int64) int32

//go:wasmimport aerospike map_put_str_str
func mapPutStrStr(handle int32, key unsafe.Pointer, keyLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike map_put_str_handle
func mapPutStrHandle(handle int32, key unsafe.Pointer, keyLen int32, value int32) int32

//go:wasmimport aerospike list_push_values
func listPushValues(handle int32, vals unsafe.Pointer, valsLen int32) int32

//go:wasmimport aerospike map_put_entries
func mapPutEntries(handle int32, pairs unsafe.Pointer, pairsLen int32) int32

//go:wasmimport aerospike result_nil
func resultNil()

//go:wasmimport aerospike result_int
func resultInt(value int64)

//go:wasmimport aerospike result_double
func resultDouble(value float64)

//go:wasmimport aerospike result_bool
func resultBool(value int32)

//go:wasmimport aerospike result_str
func resultStr(value unsafe.Pointer, valueLen int32)

//go:wasmimport aerospike result_blob
func resultBlob(value unsafe.Pointer, valueLen int32)

//go:wasmimport aerospike result_handle
func resultHandle(handle int32) int32

//go:wasmimport aerospike result_at
func resultAt(src int32, at int32) int32

//go:wasmimport aerospike fail
func fail(message unsafe.Pointer, messageLen int32)

//go:wasmimport aerospike gen
func gen() int64

//go:wasmimport aerospike ttl
func ttl() int64

//go:wasmimport aerospike key_type
func keyType() int32

//go:wasmimport aerospike key_int
func keyInt() int64

//go:wasmimport aerospike key_bytes
func keyBytes(out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike digest
func digest(out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike rec_create
func recCreate() int32

//go:wasmimport aerospike rec_update
func recUpdate() int32

//go:wasmimport aerospike rec_exists
func recExists() int32

//go:wasmimport aerospike log
func log(level int32, message unsafe.Pointer, messageLen int32)

//==========================================================
// The contract's constants.
//

// Status codes a host call returns, and the Status of an *Error.
//
// OK is success. Err is a call the host could not carry out: a bin name that is empty, longer than 15
// bytes or has a zero byte in it, a handle that is not what the call needs (one already closed
// included), or a call that has spent the memory it may hold on the server. NotFound is a bin, map key, list index or argument that is not there.
// TypeMismatch is a value of another kind than the one asked for. Refused is a write the host would not
// take: the record is read-only here (a stream's record, an event handler), the call has spent what it
// may build on the host, or the server turned the write down.
//
// A call that fills a buffer (Bytes, StoredBytes, Record.Key) answers a status at or below NeededBase
// when the buffer is too small. Nothing was copied, and -(rc) - 16 is the size to call again with;
// Error.Needed gives it for an *Error.
//
// Every failure the SDK reports carries one of these: the functions that answer an int32 answer the
// status itself, and every error the SDK returns is an *Error whose Status is the status. StatusOf
// reads it from an error of any type.
const (
	OK           = 0
	Err          = -1
	NotFound     = -2
	TypeMismatch = -3
	Refused      = -4
	NeededBase   = -16
)

// MaxArgs is how many arguments a UDF takes at most, in total. A call with more fails: the generated
// export of a function whose last parameter takes the rest checks the count with Arity, and nothing
// is ever dropped from the end of an argument list.
const MaxArgs = 128

// Kind says what a Value holds. A function on the typed tier seldom reads it, since the Is tests and
// the accessors of Value do; it is there for a switch over v.Kind.
type Kind int32

// The kinds of a Value.
//
// KindAbsent stands for no value at all (a missing bin, map entry or argument), as distinct from a
// stored nil (KindNil). KindBool, KindInt and KindDouble carry the value in the Value itself. KindStr,
// KindBlob, KindGeoJSON, KindHLL, KindList and KindMap are values the host holds, which the Value
// points at. KindOther is a value a UDF cannot interpret, and Record.Set refuses to write one.
//
// The last four are never read from the host. They are what the constructors make: StringValue and
// StringBytesValue a KindGuestStr, BlobValue a KindGuestBlob, GeoJSONValue a KindGuestGeoJSON, and
// Map.Value and List.Value a KindHandle.
const (
	KindAbsent  Kind = -1
	KindNil     Kind = 0
	KindBool    Kind = 1
	KindInt     Kind = 2
	KindDouble  Kind = 3
	KindStr     Kind = 4
	KindBlob    Kind = 5
	KindList    Kind = 6
	KindMap     Kind = 7
	KindOther   Kind = 8
	KindGeoJSON Kind = 9
	KindHLL     Kind = 10

	// Kinds a guest uses only in a batch it hands the host: bytes of its own by address, a built
	// collection by handle.
	KindGuestStr     Kind = 16
	KindGuestBlob    Kind = 17
	KindGuestGeoJSON Kind = 18
	KindHandle       Kind = 19
)

// Log levels for Log, most severe first: the server's warning, info and debug levels.
const (
	LogWarn  = 0
	LogInfo  = 1
	LogDebug = 2
)

// Value is one value as the host hands it out, be it a bin, a list element, a map key or entry or an
// argument, and the form in which a guest hands one back. It is 16 bytes and is copied freely.
//
// Kind says what it holds. An integer, double or boolean is in the Value itself, and Int, Double and
// Bool read it. A string, blob, GeoJSON, HLL, list or map stays on the host: Payload says where (Src
// and At), and Len is its length in bytes, or the number of elements of a list or of entries of a map,
// known without a walk. String, Blob and Bytes copy the bytes out; List, Map, Index and Lookup read a
// collection where it lies. The kind tests (IsInt, IsString, IsGeoJSON and the rest) say which it is,
// and no accessor converts: each reads its own kind and answers an error for any other.
//
// A value the host holds is good for the call it was read in, and in a stream for the record it was
// read from. It stays readable when its bin is written again and when the record is deleted. After a
// Save that fails, a list, map or blob read before it answers an error: read the bin again.
//
// The constructors (IntValue, StringValue, BlobValue and the rest) make values of the guest's own, for
// Record.Set, Map.Put, List.Add, Result and the batch calls Map.PutEntries and List.AddValues.
//
//	profile, err := rec.Bin("profile")
//	if err != nil {
//		return "", err
//	}
//	name, ok, err := profile.Lookup("name") // one entry of a map bin, in one import
//	if err != nil {
//		return "", err // the bin is not a map
//	}
//	if !ok {
//		return "", nil // the map has no "name"
//	}
//	return name.String()
type Value struct {
	Kind    Kind
	Len     uint32
	Payload int64
}

// Src is the host's handle of the source a stored value sits in, the high half of Payload. It means
// something only for a value the host holds (IsStored); the typed tier never needs it.
func (v Value) Src() int32 { return int32(v.Payload >> 32) }

// At is the value's byte offset in the source Src names, the low half of Payload. Like Src it means
// something only for a value the host holds.
func (v Value) At() int32 { return int32(v.Payload & 0xffffffff) }

// double is the payload as a float64, whatever the kind; Double is the checked form.
func (v Value) double() float64 { return math.Float64frombits(uint64(v.Payload)) }

// IntValue is an integer as a Value of the guest's, for Record.Set, Map.Put, List.Add, Result or a
// batch (Map.PutEntries, List.AddValues).
func IntValue(i int64) Value { return Value{Kind: KindInt, Payload: i} }

// DoubleValue is a double as a Value of the guest's, for the same calls as IntValue.
func DoubleValue(d float64) Value {
	return Value{Kind: KindDouble, Payload: int64(math.Float64bits(d))}
}

// guestAddr is the address a guest value was made over (StringValue, BlobValue, StringBytesValue) as a
// pointer again. The value keeps the address as an integer, because a Value is the 16-byte layout the
// host reads, and that is sound only because of how TinyGo collects: with -gc=leaking nothing is freed,
// and the conservative collector takes every aligned word that points into an object as a reference, which
// the address's low word in the payload is on wasm32. The precise collector would not, so a build with it
// is refused (gc_precise.go). Built with unsafe.Add rather than a conversion from uintptr: the result is
// the same, and the one place the SDK relies on the rule is this one, with its reason.
func guestAddr(payload int64) unsafe.Pointer {
	return unsafe.Add(unsafe.Pointer(nil), uintptr(payload))
}

// StringValue is a string of the guest's as a Value, for the same calls as IntValue. It holds the
// address of s's bytes, not a copy, so s must stay alive and unchanged until the call that is given
// the value has returned; the host copies the bytes during that call.
func StringValue(s string) Value {
	return Value{Kind: KindGuestStr, Len: uint32(len(s)), Payload: int64(uintptr(unsafe.Pointer(unsafe.StringData(s))))}
}

// BlobValue is a blob of the guest's as a Value, for the same calls as IntValue. It holds the address
// of b's bytes, not a copy, so b must stay alive and unchanged until the call that is given the value
// has returned; the host copies the bytes during that call. A nil or empty b is an empty blob.
func BlobValue(b []byte) Value {
	if len(b) == 0 {
		return Value{Kind: KindGuestBlob}
	}
	return Value{Kind: KindGuestBlob, Len: uint32(len(b)), Payload: int64(uintptr(unsafe.Pointer(&b[0])))}
}

// handleValue is a built list or map as a value, by its handle; Map.Value and List.Value are its public face.
func handleValue(h int32) Value { return Value{Kind: KindHandle, Payload: int64(h)} }

// StringBytesValue is a string Value over bytes the caller holds, without making a Go string of them:
// the form for text that is already in a buffer. It holds b's address like BlobValue, with the same
// rule on b's lifetime. Record.Key answers one over the buffer it was given.
func StringBytesValue(b []byte) Value {
	if len(b) == 0 {
		return Value{Kind: KindGuestStr}
	}
	return Value{Kind: KindGuestStr, Len: uint32(len(b)), Payload: int64(uintptr(unsafe.Pointer(&b[0])))}
}

//==========================================================
// Reading.
//

func ptr(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}

func strptr(s string) unsafe.Pointer {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.StringData(s))
}

// Bytes copies the raw bytes of a string, blob, GeoJSON or HLL value into buf and returns how many: a
// string's text or a GeoJSON document with no terminator, a blob's bytes as they were written. It
// allocates nothing, which is why a path that runs once per record uses it where Value.String and
// Value.Bytes would allocate. A value of the guest's own (StringValue, BlobValue, GeoJSONValue) is
// copied the same way.
//
// A negative return is a status, and nothing was copied. At or below NeededBase the buffer was too
// small - a value is never truncated - and -(rc) - 16 is the length needed, which v.Len gives
// beforehand. TypeMismatch is a value of any other kind: a scalar, a nil, a list, a map, a built
// collection. Err is a value the host can no longer read.
//
//	var buf [64]byte
//	n := udf.Bytes(v, buf[:])
//	if n < 0 {
//		return udf.Fail("the name is not a string of at most 64 bytes")
//	}
//	name := buf[:n]
func Bytes(v Value, buf []byte) int32 {
	switch v.Kind {
	case KindStr, KindBlob, KindGeoJSON, KindHLL:
		if v.Len == 0 {
			return 0
		}
		return cursorBytesAt(v.Src(), v.At(), ptr(buf), int32(len(buf)))
	case KindGuestStr, KindGuestBlob, KindGuestGeoJSON:
		if int(v.Len) > len(buf) {
			return NeededBase - int32(v.Len)
		}
		copy(buf, guestBytes(v))
		return int32(v.Len)
	}
	return TypeMismatch
}

// StoredBytes copies a stored value's bytes as the server stores them into buf and returns how many:
// the encoding a convergent merge compares when nothing else tells two candidates apart. For a value
// inside a list or map, and for a whole list or map, that is its msgpack encoding, header included.
// For a whole string, blob or HLL bin it is the bytes alone, with no header.
//
// A negative return is a status, and nothing was copied: at or below NeededBase the buffer was too
// small and -(rc) - 16 is the size needed; TypeMismatch is a value the host does not hold (IsStored
// is false: a scalar, a nil, a value of the guest's own); Err is one it can no longer read. To order
// two values without copying either, use CmpStored.
func StoredBytes(v Value, buf []byte) int32 {
	if !v.IsStored() {
		return TypeMismatch
	}
	return collBytesAt(v.Src(), v.At(), ptr(buf), int32(len(buf)))
}

// CmpStored compares two stored values byte by byte, as memcmp would on the encodings StoredBytes
// gives, without copying either into the guest. The ordering is exactly -1, 0 or 1: the first byte
// that differs decides, and an encoding that is a prefix of the other comes first. The order is of
// the stored bytes, not of the values, so it is arbitrary but the same on every cluster, which is
// what a merge needs of a tie-break.
//
// Both values must be ones the host holds (IsStored). The error, an *Error with Status TypeMismatch,
// says one of them is not - an integer, double or boolean, a nil, a value of the guest's own; compare
// those in Go - or that the host could not find one of them. A status is never returned as if it
// were an ordering.
//
//	// The same winner on both clusters, whichever side each calls a and b.
//	order, err := udf.CmpStored(a, b)
//	if err != nil {
//		return err
//	}
//	if order >= 0 {
//		out.Put("note", a)
//	} else {
//		out.Put("note", b)
//	}
func CmpStored(a, b Value) (int, error) {
	if !a.IsStored() || !b.IsStored() {
		return 0, errorf(TypeMismatch, "could not compare: both values must be stored strings, blobs, lists or maps")
	}
	rc := collCmpAt(a.Src(), a.At(), b.Src(), b.At())
	if rc < -1 || rc > 1 {
		return 0, statusError(rc, "could not compare the stored values")
	}
	return int(rc), nil
}

// Args decodes the call's arguments into out, in one import, and returns how many it decoded: the
// number the client passed, or len(out) when that is smaller. handle and argc are the two parameters
// an export is called with. An argument passed as nil is a Value of KindNil. It answers 0 when the
// call has no arguments, out is empty, or the arguments could not be read.
//
// It reads no more than out holds, and does not say so: argc is the number the client passed, and
// Arity(fn, len(out), argc) is the check that fails a call that passed more. A UDF takes MaxArgs
// arguments at most, so a window of MaxArgs values holds any call that is allowed.
//
// The generated exports call it, and Arity after it. An export in the raw form calls both itself,
// once, and reads the arguments from out with the Arg and Opt functions or directly:
//
//	//udf:function half params="(n: int)"
//	func half(args, argc int32) int32 {
//		var a [1]udf.Value
//		n := udf.Args(args, argc, a[:])
//		if err := udf.Arity("half", 1, argc); err != nil {
//			return udf.FailErr(err)
//		}
//		x, err := udf.ArgInt(a[:], n, 0, "half", "n")
//		if err != nil {
//			return udf.FailErr(err)
//		}
//		udf.ResultInt(x / 2)
//		return 0
//	}
func Args(handle, argc int32, out []Value) int32 {
	if handle <= 0 || argc <= 0 || len(out) == 0 {
		return 0
	}
	n := argsValues(handle, unsafe.Pointer(&out[0]), int32(len(out)*16))
	if n < 0 {
		return 0
	}
	return n
}

// enter opens a cursor over the list or map at a value's position. 0 when it could not.
func enter(v Value) int32 { return cursorEnterAt(v.Src(), v.At()) }

// next fills out with the next values of a cursor, 128 at most per import. Returns how many, or a status.
func next(cursor int32, out []Value) int32 {
	return cursorValues(cursor, unsafe.Pointer(&out[0]), int32(len(out)*16))
}

// closeCursor gives a cursor back to the host, which reuses its slot.
func closeCursor(cursor int32) { cursorClose(cursor) }

// get looks a string key up in the map at a value's position, in one import.
func get(m Value, key string, out *Value) int32 {
	return collGet(m.Src(), m.At(), strptr(key), int32(len(key)), unsafe.Pointer(out), 16)
}

//==========================================================
// Building and reporting. The builders in coll.go and the event answers sit on these.
//

// one is the answer of a batch import that was handed one element, as a status. The host answers how
// many elements it took: 1 is OK, a negative answer is a status of its own (the handle is not a
// collection being built), and 0 is the element it would not take, which is a refusal.
func one(rc int32) int32 {
	if rc == 1 {
		return OK
	}
	if rc < 0 {
		return rc
	}
	return Refused
}

// listPushValue appends any value through the by-value batch import, with one element: a scalar by
// value, guest bytes by address, a stored value by position, a built collection by handle. It takes
// none of the call's handles, whatever the kind.
func listPushValue(list int32, v Value) int32 {
	switch v.Kind {
	case KindAbsent:
		return NotFound
	case KindOther:
		return TypeMismatch
	}
	return one(listPushValues(list, unsafe.Pointer(&v), 16))
}

// mapPutStr puts a string under a string key in a built map; the host copies the bytes. The status is Err
// when m is not a built map.
func mapPutStr(m int32, key, value string) int32 {
	return mapPutStrStr(m, strptr(key), int32(len(key)), strptr(value), int32(len(value)))
}

// mapPutInt puts an integer under a string key in a built map. The status is Err when m is not a built
// map.
func mapPutInt(m int32, key string, value int64) int32 {
	return mapPutStrInt(m, strptr(key), int32(len(key)), value)
}

// mapPutHandle puts a value held by handle (a built collection, or a value the host made) under a string
// key in a built map. The status is Err when m is not a built map or value is not a usable handle.
func mapPutHandle(m int32, key string, value int32) int32 {
	return mapPutStrHandle(m, strptr(key), int32(len(key)), value)
}

// mapPutValue puts any value under a key, a string or an integer as a Value, through the by-value
// batch import with one entry: the kinds as listPushValue takes them, and no handle taken.
func mapPutValue(m int32, key Value, v Value) int32 {
	switch v.Kind {
	case KindAbsent:
		return NotFound
	case KindOther:
		return TypeMismatch
	}
	pair := [2]Value{key, v}
	return one(mapPutEntries(m, unsafe.Pointer(&pair[0]), 32))
}

// ResultNil sets nil as the call's result, which is also what a call that sets no result returns.
//
// The rules are the same for every Result call. Of several, the last one wins. A result is dropped
// when the call then fails. In a stream only the result End sets is the answer: one set while a record
// is handled is dropped before the next record. A function under a `//udf:function` directive does not
// call these; it returns its value and the generated export sets it.
func ResultNil() { resultNil() }

// ResultInt sets an integer as the call's result; see ResultNil for the rules all Result calls share.
func ResultInt(v int64) { resultInt(v) }

// ResultDouble sets a double as the call's result.
func ResultDouble(v float64) { resultDouble(v) }

// ResultBool sets a boolean as the call's result.
func ResultBool(v bool) {
	if v {
		resultBool(1)
	} else {
		resultBool(0)
	}
}

// ResultString sets a string as the call's result. The host copies the bytes during the call, so s
// need not outlive it. A string larger than the call may still build on the host fails the call.
func ResultString(s string) { resultStr(strptr(s), int32(len(s))) }

// ResultBlob sets a blob as the call's result. The host copies the bytes during the call, so b need
// not outlive it. A nil or empty b is an empty blob.
func ResultBlob(b []byte) { resultBlob(ptr(b), int32(len(b))) }

// Fail fails the call with a message the client sees, and returns the non-zero status for the export
// to return. It does not stop the function: return what it answers.
//
// The host keeps the first 511 bytes of the message. Once Fail has been called the call has failed
// whatever the export goes on to return, any result set is dropped, and of several Fail calls the
// last message is the one reported. A function under a `//udf:function` directive returns an error
// instead, and the generated export calls Fail with its message.
//
// A call that fails writes nothing, even what it had already saved: a Record.Save that succeeded
// before the failure, and a SetInts or SetDoubles, are undone.
//
//	//udf:function half params="(n: int)"
//	func half(args, argc int32) int32 {
//		var a [1]udf.Value
//		if udf.Args(args, argc, a[:]) < 1 || !a[0].IsInt() {
//			return udf.Fail("half: n must be an int")
//		}
//		udf.ResultInt(a[0].Payload / 2)
//		return 0
//	}
func Fail(message string) int32 {
	fail(strptr(message), int32(len(message)))
	return 1
}

// Log writes a message to the server log at a level: LogWarn, LogInfo or LogDebug, any other value
// logging as debug.
//
// The host keeps the first 511 bytes of the message and escapes control bytes, so a message is one
// line of the log whatever it contains. A call may write 16 lines and a module 100 a second; lines
// over either limit are dropped, and the server logs how many. In an event handler nothing is
// written.
func Log(level int32, message string) { log(level, strptr(message), int32(len(message))) }

// Signature is what a `__udf_sig_<name>` export returns: the address of the signature text, which the
// host reads up to its terminating zero byte and serves as that call's result, and which `event-set`
// checks before it binds a handler. text must end in a zero byte and must not be empty, and it has to
// stay where it is, so keep it in a package-level variable. udfgen writes these exports; this is for
// one written by hand.
//
//	var sigHalf = []byte("fn half(n: int) -> value\x00")
//
//	//export __udf_sig_half
//	func udfSigHalf() int32 { return udf.Signature(&sigHalf) }
func Signature(text *[]byte) int32 {
	return int32(uintptr(unsafe.Pointer(&(*text)[0])))
}

//==========================================================
// Events.
//
// An event handler is a function the server calls at a point in its own processing. The one event,
// `xdr-merge`, hands it the current record and the incoming XDR write as two records (read through
// EventRecord, never the record imports), and takes back a verdict or the final record. What the
// server says about the merge beyond the two records' bins - a record delete, a partial write, the bins
// that would lose - is a fact about one of the two, and is read from that record. Export the handler under
// its name and its signature under `__udf_sig_<name>` (see XdrMergeSignature), which `event-set` checks
// before binding. A handler must be pure and symmetric: both DCs resolve the same pair with the sides
// swapped and must land on the same record. Pair.IncomingIsNewer is symmetric; "mine first" is not.
//

//go:wasmimport aerospike event_bin_value
func eventBinValue(which int32, name unsafe.Pointer, nameLen int32, out unsafe.Pointer, cap int32) int32

// EventRecord is one of the two records an xdr-merge handler is given: the current one, as it is on
// this cluster, or the incoming one, as it arrived over XDR.
//
// It is read like the record of a UDF, with the methods of RecordReader: Bin, Get, the typed readers
// Int, Double, Bool, String, Blob and Bytes, List and Map. Beyond those it names its bins (BinNames) and the
// bins its side deleted (DeletedBins), says when and by which cluster a bin was last written
// (BinLastUpdateTime, BinSourceID), and carries what the server says of the merge: IsDelete,
// IsPartial of the incoming record, IsTombstone of the current one.
//
// It has no writing methods. A handler changes the record through the Resolution it returns, and it
// cannot read the clock (Now answers -1) or write to the log: a Log line is dropped. Generation and
// LastUpdateTime are 0 on both records. The two records are good for the call.
//
//	//udf:on xdr-merge merge_visits
//	func mergeVisits(current, incoming *udf.EventRecord) (udf.Resolution, error) {
//		pair := udf.PairOf(current, incoming)
//		out := udf.NewMap(4)
//
//		visits, err := pair.Int("visits")
//		if err != nil {
//			return udf.Resolution{}, err
//		}
//		if most, ok := udf.Max(visits); ok {
//			out.Put("visits", most)
//		}
//
//		return udf.Resolve(pair.WithNewer(out)), out.Err()
//	}
type EventRecord struct {
	which              int32 // 0 current, 1 incoming
	generation         int64
	lastUpdateTime     int64
	ttl                int64
	sourceID           int64
	binNames           Value // list of bin names
	deletedBins        Value // list of bin names this side deleted; Absent kind when none
	binLastUpdateTimes Value // map name -> last update time per bin, when the server keeps one; Absent kind otherwise
	binSourceIDs       Value // map name -> source id, likewise

	// What the server says of the merge, each kept on the record it is true of and read through the
	// method of the same name. On the other record they are the zero value, so the method answers false
	// or walks nothing.
	isDelete       bool
	partial        bool
	tombstone      bool
	binConvergence bool
}

// The event's description is a map the server built, read with these: an entry that is missing or of
// another kind than the server writes reads as absent, false or the default.

// mapGet is the entry under a string key of a stored map, in one import; a Value of KindAbsent when
// the map has no such key or m is not a map.
func mapGet(m Value, key string) Value {
	v := Value{Kind: KindAbsent}
	if m.Kind == KindMap {
		if get(m, key, &v) != OK {
			v = Value{Kind: KindAbsent}
		}
	}
	return v
}

// mapBool is a boolean entry of a stored map; false when there is none.
func mapBool(m Value, key string) bool {
	v := mapGet(m, key)
	return v.Kind == KindBool && v.Payload != 0
}

// mapInt is an integer entry of a stored map, or dflt when there is none.
func mapInt(m Value, key string, dflt int64) int64 {
	v := mapGet(m, key)
	if v.Kind != KindInt {
		return dflt
	}
	return v.Payload
}

// StringIs reports whether a string value reads as text, without allocating for text of up to 64
// bytes; longer text is compared through a buffer of its own length, so text of any length matches
// when it is equal. It is false for a value that is not a string - a blob or a GeoJSON with the same
// bytes included - and for a string the host can no longer read.
func StringIs(v Value, text string) bool {
	if (v.Kind != KindStr && v.Kind != KindGuestStr) || int(v.Len) != len(text) {
		return false
	}
	if len(text) == 0 {
		return true
	}
	var stack [64]byte
	buf := stack[:]
	if len(text) > len(stack) {
		buf = make([]byte, len(text))
	}
	n := Bytes(v, buf)
	return n == int32(len(text)) && string(buf[:n]) == text
}

func eventSide(description Value, key string, which int32, out *EventRecord) bool {
	m := mapGet(description, key)
	if m.Kind != KindMap {
		return false
	}
	*out = EventRecord{}
	out.which = which
	out.generation = mapInt(m, "gen", 0)
	out.lastUpdateTime = mapInt(m, "lut", 0)
	out.ttl = mapInt(m, "ttl", 0)
	out.sourceID = mapInt(m, "src-id", 0)
	out.binNames = mapGet(m, "bins")
	out.deletedBins = mapGet(m, "deleted")
	out.binLastUpdateTimes = mapGet(m, "bin-luts")
	out.binSourceIDs = mapGet(m, "bin-srcs")
	return out.binNames.Kind == KindList
}

// DecodeMerge reads an xdr-merge call's arguments into the two records, and answers false when the
// call is not that event, in which case current and incoming are not to be read. args and argc are
// the two parameters the export was called with.
//
// The generated export of a `//udf:on xdr-merge` handler calls it. A handler in the raw form, which
// is handed (args, argc) as they come, calls it first to get the records it then reads:
//
//	//udf:on xdr-merge keep_richer
//	func keepRicher(args, argc int32) int32 {
//		var current, incoming udf.EventRecord
//		if !udf.DecodeMerge(args, argc, &current, &incoming) {
//			return udf.Fail("keep_richer: not an xdr-merge call")
//		}
//		theirs, err := incoming.IntOr("balance", 0)
//		if err != nil {
//			return udf.FailErr(err)
//		}
//		mine, err := current.IntOr("balance", 0)
//		if err != nil {
//			return udf.FailErr(err)
//		}
//		if theirs > mine {
//			return udf.TakeRemote().Deliver()
//		}
//		return udf.KeepLocal().Deliver()
//	}
func DecodeMerge(args, argc int32, current, incoming *EventRecord) bool {
	var a [1]Value
	if Args(args, argc, a[:]) < 1 || a[0].Kind != KindMap {
		return false
	}
	if !eventSide(a[0], "current", 0, current) || !eventSide(a[0], "incoming", 1, incoming) {
		return false
	}
	convergence := StringIs(mapGet(a[0], "policy"), "bin-lut")
	current.binConvergence = convergence
	incoming.binConvergence = convergence
	current.tombstone = mapBool(mapGet(a[0], "current"), "tombstone")
	side := mapGet(a[0], "incoming")
	incoming.partial = mapBool(side, "partial")
	incoming.isDelete = mapBool(side, "deleted-record")
	return true
}

// BinLastUpdateTime is when this side last wrote the bin, where the server keeps a time per bin, and
// the record's LastUpdateTime otherwise. It is in milliseconds since the Citrusleaf epoch,
// 2010-01-01T00:00:00Z (Unix milliseconds minus 1 262 304 000 000), like LastUpdateTime. It is for
// ordering the two sides of one bin, which is what decides a bin under bin convergence;
// Pair.IncomingIsNewer and Both.Newer do that with it.
//
// The server keeps a time per bin only under bin convergence (UsesBinConvergence). Without it this
// is the record's time, which an xdr-merge reports as 0 on both sides, so nothing can be ordered by
// it. name need not be a bin the side has: a bin with no time of its own gets the record's.
func (r *EventRecord) BinLastUpdateTime(name string) int64 {
	return mapInt(r.binLastUpdateTimes, name, r.lastUpdateTime)
}

// BinSourceID is the source id of the cluster that last wrote the bin on this side, where the server
// keeps one per bin (under bin convergence), and the record's SourceID otherwise. It is the tie-break
// when two sides wrote a bin at the same time.
func (r *EventRecord) BinSourceID(name string) int64 {
	return mapInt(r.binSourceIDs, name, r.sourceID)
}

// EachString calls fn with every string of a stored list, in order: name is the string's bytes,
// copied into buf, and v the stored value itself. name is good only until fn returns; copy what is to
// be kept.
//
// A string of any length is handed over. One that fits buf is copied into it and nothing is
// allocated, so give it a buffer as long as the longest string expected (15 bytes for bin names); a
// longer one is copied into a buffer made for it. An element that is not a string is not a string of
// the list, and is passed over.
//
// A list that is absent or nil has no strings, and the answer is nil. The error is an *Error: Status
// TypeMismatch when list is a value of another kind, and the host's status when the list could not be
// walked - the call has spent the memory it may hold on the server - or a string could not be read;
// the walk stops there. The walk's cursor is given back before EachString returns, fn panicking
// included.
func EachString(list Value, buf []byte, fn func(name []byte, v Value)) error {
	if list.IsNull() {
		return nil
	}
	if list.Kind != KindList {
		return list.kindError("a list")
	}
	cur := enter(list)
	if cur <= 0 {
		return errorf(Err, "could not walk the list: the call has spent the memory it may hold on the server")
	}
	defer closeCursor(cur)
	var w [32]Value
	for {
		n := next(cur, w[:])
		if n < 0 {
			return statusError(n, "could not walk the list")
		}
		for i := int32(0); i < n; i++ {
			if w[i].Kind != KindStr {
				continue
			}
			b := buf
			if int(w[i].Len) > len(b) {
				b = make([]byte, w[i].Len)
			}
			k := Bytes(w[i], b)
			if k < 0 {
				return statusError(k, "could not read a string of the list")
			}
			fn(b[:k], w[i])
		}
		if n < 32 {
			return nil
		}
	}
}

// resolveRecord returns the final record: bins (a built map; a bin absent or nil is deleted) and a TTL
// with a write's meaning, or -1 to keep the one the incoming write asked for.
func resolveRecord(bins int32, ttl int64) int32 {
	out := NewMap(3)
	if out.err != nil {
		return Fail("resolve: could not build the resolution")
	}
	// The result is the host's own reference, so the map the guest never sees is given back at once.
	defer out.Close()
	rc := mapPutStr(out.handle, "verdict", "record")
	if rc == OK {
		rc = mapPutHandle(out.handle, "bins", bins)
	}
	if rc == OK && ttl >= 0 {
		rc = mapPutInt(out.handle, "ttl", ttl)
	}
	if rc != OK {
		return Fail("resolve: could not build the resolution")
	}
	if resultHandle(out.handle) != OK {
		return Fail("resolve: could not return the record")
	}
	return 0
}

// XdrMergeSignature is the signature text of an xdr-merge handler, NUL-terminated: what the export
// `__udf_sig_<name>` serves and `event-set` checks before it binds the handler. name is the name the
// handler is exported under. udfgen writes this for a `//udf:on xdr-merge` handler; by hand, keep the
// text in a package-level variable and serve it with Signature:
//
//	var sigMerge = udf.XdrMergeSignature("merge_visits")
//
//	//export __udf_sig_merge_visits
//	func udfSigMerge() int32 { return udf.Signature(&sigMerge) }
func XdrMergeSignature(name string) []byte {
	return []byte("event xdr-merge/1 " + name + "(current: record, incoming: record) -> resolution\x00")
}

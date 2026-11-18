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

//go:wasmimport aerospike pack_handle
func packHandle(handle int32) int32

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

//go:wasmimport aerospike val_nil
func valNil() int32

//go:wasmimport aerospike val_double
func valDouble(value float64) int32

//go:wasmimport aerospike val_bool
func valBool(value int32) int32

//go:wasmimport aerospike list_new
func listNew(capacity int32) int32

//go:wasmimport aerospike map_new
func mapNew(capacity int32) int32

//go:wasmimport aerospike list_push
func listPush(handle int32, value int32) int32

//go:wasmimport aerospike list_push_int
func listPushInt(handle int32, value int64) int32

//go:wasmimport aerospike list_push_str
func listPushStr(handle int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike map_put_str_int
func mapPutStrInt(handle int32, key unsafe.Pointer, keyLen int32, value int64) int32

//go:wasmimport aerospike map_put_str_str
func mapPutStrStr(handle int32, key unsafe.Pointer, keyLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike map_put_str_handle
func mapPutStrHandle(handle int32, key unsafe.Pointer, keyLen int32, value int32) int32

//go:wasmimport aerospike list_push_at
func listPushAt(handle int32, src int32, at int32) int32

//go:wasmimport aerospike map_put_str_at
func mapPutStrAt(handle int32, key unsafe.Pointer, keyLen int32, src int32, at int32) int32

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

// Status codes a native returns. Anything at or below NeededBase means the buffer was too small, and
// -(rc) - 16 is the size needed.
const (
	OK           = 0
	Err          = -1
	NotFound     = -2
	TypeMismatch = -3
	Refused      = -4
	NeededBase   = -16
)

// Kind is a value's kind, as the host hands it out.
type Kind int32

// The kinds of a Value. KindAbsent stands for no value at all (a missing bin, map entry or argument), as
// distinct from a stored nil (KindNil). KindOther is a value a UDF cannot interpret.
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

// Log levels for Log, most severe first.
const (
	LogWarn  = 0
	LogInfo  = 1
	LogDebug = 2
)

// Value is the 16-byte value the host hands out, and the one a guest hands back in a batch: the kind, the
// length (bytes, elements or entries) and the payload (the scalar, or source<<32 | offset).
type Value struct {
	Kind    Kind
	Len     uint32
	Payload int64
}

// Src is the source handle a string, blob or collection sits in.
func (v Value) Src() int32 { return int32(v.Payload >> 32) }

// At is the offset in that source.
func (v Value) At() int32 { return int32(v.Payload & 0xffffffff) }

// double is the payload as a float64, whatever the kind; Double is the checked form.
func (v Value) double() float64 { return math.Float64frombits(uint64(v.Payload)) }

// IntValue is an integer as a value of the guest's, for a batch.
func IntValue(i int64) Value { return Value{Kind: KindInt, Payload: i} }

// DoubleValue is a double as a value of the guest's, for a batch.
func DoubleValue(d float64) Value {
	return Value{Kind: KindDouble, Payload: int64(math.Float64bits(d))}
}

// StringValue is a string of the guest's as a value, for a batch. It holds the address of s's bytes, not a
// copy, so s must stay alive until the host has taken the value.
func StringValue(s string) Value {
	return Value{Kind: KindGuestStr, Len: uint32(len(s)), Payload: int64(uintptr(unsafe.Pointer(unsafe.StringData(s))))}
}

// BlobValue is a blob of the guest's as a value, for a batch. It holds the address of b's bytes, not a
// copy, so b must stay alive until the host has taken the value.
func BlobValue(b []byte) Value {
	if len(b) == 0 {
		return Value{Kind: KindGuestBlob}
	}
	return Value{Kind: KindGuestBlob, Len: uint32(len(b)), Payload: int64(uintptr(unsafe.Pointer(&b[0])))}
}

// handleValue is a built list or map as a value, by its handle; Map.Value and List.Value are its public face.
func handleValue(h int32) Value { return Value{Kind: KindHandle, Payload: int64(h)} }

// StringBytesValue is a string value over bytes the caller holds, without making a Go string of them.
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

// Bytes copies the bytes of a string, blob, GeoJSON or HLL value into buf. Returns the length, or a status.
func Bytes(v Value, buf []byte) int32 {
	return cursorBytesAt(v.Src(), v.At(), ptr(buf), int32(len(buf)))
}

// StoredBytes copies a stored value's bytes as the server stores them, msgpack header included, into buf:
// the encoding a convergent merge compares when nothing else tells two candidates apart. Returns the
// length, or a status.
func StoredBytes(v Value, buf []byte) int32 {
	return collBytesAt(v.Src(), v.At(), ptr(buf), int32(len(buf)))
}

// CmpStored compares two stored values byte by byte, as memcmp would on their stored encodings, without
// copying either: negative, zero or positive; TypeMismatch when a value is not a stored one.
func CmpStored(a, b Value) int32 { return collCmpAt(a.Src(), a.At(), b.Src(), b.At()) }

// Args decodes the call's arguments into out, in one import. Returns how many there are, bounded by len(out).
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

// closeCursor gives a cursor's slot back.
func closeCursor(cursor int32) { cursorClose(cursor) }

// get looks a string key up in the map at a value's position, in one import.
func get(m Value, key string, out *Value) int32 {
	return collGet(m.Src(), m.At(), strptr(key), int32(len(key)), unsafe.Pointer(out), 16)
}

//==========================================================
// Building and reporting. The builders in coll.go and the event answers sit on these.
//

// listPushValue appends any value: a scalar by value, guest bytes by address, a stored value by position,
// a built collection by handle.
func listPushValue(list int32, v Value) int32 {
	switch v.Kind {
	case KindInt:
		return listPushInt(list, v.Payload)
	case KindDouble:
		return listPush(list, valDouble(v.double()))
	case KindBool:
		return listPush(list, valBool(int32(v.Payload)))
	case KindNil:
		return listPush(list, valNil())
	case KindGuestStr:
		return listPushStr(list, guestPtr(v), int32(v.Len))
	case KindGuestBlob:
		return listPush(list, valBlob(guestPtr(v), int32(v.Len)))
	case KindGuestGeoJSON:
		return listPush(list, valGeoJSON(guestPtr(v), int32(v.Len)))
	case KindHandle:
		return listPush(list, int32(v.Payload))
	case KindAbsent:
		return NotFound
	}
	return listPushAt(list, v.Src(), v.At())
}

// mapPutInt puts an integer under a string key in a built map. The status is Err when m is not a built
// map.
func mapPutInt(m int32, key string, value int64) int32 {
	return mapPutStrInt(m, strptr(key), int32(len(key)), value)
}

// mapPutStr puts a string under a string key in a built map; the host copies the bytes. The status is Err
// when m is not a built map.
func mapPutStr(m int32, key, value string) int32 {
	return mapPutStrStr(m, strptr(key), int32(len(key)), strptr(value), int32(len(value)))
}

// mapPutHandle puts a value held by handle (a built collection, or a value the host made) under a string
// key in a built map. The status is Err when m is not a built map or value is not a usable handle.
func mapPutHandle(m int32, key string, value int32) int32 {
	return mapPutStrHandle(m, strptr(key), int32(len(key)), value)
}

// mapPutValue puts any value under a string key: a scalar by value, guest bytes by address, a stored value
// by position, a built collection by handle.
func mapPutValue(m int32, key string, v Value) int32 {
	switch v.Kind {
	case KindInt:
		return mapPutStrInt(m, strptr(key), int32(len(key)), v.Payload)
	case KindDouble:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valDouble(v.double()))
	case KindBool:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valBool(int32(v.Payload)))
	case KindNil:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valNil())
	case KindGuestStr:
		return mapPutStrStr(m, strptr(key), int32(len(key)), unsafe.Pointer(uintptr(v.Payload)), int32(v.Len))
	case KindGuestBlob:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valBlob(guestPtr(v), int32(v.Len)))
	case KindGuestGeoJSON:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valGeoJSON(guestPtr(v), int32(v.Len)))
	case KindHandle:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), int32(v.Payload))
	case KindAbsent:
		return NotFound
	}
	return mapPutStrAt(m, strptr(key), int32(len(key)), v.Src(), v.At())
}

// ResultNil sets nil as the call's result. Of several Result calls, the last one wins.
func ResultNil() { resultNil() }

// ResultInt sets an integer as the call's result.
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

// ResultString sets a string as the call's result; the host copies the bytes.
func ResultString(s string) { resultStr(strptr(s), int32(len(s))) }

// ResultBlob sets a blob as the call's result; the host copies the bytes.
func ResultBlob(b []byte) { resultBlob(ptr(b), int32(len(b))) }

// Fail fails the call with a message the client sees, and returns the non-zero status a UDF then returns.
func Fail(message string) int32 {
	fail(strptr(message), int32(len(message)))
	return 1
}

// Log writes a message to the server log at a level: LogWarn, LogInfo or LogDebug, any other value
// logging as debug. The host keeps the first 511 bytes of the message.
func Log(level int32, message string) { log(level, strptr(message), int32(len(message))) }

// Signature is the text behind a `__udf_sig_<name>` export: the address of a NUL-terminated string, which the
// host serves as the call's result. Keep the string in a package-level variable so its address stays valid.
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

// EventRecord is one of the two records of a merge: the current one, as it is on this cluster, or the
// incoming one, as it arrived over XDR.
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
	lost           Value // the incoming bins that would lose under bin convergence
	isDelete       bool
	partial        bool
	tombstone      bool
	binConvergence bool
}

// MapGet is a map entry by string key; a value of Absent kind when there is none or m is not a map.
func MapGet(m Value, key string) Value {
	v := Value{Kind: KindAbsent}
	if m.Kind == KindMap {
		get(m, key, &v)
	}
	return v
}

// MapString is a string entry of a map, or "" when absent.
func MapString(m Value, key string) string {
	v := MapGet(m, key)
	if v.Kind != KindStr {
		return ""
	}
	var buf [128]byte
	n := Bytes(v, buf[:])
	if n < 0 {
		return ""
	}
	return string(buf[:n])
}

// MapBool is a boolean entry of a map, false when absent.
func MapBool(m Value, key string) bool {
	v := MapGet(m, key)
	return v.Kind == KindBool && v.Payload != 0
}

// MapInt is an integer map entry, or dflt.
func MapInt(m Value, key string, dflt int64) int64 {
	v := MapGet(m, key)
	if v.Kind != KindInt {
		return dflt
	}
	return v.Payload
}

// StringIs reports whether a stored string value reads as text.
func StringIs(v Value, text string) bool {
	if v.Kind != KindStr {
		return false
	}
	var buf [64]byte
	n := Bytes(v, buf[:])
	if n < 0 || n != int32(len(text)) {
		return false
	}
	return string(buf[:n]) == text
}

func eventSide(description Value, key string, which int32, out *EventRecord) bool {
	m := MapGet(description, key)
	if m.Kind != KindMap {
		return false
	}
	*out = EventRecord{lost: Value{Kind: KindAbsent}}
	out.which = which
	out.generation = MapInt(m, "gen", 0)
	out.lastUpdateTime = MapInt(m, "lut", 0)
	out.ttl = MapInt(m, "ttl", 0)
	out.sourceID = MapInt(m, "src-id", 0)
	out.binNames = MapGet(m, "bins")
	out.deletedBins = MapGet(m, "deleted")
	out.binLastUpdateTimes = MapGet(m, "bin-luts")
	out.binSourceIDs = MapGet(m, "bin-srcs")
	return out.binNames.Kind == KindList
}

// DecodeMerge reads an xdr-merge call's arguments into the two records. False when they are not that
// event. The generated export of a `//udf:on xdr-merge` handler calls it; a raw handler, which is handed
// (args, argc) as they come, calls it first to get the records it then reads.
func DecodeMerge(args, argc int32, current, incoming *EventRecord) bool {
	var a [1]Value
	if Args(args, argc, a[:]) < 1 || a[0].Kind != KindMap {
		return false
	}
	if !eventSide(a[0], "current", 0, current) || !eventSide(a[0], "incoming", 1, incoming) {
		return false
	}
	convergence := StringIs(MapGet(a[0], "policy"), "bin-lut")
	current.binConvergence = convergence
	incoming.binConvergence = convergence
	current.tombstone = MapBool(MapGet(a[0], "current"), "tombstone")
	side := MapGet(a[0], "incoming")
	incoming.partial = MapBool(side, "partial")
	incoming.isDelete = MapBool(side, "deleted-record")
	incoming.lost = MapGet(a[0], "lost")
	return true
}

// BinLastUpdateTime is when this side last wrote the bin: per bin where the server keeps one, the
// record's otherwise.
func (r *EventRecord) BinLastUpdateTime(name string) int64 {
	return MapInt(r.binLastUpdateTimes, name, r.lastUpdateTime)
}

// BinSourceID is the source id that wrote the bin, likewise.
func (r *EventRecord) BinSourceID(name string) int64 {
	return MapInt(r.binSourceIDs, name, r.sourceID)
}

// EachString calls fn with every string of a stored list, as bytes into buf.
func EachString(list Value, buf []byte, fn func(name []byte, v Value)) {
	if list.Kind != KindList {
		return
	}
	cur := enter(list)
	if cur <= 0 {
		return
	}
	var w [32]Value
	for {
		n := next(cur, w[:])
		for i := int32(0); i < n; i++ {
			if k := Bytes(w[i], buf); k >= 0 && w[i].Kind == KindStr {
				fn(buf[:k], w[i])
			}
		}
		if n < 32 {
			break
		}
	}
	closeCursor(cur)
}

// resolveRecord returns the final record: bins (a built map; a bin absent or nil is deleted) and a TTL
// with a write's meaning, or -1 to keep the one the incoming write asked for.
func resolveRecord(bins int32, ttl int64) int32 {
	out := NewMap(3)
	if out.err != nil {
		return Fail("resolve: could not build the resolution")
	}
	mapPutStr(out.handle, "verdict", "record")
	mapPutHandle(out.handle, "bins", bins)
	if ttl >= 0 {
		mapPutInt(out.handle, "ttl", ttl)
	}
	if resultHandle(out.handle) != OK {
		return Fail("resolve: could not return the record")
	}
	return 0
}

// XdrMergeSignature is the signature text an xdr-merge handler exports under `__udf_sig_<name>`,
// NUL-terminated. Keep it in a package-level variable and serve it with Signature.
func XdrMergeSignature(name string) []byte {
	return []byte("event xdr-merge/1 " + name + "(current: record, incoming: record) -> resolution\x00")
}

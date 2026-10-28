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
// and fails with Fail. A stream (aggregation) UDF exports `<name>_record` and `<name>_end`.
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

// Double is the payload as a float64.
func (v Value) Double() float64 { return math.Float64frombits(uint64(v.Payload)) }

// Values of the guest's, for a batch.
func IntValue(i int64) Value { return Value{Kind: KindInt, Payload: i} }
func DoubleValue(d float64) Value {
	return Value{Kind: KindDouble, Payload: int64(math.Float64bits(d))}
}
func StrValue(s string) Value {
	return Value{Kind: KindGuestStr, Len: uint32(len(s)), Payload: int64(uintptr(unsafe.Pointer(unsafe.StringData(s))))}
}
func BlobValue(b []byte) Value {
	if len(b) == 0 {
		return Value{Kind: KindGuestBlob}
	}
	return Value{Kind: KindGuestBlob, Len: uint32(len(b)), Payload: int64(uintptr(unsafe.Pointer(&b[0])))}
}
func HandleValue(h int32) Value { return Value{Kind: KindHandle, Payload: int64(h)} }

// StrBytesValue is a string value over bytes the caller holds, without making a Go string of them.
func StrBytesValue(b []byte) Value {
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

// Bin_ reads a bin of any kind in one import. The status is NotFound for no such bin. (Bin is the typed
// form.)
func Bin_(name string, out *Value) int32 {
	return binValue(strptr(name), int32(len(name)), unsafe.Pointer(out), 16)
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

// Enter opens a cursor over the list or map at a value's position. 0 when it could not.
func Enter(v Value) int32 { return cursorEnterAt(v.Src(), v.At()) }

// Next fills out with the next values of a cursor, 128 at most per import. Returns how many, or a status.
func Next(cursor int32, out []Value) int32 {
	return cursorValues(cursor, unsafe.Pointer(&out[0]), int32(len(out)*16))
}

// Close gives a cursor's slot back.
func Close(cursor int32) { cursorClose(cursor) }

// Get looks a string key up in the map at a value's position, in one import.
func Get(m Value, key string, out *Value) int32 {
	return collGet(m.Src(), m.At(), strptr(key), int32(len(key)), unsafe.Pointer(out), 16)
}

// Digest copies the record's digest (20 bytes) into buf. Returns the length, or a status.
func Digest(buf []byte) int32 { return digest(ptr(buf), int32(len(buf))) }

// KeyType reports the kind of the record's user key, or Absent when none was stored.
func KeyType() Kind { return Kind(keyType()) }

// KeyInt is the user key when it is an integer.
func KeyInt() int64 { return keyInt() }

// KeyBytes copies a string or blob user key into buf. Returns the length, or a status.
func KeyBytes(buf []byte) int32 { return keyBytes(ptr(buf), int32(len(buf))) }

// Generation is the record's modification count.
func Generation() int64 { return gen() }

// Exists reports whether the record exists.
func Exists() bool { return recExists() != 0 }

//==========================================================
// Writing, building, reporting.
//

func SetInt(name string, value int64) int32 {
	return setInt(strptr(name), int32(len(name)), value)
}

func SetDouble(name string, value float64) int32 {
	return setDouble(strptr(name), int32(len(name)), value)
}

func SetStr(name, value string) int32 {
	return setStr(strptr(name), int32(len(name)), strptr(value), int32(len(value)))
}

func SetBlob(name string, value []byte) int32 {
	return setBlob(strptr(name), int32(len(name)), ptr(value), int32(len(value)))
}

// SetHandle writes a built list or map to a bin.
func SetHandle(name string, handle int32) int32 {
	return setHandle(strptr(name), int32(len(name)), handle)
}

// SetValue writes a value read from the record to a bin, by position.
func SetValue(name string, v Value) int32 {
	return setAt(strptr(name), int32(len(name)), v.Src(), v.At())
}

func Remove(name string) int32 { return binRemove(strptr(name), int32(len(name))) }

// Update commits the staged writes to an existing record; Create to one that does not exist yet.
func Update() int32 { return recUpdate() }
func Create() int32 { return recCreate() }

// NewListHandle and NewMapHandle start a collection on the host as a bare handle, for the low-level
// calls; NewList and NewMap are the typed builders.
func NewListHandle(capacity int) int32 { return listNew(int32(capacity)) }
func NewMapHandle(capacity int) int32  { return mapNew(int32(capacity)) }

// NilHandle is a nil value as a handle, for a map entry that means "no such bin" in a record resolution.
func NilHandle() int32 { return valNil() }

// PackHandle packs a built collection into a source the host holds, for reading it back by position
// (Get, Enter); the source lives until the call ends.
func PackHandle(handle int32) int32 { return packHandle(handle) }

// MapHasKey reports whether a built map holds key. It packs the map, so it is for the occasional check,
// not a loop.
func MapHasKey(m int32, key string) bool {
	src := packHandle(m)
	if src <= 0 {
		return false
	}
	var v Value
	return collGet(src, 0, strptr(key), int32(len(key)), unsafe.Pointer(&v), int32(unsafe.Sizeof(v))) == OK
}

func ListPushInt(list int32, value int64) int32 { return listPushInt(list, value) }
func ListPushStr(list int32, value string) int32 {
	return listPushStr(list, strptr(value), int32(len(value)))
}
func ListPushHandle(list, value int32) int32 { return listPush(list, value) }

// ListPushValue appends any value: a scalar by value, guest bytes by address, a stored value by position,
// a built collection by handle.
func ListPushValue(list int32, v Value) int32 {
	switch v.Kind {
	case KindInt:
		return listPushInt(list, v.Payload)
	case KindDouble:
		return listPush(list, valDouble(v.Double()))
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

// ListPushValues appends a batch of values in one import: scalars, guest bytes by address, stored values by
// position, built collections by handle. Returns how many went in.
func ListPushValues(list int32, vals []Value) int32 {
	if len(vals) == 0 {
		return 0
	}
	return listPushValues(list, unsafe.Pointer(&vals[0]), int32(len(vals)*16))
}

func MapPutInt(m int32, key string, value int64) int32 {
	return mapPutStrInt(m, strptr(key), int32(len(key)), value)
}
func MapPutStr(m int32, key, value string) int32 {
	return mapPutStrStr(m, strptr(key), int32(len(key)), strptr(value), int32(len(value)))
}
func MapPutHandle(m int32, key string, value int32) int32 {
	return mapPutStrHandle(m, strptr(key), int32(len(key)), value)
}

// MapPutValue puts any value under a string key: a scalar by value, guest bytes by address, a stored value
// by position, a built collection by handle.
func MapPutValue(m int32, key string, v Value) int32 {
	switch v.Kind {
	case KindInt:
		return mapPutStrInt(m, strptr(key), int32(len(key)), v.Payload)
	case KindDouble:
		return mapPutStrHandle(m, strptr(key), int32(len(key)), valDouble(v.Double()))
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

// MapPutEntries puts a batch of key-value pairs (pairs[0], pairs[1] is the first) in one import. Returns how
// many pairs went in.
func MapPutEntries(m int32, pairs []Value) int32 {
	if len(pairs) < 2 {
		return 0
	}
	return mapPutEntries(m, unsafe.Pointer(&pairs[0]), int32(len(pairs)*16))
}

func Size(handle int32) int { return int(collSize(handle)) }

func ResultNil()             { resultNil() }
func ResultInt(v int64)      { resultInt(v) }
func ResultDouble(v float64) { resultDouble(v) }
func ResultBool(v bool) {
	if v {
		resultBool(1)
	} else {
		resultBool(0)
	}
}
func ResultStr(s string)         { resultStr(strptr(s), int32(len(s))) }
func ResultBlob(b []byte)        { resultBlob(ptr(b), int32(len(b))) }
func ResultHandle(h int32) int32 { return resultHandle(h) }

// ResultValue hands a value read from the record back as the result, built by the server from its bytes.
func ResultValue(v Value) int32 { return resultAt(v.Src(), v.At()) }

// Fail fails the call with a message the client sees, and returns the non-zero status a UDF then returns.
func Fail(message string) int32 {
	fail(strptr(message), int32(len(message)))
	return 1
}

func Log(level int32, message string) { log(level, strptr(message), int32(len(message))) }

// Signature is the text behind a `__udf_sig_<name>` export: the address of a NUL-terminated string, which the
// host serves as the call's result. Keep the string in a package-level variable so its address stays valid.
func Signature(text *[]byte) int32 {
	return int32(uintptr(unsafe.Pointer(&(*text)[0])))
}

//==========================================================
// Events.
//
// An event handler is a function the server calls at a point in its own processing. The first event,
// `xdr-merge`, hands it the local record and the incoming XDR write as two records (Which 0 and 1, read
// through EventRecord.Bin, never the record imports) and a description of the conflict, and takes back a
// verdict or the final record. Export the handler under its name and its signature under
// `__udf_sig_<name>` (see XdrMergeSignature), which `event-set` checks before binding. A handler must be
// pure and symmetric: both DCs resolve the same pair with the sides swapped and must land on the same
// record. Conflict.RemoteNewer is symmetric; "mine first" is not.
//

//go:wasmimport aerospike event_bin_value
func eventBinValue(which int32, name unsafe.Pointer, nameLen int32, out unsafe.Pointer, cap int32) int32

// EventRecord is one side of a conflict.
type EventRecord struct {
	Which   int32 // 0 local, 1 remote
	Gen     int64
	LUT     int64
	TTL     int64
	SrcID   int64
	Bins    Value // list of bin names
	Deleted Value // list of bin names this side deleted; Absent kind when none
	BinLUTs Value // map name -> LUT per bin, when the server keeps one; Absent kind otherwise
	BinSrcs Value // map name -> source id, likewise
}

// Conflict is what the event is about.
type Conflict struct {
	Description         Value // the whole map, for anything not decoded here
	Lost                Value // list: the incoming bins that would lose
	DryRun              bool
	ConflictTrigger     bool        // bound with trigger=conflict: a bin would lose under bin convergence
	BinConvergence      bool        // the policy stood in for is bin-lut rather than a plain apply
	RemotePartial       bool        // the incoming write carries only changed bins
	RemoteDeletedRecord bool        // the incoming write is a record delete; Remote has no bins
	LocalTombstone      bool        // the current record is a tombstone: exists, no bins
	Local               EventRecord // the current record
	Remote              EventRecord // the incoming record
}

// MapGet is a map entry by string key; a value of Absent kind when there is none or m is not a map.
func MapGet(m Value, key string) Value {
	v := Value{Kind: KindAbsent}
	if m.Kind == KindMap {
		Get(m, key, &v)
	}
	return v
}

// MapInt is an integer map entry, or dflt.
func MapInt(m Value, key string, dflt int64) int64 {
	v := MapGet(m, key)
	if v.Kind != KindInt {
		return dflt
	}
	return v.Payload
}

// StrIs reports whether a stored string value reads as text.
func StrIs(v Value, text string) bool {
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

func eventSide(conflict Value, key string, which int32, out *EventRecord) bool {
	m := MapGet(conflict, key)
	if m.Kind != KindMap {
		return false
	}
	out.Which = which
	out.Gen = MapInt(m, "gen", 0)
	out.LUT = MapInt(m, "lut", 0)
	out.TTL = MapInt(m, "ttl", 0)
	out.SrcID = MapInt(m, "src-id", 0)
	out.Bins = MapGet(m, "bins")
	out.Deleted = MapGet(m, "deleted")
	out.BinLUTs = MapGet(m, "bin-luts")
	out.BinSrcs = MapGet(m, "bin-srcs")
	return out.Bins.Kind == KindList
}

// DecodeConflict reads an event call's arguments into out. False when they are not a conflict.
func DecodeConflict(handle, argc int32, out *Conflict) bool {
	var a [1]Value
	if Args(handle, argc, a[:]) < 1 || a[0].Kind != KindMap {
		return false
	}
	out.Description = a[0]
	out.Lost = MapGet(a[0], "lost")
	out.DryRun = StrIs(MapGet(a[0], "mode"), "dry-run")
	out.ConflictTrigger = StrIs(MapGet(a[0], "trigger"), "conflict")
	out.BinConvergence = StrIs(MapGet(a[0], "policy"), "bin-lut")
	if !eventSide(a[0], "current", 0, &out.Local) || !eventSide(a[0], "incoming", 1, &out.Remote) {
		return false
	}
	remote := MapGet(a[0], "incoming")
	out.RemotePartial = MapBool(remote, "partial")
	out.RemoteDeletedRecord = MapBool(remote, "deleted-record")
	out.LocalTombstone = MapBool(MapGet(a[0], "current"), "tombstone")
	return true
}

// Bin reads one of this side's bins. NotFound when the side has no such bin.
func (r *EventRecord) Bin(name string, out *Value) int32 {
	return eventBinValue(r.Which, strptr(name), int32(len(name)), unsafe.Pointer(out), int32(unsafe.Sizeof(*out)))
}

// BinLUT is when this side last wrote the bin: per bin where the server keeps one, the record's otherwise.
func (r *EventRecord) BinLUT(name string) int64 { return MapInt(r.BinLUTs, name, r.LUT) }

// BinSrc is the source id that wrote the bin, likewise.
func (r *EventRecord) BinSrc(name string) int64 { return MapInt(r.BinSrcs, name, r.SrcID) }

// RemoteNewer reports whether the remote side wrote the bin later: by LUT, then source id - the order the
// server's bin convergence uses, so both DCs answer the same.
func (c *Conflict) RemoteNewer(name string) bool {
	ll, rl := c.Local.BinLUT(name), c.Remote.BinLUT(name)
	return rl > ll || (rl == ll && c.Remote.BinSrc(name) > c.Local.BinSrc(name))
}

// EachStr calls fn with every string of a stored list, as bytes into buf.
func EachStr(list Value, buf []byte, fn func(name []byte, v Value)) {
	if list.Kind != KindList {
		return
	}
	cur := Enter(list)
	if cur <= 0 {
		return
	}
	var w [32]Value
	for {
		n := Next(cur, w[:])
		for i := int32(0); i < n; i++ {
			if k := Bytes(w[i], buf); k >= 0 && w[i].Kind == KindStr {
				fn(buf[:k], w[i])
			}
		}
		if n < 32 {
			break
		}
	}
	Close(cur)
}

// ListHasStr reports whether a stored list of strings holds text.
func ListHasStr(list Value, text string) bool {
	found := false
	var buf [64]byte
	EachStr(list, buf[:], func(name []byte, v Value) {
		if !found && string(name) == text {
			found = true
		}
	})
	return found
}

// resolveRecord returns the final record: bins (a built map; a bin absent or nil is deleted) and a TTL
// with a write's meaning, or -1 to keep the one the incoming write asked for.
func resolveRecord(bins int32, ttl int64) int32 {
	out := NewMap(3)
	if out.err != nil {
		return Fail("resolve: could not build the resolution")
	}
	MapPutStr(out.handle, "verdict", "record")
	MapPutHandle(out.handle, "bins", bins)
	if ttl >= 0 {
		MapPutInt(out.handle, "ttl", ttl)
	}
	if ResultHandle(out.handle) != OK {
		return Fail("resolve: could not return the record")
	}
	return 0
}

//----------------------------------------------------------
// The other events. Each handler is an exported `func name(args, argc int32) int32`; EventDesc reads the
// description, the *Info decoders read its parts, EventRecord reads a record (Which 0 for the first record
// the event hands over, 1 for the second), and Answer delivers the answer. Export the signature
// EventSignature gives under `__udf_sig_<name>`.
//

//go:wasmimport aerospike timer_set
func timerSet(name unsafe.Pointer, nameLen int32, delayMs int64) int32

//go:wasmimport aerospike timer_cancel
func timerCancel(name unsafe.Pointer, nameLen int32) int32

// TimerSet runs this set's timer handler on the record in hand delayMs from now, under name. Only from a
// record UDF or a timer handler.
func TimerSet(name string, delayMs int64) int32 {
	return timerSet(strptr(name), int32(len(name)), delayMs)
}

// TimerCancel cancels a timer set on the record in hand.
func TimerCancel(name string) int32 { return timerCancel(strptr(name), int32(len(name))) }

// EventDesc is the description map of an event call; false when the arguments are not one.
func EventDesc(handle, argc int32, out *Value) bool {
	var a [1]Value
	if Args(handle, argc, a[:]) < 1 || a[0].Kind != KindMap {
		return false
	}
	*out = a[0]
	return true
}

// EventSide fills an EventRecord from the description's entry (`before`, `after`, `record`, `local`,
// `remote`); which is the index the record is read by.
func EventSide(desc Value, key string, which int32, out *EventRecord) bool {
	return eventSide(desc, key, which, out)
}

// MapStr is a string entry of a map, or "" when absent.
func MapStr(m Value, key string) string {
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

// WriteInfo describes the write a before-write or after-write handler is asked about.
type WriteInfo struct {
	Source        string // client, batch, udf, ops, xdr
	Generation    int64  // the generation the client asked to match, 0 for none
	TTL           int64  // the TTL the write asked for, with a write's meaning
	Create        bool   // the record did not exist
	DurableDelete bool
}

func EventWriteInfo(desc Value, out *WriteInfo) bool {
	m := MapGet(desc, "write")
	if m.Kind != KindMap {
		return false
	}
	out.Source = MapStr(m, "source")
	out.Generation = MapInt(m, "generation", 0)
	out.TTL = MapInt(m, "ttl", 0)
	out.Create = MapBool(m, "create")
	out.DurableDelete = MapBool(m, "durable-delete")
	return true
}

type DeleteInfo struct {
	Source  string
	Durable bool
}

func EventDeleteInfo(desc Value, out *DeleteInfo) bool {
	m := MapGet(desc, "delete")
	if m.Kind != KindMap {
		return false
	}
	out.Source = MapStr(m, "source")
	out.Durable = MapBool(m, "durable")
	return true
}

type TruncateInfo struct {
	Namespace string
	Set       string // "" for the whole namespace
	LUT       int64  // records last updated before this (ms since the epoch) go
}

func EventTruncateInfo(desc Value, out *TruncateInfo) bool {
	m := MapGet(desc, "truncate")
	if m.Kind != KindMap {
		return false
	}
	out.Namespace = MapStr(m, "namespace")
	out.Set = MapStr(m, "set")
	out.LUT = MapInt(m, "lut", 0)
	return true
}

// ExpireInfo describes an expiry (before-expire) or an eviction (before-evict).
type ExpireInfo struct {
	VoidTime      int64 // when the record was due, seconds since the Aerospike epoch
	Now           int64
	EvictVoidTime int64 // before-evict: records due before this are being evicted
}

func EventExpireInfo(desc Value, out *ExpireInfo) bool {
	m := MapGet(desc, "expire")
	if m.Kind != KindMap {
		m = MapGet(desc, "evict")
	}
	if m.Kind != KindMap {
		return false
	}
	out.VoidTime = MapInt(m, "void-time", 0)
	out.Now = MapInt(m, "now", 0)
	out.EvictVoidTime = MapInt(m, "evict-void-time", 0)
	return true
}

type ReadInfo struct {
	Source string
	All    bool // every bin was asked for, not named ones
}

func EventReadInfo(desc Value, out *ReadInfo) bool {
	m := MapGet(desc, "read")
	if m.Kind != KindMap {
		return false
	}
	out.Source = MapStr(m, "source")
	out.All = MapBool(m, "all")
	return true
}

type ShipInfo struct {
	DC string
}

func EventShipInfo(desc Value, out *ShipInfo) bool {
	m := MapGet(desc, "ship")
	if m.Kind != KindMap {
		return false
	}
	out.DC = MapStr(m, "dc")
	return true
}

type LifecycleInfo struct {
	Phase   string // service-ready, shutdown, binding-deployed
	Node    string // this node's id, hex
	Binding Value  // binding-deployed: a map of event, namespace, set, name, module, function; else Absent
}

func EventLifecycleInfo(desc Value, out *LifecycleInfo) bool {
	m := MapGet(desc, "lifecycle")
	if m.Kind != KindMap {
		return false
	}
	out.Phase = MapStr(m, "phase")
	out.Node = MapStr(m, "node")
	out.Binding = MapGet(m, "binding")
	return true
}

type ClusterInfo struct {
	Key   string
	Size  int64
	Self  string
	Nodes Value // list of hex node ids in succession order
}

func EventClusterInfo(desc Value, out *ClusterInfo) bool {
	m := MapGet(desc, "cluster")
	if m.Kind != KindMap {
		return false
	}
	out.Key = MapStr(m, "key")
	out.Self = MapStr(m, "self")
	out.Size = MapInt(m, "size", 0)
	out.Nodes = MapGet(m, "nodes")
	return true
}

type TransferInfo struct {
	Namespace string
	Partition int64
	Arrived   bool // true: arrived here; false: departed
	Peer      string
}

func EventTransferInfo(desc Value, out *TransferInfo) bool {
	m := MapGet(desc, "transfer")
	if m.Kind != KindMap {
		return false
	}
	out.Namespace = MapStr(m, "namespace")
	out.Partition = MapInt(m, "partition", 0)
	out.Arrived = MapStr(m, "direction") == "arrived"
	out.Peer = MapStr(m, "peer")
	return true
}

// TimerInfo is a timer handler's argument: the timer that came due. The handler runs as a record UDF.
type TimerInfo struct {
	Name string
	Due  int64
	Now  int64
}

func EventTimerInfo(handle, argc int32, out *TimerInfo) bool {
	var a [1]Value
	if Args(handle, argc, a[:]) < 1 || a[0].Kind != KindMap {
		return false
	}
	out.Name = MapStr(a[0], "name")
	out.Due = MapInt(a[0], "due", 0)
	out.Now = MapInt(a[0], "now", 0)
	return true
}

// answer delivers a verdict word with a code, a message, a bins map and a TTL where the kind takes them
// (0, "", 0 and -1 for none). Returns the status the handler returns.
func answer(verdict string, code int64, message string, bins int32, ttl int64) int32 {
	if code == 0 && message == "" && bins == 0 && ttl < 0 {
		ResultStr(verdict)
		return 0
	}
	out := NewMap(5)
	if out.err != nil {
		return Fail("answer: could not build the answer")
	}
	MapPutStr(out.handle, "verdict", verdict)
	if code != 0 {
		MapPutInt(out.handle, "code", code)
	}
	if message != "" {
		MapPutStr(out.handle, "message", message)
	}
	if bins != 0 {
		MapPutHandle(out.handle, "bins", bins)
	}
	if ttl >= 0 {
		MapPutInt(out.handle, "ttl", ttl)
	}
	if ResultHandle(out.handle) != OK {
		return Fail("answer: could not return the answer")
	}
	return 0
}

// EventSignature is the signature text for a handler of `kind` with the given parameter text, for example
// EventSignature("before-write", "guard", "(before: record, after: record, write: write) -> decision").
// NUL-terminated; keep it in a package-level variable and serve it with Signature.
func EventSignature(kind, name, params string) []byte {
	return []byte("event " + kind + "/1 " + name + params + "\x00")
}

// The parameter text of each kind, for EventSignature.
const (
	ParamsBeforeWrite       = "(before: record, after: record, write: write) -> decision"
	ParamsBeforeDelete      = "(record: record, delete: delete) -> gate"
	ParamsAfterWrite        = "(before: record, after: record, write: write) -> ()"
	ParamsAfterDelete       = "(record: record, delete: delete) -> ()"
	ParamsBeforeExpire      = "(record: record, expire: expire) -> expiry"
	ParamsBeforeEvict       = "(record: record, evict: evict) -> eviction"
	ParamsAfterRead         = "(record: record, read: read) -> read-result"
	ParamsBeforeTruncate    = "(truncate: truncate) -> gate"
	ParamsAfterTruncate     = "(truncate: truncate) -> ()"
	ParamsBeforeShip        = "(record: record, ship: ship) -> ship-decision"
	ParamsPartitionTransfer = "(transfer: transfer) -> ()"
	ParamsClusterChange     = "(cluster: cluster) -> ()"
	ParamsNodeLifecycle     = "(lifecycle: lifecycle) -> ()"
	ParamsTimer             = "(timer: timer) -> ()"
)

// XdrMergeSignature is the signature text an xdr-merge handler exports under `__udf_sig_<name>`,
// NUL-terminated. Keep it in a package-level variable and serve it with Signature.
func XdrMergeSignature(name string) []byte {
	return []byte("event xdr-merge/1 " + name + "(current: record, incoming: record, conflict: conflict) -> resolution\x00")
}

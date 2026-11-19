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

package udf

import (
	"strconv"
	"unsafe"
)

//==========================================================
// The record: the Go face of the Rust SDK's Record and ReadRecord.
//

//go:wasmimport aerospike set_bool
func setBool(name unsafe.Pointer, nameLen int32, value int32) int32

//go:wasmimport aerospike set_nil
func setNil(name unsafe.Pointer, nameLen int32) int32

//go:wasmimport aerospike set_geojson
func setGeoJSON(name unsafe.Pointer, nameLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike set_list_ints
func setListInts(name unsafe.Pointer, nameLen int32, vals unsafe.Pointer, valsLen int32) int32

//go:wasmimport aerospike set_list_doubles
func setListDoubles(name unsafe.Pointer, nameLen int32, vals unsafe.Pointer, valsLen int32) int32

//go:wasmimport aerospike last_update
func lastUpdate() int64

//go:wasmimport aerospike record_size
func recordSize() int64

//go:wasmimport aerospike numbins
func numbins() int64

//go:wasmimport aerospike set_name
func setName(out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike set_ttl
func setTTL(seconds int32) int32

//go:wasmimport aerospike rec_remove
func recRemove() int32

// RecordReader is what any record can be asked: the one a record UDF is called on (*Record) and either
// side of an event (*EventRecord). A helper that takes a RecordReader reads a bin the same way from
// both.
//
// Bin is the form for a bin that must be there: an error naming the bin when it is not. The others
// are for a bin that may not be, and have three outcomes a caller tells apart without reading a
// message: the value and true; the zero value and false, with a nil error, when the bin is absent or
// nil; and an error, an *Error with Status TypeMismatch, when the bin holds another kind than the
// one asked for. Nothing is converted: an integer is not a double, a string is not a blob.
//
//	// The balance of any record: the one a UDF is called on, or a side of a merge.
//	func balanceOf(rec udf.RecordReader) (int64, error) {
//		return rec.IntOr("balance", 0) // 0 for no bin; an error for a bin that is not an integer
//	}
type RecordReader interface {
	// Bin is a bin, whatever it holds; an error naming the bin when the record has none by that name.
	Bin(name string) (Value, error)
	// Get is a bin that may be absent, whatever it holds: false, and a Value of KindAbsent, when it
	// is absent or nil; an error when it could not be read.
	Get(name string) (Value, bool, error)
	// Has reports whether the record has the bin.
	Has(name string) bool
	// Int is an integer bin; false when absent or nil, an error when of another kind.
	Int(name string) (int64, bool, error)
	// Double is a double bin; false when absent or nil, an error when of another kind, an integer
	// included.
	Double(name string) (float64, bool, error)
	// Bool is a boolean bin; the second result is false when absent or nil; an error when of
	// another kind.
	Bool(name string) (bool, bool, error)
	// String is the text of a string bin, copied out; false when absent or nil, an error when of
	// another kind, a GeoJSON included.
	String(name string) (string, bool, error)
	// Blob is the bytes of a blob bin, copied out; false when absent or nil, an error when of
	// another kind, a string, GeoJSON or HLL included.
	Blob(name string) ([]byte, bool, error)
	// Bytes is the raw bytes of a string, blob, GeoJSON or HLL bin, copied out; false when absent
	// or nil, an error when the bin is a scalar, a list or a map.
	Bytes(name string) ([]byte, bool, error)
	// IntOr is an integer bin, or dflt when it is absent or nil; an error when of another kind.
	IntOr(name string, dflt int64) (int64, error)
	// DoubleOr is a double bin, or dflt when it is absent or nil; an error when of another kind.
	DoubleOr(name string, dflt float64) (float64, error)
	// BoolOr is a boolean bin, or dflt when it is absent or nil; an error when of another kind.
	BoolOr(name string, dflt bool) (bool, error)
	// List is a list bin, as the stored Value to range over; false when absent or nil, an error when
	// of another kind.
	List(name string) (Value, bool, error)
	// Map is a map bin, as the stored Value to range over or look up in; false when absent or nil,
	// an error when of another kind.
	Map(name string) (Value, bool, error)
	// BinCount is how many bins the record has. Of the record a UDF is called on, a bin that is staged
	// is not counted until it is saved.
	BinCount() int64
	// Generation is the record's modification count; 0 on either side of an xdr-merge.
	Generation() int64
	// TTL is the record's time to live, in seconds; 0 when it does not expire.
	TTL() int64
	// LastUpdateTime is when the record was last written, in milliseconds since the Citrusleaf epoch,
	// 2010-01-01T00:00:00Z (Unix milliseconds minus 1 262 304 000 000); 0 on either side of an
	// xdr-merge, and on a key with no record.
	LastUpdateTime() int64
}

// binValueOf is a bin read's status and value as the three outcomes of Get.
func binValueOf(name string, v Value, rc int32) (Value, bool, error) {
	if rc == NotFound {
		return Value{Kind: KindAbsent}, false, nil
	}
	if rc != OK {
		return Value{Kind: KindAbsent}, false, statusError(rc, "bin "+name+" could not be read")
	}
	if v.Kind == KindNil {
		return Value{Kind: KindAbsent}, false, nil
	}
	return v, true, nil
}

// wrongKind is the error of a typed reader whose bin holds another kind.
func wrongKind(name, want string, v Value) error {
	return errorf(TypeMismatch, "bin ", name, " is not ", want, " (kind ", strconv.Itoa(int(v.Kind)), ")")
}

// The typed readers, written once over Get for both kinds of record.

func readInt(r RecordReader, name string) (int64, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return 0, false, err
	}
	if v.Kind != KindInt {
		return 0, false, wrongKind(name, "an int", v)
	}
	return v.Payload, true, nil
}

func readDouble(r RecordReader, name string) (float64, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return 0, false, err
	}
	if v.Kind != KindDouble {
		return 0, false, wrongKind(name, "a double", v)
	}
	return v.double(), true, nil
}

func readBool(r RecordReader, name string) (bool, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return false, false, err
	}
	if v.Kind != KindBool {
		return false, false, wrongKind(name, "a bool", v)
	}
	return v.Payload != 0, true, nil
}

func readString(r RecordReader, name string) (string, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return "", false, err
	}
	if v.Kind != KindStr {
		return "", false, wrongKind(name, "a string", v)
	}
	s, err := v.String()
	return s, err == nil, err
}

func readBlob(r RecordReader, name string) ([]byte, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return nil, false, err
	}
	if v.Kind != KindBlob {
		return nil, false, wrongKind(name, "a blob", v)
	}
	b, err := v.Bytes()
	return b, err == nil, err
}

func readBytes(r RecordReader, name string) ([]byte, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return nil, false, err
	}
	switch v.Kind {
	case KindStr, KindBlob, KindGeoJSON, KindHLL:
		b, err := v.Bytes()
		return b, err == nil, err
	}
	return nil, false, wrongKind(name, "a string, blob, GeoJSON or HLL", v)
}

func readKind(r RecordReader, name string, kind Kind, want string) (Value, bool, error) {
	v, ok, err := r.Get(name)
	if err != nil || !ok {
		return Value{Kind: KindAbsent}, false, err
	}
	if v.Kind != kind {
		return Value{Kind: KindAbsent}, false, wrongKind(name, want, v)
	}
	return v, true, nil
}

func readIntOr(r RecordReader, name string, dflt int64) (int64, error) {
	x, ok, err := r.Int(name)
	if err != nil || !ok {
		return dflt, err
	}
	return x, nil
}

func readDoubleOr(r RecordReader, name string, dflt float64) (float64, error) {
	x, ok, err := r.Double(name)
	if err != nil || !ok {
		return dflt, err
	}
	return x, nil
}

func readBoolOr(r RecordReader, name string, dflt bool) (bool, error) {
	x, ok, err := r.Bool(name)
	if err != nil || !ok {
		return dflt, err
	}
	return x, nil
}

// Record is the record a UDF was called on.
//
// A record UDF, and the per-record method of a stream UDF, takes it as its first parameter, before the
// call's arguments. It is not one of them: a client does not pass it, it is not counted when the
// arguments are checked, and the function's signature export does not list it. A function that does not
// touch the record leaves the parameter out.
//
//	//udf:function withdraw
//	func withdraw(rec *udf.Record, amount int64) (int64, error) {
//		balance, err := rec.Bin("balance")
//		if err != nil {
//			return 0, err
//		}
//		have, err := balance.Int()
//		if err != nil {
//			return 0, err
//		}
//		if err := rec.Set("balance", have-amount); err != nil {
//			return 0, err
//		}
//		return have - amount, rec.Save()
//	}
//
// A bin is addressed by its name, 1 to 15 bytes with no zero byte among them, on each call: there is
// no bin object. A bin write is staged: Set and Remove change the record in hand, a read after them
// sees what was staged, and nothing reaches the server's record until Save. Delete is the exception
// and needs no Save.
//
// A call that fails writes nothing, even what it had already saved: when the function returns an error
// (or calls Fail), a Save that succeeded earlier in the call, and a SetInts or SetDoubles, are undone.
//
// There is one Record type, for the places that may write and the places that may only read. Where the
// host does not take a write - the record a stream UDF is shown - every writing method returns an error
// that says so, and nothing is changed.
//
// The reading methods are also on RecordReader, which *EventRecord implements too.
type Record struct {
	// It holds nothing, because the host knows which record a call is on.
	_ [0]byte
}

var current Record

// CurrentRecord is the record of the call in progress. The generated exports pass it as the first
// parameter of a function that declares one; it is for an export written without udfgen. Every call
// answers the same pointer, which holds nothing: the host knows which record a call is on.
//
// An event handler has no such record. There its bins all read as absent and Save is refused; a
// handler reads the two EventRecords it is given.
func CurrentRecord() *Record { return &current }

//----------------------------------------------------------
// Reading bins.
//

// Bin is a bin, whatever it holds, in one import. It is the form for a bin that must be there: the
// error names the bin when the record has none by that name, and its Status is NotFound. A nil bin is
// absent too, since a bin cannot hold nil, and so is a name that is empty or longer than 15 bytes.
//
// The value is what the call has staged for the bin, if it has staged anything, and what is stored
// otherwise. Reading a string, blob, list or map bin takes one of the 256 handles a call may hold,
// each time it is read: read such a bin once and keep the Value.
//
//	balance, err := rec.Bin("balance")
//	if err != nil {
//		return 0, err // "bin balance is absent"
//	}
//	have, err := balance.Int()
func (r *Record) Bin(name string) (Value, error) {
	var v Value
	rc := binValue(strptr(name), int32(len(name)), unsafe.Pointer(&v), 16)
	if rc == NotFound {
		return Value{Kind: KindAbsent}, errorf(rc, "bin ", name, " is absent")
	}
	if rc != OK {
		return Value{Kind: KindAbsent}, statusError(rc, "bin "+name+" could not be read")
	}
	return v, nil
}

// Get is a bin that may be absent, whatever it holds: the value and true, or a Value of KindAbsent
// and false, with a nil error, when the bin is absent or nil. It is the untyped form of the typed
// readers. It allocates nothing for an absent bin, where Bin builds an error, so it is the form for a
// stream's per-record path.
//
// The error is the bin that could not be read at all, which is not the same as absent: Status Err
// for a name that is not a bin name - empty, longer than 15 bytes - or for a call that holds its 256
// handles.
func (r *Record) Get(name string) (Value, bool, error) {
	var v Value
	rc := binValue(strptr(name), int32(len(name)), unsafe.Pointer(&v), 16)
	return binValueOf(name, v, rc)
}

// Has reports whether the record has the bin: false when it is absent, or when the name is not a bin
// name. A bin the call has staged counts, and one it has removed does not. It costs the same one
// import as reading the bin, and for a string, blob, list or map bin the same handle.
func (r *Record) Has(name string) bool {
	_, ok, _ := r.Get(name)
	return ok
}

// Int is an integer bin. It has three outcomes: the value and true; 0 and false, with a nil error,
// when the bin is absent or nil; and an error, an *Error with Status TypeMismatch, when the bin
// holds another kind. Nothing is converted: a double is not narrowed.
//
// The typed readers - Int, Double, Bool, String, Blob, Bytes, List, Map - all answer this way, and
// IntOr, DoubleOr and BoolOr take a default for the absent bin in the same call. The default never
// stands in for a bin of another kind.
//
//	visits, ok, err := rec.Int("visits") // ok is false when there is no bin "visits"
//	if err != nil {
//		return err // "bin visits is not an int (kind 4)"
//	}
//	limit, err := rec.IntOr("limit", 100)
func (r *Record) Int(name string) (int64, bool, error) { return readInt(r, name) }

// Double is a double bin; false when the bin is absent or nil, and an error with Status TypeMismatch
// when it holds another kind. An integer bin is another kind: it is not widened.
func (r *Record) Double(name string) (float64, bool, error) { return readDouble(r, name) }

// Bool is a boolean bin. The second result says whether there is one: it is false when the bin is
// absent or nil, and the first result is then false too. A bin of another kind, an integer included,
// is an error with Status TypeMismatch.
func (r *Record) Bool(name string) (bool, bool, error) { return readBool(r, name) }

// String is the text of a string bin, copied into a new Go string; false, and "", when the bin is
// absent or nil. A bin of another kind is an error with Status TypeMismatch, a GeoJSON bin and a blob
// bin included; Bytes reads those.
func (r *Record) String(name string) (string, bool, error) { return readString(r, name) }

// Blob is the bytes of a blob bin, copied into a new slice the caller owns; false, and nil, when the
// bin is absent or nil. It is strict: a bin of any other kind is an error with Status TypeMismatch,
// and a string, a GeoJSON and an HLL bin are other kinds. Bytes reads the raw bytes of all four.
func (r *Record) Blob(name string) ([]byte, bool, error) { return readBlob(r, name) }

// Bytes is the raw bytes of a string, blob, GeoJSON or HLL bin, copied into a new slice the caller
// owns: a string's text, a GeoJSON's document, a blob's and an HLL's bytes as stored. False, and
// nil, when the bin is absent or nil; an error with Status TypeMismatch when the bin is a scalar, a
// list or a map. It is the reader for code that takes bytes whatever they are labelled as, where Blob
// and String each take one kind.
func (r *Record) Bytes(name string) ([]byte, bool, error) { return readBytes(r, name) }

// IntOr is an integer bin, or dflt when it is absent or nil. The default stands in for a missing bin
// only: a bin of another kind is the same error Int answers, with Status TypeMismatch.
func (r *Record) IntOr(name string, dflt int64) (int64, error) { return readIntOr(r, name, dflt) }

// DoubleOr is a double bin, or dflt when it is absent or nil. A bin of another kind, an integer
// included, is an error with Status TypeMismatch.
func (r *Record) DoubleOr(name string, dflt float64) (float64, error) {
	return readDoubleOr(r, name, dflt)
}

// BoolOr is a boolean bin, or dflt when it is absent or nil. A bin of another kind is an error with
// Status TypeMismatch.
func (r *Record) BoolOr(name string, dflt bool) (bool, error) { return readBoolOr(r, name, dflt) }

// List is the list in a bin, as the stored Value: range over it with Value.List, read one element
// with Value.Index, and its Len is the number of elements. The three outcomes tell an absent bin, a
// bin of another kind and an empty list apart: false with a nil error when the bin is absent or nil;
// an error with Status TypeMismatch when it holds anything but a list, a map included; and the list
// and true, with Len 0, when it is empty.
//
//	amounts, ok, err := rec.List("amounts")
//	if err != nil {
//		return 0, err
//	}
//	if !ok {
//		return 0, nil // no such bin
//	}
//	var total int64
//	for v := range amounts.List() {
//		n, err := v.Int()
//		if err != nil {
//			return 0, err
//		}
//		total += n
//	}
func (r *Record) List(name string) (Value, bool, error) { return readKind(r, name, KindList, "a list") }

// Map is the map in a bin, as the stored Value: range over its entries with Value.Map, read one with
// Value.Lookup or Value.LookupInt, and its Len is the number of entries. The outcomes are List's:
// false when the bin is absent or nil, an error with Status TypeMismatch when it holds anything but a
// map, and the map and true, with Len 0, when it is empty.
//
//	counts, _, err := rec.Map("counts")
//	if err != nil {
//		return "", err
//	}
//	var best int64
//	var where string
//	for site, count := range counts.Map() { // nothing, when there is no such bin
//		if n, err := count.Int(); err == nil && n > best {
//			best = n
//			where, _ = site.String() // the entry's key
//		}
//	}
func (r *Record) Map(name string) (Value, bool, error) { return readKind(r, name, KindMap, "a map") }

// BinCount is how many bins the record has. A bin that is staged is not counted until it is saved,
// though Has and the readers see it at once. It is 0 on a key with no record.
func (r *Record) BinCount() int64 { return numbins() }

//----------------------------------------------------------
// Writing bins.
//

// writeError is a write native's status as an error, nil for OK. A refusal is spelled out, since it is
// what a write gets where the record may only be read.
func writeError(rc int32, what string) error {
	if rc == Refused {
		return errorf(rc, what, ": the host refused the write; the record is read-only here, as in a stream, or the server would not take it")
	}
	return statusError(rc, what)
}

// Set writes a value to a bin, staged until Save: an int, int32, int64, float32, float64, bool, string,
// []byte (a blob), Value, *Map or *List. Any other Go type - an unsigned integer, a Go slice or map -
// is an error, and nothing is written.
//
// A Value read from a record is written back from its stored bytes, without the guest decoding it, which
// is how a sub-document is copied from one bin to another and how a GeoJSON or HLL keeps its kind. A
// built *Map or *List is handed over whole; when it kept a refusal while it was built, that error is
// returned and nothing is written. A string or blob is copied by the host during the call.
// udf.GeoJSONValue(doc) writes a GeoJSON document as the geospatially indexable kind.
//
// Writing nil removes the bin, as Remove does: a bin cannot hold nil.
//
// The error is an *Error. Its Status is Err when the name is empty, longer than 15 bytes or has a zero
// byte in it; NotFound for a Value of KindAbsent; TypeMismatch for a Go type or a Value that cannot be
// written, and for a blob written over a bin that holds bytes of another subtype such as an HLL, which
// it would relabel; and Refused when
// the host would not take the write: the record is read-only here, as in a stream, the value nests
// lists and maps more than 64 deep, or the call has spent what it may build on the host.
//
//	if err := rec.Set("owner", owner); err != nil {
//		return err
//	}
//	if err := rec.Set("tags", udf.NewList(2).Add("new").Add("trial")); err != nil {
//		return err
//	}
//	return rec.Save() // nothing is stored until here
func (r *Record) Set(name string, value any) error {
	v, ok := valueOf(value)
	if !ok {
		return errorf(TypeMismatch, "could not write bin ", name, ": unsupported Go type")
	}
	if c, is := value.(*Map); is && c.err != nil {
		return c.err
	}
	if c, is := value.(*List); is && c.err != nil {
		return c.err
	}

	n, nl := strptr(name), int32(len(name))
	var rc int32

	switch v.Kind {
	case KindInt:
		rc = setInt(n, nl, v.Payload)
	case KindDouble:
		rc = setDouble(n, nl, v.double())
	case KindBool:
		rc = setBool(n, nl, int32(v.Payload))
	case KindNil:
		rc = setNil(n, nl)
	case KindGuestStr:
		rc = setStr(n, nl, guestPtr(v), int32(v.Len))
	case KindGuestBlob:
		rc = setBlob(n, nl, guestPtr(v), int32(v.Len))
	case KindGuestGeoJSON:
		rc = setGeoJSON(n, nl, guestPtr(v), int32(v.Len))
	case KindHandle:
		rc = setHandle(n, nl, int32(v.Payload))
	case KindAbsent:
		return errorf(NotFound, "could not write an absent value to bin ", name)
	case KindOther:
		return errorf(TypeMismatch, "could not write bin ", name, ": the value is of a kind a UDF cannot write")
	default:
		rc = setAt(n, nl, v.Src(), v.At())
	}

	return writeError(rc, "could not write bin "+name)
}

// SetInts writes a slice of integers to a bin as a list in one import, no value built per element. An
// empty or nil slice writes an empty list.
//
// It is not staged. The host applies it to the record at once, as one write operation of its own,
// which is committed when the function returns, so Save is not needed for it. It is undone, like
// everything else the call wrote, when the call then fails. The error is as Set's: Err for a bad
// name, Refused where the record is read-only, and Refused on a key with no record, where it creates
// nothing: such a record is created by Set and Save. After a refusal by the server, as after a Save
// that fails, a list, map or blob read earlier in the call has to be read again.
func (r *Record) SetInts(name string, values []int64) error {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return writeError(setListInts(strptr(name), int32(len(name)), p, int32(len(values)*8)), "could not write bin "+name)
}

// SetDoubles writes a slice of doubles to a bin as a list in one import. It is SetInts for float64:
// applied at once and not staged, undone when the call then fails, an empty slice writing an empty
// list, with the same errors, the refusal on a key with no record included.
func (r *Record) SetDoubles(name string, values []float64) error {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return writeError(setListDoubles(strptr(name), int32(len(name)), p, int32(len(values)*8)), "could not write bin "+name)
}

// SetBlob writes bytes to a bin as a blob, staged until Save; Set with a []byte does the same. The
// host copies the bytes during the call. A nil or empty value is an empty blob. The error is as
// Set's, with Status TypeMismatch when the bin holds bytes of another subtype, such as an
// HLL, which a plain blob would relabel.
func (r *Record) SetBlob(name string, value []byte) error {
	return writeError(setBlob(strptr(name), int32(len(name)), ptr(value), int32(len(value))), "could not write bin "+name)
}

// SetGeoJSON writes a GeoJSON document to a bin as the geospatially indexable kind, staged until Save;
// Set with a GeoJSONValue does the same. Written as a string, with Set, the same text would be a
// string bin and not queryable by region.
//
// Staging takes any text. It is Save that fails when doc is not a document the server would accept
// as a client's write. The other errors are as Set's.
func (r *Record) SetGeoJSON(name string, doc string) error {
	return writeError(setGeoJSON(strptr(name), int32(len(name)), strptr(doc), int32(len(doc))), "could not write bin "+name)
}

// Remove removes a bin, staged until Save like a write; it is Set with nil. Removing a bin that is
// not there is not an error. The error is as Set's: Err for a name that is not a bin name, Refused
// where the record is read-only.
func (r *Record) Remove(name string) error {
	return writeError(binRemove(strptr(name), int32(len(name))), "could not remove bin "+name)
}

//----------------------------------------------------------
// Metadata.
//

// Exists reports whether the record exists. A UDF can be called on a key that has no record yet, and
// Save then creates it; a function that must not overwrite checks this first. A Delete made earlier
// in the call does not change the answer.
//
// On a key with no record it is false; Generation, LastUpdateTime, Size and BinCount are 0, there is
// no TTL, every bin reads as absent, and reading creates nothing.
func (r *Record) Exists() bool { return recExists() != 0 }

// Generation is the record's generation, its modification count; 0 on a key with no record.
func (r *Record) Generation() int64 { return gen() }

// TTL is the time the record has left to live, in seconds; 0 when it never expires. A key with no
// record has no TTL.
func (r *Record) TTL() int64 { return ttl() }

// SetTTL sets the record's time to live, in seconds: 0 for the namespace default, -1 for never expire,
// subject to the namespace's rule on TTL reductions. It is the TTL of the write, so it takes effect when
// Save writes the record, and does nothing without one. With -1 the record never expires.
//
// On a key with no record a TTL is not a bin: a Save with a TTL set and no bin staged creates nothing,
// and the call fails with "key not found".
//
// The error has Status Refused where there is no record that can take a TTL: a stream's record, and
// an event handler.
func (r *Record) SetTTL(seconds int32) error {
	return writeError(setTTL(seconds), "could not set the TTL")
}

// LastUpdateTime is when the record was last written, in milliseconds since the Citrusleaf epoch,
// 2010-01-01T00:00:00Z, which is how the server stores it: Unix milliseconds minus 1 262 304 000 000.
// It is not on the same scale as Now, which counts from the Unix epoch. It is 0 on a key with no
// record.
func (r *Record) LastUpdateTime() int64 { return lastUpdate() }

// Size is the record's size on device, in bytes; 0 on a key with no record.
func (r *Record) Size() int64 { return recordSize() }

// SetName is the name of the set the record is in, "" when it is in no set. It reads the set; it does
// not write anything, the "Set" being the noun.
func (r *Record) SetName() string {
	var buf [64]byte
	n := setName(unsafe.Pointer(&buf[0]), int32(len(buf)))
	if n <= NeededBase {
		// Longer than a set name can be today; read it whole all the same.
		long := make([]byte, int(-n)-16)
		if n = setName(ptr(long), int32(len(long))); n <= 0 {
			return ""
		}
		return string(long[:n])
	}
	if n <= 0 {
		return ""
	}
	return string(buf[:n])
}

// Key is the record's user key, when the record was written with the key stored: an integer Value, or
// a string or blob whose bytes are copied into buf.
//
// It has three outcomes. The key and true. A Value of KindAbsent and false, with a nil error, when
// the record carries no key: the server stores one only when the client asks it to. And an error
// when buf is too small for a string or blob key: nothing was copied, the key is never cut, and the
// error is an *Error whose Status is at or below NeededBase and whose Needed is the length to call
// again with. A user key is at most a few kilobytes.
//
// A string or blob key points at buf and is not a copy: it is good until buf is written again, and
// that includes the string Value.String makes of it. An integer key does not use buf.
//
//	var buf [64]byte
//	key, ok, err := rec.Key(buf[:])
//	if err != nil {
//		return err // a key longer than 64 bytes: err.(*udf.Error).Needed() says how long
//	}
//	if ok {
//		out.Put("key", key) // copied by the host here
//	}
func (r *Record) Key(buf []byte) (Value, bool, error) {
	kind := Kind(keyType())
	switch kind {
	case KindInt:
		return IntValue(keyInt()), true, nil
	case KindStr, KindBlob:
		n := keyBytes(ptr(buf), int32(len(buf)))
		if n == NotFound {
			return Value{Kind: KindAbsent}, false, nil
		}
		if n <= NeededBase {
			return Value{Kind: KindAbsent}, false, errorf(n, "the buffer is too small for the record's key: it needs ", strconv.Itoa(int(-n)-16), " bytes")
		}
		if n < 0 {
			return Value{Kind: KindAbsent}, false, statusError(n, "could not read the record's key")
		}
		if kind == KindStr {
			return StringBytesValue(buf[:n]), true, nil
		}
		return BlobValue(buf[:n]), true, nil
	case KindAbsent:
		return Value{Kind: KindAbsent}, false, nil
	}
	return Value{Kind: KindAbsent}, false, errorf(TypeMismatch, "the record's key is of a kind a UDF cannot read (kind ", strconv.Itoa(int(kind)), ")")
}

// Digest copies the record's digest into buf and returns that part of it: 20 bytes, always present. It
// is what identifies a record a UDF wants to point at rather than ship - a top-k hit, say - since a
// client can fetch a record by its digest afterwards.
//
// buf must hold 20 bytes. A shorter one is an error, with a Status at or below NeededBase, and
// nothing is copied; the digest is never cut. It allocates nothing.
//
//	var buf [20]byte
//	digest, err := rec.Digest(buf[:])
//	if err != nil {
//		return err
//	}
//	out.Put("digest", digest) // a blob
func (r *Record) Digest(buf []byte) ([]byte, error) {
	n := digest(ptr(buf), int32(len(buf)))
	if n < 0 {
		return nil, statusError(n, "could not read the digest")
	}
	return buf[:n], nil
}

//----------------------------------------------------------
// Saving and deleting.
//

// Save writes the staged bins to the server's record, creating the record when it does not exist yet.
//
// It is the one way a staged write takes effect: a UDF that returns without saving has changed nothing.
// A UDF that must not overwrite checks Exists first; the record is locked for the whole call, so nothing
// can come between the check and the save.
//
// With nothing staged it is still a write: the server treats it as a touch, and the record's generation
// and last-update time move. On a key with no record a Save with a bin staged creates the record. A Save
// with no bin staged - nothing at all, or only a Remove or a SetTTL - creates nothing, since a record has
// to have a bin, and the call fails: the client is answered "key not found".
//
// A Save that succeeded is not final until the call succeeds. When the function then returns an error,
// or calls Fail, what it saved is undone with the rest: a call that fails writes nothing.
//
// The error has Status Refused: the record is read-only here, as a stream's is, or the server would
// not take the write, as when a GeoJSON bin was staged with text that is not GeoJSON. After a Save
// that fails, a list, map or blob Value read earlier in the call answers an
// error; read the bin again before using it.
//
//	if rec.Exists() {
//		return false, nil // must not overwrite
//	}
//	if err := rec.Set("owner", owner); err != nil {
//		return false, err
//	}
//	return true, rec.Save() // creates the record
func (r *Record) Save() error {
	if recExists() != 0 {
		return writeError(recUpdate(), "could not save the record")
	}
	return writeError(recCreate(), "could not create the record")
}

// Delete deletes the record. It is not staged: it needs no Save after it, and whatever was staged goes
// with the record. The server carries it out when the call returns, so for the rest of the call Exists
// answers as it did before.
//
// The error has Status Refused where the record is read-only, as a stream's is, and on a key with no
// record, where there is nothing to delete and nothing is created.
//
//	//udf:function close_account
//	func closeAccount(rec *udf.Record) error {
//		return rec.Delete() // no Save: the delete is the write
//	}
func (r *Record) Delete() error {
	return writeError(recRemove(), "could not delete the record")
}

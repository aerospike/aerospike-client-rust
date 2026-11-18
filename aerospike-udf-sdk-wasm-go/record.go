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
	"iter"
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
// side of an event (*EventRecord). A helper that takes a RecordReader reads a bin the same way from both.
//
// Bin is the form for a bin that must be there: an error naming the bin when it is not. The others are
// for a bin that may not be: the typed readers answer false, and the walks yield nothing, when the bin is
// absent, nil or of another kind.
type RecordReader interface {
	// Bin is a bin, whatever it holds; an error naming the bin when the record has none by that name.
	Bin(name string) (Value, error)
	// Get is a bin that may be absent: false when it is absent or nil.
	Get(name string) (Value, bool)
	// Has reports whether the record has the bin.
	Has(name string) bool
	// Int is an integer bin; false when absent, nil or of another kind.
	Int(name string) (int64, bool)
	// Double is a double bin; false when absent, nil or of another kind. An integer is not widened.
	Double(name string) (float64, bool)
	// Bool is a boolean bin; false when absent, nil or of another kind.
	Bool(name string) (bool, bool)
	// String is the text of a string bin, copied out; false when absent, nil or of another kind.
	String(name string) (string, bool)
	// Blob is the bytes of a blob bin, copied out; false when absent, nil or of another kind.
	Blob(name string) ([]byte, bool)
	// IntOr is an integer bin, or dflt when it is absent, nil or of another kind.
	IntOr(name string, dflt int64) int64
	// DoubleOr is a double bin, or dflt when it is absent, nil or of another kind.
	DoubleOr(name string, dflt float64) float64
	// BoolOr is a boolean bin, or dflt when it is absent, nil or of another kind.
	BoolOr(name string, dflt bool) bool
	// List walks a list bin; nothing when absent or not a list.
	List(name string) iter.Seq[Value]
	// Map walks a map bin's entries, key then value; nothing when absent or not a map.
	Map(name string) iter.Seq2[Value, Value]
	// BinCount is how many bins the record has.
	BinCount() int64
	// Generation is the record's modification count.
	Generation() int64
	// TTL is the record's time to live, in seconds.
	TTL() int64
	// LastUpdateTime is when the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC).
	LastUpdateTime() int64
}

// The typed readers, written once over Get for both kinds of record.

func readInt(r RecordReader, name string) (int64, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindInt {
		return 0, false
	}
	return v.Payload, true
}

func readDouble(r RecordReader, name string) (float64, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindDouble {
		return 0, false
	}
	return v.double(), true
}

func readBool(r RecordReader, name string) (bool, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindBool {
		return false, false
	}
	return v.Payload != 0, true
}

func readString(r RecordReader, name string) (string, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindStr {
		return "", false
	}
	s, err := v.String()
	return s, err == nil
}

func readBlob(r RecordReader, name string) ([]byte, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindBlob {
		return nil, false
	}
	b, err := v.Blob()
	return b, err == nil
}

func readIntOr(r RecordReader, name string, dflt int64) int64 {
	if x, ok := r.Int(name); ok {
		return x
	}
	return dflt
}

func readDoubleOr(r RecordReader, name string, dflt float64) float64 {
	if x, ok := r.Double(name); ok {
		return x
	}
	return dflt
}

func readBoolOr(r RecordReader, name string, dflt bool) bool {
	if x, ok := r.Bool(name); ok {
		return x
	}
	return dflt
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
// A bin write is staged: Set and Remove change the record in hand, and nothing reaches the server's
// record until Save. Delete is the exception and needs no Save.
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
// answers the same record.
func CurrentRecord() *Record { return &current }

//----------------------------------------------------------
// Reading bins.
//

// Bin is a bin, whatever it holds, in one import. An error naming the bin when the record has none by
// that name; a nil bin is absent too, since a bin cannot hold nil.
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

// Get is a bin that may be absent: false when it is absent or nil. It allocates nothing, so it is the
// form for a stream's per-record path.
func (r *Record) Get(name string) (Value, bool) {
	var v Value
	if binValue(strptr(name), int32(len(name)), unsafe.Pointer(&v), 16) != OK || v.Kind == KindNil {
		return Value{Kind: KindAbsent}, false
	}
	return v, true
}

// Has reports whether the record has the bin.
func (r *Record) Has(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// Int is an integer bin; false when absent, nil or of another kind.
func (r *Record) Int(name string) (int64, bool) { return readInt(r, name) }

// Double is a double bin; false when absent, nil or of another kind. An integer bin is not widened.
func (r *Record) Double(name string) (float64, bool) { return readDouble(r, name) }

// Bool is a boolean bin; false when absent, nil or of another kind.
func (r *Record) Bool(name string) (bool, bool) { return readBool(r, name) }

// String is the text of a string bin, copied out; false when absent, nil or of another kind.
func (r *Record) String(name string) (string, bool) { return readString(r, name) }

// Blob is the bytes of a blob bin, copied out; false when absent, nil or of another kind. The bytes of
// a GeoJSON or HLL bin are read through Get and Value.Blob.
func (r *Record) Blob(name string) ([]byte, bool) { return readBlob(r, name) }

// IntOr is an integer bin, or dflt when it is absent, nil or of another kind.
func (r *Record) IntOr(name string, dflt int64) int64 { return readIntOr(r, name, dflt) }

// DoubleOr is a double bin, or dflt when it is absent, nil or of another kind, an integer included.
func (r *Record) DoubleOr(name string, dflt float64) float64 { return readDoubleOr(r, name, dflt) }

// BoolOr is a boolean bin, or dflt when it is absent, nil or of another kind.
func (r *Record) BoolOr(name string, dflt bool) bool { return readBoolOr(r, name, dflt) }

// List walks the list in a bin; nothing when the bin is absent or not a list.
func (r *Record) List(name string) iter.Seq[Value] {
	v, _ := r.Get(name)
	return v.List()
}

// Map walks the entries of the map in a bin, key then value; nothing when the bin is absent or not a map.
func (r *Record) Map(name string) iter.Seq2[Value, Value] {
	v, _ := r.Get(name)
	return v.Map()
}

// BinCount is how many bins the record has.
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
// []byte (a blob), Value, *Map or *List.
//
// A Value read from a record is written back from its stored bytes, without the guest decoding it, which
// is how a sub-document is copied from one bin to another and how a GeoJSON or HLL keeps its kind. A
// built *Map or *List is handed over whole; when it kept a refusal while it was built, that error is
// returned and nothing is written. A string or blob is copied by the host during the call.
// udf.GeoJSONValue(doc) writes a GeoJSON document as the geospatially indexable kind.
//
// Writing nil removes the bin, as Remove does: a bin cannot hold nil.
//
// The error says the name is empty or longer than 15 bytes, that a blob would relabel bytes of another
// subtype such as an HLL, or that the host refused the write.
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

// SetInts writes a slice of integers to a bin as a list in one import, no value built per element. The
// host applies it at once rather than staging it: Save is not needed for it.
func (r *Record) SetInts(name string, values []int64) error {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return writeError(setListInts(strptr(name), int32(len(name)), p, int32(len(values)*8)), "could not write bin "+name)
}

// SetDoubles is SetInts for doubles.
func (r *Record) SetDoubles(name string, values []float64) error {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return writeError(setListDoubles(strptr(name), int32(len(name)), p, int32(len(values)*8)), "could not write bin "+name)
}

// SetBlob writes bytes to a bin as a blob, staged until Save; Set with a []byte does the same.
func (r *Record) SetBlob(name string, value []byte) error {
	return writeError(setBlob(strptr(name), int32(len(name)), ptr(value), int32(len(value))), "could not write bin "+name)
}

// SetGeoJSON writes a GeoJSON document to a bin as the geospatially indexable kind, staged until Save;
// Set with a GeoJSONValue does the same.
func (r *Record) SetGeoJSON(name string, doc string) error {
	return writeError(setGeoJSON(strptr(name), int32(len(name)), strptr(doc), int32(len(doc))), "could not write bin "+name)
}

// Remove removes a bin, staged until Save like a write.
func (r *Record) Remove(name string) error {
	return writeError(binRemove(strptr(name), int32(len(name))), "could not remove bin "+name)
}

//----------------------------------------------------------
// Metadata.
//

// Exists reports whether the record exists. A UDF can be called on a key that has no record yet.
func (r *Record) Exists() bool { return recExists() != 0 }

// Generation is the record's modification count.
func (r *Record) Generation() int64 { return gen() }

// TTL is the record's time to live, in seconds, as the host reports it.
func (r *Record) TTL() int64 { return ttl() }

// SetTTL sets the record's time to live, in seconds: 0 for the namespace default, -1 for never expire,
// subject to the namespace's rule on TTL reductions. It is the TTL of the write, so it takes effect when
// Save writes the record.
func (r *Record) SetTTL(seconds int32) error {
	return writeError(setTTL(seconds), "could not set the TTL")
}

// LastUpdateTime is when the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC),
// which is how the server stores it. It is not on the same scale as Now, which counts from the Unix epoch.
func (r *Record) LastUpdateTime() int64 { return lastUpdate() }

// Size is the record's size on device, in bytes.
func (r *Record) Size() int64 { return recordSize() }

// SetName is the record's set name, "" when it is in no set.
func (r *Record) SetName() string {
	var buf [64]byte
	n := setName(unsafe.Pointer(&buf[0]), int32(len(buf)))
	if n <= 0 {
		return ""
	}
	return string(buf[:n])
}

// Key is the record's user key, when it was written with the key stored: an integer, or a string or
// blob whose bytes go into buf, which the value then points at. False when the record carries none, or
// buf is too small for it.
func (r *Record) Key(buf []byte) (Value, bool) {
	switch Kind(keyType()) {
	case KindInt:
		return IntValue(keyInt()), true
	case KindStr:
		if n := keyBytes(ptr(buf), int32(len(buf))); n >= 0 {
			return StringBytesValue(buf[:n]), true
		}
	case KindBlob:
		if n := keyBytes(ptr(buf), int32(len(buf))); n >= 0 {
			return BlobValue(buf[:n]), true
		}
	}
	return Value{Kind: KindAbsent}, false
}

// Digest copies the record's digest into buf and returns that part of it: 20 bytes, always present. It
// is what identifies a record a UDF wants to point at rather than ship - a top-k hit, say - since a
// client can fetch a record by its digest afterwards.
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
// and last-update time move. On a key with no record it makes nothing, since a record has to have a bin,
// and the client is answered "key not found".
func (r *Record) Save() error {
	if recExists() != 0 {
		return writeError(recUpdate(), "could not save the record")
	}
	return writeError(recCreate(), "could not create the record")
}

// Delete deletes the record. It is not staged: it needs no Save after it, and whatever was staged goes
// with the record. The server carries it out when the call returns, so for the rest of the call Exists
// answers as it did before.
func (r *Record) Delete() error {
	return writeError(recRemove(), "could not delete the record")
}

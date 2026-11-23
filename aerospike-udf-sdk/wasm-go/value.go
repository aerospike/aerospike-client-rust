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
	"strconv"
	"unsafe"
)

//==========================================================
// Values as Go values: the typed tier over Value. Each accessor answers an error when the kind is another,
// the way the Rust SDK's Value::int() does; the kind tests are for a switch.
//

// IsNull reports whether there is nothing to read: the value is a stored nil or is absent. It is not
// another name for IsNil, which is false of an absent value.
func (v Value) IsNull() bool { return v.Kind == KindNil || v.Kind == KindAbsent }

// IsNil reports whether the value is a nil that is there - a nil argument, a nil entry of a list or
// map - as distinct from absent.
func (v Value) IsNil() bool { return v.Kind == KindNil }

// IsAbsent reports whether there is no value at all: a missing bin, map entry or argument. It is what
// the lookups answer beside their false.
func (v Value) IsAbsent() bool { return v.Kind == KindAbsent }

// IsInt reports whether the value is an integer.
func (v Value) IsInt() bool { return v.Kind == KindInt }

// IsDouble reports whether the value is a double. An integer is not one, and Double does not widen it.
func (v Value) IsDouble() bool { return v.Kind == KindDouble }

// IsBool reports whether the value is a boolean.
func (v Value) IsBool() bool { return v.Kind == KindBool }

// IsString reports whether the value is a string the host holds: one read from a record, a collection
// or the arguments. It is false of a value StringValue made, which is the guest's own.
func (v Value) IsString() bool { return v.Kind == KindStr }

// IsBlob reports whether the value is a plain blob the host holds; GeoJSON and HLL are kinds of their
// own, and a value BlobValue made is the guest's.
func (v Value) IsBlob() bool { return v.Kind == KindBlob }

// IsGeoJSON reports whether the value is a GeoJSON document the host holds. It is a kind of its own:
// neither String nor Blob reads it, and Bytes gives its text.
func (v Value) IsGeoJSON() bool { return v.Kind == KindGeoJSON }

// IsHLL reports whether the value is a HyperLogLog the host holds. It is a kind of its own: Blob does
// not read it, and Bytes gives its bytes.
func (v Value) IsHLL() bool { return v.Kind == KindHLL }

// IsList reports whether the value is a list the host holds, which List walks and Index reads.
func (v Value) IsList() bool { return v.Kind == KindList }

// IsMap reports whether the value is a map the host holds, which Map walks and Lookup reads.
func (v Value) IsMap() bool { return v.Kind == KindMap }

// IsStored reports whether the value is a string, blob, GeoJSON, HLL, list or map the host holds: the
// kinds Bytes, StoredBytes and CmpStored are for. False of a scalar, and of a value of the guest's own
// making.
func (v Value) IsStored() bool {
	switch v.Kind {
	case KindStr, KindBlob, KindGeoJSON, KindHLL, KindList, KindMap:
		return true
	}
	return false
}

func (v Value) kindError(want string) error {
	return errorf(TypeMismatch, "the value is not ", want, " (kind ", strconv.Itoa(int(v.Kind)), ")")
}

// Int is the value as an integer. The error, an *Error with Status TypeMismatch, says the value is of
// another kind; a double is not narrowed, and a nil or absent value is an error too.
//
// The typed accessors - Int, Double, Bool, String, Blob - all answer this way and convert nothing.
// They are how an element of a stored list or map is read, once Index, Lookup or a walk has handed
// it over: "not there" is what the lookup answers, and "of another kind" is the accessor's error.
//
//	n, err := v.Int()
//	if err != nil {
//		return 0, err // "the value is not an int (kind 4)"
//	}
func (v Value) Int() (int64, error) {
	if v.Kind != KindInt {
		return 0, v.kindError("an int")
	}
	return v.Payload, nil
}

// Double is the value as a double; an *Error with Status TypeMismatch for any other kind. An integer
// is not widened: a caller that takes either tests IsInt and converts.
func (v Value) Double() (float64, error) {
	if v.Kind != KindDouble {
		return 0, v.kindError("a double")
	}
	return v.double(), nil
}

// Bool is the value as a boolean; an *Error with Status TypeMismatch for any other kind. An integer
// is not read as one.
func (v Value) Bool() (bool, error) {
	if v.Kind != KindBool {
		return false, v.kindError("a bool")
	}
	return v.Payload != 0, nil
}

// String is the text of a string value, copied from the host into a new Go string, which allocates;
// the package's Bytes copies into a buffer of the caller's where that matters. The bytes are not
// checked to be UTF-8.
//
// The error, with Status TypeMismatch, says the value is of another kind: a blob or a GeoJSON is not
// read as a string, and Value.Bytes reads their bytes. Any other status is the host no longer being
// able to read the value. Of a value StringValue or StringBytesValue made, the string is not a copy:
// it shares the bytes the value was made over.
func (v Value) String() (string, error) {
	if v.Kind == KindGuestStr {
		return unsafe.String((*byte)(guestAddr(v.Payload)), int(v.Len)), nil
	}
	if v.Kind != KindStr {
		return "", v.kindError("a string")
	}
	b, err := v.Bytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Blob is the bytes of a blob, copied from the host into a new slice the caller owns, which
// allocates. It is strict: only a blob is read. A string, a GeoJSON and an HLL are kinds of their
// own and are an *Error with Status TypeMismatch here, like every other kind; Bytes reads the raw
// bytes of all four. An empty blob is an empty slice, not nil.
func (v Value) Blob() ([]byte, error) {
	if v.Kind != KindBlob && v.Kind != KindGuestBlob {
		return nil, v.kindError("a blob")
	}
	return v.Bytes()
}

// Bytes is the raw bytes of a string, blob, GeoJSON or HLL value, copied into a new slice the caller
// owns, which allocates: a string its text, a GeoJSON its document, a blob and an HLL their bytes as
// stored. It is the reader for code that takes bytes whatever they are labelled as; Blob and String
// are the strict ones. A value of the guest's own (StringValue, BlobValue, GeoJSONValue) gives a copy
// of its bytes. An empty value is an empty slice, not nil.
//
// The error has Status TypeMismatch when the value is a scalar, a nil, a list or a map, and the
// host's status when it could no longer read the value, as after a Save that failed. The package's
// Bytes is the same read into a buffer of the caller's.
func (v Value) Bytes() ([]byte, error) {
	switch v.Kind {
	case KindStr, KindBlob, KindGeoJSON, KindHLL, KindGuestStr, KindGuestBlob, KindGuestGeoJSON:
		out := make([]byte, v.Len)
		if rc := Bytes(v, out); rc < 0 {
			return nil, statusError(rc, "could not read the value")
		}
		return out, nil
	}
	return nil, v.kindError("a string, blob, GeoJSON or HLL")
}

// Floats copies a blob of 32-bit floats straight into dst, element for element: how a vector is read
// without converting it, and without allocating. The blob's bytes are taken as little-endian IEEE 754
// floats, four bytes each. It returns how many elements were copied, v.Len / 4.
//
// dst must be long enough for the whole blob. When the blob is longer than 4*len(dst) bytes it is an
// error and nothing is copied, so compare v.Len first when the length is not known. A shorter blob
// leaves the rest of dst as it was. Only a plain blob is read: any other kind is an error.
//
//	var vec [128]float32
//	n, err := v.Floats(vec[:])
//	if err != nil {
//		return err
//	}
//	query := vec[:n]
func (v Value) Floats(dst []float32) (int, error) {
	if v.Kind != KindBlob {
		return 0, v.kindError("a blob")
	}
	if len(dst) == 0 {
		return 0, nil
	}
	rc := cursorBytesAt(v.Src(), v.At(), unsafe.Pointer(&dst[0]), int32(len(dst)*4))
	if rc < 0 {
		return 0, statusError(rc, "the blob does not fit the array")
	}
	return int(rc) / 4, nil
}

// walkFailed fails the call when a walk that had elements to yield could not go on: a range loop has
// no error to hand back, and a walk that stopped short without a word would read as a shorter
// collection.
func walkFailed(what string, rc int32) {
	Fail("could not walk the " + what + " (" + strconv.Itoa(int(rc)) + "): the call has spent the memory it may hold on the server, or the host can no longer read the value")
}

// List walks a stored list, one Value per element, in order; nothing when the value is not a list
// (IsList tells, and Record.List answers an error for a bin of another kind). It is a Go iterator, to
// range over. The elements come 32 per import, and the cursor is released when the loop ends,
// breaks, returns or panics, so a loop left early holds nothing; through iter.Pull, when stop is called.
//
// Lists nest: an element that is itself a list or map can be ranged over inside the loop, since the
// inner cursor closes before the outer steps on. A cursor is a handle on the host, given back when the
// loop ends; it is charged to the call's memory on the server. When that has no room for one, or the
// host can no longer read the list, the walk cannot say so through a range
// loop, so it fails the call, as Fail does, and yields no more: a list is never walked short in
// silence. v.Len is the number of elements, with no walk, and Index reads one element without
// walking to it from the guest.
//
//	for row := range v.List() {
//		for cell := range row.List() { // a list of lists
//			if n, err := cell.Int(); err == nil {
//				total += n
//			}
//		}
//	}
func (v Value) List() iter.Seq[Value] {
	return func(yield func(Value) bool) {
		if v.Kind != KindList || v.Len == 0 {
			return
		}
		cur := enter(v)
		if cur <= 0 {
			walkFailed("list", cur)
			return
		}
		defer closeCursor(cur)
		var w [32]Value
		for {
			n := next(cur, w[:])
			if n < 0 {
				walkFailed("list", n)
				return
			}
			for i := int32(0); i < n; i++ {
				if !yield(w[i]) {
					return
				}
			}
			if n < 32 {
				return
			}
		}
	}
}

// Map walks a stored map's entries, key then value, both as Values; nothing when the value is not a
// map (IsMap tells). A key is a Value like any other: a string key is read with String, an integer
// key with Int. The entries come 16 per import, and the cursor is released when the loop ends or
// breaks, as with List; a walk that cannot go on fails the call, as List's does. v.Len is the number
// of entries, and Lookup reads one entry without a walk from the guest.
//
//	for key, entry := range v.Map() {
//		name, err := key.String()
//		if err != nil {
//			continue // not a string key
//		}
//		out.Put(name, entry)
//	}
func (v Value) Map() iter.Seq2[Value, Value] {
	return func(yield func(Value, Value) bool) {
		if v.Kind != KindMap || v.Len == 0 {
			return
		}
		cur := enter(v)
		if cur <= 0 {
			walkFailed("map", cur)
			return
		}
		defer closeCursor(cur)
		var w [32]Value
		for {
			n := next(cur, w[:])
			if n < 0 {
				walkFailed("map", n)
				return
			}
			for i := int32(0); i+1 < n; i += 2 {
				if !yield(w[i], w[i+1]) {
					return
				}
			}
			if n < 32 {
				return
			}
		}
	}
}

// Index is element i of a stored list, counting from 0, in one import: the host steps to it, nothing
// is walked from the guest.
//
// It has three outcomes, like every lookup. The element and true. A Value of KindAbsent and false,
// with no error, when it is not there: i is past the end or negative (there is no counting from the
// end), or the value is absent or nil and so has no elements. And an error, an *Error with Status
// TypeMismatch, when the value is of another kind than a list.
//
//	last, ok, err := history.Index(int(history.Len) - 1)
//	if err != nil {
//		return udf.NilValue(), err // not a list
//	}
//	if !ok {
//		return udf.NilValue(), nil // an empty list
//	}
func (v Value) Index(i int) (Value, bool, error) {
	if v.IsNull() {
		return Value{Kind: KindAbsent}, false, nil
	}
	if v.Kind != KindList {
		return Value{Kind: KindAbsent}, false, v.kindError("a list")
	}
	if i < 0 || uint32(i) >= v.Len {
		return Value{Kind: KindAbsent}, false, nil
	}
	var out Value
	rc := listGet(v.Src(), v.At(), int32(i), unsafe.Pointer(&out), 16)
	if rc == NotFound {
		return Value{Kind: KindAbsent}, false, nil
	}
	if rc != OK {
		return Value{Kind: KindAbsent}, false, statusError(rc, "could not read the element")
	}
	return out, true, nil
}

// found is a map lookup's status as the three outcomes of Lookup.
func found(out Value, rc int32) (Value, bool, error) {
	if rc == NotFound {
		return Value{Kind: KindAbsent}, false, nil
	}
	if rc != OK {
		return Value{Kind: KindAbsent}, false, statusError(rc, "could not look the key up")
	}
	return out, true, nil
}

// Lookup is the entry under a string key of a stored map, in one import; the host walks the entries,
// the guest none. The key is compared whole, whatever its length.
//
// The outcomes are Index's: the entry and true; a Value of KindAbsent and false, with no error, when
// the map has no such key, or the value is absent or nil; an *Error with Status TypeMismatch when
// the value is of another kind than a map. A stored nil under the key is an entry: true, and a Value
// of KindNil.
func (v Value) Lookup(key string) (Value, bool, error) {
	if v.IsNull() {
		return Value{Kind: KindAbsent}, false, nil
	}
	if v.Kind != KindMap {
		return Value{Kind: KindAbsent}, false, v.kindError("a map")
	}
	var out Value
	rc := get(v, key, &out)
	return found(out, rc)
}

// LookupInt is the entry under an integer key of a stored map, in one import, with the outcomes of
// Lookup. An integer key and a string key that reads the same are different keys.
func (v Value) LookupInt(key int64) (Value, bool, error) {
	if v.IsNull() {
		return Value{Kind: KindAbsent}, false, nil
	}
	if v.Kind != KindMap {
		return Value{Kind: KindAbsent}, false, v.kindError("a map")
	}
	var out Value
	rc := getI(v, key, &out)
	return found(out, rc)
}

// GeoJSONValue is a GeoJSON document of the guest's as a Value: written with Record.Set it makes a
// geospatially indexable bin, and put in a collection or returned with Result it stays GeoJSON, where
// the same text as a string would be a string. It holds the address of doc's bytes, not a copy, so
// doc must stay alive until the call that is given the value has returned. The text is not checked
// here.
func GeoJSONValue(doc string) Value {
	return Value{Kind: KindGuestGeoJSON, Len: uint32(len(doc)), Payload: int64(uintptr(strptr(doc)))}
}

// BoolValue is a boolean as a Value of the guest's, for the same calls as IntValue.
func BoolValue(b bool) Value {
	if b {
		return Value{Kind: KindBool, Payload: 1}
	}
	return Value{Kind: KindBool}
}

// NilValue is a nil as a Value: a nil entry in a list or map, a nil result. Written to a bin with
// Record.Set it removes the bin, since a bin cannot hold nil.
func NilValue() Value { return Value{Kind: KindNil} }

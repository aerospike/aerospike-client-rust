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

// IsNil reports whether the value is a nil, as distinct from absent.
func (v Value) IsNil() bool { return v.Kind == KindNil }

// IsAbsent reports whether there is no value at all: a missing bin, map entry or argument.
func (v Value) IsAbsent() bool { return v.Kind == KindAbsent }

// IsInt reports whether the value is an integer.
func (v Value) IsInt() bool { return v.Kind == KindInt }

// IsDouble reports whether the value is a double. An integer is not one, though Double widens it.
func (v Value) IsDouble() bool { return v.Kind == KindDouble }

// IsBool reports whether the value is a boolean.
func (v Value) IsBool() bool { return v.Kind == KindBool }

// IsString reports whether the value is a string the host holds.
func (v Value) IsString() bool { return v.Kind == KindStr }

// IsBlob reports whether the value is a plain blob the host holds; GeoJSON and HLL are kinds of their own.
func (v Value) IsBlob() bool { return v.Kind == KindBlob }

// IsList reports whether the value is a list the host holds.
func (v Value) IsList() bool { return v.Kind == KindList }

// IsMap reports whether the value is a map the host holds.
func (v Value) IsMap() bool { return v.Kind == KindMap }

// IsStored reports whether the value is a string, blob, GeoJSON, HLL, list or map the host holds.
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

// Int is the value as an integer.
func (v Value) Int() (int64, error) {
	if v.Kind != KindInt {
		return 0, v.kindError("an int")
	}
	return v.Payload, nil
}

// Double is the value as a double, or an integer widened.
func (v Value) Double() (float64, error) {
	switch v.Kind {
	case KindDouble:
		return v.double(), nil
	case KindInt:
		return float64(v.Payload), nil
	}
	return 0, v.kindError("a double")
}

// Bool is the value as a boolean.
func (v Value) Bool() (bool, error) {
	if v.Kind != KindBool {
		return false, v.kindError("a bool")
	}
	return v.Payload != 0, nil
}

// String is the text of a string, copied out.
func (v Value) String() (string, error) {
	if v.Kind == KindGuestStr {
		return unsafe.String((*byte)(unsafe.Pointer(uintptr(v.Payload))), int(v.Len)), nil
	}
	if v.Kind != KindStr {
		return "", v.kindError("a string")
	}
	b, err := v.Blob()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Blob is the bytes of a blob, copied out. A string, GeoJSON or HLL value gives its bytes the same way.
func (v Value) Blob() ([]byte, error) {
	switch v.Kind {
	case KindGuestStr, KindGuestBlob, KindGuestGeoJSON:
		out := make([]byte, v.Len)
		copy(out, unsafe.Slice((*byte)(unsafe.Pointer(uintptr(v.Payload))), int(v.Len)))
		return out, nil
	case KindStr, KindBlob, KindGeoJSON, KindHLL:
		out := make([]byte, v.Len)
		if v.Len > 0 {
			if rc := Bytes(v, out); rc < 0 {
				return nil, statusError(rc, "could not read the value")
			}
		}
		return out, nil
	}
	return nil, v.kindError("a string or blob")
}

// Floats copies a blob of 32-bit floats straight into dst, element for element: how a vector is read
// without converting it. Returns how many elements were copied.
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

// List walks a stored list, 32 values per import; nothing when the value is not a list. The cursor is
// released when the loop ends or breaks. Lists nest: a value that is itself a list can be ranged over
// inside the loop, since the inner cursor closes before the outer steps on.
func (v Value) List() iter.Seq[Value] {
	return func(yield func(Value) bool) {
		if v.Kind != KindList {
			return
		}
		cur := enter(v)
		if cur <= 0 {
			return
		}
		defer closeCursor(cur)
		var w [32]Value
		for {
			n := next(cur, w[:])
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

// Map walks a stored map's entries, key then value; nothing when the value is not a map.
func (v Value) Map() iter.Seq2[Value, Value] {
	return func(yield func(Value, Value) bool) {
		if v.Kind != KindMap {
			return
		}
		cur := enter(v)
		if cur <= 0 {
			return
		}
		defer closeCursor(cur)
		var w [32]Value
		for {
			n := next(cur, w[:])
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

// Index is element i of a stored list, in one import: the host steps to it, nothing is walked from the
// guest. False past the end, for a negative index, or when the value is not a list.
func (v Value) Index(i int) (Value, bool) {
	var out Value
	if v.Kind != KindList || i < 0 || uint32(i) >= v.Len {
		return Value{Kind: KindAbsent}, false
	}
	if listGet(v.Src(), v.At(), int32(i), unsafe.Pointer(&out), 16) != OK {
		return Value{Kind: KindAbsent}, false
	}
	return out, true
}

// Lookup is the entry under a string key of a stored map; false when there is none.
func (v Value) Lookup(key string) (Value, bool) {
	var out Value
	if v.Kind != KindMap || get(v, key, &out) != OK {
		return Value{Kind: KindAbsent}, false
	}
	return out, true
}

// LookupInt is the entry under an integer key of a stored map; false when there is none.
func (v Value) LookupInt(key int64) (Value, bool) {
	var out Value
	if v.Kind != KindMap || getI(v, key, &out) != OK {
		return Value{Kind: KindAbsent}, false
	}
	return out, true
}

// GeoJSONValue is a GeoJSON document of the guest's, for a batch, a result or a collection.
func GeoJSONValue(doc string) Value {
	return Value{Kind: KindGuestGeoJSON, Len: uint32(len(doc)), Payload: int64(uintptr(strptr(doc)))}
}

// BoolValue is a boolean as a value of the guest's, for a batch.
func BoolValue(b bool) Value {
	if b {
		return Value{Kind: KindBool, Payload: 1}
	}
	return Value{Kind: KindBool}
}

// NilValue is a nil value, for a batch.
func NilValue() Value { return Value{Kind: KindNil} }

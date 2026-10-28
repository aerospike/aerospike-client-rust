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

import "iter"

// A Bin is a bin of the record in hand, by name: reads as the type expected, writes of every kind, and
// the walk of a collection bin. Keep one as a package-level variable (`var ticks = udf.NewBin("ticks")`).
// A getter answers an error naming the bin when it is absent or of another kind; the Or form answers the
// default for absent and nil. A setter stages a write until Commit.
//
//	n := ticks.IntOr(0) + 1
//	if err := ticks.SetInt(n); err != nil { return err }
//	return udf.Commit()
type Bin struct {
	name string
}

func NewBin(name string) Bin { return Bin{name: name} }

func (b Bin) Name() string { return b.name }

//==========================================================
// Reading.
//

// Get is the bin as a value; false when the record has no such bin.
func (b Bin) Get() (Value, bool) {
	var v Value
	if Bin_(b.name, &v) != OK {
		return Value{Kind: KindAbsent}, false
	}
	return v, true
}

// Exists reports whether the record has this bin.
func (b Bin) Exists() bool {
	var v Value
	return Bin_(b.name, &v) == OK
}

func (b Bin) typed(kind Kind, what string) (Value, error) {
	var v Value
	rc := Bin_(b.name, &v)
	if rc == NotFound {
		return v, errorf(rc, "bin ", b.name, " is absent")
	}
	if rc != OK {
		return v, statusError(rc, "bin "+b.name+" could not be read")
	}
	if v.Kind != kind {
		return v, errorf(TypeMismatch, "bin ", b.name, " is not ", what)
	}
	return v, nil
}

// present is the value when the bin exists and is not nil; the ok is false for absent or nil, and an
// error says it is of another kind.
func (b Bin) present(kind Kind, what string) (Value, bool, error) {
	var v Value
	rc := Bin_(b.name, &v)
	if rc == NotFound || (rc == OK && v.Kind == KindNil) {
		return v, false, nil
	}
	if rc != OK {
		return v, false, statusError(rc, "bin "+b.name+" could not be read")
	}
	if v.Kind != kind {
		return v, false, errorf(TypeMismatch, "bin ", b.name, " is not ", what)
	}
	return v, true, nil
}

func (b Bin) Int() (int64, error) {
	v, err := b.typed(KindInt, "an int")
	return v.Payload, err
}

// IntOr is the bin as an integer, or dflt when absent or nil; a bin of another kind also answers dflt.
func (b Bin) IntOr(dflt int64) int64 {
	v, ok, err := b.present(KindInt, "an int")
	if !ok || err != nil {
		return dflt
	}
	return v.Payload
}

func (b Bin) Float64() (float64, error) {
	v, err := b.typed(KindDouble, "a double")
	return v.Double(), err
}

func (b Bin) Float64Or(dflt float64) float64 {
	v, ok, err := b.present(KindDouble, "a double")
	if !ok || err != nil {
		return dflt
	}
	return v.Double()
}

func (b Bin) Bool() (bool, error) {
	v, err := b.typed(KindBool, "a bool")
	return v.Payload != 0, err
}

func (b Bin) BoolOr(dflt bool) bool {
	v, ok, err := b.present(KindBool, "a bool")
	if !ok || err != nil {
		return dflt
	}
	return v.Payload != 0
}

func (b Bin) Str() (string, error) {
	v, err := b.typed(KindStr, "a string")
	if err != nil {
		return "", err
	}
	return v.Str()
}

func (b Bin) StrOr(dflt string) string {
	v, ok, err := b.present(KindStr, "a string")
	if !ok || err != nil {
		return dflt
	}
	s, err := v.Str()
	if err != nil {
		return dflt
	}
	return s
}

// Bytes is the bytes of a string, blob, GeoJSON or HLL bin.
func (b Bin) Bytes() ([]byte, error) {
	var v Value
	rc := Bin_(b.name, &v)
	if rc == NotFound {
		return nil, errorf(rc, "bin ", b.name, " is absent")
	}
	if rc != OK {
		return nil, statusError(rc, "bin "+b.name+" could not be read")
	}
	return v.Bytes()
}

// Floats copies a blob of 32-bit floats straight into dst; how many elements were copied.
func (b Bin) Floats(dst []float32) (int, error) {
	v, err := b.typed(KindBlob, "a blob")
	if err != nil {
		return 0, err
	}
	return v.Floats(dst)
}

// List walks the list in this bin; nothing when the bin is absent or not a list.
func (b Bin) List() iter.Seq[Value] {
	v, _ := b.Get()
	return v.List()
}

// Map walks the entries of the map in this bin; nothing when the bin is absent or not a map.
func (b Bin) Map() iter.Seq2[Value, Value] {
	v, _ := b.Get()
	return v.Map()
}

//==========================================================
// Writing: staged until Commit.
//

func (b Bin) check(rc int32) error {
	return statusError(rc, "could not write bin "+b.name)
}

func (b Bin) SetInt(value int64) error       { return b.check(SetInt(b.name, value)) }
func (b Bin) SetFloat64(value float64) error { return b.check(SetDouble(b.name, value)) }
func (b Bin) SetBool(value bool) error       { return b.check(SetBool(b.name, value)) }
func (b Bin) SetStr(value string) error      { return b.check(SetStr(b.name, value)) }

// SetBytes writes a blob.
func (b Bin) SetBytes(value []byte) error { return b.check(SetBlob(b.name, value)) }

// SetGeoJSON writes a GeoJSON document, kept as the geospatially indexable kind rather than a string.
func (b Bin) SetGeoJSON(doc string) error { return b.check(SetGeoJSON(b.name, doc)) }

// SetNull writes a nil bin.
func (b Bin) SetNull() error { return b.check(SetNull(b.name)) }

// SetInts writes a list of integers from the slice in one import, no value built per element.
func (b Bin) SetInts(values []int64) error { return b.check(SetInts(b.name, values)) }

func (b Bin) SetFloat64s(values []float64) error { return b.check(SetFloat64s(b.name, values)) }

// SetValue writes a value: one read from the record by position, keeping its kind; or a scalar, guest
// bytes or a built collection.
func (b Bin) SetValue(v Value) error {
	switch v.Kind {
	case KindInt:
		return b.SetInt(v.Payload)
	case KindDouble:
		return b.SetFloat64(v.Double())
	case KindBool:
		return b.SetBool(v.Payload != 0)
	case KindNil:
		return b.SetNull()
	case KindGuestStr:
		return b.check(setStr(strptr(b.name), int32(len(b.name)), guestPtr(v), int32(v.Len)))
	case KindGuestBlob:
		return b.check(setBlob(strptr(b.name), int32(len(b.name)), guestPtr(v), int32(v.Len)))
	case KindGuestGeoJSON:
		return b.check(setGeoJSON(strptr(b.name), int32(len(b.name)), guestPtr(v), int32(v.Len)))
	case KindHandle:
		return b.check(SetHandle(b.name, int32(v.Payload)))
	case KindAbsent:
		return errorf(NotFound, "could not write an absent value to bin ", b.name)
	}
	return b.check(SetValue(b.name, v))
}

func (b Bin) SetMap(m *Map) error {
	if m.err != nil {
		return m.err
	}
	return b.check(SetHandle(b.name, m.handle))
}

func (b Bin) SetList(l *List) error {
	if l.err != nil {
		return l.err
	}
	return b.check(SetHandle(b.name, l.handle))
}

func (b Bin) Remove() error { return b.check(Remove(b.name)) }

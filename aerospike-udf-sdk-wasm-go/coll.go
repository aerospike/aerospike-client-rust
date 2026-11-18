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
// Collections built on the host: the Go face of the Rust SDK's List and Map. Put and Add take a value of
// any supported Go type and chain; the first refusal is kept and answered by Err, the way strings.Builder
// and bufio.Writer keep theirs, and a generated export checks it when a *Map or *List is returned.
//

// Map is a map built on the host, to be written to a bin, returned as a result, nested, or handed back
// from an event handler. It lives until the call ends.
type Map struct {
	handle int32
	err    error
}

// List is a list built on the host.
type List struct {
	handle int32
	err    error
}

// NewMap starts a map with room for capacity entries.
func NewMap(capacity int) *Map {
	h := mapNew(int32(capacity))
	if h <= 0 {
		return &Map{err: errorf(Err, "could not build a map")}
	}
	return &Map{handle: h}
}

// NewList starts a list with room for capacity elements.
func NewList(capacity int) *List {
	h := listNew(int32(capacity))
	if h <= 0 {
		return &List{err: errorf(Err, "could not build a list")}
	}
	return &List{handle: h}
}

// Err is the first refusal, nil when every put went in.
func (m *Map) Err() error { return m.err }

// Err is the first refusal, nil when every add went in.
func (l *List) Err() error { return l.err }

// Value is the collection as a value, for a batch or a result.
func (m *Map) Value() Value { return handleValue(m.handle) }

// Value is the list as a value, for a batch or a result.
func (l *List) Value() Value { return handleValue(l.handle) }

// Len is how many entries the map holds; -1 when it could not be built, as after a failed NewMap.
func (m *Map) Len() int { return int(collSize(m.handle)) }

// Len is how many elements the list holds; -1 when it could not be built, as after a failed NewList.
func (l *List) Len() int { return int(collSize(l.handle)) }

// mapKey is a Go value as a map key: a string, or an integer. ok is false for any other type.
func mapKey(key any) (text string, number int64, isInt bool, ok bool) {
	switch k := key.(type) {
	case string:
		return k, 0, false, true
	case int64:
		return "", k, true, true
	case int:
		return "", int64(k), true, true
	case int32:
		return "", int64(k), true, true
	}
	return "", 0, false, false
}

// Contains reports whether the map holds key, a string or an integer, in one import: the host answers
// from the map as built, nothing is packed.
func (m *Map) Contains(key any) bool {
	text, number, isInt, ok := mapKey(key)
	if !ok {
		return false
	}
	if isInt {
		return mapHasI(m.handle, number) == 1
	}
	return mapHas(m.handle, strptr(text), int32(len(text))) == 1
}

func (m *Map) fail(rc int32, key string) *Map {
	if rc != OK && m.err == nil {
		m.err = errorf(rc, "could not put ", key, " (", strconv.Itoa(int(rc)), ")")
	}
	return m
}

func (l *List) fail(rc int32) *List {
	if rc != OK && l.err == nil {
		l.err = errorf(rc, "could not add to the list (", strconv.Itoa(int(rc)), ")")
	}
	return l
}

// valueOf is any supported Go value as a Value: a scalar by value, a string or blob by the address of its
// bytes, a built collection by its handle. ok is false for an unsupported type.
func valueOf(v any) (Value, bool) {
	switch x := v.(type) {
	case nil:
		return NilValue(), true
	case int64:
		return IntValue(x), true
	case int:
		return IntValue(int64(x)), true
	case int32:
		return IntValue(int64(x)), true
	case float64:
		return DoubleValue(x), true
	case float32:
		return DoubleValue(float64(x)), true
	case bool:
		return BoolValue(x), true
	case string:
		return StrValue(x), true
	case []byte:
		return BlobValue(x), true
	case Value:
		return x, true
	case *Map:
		return x.Value(), true
	case *List:
		return x.Value(), true
	}
	return Value{Kind: KindAbsent}, false
}

// Put puts a value under a key. The key is a string or an integer (int, int32, int64); the value an int,
// int32, int64, float32, float64, bool, string, []byte (a blob), Value, *Map, *List, or nil for a nil
// entry (in a record answer, "no such bin").
func (m *Map) Put(key any, v any) *Map {
	text, number, isInt, ok := mapKey(key)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put an entry: the key is neither a string nor an integer")
		}
		return m
	}
	if isInt {
		text = strconv.FormatInt(number, 10)
	}

	val, ok := valueOf(v)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put ", text, ": unsupported Go type")
		}
		return m
	}
	if c, is := v.(*Map); is && c.err != nil && m.err == nil {
		m.err = c.err
	}
	if c, is := v.(*List); is && c.err != nil && m.err == nil {
		m.err = c.err
	}

	if isInt {
		return m.fail(mapPutIValue(m.handle, number, val), text)
	}
	return m.fail(mapPutValue(m.handle, text, val), text)
}

// PutAll puts every entry of a stored map, by position.
func (m *Map) PutAll(stored Value) *Map {
	for k, v := range stored.Map() {
		if k.Kind == KindInt {
			m.Put(k.Payload, v)
		} else if s, err := k.Str(); err == nil {
			m.Put(s, v)
		} else if m.err == nil {
			m.err = err
		}
	}
	return m
}

// PutEntries puts pairs (pairs[0], pairs[1] is the first) in one import: scalars, guest bytes by address,
// stored values by position, built collections as their Value.
func (m *Map) PutEntries(pairs []Value) *Map {
	if len(pairs) < 2 {
		return m
	}
	put := mapPutEntries(m.handle, unsafe.Pointer(&pairs[0]), int32(len(pairs)*16))
	if int(put) != len(pairs)/2 && m.err == nil {
		m.err = errorf(Err, "could not put entry ", strconv.Itoa(int(put)))
	}
	return m
}

// AddValues appends values in one import, the kinds as PutEntries takes them.
func (l *List) AddValues(vals []Value) *List {
	if len(vals) == 0 {
		return l
	}
	added := listPushValues(l.handle, unsafe.Pointer(&vals[0]), int32(len(vals)*16))
	if int(added) != len(vals) && l.err == nil {
		l.err = errorf(Err, "could not add value ", strconv.Itoa(int(added)))
	}
	return l
}

// PutGeoJSON puts a GeoJSON document, kept as that kind.
func (m *Map) PutGeoJSON(key, doc string) *Map {
	return m.fail(mapPutStrHandle(m.handle, strptr(key), int32(len(key)), valGeoJSON(strptr(doc), int32(len(doc)))), key)
}

// Add appends a value: the kinds as Map.Put takes them.
func (l *List) Add(v any) *List {
	val, ok := valueOf(v)
	if !ok {
		if l.err == nil {
			l.err = errorf(TypeMismatch, "could not add to the list: unsupported Go type")
		}
		return l
	}
	if c, is := v.(*Map); is && c.err != nil && l.err == nil {
		l.err = c.err
	}
	if c, is := v.(*List); is && c.err != nil && l.err == nil {
		l.err = c.err
	}
	return l.fail(listPushValue(l.handle, val))
}

// AddGeoJSON appends a GeoJSON document, kept as that kind.
func (l *List) AddGeoJSON(doc string) *List {
	return l.fail(listPush(l.handle, valGeoJSON(strptr(doc), int32(len(doc)))))
}

// AddAll appends every element of a stored list, by position.
func (l *List) AddAll(stored Value) *List {
	for v := range stored.List() {
		l.Add(v)
	}
	return l
}

// Contains reports whether a stored list of strings holds text.
func Contains(list Value, text string) bool {
	for v := range list.List() {
		if StrIs(v, text) {
			return true
		}
	}
	return false
}

//==========================================================
// Under the builders: integer keys, and the lookups in a stored map.
//

func guestPtr(v Value) unsafe.Pointer { return unsafe.Pointer(uintptr(v.Payload)) }

// mapPutIValue puts any value under an integer key, the kinds as mapPutValue takes them.
func mapPutIValue(m int32, key int64, v Value) int32 {
	switch v.Kind {
	case KindInt:
		return mapPut(m, valInt(key), valInt(v.Payload))
	case KindDouble:
		return mapPut(m, valInt(key), valDouble(v.Double()))
	case KindBool:
		return mapPut(m, valInt(key), valBool(int32(v.Payload)))
	case KindNil:
		return mapPut(m, valInt(key), valNil())
	case KindGuestStr:
		return mapPut(m, valInt(key), valStr(guestPtr(v), int32(v.Len)))
	case KindGuestBlob:
		return mapPut(m, valInt(key), valBlob(guestPtr(v), int32(v.Len)))
	case KindGuestGeoJSON:
		return mapPut(m, valInt(key), valGeoJSON(guestPtr(v), int32(v.Len)))
	case KindHandle:
		return mapPut(m, valInt(key), int32(v.Payload))
	case KindAbsent:
		return NotFound
	}
	return mapPutIAt(m, key, v.Src(), v.At())
}

// getI looks an integer key up in the map at a value's position.
func getI(m Value, key int64, out *Value) int32 {
	return collGetI(m.Src(), m.At(), key, unsafe.Pointer(out), 16)
}

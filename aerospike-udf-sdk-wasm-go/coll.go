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

// WrapMap and WrapList take a handle the low-level calls built.
func WrapMap(handle int32) *Map { return &Map{handle: handle} }

// WrapList takes a list handle the low-level calls built.
func WrapList(handle int32) *List { return &List{handle: handle} }

// Handle is what the low-level calls take.
func (m *Map) Handle() int32 { return m.handle }

// Handle is the list's handle, as the low-level calls take it.
func (l *List) Handle() int32 { return l.handle }

// Err is the first refusal, nil when every put went in.
func (m *Map) Err() error { return m.err }

// Err is the first refusal, nil when every add went in.
func (l *List) Err() error { return l.err }

// Size is how many entries the map holds; -1 when its handle is not a collection's, as after a failed
// NewMap.
func (m *Map) Size() int { return int(collSize(m.handle)) }

// Size is how many elements the list holds; -1 when its handle is not a collection's, as after a failed
// NewList.
func (l *List) Size() int { return int(collSize(l.handle)) }

// Value is the collection as a value, for a batch or a result.
func (m *Map) Value() Value { return HandleValue(m.handle) }

// Value is the list as a value, for a batch or a result.
func (l *List) Value() Value { return HandleValue(l.handle) }

// Has reports whether the map holds key; it packs the map, so for the occasional check, not a loop.
func (m *Map) Has(key string) bool { return MapHasKey(m.handle, key) }

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

// valueOf is any supported Go value as a handle (for the kinds the typed natives do not take), or a
// Value for the ones they do. ok is false for an unsupported type.
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

// Put puts a value under a string key: an int, int32, int64, float32, float64, bool, string, []byte (a
// blob), Value, *Map, *List, or nil for a nil entry (in a record answer, "no such bin").
func (m *Map) Put(key string, v any) *Map {
	val, ok := valueOf(v)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put ", key, ": unsupported Go type")
		}
		return m
	}
	if c, is := v.(*Map); is && c.err != nil && m.err == nil {
		m.err = c.err
	}
	if c, is := v.(*List); is && c.err != nil && m.err == nil {
		m.err = c.err
	}
	return m.fail(MapPutValue(m.handle, key, val), key)
}

// PutI puts a value under an integer key; the kinds as Put takes them.
func (m *Map) PutI(key int64, v any) *Map {
	val, ok := valueOf(v)
	k := strconv.FormatInt(key, 10)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put ", k, ": unsupported Go type")
		}
		return m
	}
	return m.fail(MapPutIValue(m.handle, key, val), k)
}

// PutGeoJSON puts a GeoJSON document, kept as that kind.
func (m *Map) PutGeoJSON(key, doc string) *Map {
	return m.fail(mapPutStrHandle(m.handle, strptr(key), int32(len(key)), valGeoJSON(strptr(doc), int32(len(doc)))), key)
}

// PutAll puts every entry of a stored map, by position.
func (m *Map) PutAll(stored Value) *Map {
	for k, v := range stored.Map() {
		if k.Kind == KindInt {
			m.PutI(k.Payload, v)
		} else if s, err := k.Str(); err == nil {
			m.Put(s, v)
		} else if m.err == nil {
			m.err = err
		}
	}
	return m
}

// PutEntries puts pairs (pairs[0], pairs[1] is the first) in one import.
func (m *Map) PutEntries(pairs []Value) *Map {
	if put := MapPutEntries(m.handle, pairs); int(put) != len(pairs)/2 && m.err == nil {
		m.err = errorf(Err, "could not put entry ", strconv.Itoa(int(put)))
	}
	return m
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
	return l.fail(ListPushValue(l.handle, val))
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

// AddValues appends values in one import.
func (l *List) AddValues(vals []Value) *List {
	if added := ListPushValues(l.handle, vals); int(added) != len(vals) && l.err == nil {
		l.err = errorf(Err, "could not add value ", strconv.Itoa(int(added)))
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
// The kinds the low-level MapPutValue and ListPushValue did not take, and integer keys.
//

func guestPtr(v Value) unsafe.Pointer { return unsafe.Pointer(uintptr(v.Payload)) }

// MapPutIValue puts any value under an integer key, the kinds as MapPutValue takes them.
func MapPutIValue(m int32, key int64, v Value) int32 {
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

// MapPut puts a built value under a built key, both by handle; IntHandle and StrHandle make a key.
func MapPut(m, key, value int32) int32 { return mapPut(m, key, value) }

// IntHandle is an integer as a handle, for the calls that take a key or value by handle. 0 when the host
// could not make one.
func IntHandle(i int64) int32 { return valInt(i) }

// StrHandle is a string as a handle, copied by the host, for the calls that take a key or value by
// handle. 0 when the host could not make one.
func StrHandle(s string) int32 { return valStr(strptr(s), int32(len(s))) }

// GetI looks an integer key up in the map at a value's position.
func GetI(m Value, key int64, out *Value) int32 {
	return collGetI(m.Src(), m.At(), key, unsafe.Pointer(out), 16)
}
